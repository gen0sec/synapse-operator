package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// failingReader is a client whose every list fails with err.
func failingReader(t *testing.T, err error) client.Reader {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(proxyTestScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return err },
	}).Build()
}

// An operator that cannot read SynapseProxy resources never starts: the
// manager waits for their cache before it starts anything. So what is missing
// has to be found, and said, before there is a manager.
func TestCheckProxyAccess(t *testing.T) {
	ctx := context.Background()

	t.Run("the role is not bound", func(t *testing.T) {
		// Whoever this is, nothing was ever granted to them.
		cfg := rest.CopyConfig(apiServer(t))
		cfg.Impersonate = rest.ImpersonationConfig{
			UserName: fmt.Sprintf("system:serviceaccount:synapse-os:nobody-%d", rbacTestRuns.Add(1)),
		}
		nobody, err := client.New(cfg, client.Options{Scheme: proxyTestScheme(t)})
		if err != nil {
			t.Fatal(err)
		}
		err = CheckProxyAccess(ctx, nobody, "")
		if err == nil {
			t.Fatal("no error for an operator that may not list SynapseProxy resources")
		}
		for _, want := range []string{"not allowed", "config/proxy-controller"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error does not say %q: %v", want, err)
			}
		}
	})

	t.Run("the role is bound", func(t *testing.T) {
		role, binding := loadProxyRole(t)
		cfg, _ := operatorHolding(t, rbacAdmin(t), role, binding)
		operator, err := client.New(cfg, client.Options{Scheme: proxyTestScheme(t)})
		if err != nil {
			t.Fatal(err)
		}
		if err := CheckProxyAccess(ctx, operator, ""); err != nil {
			t.Errorf("an operator holding the role is turned away: %v", err)
		}
		if err := CheckProxyAccess(ctx, operator, "some-namespace"); err != nil {
			t.Errorf("an operator holding the role is turned away from one namespace: %v", err)
		}
	})

	t.Run("the CRD is not installed", func(t *testing.T) {
		missing := &meta.NoKindMatchError{
			GroupKind:        schema.GroupKind{Group: synapsev1alpha1.GroupVersion.Group, Kind: "SynapseProxy"},
			SearchedVersions: []string{synapsev1alpha1.GroupVersion.Version},
		}
		err := CheckProxyAccess(ctx, failingReader(t, missing), "")
		if err == nil {
			t.Fatal("no error without the CRD")
		}
		for _, want := range []string{"CRD is not installed", "config/proxy-controller"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error does not say %q: %v", want, err)
			}
		}
	})

	t.Run("anything else is passed on as it is", func(t *testing.T) {
		down := errors.New("connection refused")
		err := CheckProxyAccess(ctx, failingReader(t, down), "")
		if !errors.Is(err, down) {
			t.Fatalf("got %v, want the reader's own error", err)
		}
		if strings.Contains(err.Error(), "config/proxy-controller") {
			t.Errorf("an unreachable API server is blamed on the install: %v", err)
		}
	})
}
