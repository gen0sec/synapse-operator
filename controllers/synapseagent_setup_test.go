package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// An operator that cannot read SynapseAgent resources never starts: the
// manager waits for their cache, which is indexed. So what is missing has to
// be found, and said, before there is a manager.
func TestCheckAgentAccess_TheRoleIsNotBound(t *testing.T) {
	// Whoever this is, nothing was ever granted to them.
	cfg := rest.CopyConfig(apiServer(t))
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: fmt.Sprintf("system:serviceaccount:synapse-os:nobody-%d", rbacTestRuns.Add(1)),
	}
	nobody, err := client.New(cfg, client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	err = CheckAgentAccess(context.Background(), nobody, "")
	if err == nil {
		t.Fatal("no error for an operator that may not list SynapseAgent resources")
	}
	for _, want := range []string{"not allowed", "config/agent-controller"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

func TestCheckAgentAccess_TheRoleIsBound(t *testing.T) {
	role, binding := loadRole(t, agentRoleManifest)
	cfg, _ := operatorHolding(t, rbacAdmin(t), role, binding)
	operator, err := client.New(cfg, client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"", "some-namespace"} {
		if err := CheckAgentAccess(context.Background(), operator, namespace); err != nil {
			t.Errorf("an operator holding the role is turned away (namespace %q): %v", namespace, err)
		}
	}
}

func TestCheckAgentAccess_TheCRDIsNotInstalled(t *testing.T) {
	// The test API server has the CRD. So the kind is given, in this
	// client's eyes only, a group the server has never heard of: what it
	// gets back is what a client gets from a cluster without the CRD.
	absent := schema.GroupVersion{Group: "absent." + synapsev1alpha1.GroupVersion.Group, Version: "v1alpha1"}
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(absent, &synapsev1alpha1.SynapseAgent{}, &synapsev1alpha1.SynapseAgentList{})
	metav1.AddToGroupVersion(scheme, absent)
	nowhere, err := client.New(apiServer(t), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	err = CheckAgentAccess(context.Background(), nowhere, "")
	if err == nil {
		t.Fatal("no error without the CRD")
	}
	for _, want := range []string{"CRD is not installed", "config/agent-controller"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

func TestCheckAgentAccess_AnythingElseIsPassedOnAsItIs(t *testing.T) {
	down := errors.New("connection refused")
	err := CheckAgentAccess(context.Background(), failingReader(t, down), "")
	if !errors.Is(err, down) {
		t.Fatalf("got %v, want the reader's own error", err)
	}
	if strings.Contains(err.Error(), "config/agent-controller") {
		t.Errorf("an unreachable API server is blamed on the install: %v", err)
	}
}
