package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// An adopted sticky key has no owner reference, so owning Secrets does not
// report it. Nothing else touches the proxy while this runs, so each reaction
// can only come from the Secret itself.
func TestStickyKey_AdoptedSecretIsWatched(t *testing.T) {
	cfg := apiServer(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 proxyTestScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&SynapseRouteReconciler{Client: mgr.GetClient(), ClusterDomain: "cluster.local"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&SynapseProxyReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	e := newProxyEnv(t)
	adopted := unownedKey(e, 40)
	e.proxy()
	key := client.ObjectKey{Namespace: e.ns, Name: "edge"}
	eventually(t, "the proxy runs on the adopted key", func() string {
		var d appsv1.Deployment
		if err := e.k8s.Get(ctx, key, &d); err != nil {
			return err.Error()
		}
		return ""
	})
	ready := func(what string, want func(*metav1.Condition) bool) {
		t.Helper()
		eventually(t, what, func() string {
			var p synapsev1alpha1.SynapseProxy
			if err := e.k8s.Get(ctx, key, &p); err != nil {
				return err.Error()
			}
			c := meta.FindStatusCondition(p.Status.Conditions, ConditionReady)
			if c == nil || !want(c) {
				return fmt.Sprintf("Ready is %+v", c)
			}
			return ""
		})
	}

	ready("the proxy has settled", func(c *metav1.Condition) bool { return c.Reason == ReasonRollingOut })
	// Let the events of that first rollout drain, so that none is left to
	// stand in for the one under test.
	time.Sleep(time.Second)

	if err := e.k8s.Get(ctx, client.ObjectKeyFromObject(adopted), adopted); err != nil {
		t.Fatal(err)
	}
	adopted.Data["sticky.key"] = adopted.Data["sticky.key"][:10]
	if err := e.k8s.Update(ctx, adopted); err != nil {
		t.Fatal(err)
	}
	ready("a shortened adopted key is reported", func(c *metav1.Condition) bool { return c.Reason == ReasonStickyKeyUnusable })

	if err := e.k8s.Delete(ctx, adopted); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a deleted adopted key is created anew and owned", func() string {
		var s corev1.Secret
		if err := e.k8s.Get(ctx, client.ObjectKeyFromObject(adopted), &s); err != nil {
			return err.Error()
		}
		if s.UID == adopted.UID || len(s.OwnerReferences) != 1 || len(s.Data["sticky.key"]) < proxyStickyKeyLen {
			return fmt.Sprintf("Secret is %+v", s.ObjectMeta)
		}
		return ""
	})
	ready("and the proxy is no longer refused", func(c *metav1.Condition) bool { return c.Reason != ReasonStickyKeyUnusable })
}

// The map function enqueues a proxy for the Secret of its sticky key and for
// nothing else, so a busy namespace does not cost a reconcile per Secret.
func TestStickyKey_OnlyItsOwnNameIsMapped(t *testing.T) {
	e := newProxyEnv(t)
	p := e.proxy()
	if err := e.k8s.Get(e.ctx, client.ObjectKeyFromObject(p), p); err != nil {
		t.Fatal(err)
	}
	secret := func(ns, name string, owner *synapsev1alpha1.SynapseProxy) *corev1.Secret {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
		if owner != nil {
			s.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(owner, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy"))}
		}
		return s
	}
	edge := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: e.ns, Name: "edge"}}}
	for _, tc := range []struct {
		what string
		in   *corev1.Secret
		want []reconcile.Request
	}{
		{"the adopted key of a proxy", secret(e.ns, "edge-sticky-key", nil), edge},
		{"a key of a proxy that does not exist", secret(e.ns, "ghost-sticky-key", nil), nil},
		{"the same name in another namespace", secret("other", "edge-sticky-key", nil), nil},
		{"an unrelated Secret", secret(e.ns, "edge-creds", nil), nil},
		{"the suffix with a tail", secret(e.ns, "edge-sticky-key-old", nil), nil},
		{"the suffix alone", secret(e.ns, "-sticky-key", nil), nil},
		{"the proxy's own, which owning already reports", secret(e.ns, "edge-sticky-key", p), nil},
	} {
		if got := e.proxies.stickyKeyOwner(e.ctx, tc.in); fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.what, got, tc.want)
		}
	}
}
