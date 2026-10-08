package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The other proxy tests step the reconciler by hand. This one runs both of a
// SynapseProxy's controllers in a manager, because what it is about is what
// they skip: that each thing the proxy depends on, when it changes, gets a
// reconcile, and that the two controllers find each other without help.
func TestProxy_FollowsItsInputs(t *testing.T) {
	cfg := apiServer(t)
	scheme := proxyTestScheme(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
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
	key := client.ObjectKey{Namespace: e.ns, Name: "edge"}
	deployment := func(what string, want func(*appsv1.Deployment) string) {
		t.Helper()
		eventually(t, what, func() string {
			var d appsv1.Deployment
			if err := e.k8s.Get(ctx, key, &d); err != nil {
				return err.Error()
			}
			return want(&d)
		})
	}
	exists := func(*appsv1.Deployment) string { return "" }
	condition := func(what, kind, reason string) {
		t.Helper()
		eventually(t, what, func() string {
			var p synapsev1alpha1.SynapseProxy
			if err := e.k8s.Get(ctx, key, &p); err != nil {
				return err.Error()
			}
			c := meta.FindStatusCondition(p.Status.Conditions, kind)
			if c == nil || c.Reason != reason {
				return fmt.Sprintf("%s is %+v, want reason %s", kind, c, reason)
			}
			return ""
		})
	}

	// The API key's Secret does not exist yet.
	p := e.proxy(func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Platform = &synapsev1alpha1.PlatformSpec{
			APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
		}
	})
	condition("a missing Secret is reported", ConditionConfigValid, ReasonSecretNotFound)

	// Creating it is all it takes: the routes the other controller rendered
	// in the meantime are found too.
	creds := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"}, Data: map[string][]byte{"API_KEY": []byte("k-1")}}
	e.create(creds)
	deployment("the Secret's arrival starts the proxy", exists)
	first := e.configSecretName()

	creds.Data["API_KEY"] = []byte("k-2")
	if err := e.k8s.Update(ctx, creds); err != nil {
		t.Fatal(err)
	}
	deployment("a rotated API key rolls the pods", func(d *appsv1.Deployment) string {
		if got := volumeNamed(t, d, "config").Secret.SecretName; got == first {
			return "the pods still mount " + got
		}
		return ""
	})

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) {
		replicas := int32(3)
		s.Replicas = &replicas
	})
	deployment("a spec change is applied", func(d *appsv1.Deployment) string {
		if *d.Spec.Replicas != 3 {
			return fmt.Sprintf("%d replicas", *d.Spec.Replicas)
		}
		return ""
	})

	// Whatever the proxy owns is put back when it goes missing.
	uid := e.deployment().UID
	if err := e.k8s.Delete(ctx, e.deployment()); err != nil {
		t.Fatal(err)
	}
	deployment("a deleted Deployment is put back", func(d *appsv1.Deployment) string {
		if d.UID == uid {
			return "still the old object"
		}
		return ""
	})
	var svc corev1.Service
	e.get("edge", &svc)
	if err := e.k8s.Delete(ctx, &svc); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a deleted Service is put back", func() string {
		var got corev1.Service
		if err := e.k8s.Get(ctx, key, &got); err != nil {
			return err.Error()
		}
		if got.UID == svc.UID {
			return "still the old object"
		}
		return ""
	})
	var sa corev1.ServiceAccount
	e.get("edge", &sa)
	if err := e.k8s.Delete(ctx, &sa); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a deleted ServiceAccount is put back", func() string {
		var got corev1.ServiceAccount
		if err := e.k8s.Get(ctx, key, &got); err != nil {
			return err.Error()
		}
		if got.UID == sa.UID {
			return "still the old object"
		}
		return ""
	})
	current := e.configSecretName()
	var cfgSecret corev1.Secret
	e.get(current, &cfgSecret)
	if err := e.k8s.Delete(ctx, &cfgSecret); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a deleted config Secret is put back", func() string {
		var got corev1.Secret
		if err := e.k8s.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: current}, &got); err != nil {
			return err.Error()
		}
		return ""
	})

	// The Deployment's progress reaches the proxy's status.
	d := e.deployment()
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 3, UpdatedReplicas: 3, ReadyReplicas: 3, AvailableReplicas: 3}
	if err := e.k8s.Status().Update(ctx, d); err != nil {
		t.Fatal(err)
	}
	condition("a finished rollout makes the proxy ready", ConditionReady, ReasonApplied)
}

// A proxy that is waiting for its routes has to notice when they arrive.
// Only the proxy's own controller runs here, and the two objects are made by
// hand one at a time, so each arrival can be seen to have been noticed.
func TestProxy_StartsWhenItsRoutesArrive(t *testing.T) {
	cfg := apiServer(t)
	scheme := proxyTestScheme(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	})
	if err != nil {
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
	p := e.proxy()
	key := client.ObjectKeyFromObject(p)
	waitingFor := func(what, object string) {
		t.Helper()
		eventually(t, what, func() string {
			var got synapsev1alpha1.SynapseProxy
			if err := e.k8s.Get(ctx, key, &got); err != nil {
				return err.Error()
			}
			c := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			if c == nil || c.Reason != ReasonWaitingForRoutes || !strings.Contains(c.Message, object) {
				return fmt.Sprintf("Ready is %+v, want it waiting for %s", c, object)
			}
			return ""
		})
	}
	if err := e.k8s.Get(ctx, key, p); err != nil {
		t.Fatal(err)
	}
	owned := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: e.ns, Name: name, OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(p, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy")),
		}}
	}

	waitingFor("nothing is rendered yet", "edge-upstreams")

	// Nothing else is going on around a waiting proxy, so what follows can
	// only be the controller noticing the change to the spec itself.
	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(`{"mode": "agent"}`)}
	})
	eventually(t, "a change to the spec is noticed", func() string {
		var got synapsev1alpha1.SynapseProxy
		if err := e.k8s.Get(ctx, key, &got); err != nil {
			return err.Error()
		}
		if c := meta.FindStatusCondition(got.Status.Conditions, ConditionConfigValid); c == nil || c.Reason != ReasonInvalidConfig {
			return fmt.Sprintf("ConfigValid is %+v", c)
		}
		return ""
	})
	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Config = nil })
	waitingFor("and so is the next one", "edge-upstreams")

	e.create(&corev1.ConfigMap{ObjectMeta: owned("edge-upstreams"), Data: map[string]string{UpstreamsKey: renderUpstreams(newRenderModel())}})
	waitingFor("the routes have arrived", "edge-certs")

	e.create(&corev1.Secret{ObjectMeta: owned("edge-certs")})
	eventually(t, "the certificates have arrived", func() string {
		var d appsv1.Deployment
		if err := e.k8s.Get(ctx, key, &d); err != nil {
			return err.Error()
		}
		return ""
	})
}
