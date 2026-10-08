package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var watchTestRuns atomic.Int64

// The other route tests call Reconcile by hand. This one runs the controller
// in a manager against a real API server, because what it is about is the
// part they skip: that each kind of input, when it changes, gets a render.
func TestRoutes_RenderFollowsItsInputs(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	// Unique per run, so the test can repeat against one API server.
	ns := fmt.Sprintf("routes-watch-%d", watchTestRuns.Add(1))
	class := ns
	create := func(obj client.Object) {
		t.Helper()
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}
	upstreams := func() (string, bool) {
		var cm corev1.ConfigMap
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "edge-upstreams"}, &cm); err != nil {
			return "", false
		}
		return cm.Data[UpstreamsKey], true
	}
	rendered := func(what string, want func(string) bool) {
		t.Helper()
		eventually(t, what, func() string {
			body, ok := upstreams()
			if !ok {
				return "no upstreams ConfigMap"
			}
			if !want(body) {
				return "upstreams are\n" + body
			}
			return ""
		})
	}
	floor := renderUpstreams(newRenderModel())

	create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	edge := testProxy(ns, "edge")
	edge.UID = ""
	create(edge)
	rendered("a new proxy gets its outputs", func(s string) bool { return s == floor })

	create(classFor(class, edge))
	ing := routedIngress("shop", ptr(class), "shop.example.com")
	ing.Namespace = ns
	ing.Spec.Rules[0].HTTP.Paths[0].PathType = ptr(networkingv1.PathTypePrefix)
	create(ing)
	rendered("an Ingress of a bound class is rendered", func(s string) bool {
		return strings.Contains(s, `"shop.example.com"`)
	})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "app"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "app"},
			Ports:    []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	create(svc)
	rendered("a backend Service that appears is resolved", func(s string) bool {
		return svc.Spec.ClusterIP != "" && strings.Contains(s, fmt.Sprintf("%q", svc.Spec.ClusterIP+":80"))
	})

	tls := tlsSecret(ns, "shop-tls", "CRT", "KEY")
	tls.Type = corev1.SecretTypeTLS
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(ing), ing); err != nil {
		t.Fatal(err)
	}
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"shop.example.com"}, SecretName: "shop-tls"}}
	if err := k8s.Update(ctx, ing); err != nil {
		t.Fatal(err)
	}
	// The Secret arrives after the Ingress that names it, as it does when
	// cert-manager issues a certificate. Wait for the render the Ingress
	// update causes first, so that what follows can only be the Secret's.
	rendered("the host is bound to its certificate", func(s string) bool {
		return strings.Contains(s, `certificate: "shop.example.com"`)
	})
	projected := func(what, want string) {
		t.Helper()
		eventually(t, what, func() string {
			var sec corev1.Secret
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "edge-certs"}, &sec); err != nil {
				return err.Error()
			}
			if got := string(sec.Data["shop.example.com.crt"]); got != want {
				return fmt.Sprintf("projected certificate is %q; Secret holds %v", got, keysOf(sec.Data))
			}
			return ""
		})
	}
	projected("no certificate before its Secret exists", "")
	create(tls)
	projected("a TLS Secret that appears is projected", "CRT")

	tls.Data[corev1.TLSCertKey] = []byte("CRT-RENEWED")
	if err := k8s.Update(ctx, tls); err != nil {
		t.Fatal(err)
	}
	projected("a renewed certificate is projected", "CRT-RENEWED")

	var cm corev1.ConfigMap
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "edge-upstreams"}, &cm); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	rendered("a deleted output is put back", func(s string) bool {
		return strings.Contains(s, `"shop.example.com"`)
	})

	var certs corev1.Secret
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "edge-certs"}, &certs); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, &certs); err != nil {
		t.Fatal(err)
	}
	projected("a deleted certificates Secret is put back", "CRT-RENEWED")

	var ic networkingv1.IngressClass
	if err := k8s.Get(ctx, client.ObjectKey{Name: class}, &ic); err != nil {
		t.Fatal(err)
	}
	ic.Spec.Parameters.Name = "another-proxy"
	if err := k8s.Update(ctx, &ic); err != nil {
		t.Fatal(err)
	}
	rendered("a class bound elsewhere takes its Ingresses along", func(s string) bool { return s == floor })

	if err := k8s.Delete(ctx, edge); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}
