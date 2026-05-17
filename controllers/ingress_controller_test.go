package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := networkingv1.AddToScheme(s); err != nil {
		t.Fatalf("networkingv1: %v", err)
	}
	if err := gwv1.AddToScheme(s); err != nil {
		t.Fatalf("gwv1: %v", err)
	}
	return s
}

func ptr[T any](v T) *T { return &v }

// renderUpstreams is the crux of cert-manager HTTP-01 working: an ACME
// challenge backend MUST be emitted as an internal_paths entry keyed
// EXACTLY as synapse's built-in default so it overrides the (empty)
// internal ACME server; everything else is a normal host upstream.
func TestRenderUpstreams_ACMEOverrideAndHosts(t *testing.T) {
	hosts := map[string]map[string]string{
		"app.example.com": {"/": "whoami.default.svc.cluster.local:80"},
	}
	out := renderUpstreams(hosts, "cm-acme-http-solver-x.default.svc.cluster.local:8089")

	if !strings.Contains(out, `internal_paths:`) ||
		!strings.Contains(out, `"/.well-known/acme-challenge/*":`) ||
		!strings.Contains(out, "cm-acme-http-solver-x.default.svc.cluster.local:8089") {
		t.Fatalf("ACME internal_paths override missing:\n%s", out)
	}
	// The acme entry must be plain HTTP to the solver.
	if !strings.Contains(out, "ssl_enabled: false") {
		t.Fatalf("expected ssl_enabled: false:\n%s", out)
	}
	if !strings.Contains(out, `upstreams:`) ||
		!strings.Contains(out, `"app.example.com":`) ||
		!strings.Contains(out, "whoami.default.svc.cluster.local:80") {
		t.Fatalf("host upstream missing:\n%s", out)
	}
	// internal_paths must precede upstreams (override semantics + stable).
	if strings.Index(out, "internal_paths:") > strings.Index(out, "upstreams:") {
		t.Fatalf("internal_paths must come before upstreams:\n%s", out)
	}
}

func TestRenderUpstreams_NoACMENoInternalPaths(t *testing.T) {
	out := renderUpstreams(map[string]map[string]string{
		"a.example.com": {"/": "b:80"},
	}, "")
	if strings.Contains(out, "internal_paths:") {
		t.Fatalf("no acme backend ⇒ no internal_paths block:\n%s", out)
	}
}

func TestRenderUpstreams_Deterministic(t *testing.T) {
	h := map[string]map[string]string{
		"b.example.com": {"/x": "x:1", "/a": "a:1"},
		"a.example.com": {"/": "y:2"},
	}
	if renderUpstreams(h, "s:8089") != renderUpstreams(h, "s:8089") {
		t.Fatal("renderUpstreams not deterministic (would cause spurious synapse reloads)")
	}
}

// writeIfChanged must write IN PLACE and be a no-op when unchanged
// (synapse's filewatch ignores rename/move events; only Modify(Data)).
func TestWriteIfChanged(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sub", "upstreams.yaml")
	ch, err := writeIfChanged(p, "v1")
	if err != nil || !ch {
		t.Fatalf("first write: changed=%v err=%v", ch, err)
	}
	if b, _ := os.ReadFile(p); string(b) != "v1" {
		t.Fatalf("content = %q", b)
	}
	ch, err = writeIfChanged(p, "v1")
	if err != nil || ch {
		t.Fatalf("unchanged write must be no-op: changed=%v err=%v", ch, err)
	}
	ch, _ = writeIfChanged(p, "v2")
	if !ch {
		t.Fatal("changed content must rewrite")
	}
}

func TestIsOurs(t *testing.T) {
	r := &IngressReconciler{IngressClassName: "synapse"}
	mk := func(c *string) *networkingv1.Ingress {
		return &networkingv1.Ingress{Spec: networkingv1.IngressSpec{IngressClassName: c}}
	}
	if r.isOurs(mk(nil)) {
		t.Fatal("nil class must not match")
	}
	if r.isOurs(mk(ptr("traefik"))) {
		t.Fatal("other class must not match")
	}
	if !r.isOurs(mk(ptr("synapse"))) {
		t.Fatal("synapse class must match")
	}
}

func TestBackendAddr_PortNumber(t *testing.T) {
	r := &IngressReconciler{ClusterDomain: "cluster.local"}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami",
			Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if !ok || addr != "whoami.default.svc.cluster.local:80" {
		t.Fatalf("got %q ok=%v", addr, ok)
	}
}

// End-to-end controller test (fake client, no envtest): an Ingress
// carrying the cert-manager solver path MUST produce the synapse
// internal_paths override in the rendered file — this is exactly what
// makes cert-manager HTTP-01 succeed through synapse-as-ingress.
func TestRender_IngressACMESolver(t *testing.T) {
	out := filepath.Join(t.TempDir(), "upstreams.yaml")
	ing := &networkingv1.Ingress{}
	ing.Name = "cm-acme-http-solver-abc"
	ing.Namespace = "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{
				Path: "/.well-known/acme-challenge/tok123",
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
					Name: "cm-acme-http-solver-svc",
					Port: networkingv1.ServiceBackendPort{Number: 8089},
				}},
			}},
		}},
	}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	b, _ := os.ReadFile(out)
	s := string(b)
	if !strings.Contains(s, `"/.well-known/acme-challenge/*":`) ||
		!strings.Contains(s, "cm-acme-http-solver-svc.default.svc.cluster.local:8089") {
		t.Fatalf("solver Ingress did not produce internal_paths override:\n%s", s)
	}
}

// A non-our-class Ingress must be ignored entirely.
func TestRender_IgnoresForeignClass(t *testing.T) {
	out := filepath.Join(t.TempDir(), "upstreams.yaml")
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "foreign", "default"
	ing.Spec.IngressClassName = ptr("traefik")
	ing.Spec.Rules = []networkingv1.IngressRule{{Host: "x", IngressRuleValue: networkingv1.IngressRuleValue{
		HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
			Path: "/", Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: "x", Port: networkingv1.ServiceBackendPort{Number: 1}}}}}}}}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
	if _, n, _, err := r.render(context.Background()); err != nil || n != 0 {
		t.Fatalf("foreign-class Ingress must not match: matched=%d err=%v", n, err)
	}
	if b, _ := os.ReadFile(out); strings.Contains(string(b), `"x"`) {
		t.Fatalf("foreign host leaked:\n%s", b)
	}
}
