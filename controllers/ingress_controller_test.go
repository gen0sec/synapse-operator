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

// ACME challenge backend MUST be an internal_paths override (the crux
// of cert-manager HTTP-01 working); other paths are host upstreams.
func TestRenderUpstreams_ACMEOverrideAndHosts(t *testing.T) {
	m := newRenderModel()
	m.acme = "cm-acme-http-solver-x.default.svc.cluster.local:8089"
	m.addRoute("app.example.com", "/",
		[]backend{{addr: "whoami.default.svc.cluster.local:80"}}, annSettings{}, nil, nil)
	out := renderUpstreams(m)

	if !strings.Contains(out, `internal_paths:`) ||
		!strings.Contains(out, `"/.well-known/acme-challenge/*":`) ||
		!strings.Contains(out, "cm-acme-http-solver-x.default.svc.cluster.local:8089") {
		t.Fatalf("ACME internal_paths override missing:\n%s", out)
	}
	if !strings.Contains(out, `upstreams:`) ||
		!strings.Contains(out, `"app.example.com":`) ||
		!strings.Contains(out, "whoami.default.svc.cluster.local:80") {
		t.Fatalf("host upstream missing:\n%s", out)
	}
	if strings.Index(out, "internal_paths:") > strings.Index(out, "upstreams:") {
		t.Fatalf("internal_paths must precede upstreams:\n%s", out)
	}
}

func TestRenderUpstreams_NoACMENoInternalPaths(t *testing.T) {
	m := newRenderModel()
	m.addRoute("a.example.com", "/", []backend{{addr: "b:80"}}, annSettings{}, nil, nil)
	if strings.Contains(renderUpstreams(m), "internal_paths:") {
		t.Fatal("no acme backend ⇒ no internal_paths block")
	}
}

func TestRenderUpstreams_Deterministic(t *testing.T) {
	build := func() string {
		m := newRenderModel()
		m.acme = "s:8089"
		m.addRoute("b.example.com", "/x", []backend{{addr: "x:1"}}, annSettings{}, nil, nil)
		m.addRoute("b.example.com", "/a", []backend{{addr: "a:1"}}, annSettings{}, nil, nil)
		m.addRoute("a.example.com", "/", []backend{{addr: "y:2"}}, annSettings{}, nil, nil)
		return renderUpstreams(m)
	}
	if build() != build() {
		t.Fatal("renderUpstreams not deterministic (would cause spurious synapse reloads)")
	}
}

// Annotation-driven upstream settings + weighted servers must surface
// in the rendered legacy v1 schema.
func TestRenderUpstreams_AnnotationsAndWeights(t *testing.T) {
	m := newRenderModel()
	tru := true
	var ct uint64 = 7
	m.addRoute("app.example.com", "/",
		[]backend{{addr: "a:80", weight: 3}, {addr: "b:80", weight: 1}},
		annSettings{ssl: &tru, http2: &tru, connectTimeout: &ct,
			reqHeaders: []string{"X-A: 1"}, sticky: true}, nil, []string{"X-Resp: y"})
	out := renderUpstreams(m)
	for _, want := range []string{
		"sticky_sessions: true",
		`{ address: "a:80", weight: 3 }`,
		`{ address: "b:80", weight: 1 }`,
		"ssl_enabled: true",
		"http2_enabled: true",
		"connection_timeout: 7",
		"request_headers:",
		`- "X-A: 1"`,
		"response_headers:",
		`- "X-Resp: y"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestParseAnnotations(t *testing.T) {
	a := parseAnnotations(map[string]string{
		annPrefix + "backend-protocol":   "HTTPS",
		annPrefix + "http2":              "true",
		annPrefix + "force-https":        "true",
		annPrefix + "connect-timeout":    "5",
		annPrefix + "read-timeout":       "30",
		annPrefix + "healthcheck":        "true",
		annPrefix + "disable-access-log": "false",
		annPrefix + "request-headers":    "X-A: 1, X-B: 2",
		annPrefix + "response-headers":   "X-C: 3",
		annPrefix + "sticky-sessions":    "true",
	})
	if a.ssl == nil || !*a.ssl {
		t.Fatal("backend-protocol HTTPS ⇒ ssl true")
	}
	if a.http2 == nil || !*a.http2 || a.forceHTTPS == nil || !*a.forceHTTPS {
		t.Fatal("http2/force-https not parsed")
	}
	if a.connectTimeout == nil || *a.connectTimeout != 5 || a.readTimeout == nil || *a.readTimeout != 30 {
		t.Fatalf("timeouts: %+v", a)
	}
	if a.healthcheck == nil || !*a.healthcheck || a.disableAccessLog == nil || *a.disableAccessLog {
		t.Fatal("healthcheck/disable-access-log not parsed")
	}
	if len(a.reqHeaders) != 2 || a.reqHeaders[0] != "X-A: 1" || len(a.respHeaders) != 1 || !a.sticky {
		t.Fatalf("headers/sticky: %+v", a)
	}
	// nginx-compat backend-protocol fallback.
	n := parseAnnotations(map[string]string{nginxPrefix + "backend-protocol": "HTTPS"})
	if n.ssl == nil || !*n.ssl {
		t.Fatal("nginx backend-protocol compat not honored")
	}
	// unknown / empty ⇒ all nil.
	if z := parseAnnotations(nil); z.ssl != nil || z.http2 != nil || z.sticky {
		t.Fatalf("nil annotations must yield empty settings: %+v", z)
	}
}

func TestIsOurs(t *testing.T) {
	r := &IngressReconciler{IngressClassName: "synapse"}
	mk := func(c *string) *networkingv1.Ingress {
		return &networkingv1.Ingress{Spec: networkingv1.IngressSpec{IngressClassName: c}}
	}
	if r.isOurs(mk(nil)) || r.isOurs(mk(ptr("traefik"))) {
		t.Fatal("nil/other class must not match")
	}
	if !r.isOurs(mk(ptr("synapse"))) {
		t.Fatal("synapse class must match")
	}
}

func TestBackendAddr_PortNumber(t *testing.T) {
	r := &IngressReconciler{ClusterDomain: "cluster.local"}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if !ok || addr != "whoami.default.svc.cluster.local:80" {
		t.Fatalf("got %q ok=%v", addr, ok)
	}
}

func TestWriteIfChanged(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sub", "upstreams.yaml")
	if ch, err := writeIfChanged(p, "v1"); err != nil || !ch {
		t.Fatalf("first write: changed=%v err=%v", ch, err)
	}
	if b, _ := os.ReadFile(p); string(b) != "v1" {
		t.Fatalf("content=%q", b)
	}
	if ch, err := writeIfChanged(p, "v1"); err != nil || ch {
		t.Fatalf("unchanged write must be no-op: changed=%v err=%v", ch, err)
	}
	if ch, _ := writeIfChanged(p, "v2"); !ch {
		t.Fatal("changed content must rewrite")
	}
}

// End-to-end (fake client): an Ingress with the cert-manager solver
// path + an annotation must produce the internal_paths override AND
// honor the annotation on the normal route.
func TestRender_IngressACMESolverAndAnnotation(t *testing.T) {
	out := filepath.Join(t.TempDir(), "upstreams.yaml")
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Annotations = map[string]string{annPrefix + "read-timeout": "42"}
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{
				{Path: "/.well-known/acme-challenge/tok", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: "solver", Port: networkingv1.ServiceBackendPort{Number: 8089}}}},
				{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}},
			},
		}},
	}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	s, _ := os.ReadFile(out)
	if !strings.Contains(string(s), `"/.well-known/acme-challenge/*":`) ||
		!strings.Contains(string(s), "solver.default.svc.cluster.local:8089") {
		t.Fatalf("solver internal_paths override missing:\n%s", s)
	}
	if !strings.Contains(string(s), "read_timeout: 42") {
		t.Fatalf("annotation not applied to route:\n%s", s)
	}
}

// Fix #1: addRoute is first-writer-wins (no clobber) + reports conflict.
func TestAddRoute_FirstWriterWins(t *testing.T) {
	m := newRenderModel()
	if !m.addRoute("h", "/", []backend{{addr: "first:80"}}, annSettings{}, nil, nil) {
		t.Fatal("first addRoute must succeed")
	}
	if m.addRoute("h", "/", []backend{{addr: "second:80"}}, annSettings{}, nil, nil) {
		t.Fatal("duplicate (host,path) must report conflict (false)")
	}
	if m.hosts["h"]["/"].servers[0].addr != "first:80" {
		t.Fatalf("first writer must be kept, got %v", m.hosts["h"]["/"].servers)
	}
}

// Fix #1: two Ingresses for the same host+path → deterministic
// (ns/name-sorted, first kept) regardless of input order.
func TestRender_DeterministicConflict(t *testing.T) {
	mk := func(name, svc string) *networkingv1.Ingress {
		i := &networkingv1.Ingress{}
		i.Name, i.Namespace = name, "default"
		i.Spec.IngressClassName = ptr("synapse")
		i.Spec.Rules = []networkingv1.IngressRule{{Host: "h.example.com",
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: svc, Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}}}
		return i
	}
	run := func(order ...*networkingv1.Ingress) string {
		out := filepath.Join(t.TempDir(), "u.yaml")
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).
			WithObjects(order[0], order[1]).Build()
		r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
		if _, _, _, err := r.render(context.Background()); err != nil {
			t.Fatalf("render: %v", err)
		}
		b, _ := os.ReadFile(out)
		return string(b)
	}
	a, b := mk("a-ing", "svc-a"), mk("b-ing", "svc-b")
	// "a-ing" sorts first → svc-a wins, both input orders identical.
	o1, o2 := run(a, b), run(b, a)
	if o1 != o2 {
		t.Fatalf("non-deterministic across input order:\n--o1--\n%s\n--o2--\n%s", o1, o2)
	}
	if !strings.Contains(o1, "svc-a.default.svc.cluster.local:80") ||
		strings.Contains(o1, "svc-b.default.svc.cluster.local:80") {
		t.Fatalf("ns/name-first (a-ing/svc-a) must win:\n%s", o1)
	}
}

// Fix #2: findReloadTargets identifies the synapse process (argv0
// basename "synapse"), skips self and the operator (basename
// "manager").
func TestFindReloadTargets(t *testing.T) {
	proc := t.TempDir()
	mk := func(pid, cmdline string) {
		d := filepath.Join(proc, pid)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "cmdline"), []byte(cmdline), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("10", "/synapse\x00--config=/etc/synapse/config.yaml\x00--mode=proxy\x00")
	mk("11", "/manager\x00--ingress-mode\x00--gateway-api\x00") // operator → skip
	mk("12", "/pause\x00")                                      // skip
	mk("13", "")                                                // empty → skip
	if err := os.MkdirAll(filepath.Join(proc, "notapid"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := findReloadTargets(proc, 99 /*self*/)
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("expected [10] (synapse), got %v", got)
	}
	if r := findReloadTargets(proc, 10 /*self==synapse*/); len(r) != 0 {
		t.Fatalf("must skip self pid: %v", r)
	}
}

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
		t.Fatalf("foreign-class must not match: matched=%d err=%v", n, err)
	}
}
