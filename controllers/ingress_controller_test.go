package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// Explicit spec.ingressClassName wins regardless of default.
	if r.isOurs(mk(ptr("traefik")), true) {
		t.Fatal("explicit other class must NOT match even when default is ours")
	}
	if !r.isOurs(mk(ptr("synapse")), false) {
		t.Fatal("explicit synapse class must match")
	}
	// No class, no default ⇒ not ours; no class + default ours ⇒ ours.
	if r.isOurs(mk(nil), false) {
		t.Fatal("classless + no default must not match")
	}
	if !r.isOurs(mk(nil), true) {
		t.Fatal("classless + default-is-ours must match")
	}
	// Legacy annotation honored only when spec field unset.
	leg := &networkingv1.Ingress{}
	leg.Annotations = map[string]string{"kubernetes.io/ingress.class": "synapse"}
	if !r.isOurs(leg, false) {
		t.Fatal("legacy kubernetes.io/ingress.class=synapse must match")
	}
	leg.Annotations["kubernetes.io/ingress.class"] = "nginx"
	if r.isOurs(leg, true) {
		t.Fatal("legacy annotation for another class must NOT match (even with default ours)")
	}
	// spec field beats a contradicting legacy annotation.
	both := &networkingv1.Ingress{Spec: networkingv1.IngressSpec{IngressClassName: ptr("synapse")}}
	both.Annotations = map[string]string{"kubernetes.io/ingress.class": "nginx"}
	if !r.isOurs(both, false) {
		t.Fatal("spec.ingressClassName must take precedence over legacy annotation")
	}
}

// #3 default IngressClass: a classless Ingress is programmed when an
// is-default-class IngressClass with our controller exists; foreign
// default class does not capture it.
func TestRender_DefaultIngressClass(t *testing.T) {
	mkClassless := func() *networkingv1.Ingress {
		i := &networkingv1.Ingress{}
		i.Name, i.Namespace = "noclass", "default"
		i.Spec.Rules = []networkingv1.IngressRule{{Host: "d.example.com",
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: "app", Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}}}
		return i
	}
	mkClass := func(name, controller string, def bool) *networkingv1.IngressClass {
		ic := &networkingv1.IngressClass{}
		ic.Name = name
		ic.Spec.Controller = controller
		if def {
			ic.Annotations = map[string]string{"ingressclass.kubernetes.io/is-default-class": "true"}
		}
		return ic
	}
	run := func(ics ...*networkingv1.IngressClass) string {
		out := filepath.Join(t.TempDir(), "u.yaml")
		objs := []client.Object{mkClassless()}
		for _, ic := range ics {
			objs = append(objs, ic)
		}
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
		r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
		if _, _, _, err := r.render(context.Background()); err != nil {
			t.Fatalf("render: %v", err)
		}
		b, _ := os.ReadFile(out)
		return string(b)
	}
	if s := run(mkClass("synapse", ControllerName, true)); !strings.Contains(s, "d.example.com") {
		t.Fatalf("classless Ingress must be captured by our default IngressClass:\n%s", s)
	}
	if s := run(mkClass("nginx", "k8s.io/ingress-nginx", true)); strings.Contains(s, "d.example.com") {
		t.Fatalf("classless Ingress must NOT be captured by a foreign default class:\n%s", s)
	}
	if s := run(mkClass("synapse", ControllerName, false)); strings.Contains(s, "d.example.com") {
		t.Fatalf("non-default class must NOT capture a classless Ingress:\n%s", s)
	}
}

// #4 publishStatus: IP vs hostname classification, idempotent, and a
// no-op when no addresses are configured.
func TestPublishStatus(t *testing.T) {
	mk := func() *networkingv1.Ingress {
		i := &networkingv1.Ingress{}
		i.Name, i.Namespace = "g0s", "default"
		i.Spec.IngressClassName = ptr("synapse")
		return i
	}
	// No addresses ⇒ status untouched.
	ing := mk()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).
		WithStatusSubresource(&networkingv1.Ingress{}).Build()
	r := &IngressReconciler{Client: c}
	r.publishStatus(context.Background(), []*networkingv1.Ingress{ing})
	if len(ing.Status.LoadBalancer.Ingress) != 0 {
		t.Fatal("no StatusAddresses ⇒ must not publish")
	}
	// IP + hostname classification.
	ing2 := mk()
	c2 := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing2).
		WithStatusSubresource(&networkingv1.Ingress{}).Build()
	r2 := &IngressReconciler{Client: c2, StatusAddresses: []string{"203.0.113.7", "lb.example.com"}}
	r2.publishStatus(context.Background(), []*networkingv1.Ingress{ing2})
	got := ing2.Status.LoadBalancer.Ingress
	if len(got) != 2 || got[0].IP != "203.0.113.7" || got[0].Hostname != "" ||
		got[1].Hostname != "lb.example.com" || got[1].IP != "" {
		t.Fatalf("IP/hostname classification wrong: %+v", got)
	}
	var fetched networkingv1.Ingress
	if err := c2.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "g0s"}, &fetched); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(fetched.Status.LoadBalancer.Ingress) != 2 {
		t.Fatalf("status not persisted: %+v", fetched.Status.LoadBalancer.Ingress)
	}
}

// #7 readiness: not ready until the first successful render.
func TestReadyCheck(t *testing.T) {
	out := filepath.Join(t.TempDir(), "u.yaml")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local"}
	if r.ReadyCheck(nil) == nil {
		t.Fatal("must not be ready before first render")
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	if r.ReadyCheck(nil) != nil {
		t.Fatal("must be ready after first successful render")
	}
}

// #9 reloadDebouncer: leading edge fires immediately; a burst within
// the window collapses to exactly one extra (trailing) fire, and the
// final state is always applied.
func TestReloadDebouncer(t *testing.T) {
	var n atomic.Int32
	d := newReloadDebouncer(80*time.Millisecond, func() { n.Add(1) })
	d.trigger() // leading
	if n.Load() != 1 {
		t.Fatalf("leading edge must fire immediately, got %d", n.Load())
	}
	for i := 0; i < 8; i++ {
		d.trigger() // burst within window
	}
	if n.Load() != 1 {
		t.Fatalf("burst within window must NOT fire again yet, got %d", n.Load())
	}
	time.Sleep(160 * time.Millisecond)
	if got := n.Load(); got != 2 {
		t.Fatalf("burst must collapse to exactly one trailing fire (want 2 total), got %d", got)
	}
	// window<=0 ⇒ no debounce, every trigger fires.
	var m atomic.Int32
	d0 := newReloadDebouncer(0, func() { m.Add(1) })
	d0.trigger()
	d0.trigger()
	d0.trigger()
	if m.Load() != 3 {
		t.Fatalf("window<=0 ⇒ every trigger fires, got %d", m.Load())
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

// A trailing-slash path key can only be hit by an exact request to that
// slash in synapse's longest-prefix router, so addRoute must de-slash it
// (".../swagger/" -> ".../swagger") to cover the subtree, while preserving
// the bare root "/". Regression for the /docs/<svc>/swagger 404s.
func TestAddRoute_StripsTrailingSlash(t *testing.T) {
	m := newRenderModel()
	if !m.addRoute("h", "/docs/x/swagger/", []backend{{addr: "x:80"}}, annSettings{}, nil, nil) {
		t.Fatal("addRoute must succeed")
	}
	if _, ok := m.hosts["h"]["/docs/x/swagger"]; !ok {
		t.Fatalf("trailing slash must be stripped; got keys %v", m.hosts["h"])
	}
	if _, ok := m.hosts["h"]["/docs/x/swagger/"]; ok {
		t.Fatal("slashed key must not be stored")
	}
	s := renderUpstreams(m)
	if !strings.Contains(s, `"/docs/x/swagger":`) || strings.Contains(s, `"/docs/x/swagger/":`) {
		t.Fatalf("rendered key must be de-slashed:\n%s", s)
	}
	// Root "/" must be preserved verbatim.
	if !m.addRoute("h2", "/", []backend{{addr: "r:80"}}, annSettings{}, nil, nil) {
		t.Fatal("root addRoute must succeed")
	}
	if _, ok := m.hosts["h2"]["/"]; !ok {
		t.Fatal("root path must be preserved as \"/\"")
	}
	// Slashed and bare variants collapse to one key (first-writer-wins).
	if m.addRoute("h", "/docs/x/swagger", []backend{{addr: "y:80"}}, annSettings{}, nil, nil) {
		t.Fatal("slashed + bare variants must collapse to one key (conflict)")
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
	mk("14", "/usr/local/bin/synapse-proxy\x00--x\x00")         // custom name
	if err := os.MkdirAll(filepath.Join(proc, "notapid"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := findReloadTargets(proc, 99 /*self*/, "synapse")
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("expected [10] (synapse), got %v", got)
	}
	if r := findReloadTargets(proc, 10 /*self==synapse*/, "synapse"); len(r) != 0 {
		t.Fatalf("must skip self pid: %v", r)
	}
	// Configurable process name.
	if c := findReloadTargets(proc, 99, "synapse-proxy"); len(c) != 1 || c[0] != 14 {
		t.Fatalf("custom process name must match argv0 basename, got %v", c)
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

// --- Central mode (ConfigMap output + ClusterIP resolution) ---

// svcWithIP is a one-line helper for backendAddr/render tests that need
// a Service object with a populated ClusterIP. Mirrors what the cluster
// would have for a normal namespace-scoped Service.
func svcWithIP(ns, name, clusterIP string, port int32, portName string) *corev1.Service {
	sp := corev1.ServicePort{Port: port}
	if portName != "" {
		sp.Name = portName
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.ServiceSpec{
			ClusterIP: clusterIP,
			Ports:     []corev1.ServicePort{sp},
		},
	}
}

func TestBackendAddr_ResolvesClusterIP_PortByNumber(t *testing.T) {
	svc := svcWithIP("default", "whoami", "10.20.30.40", 80, "")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", ResolveBackendClusterIPs: true}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if !ok || addr != "10.20.30.40:80" {
		t.Fatalf("got %q ok=%v (want 10.20.30.40:80)", addr, ok)
	}
}

func TestBackendAddr_ResolvesClusterIP_PortByName(t *testing.T) {
	svc := svcWithIP("default", "whoami", "10.20.30.40", 8080, "http")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", ResolveBackendClusterIPs: true}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Name: "http"},
		},
	})
	if !ok || addr != "10.20.30.40:8080" {
		t.Fatalf("got %q ok=%v (want 10.20.30.40:8080)", addr, ok)
	}
}

func TestBackendAddr_HeadlessService_FallsBackToFQDN(t *testing.T) {
	svc := svcWithIP("default", "whoami", corev1.ClusterIPNone, 80, "")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", ResolveBackendClusterIPs: true}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if !ok || addr != "whoami.default.svc.cluster.local:80" {
		t.Fatalf("headless must fall back to FQDN; got %q ok=%v", addr, ok)
	}
}

func TestBackendAddr_MissingService_PortByNumber_FallsBackToFQDN(t *testing.T) {
	// Service absent → port-by-number can still emit the FQDN (the
	// in-proxy DNS cache will resolve it lazily). port-by-name can't.
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", ResolveBackendClusterIPs: true}
	addr, ok := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if !ok || addr != "whoami.default.svc.cluster.local:80" {
		t.Fatalf("missing-svc port-by-number must fall back to FQDN; got %q ok=%v", addr, ok)
	}
}

func TestBackendAddr_NoResolveFlag_KeepsFQDN(t *testing.T) {
	// Even when a ClusterIP is available, ResolveBackendClusterIPs=false
	// must keep the FQDN — that's the legacy sidecar behaviour.
	svc := svcWithIP("default", "whoami", "10.20.30.40", 80, "")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local"}
	addr, _ := r.backendAddr(context.Background(), "default", networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80},
		},
	})
	if addr != "whoami.default.svc.cluster.local:80" {
		t.Fatalf("FQDN must be preserved when ResolveBackendClusterIPs is off; got %q", addr)
	}
}

func TestRender_WritesConfigMap_AndCreatesIfMissing(t *testing.T) {
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{
				{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{
						Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}},
			},
		}},
	}}
	svc := svcWithIP("default", "whoami", "10.0.0.42", 80, "")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing, svc).Build()
	r := &IngressReconciler{
		Client:                   c,
		IngressClassName:         "synapse",
		ClusterDomain:            "cluster.local",
		UpstreamsOutConfigMap:    types.NamespacedName{Namespace: "synapse-os", Name: "synapse-ingress-rendered"},
		ResolveBackendClusterIPs: true,
	}
	changed, matched, hosts, err := r.render(context.Background())
	if err != nil || !changed || matched != 1 || hosts != 1 {
		t.Fatalf("render: changed=%v matched=%d hosts=%d err=%v", changed, matched, hosts, err)
	}
	var got corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "synapse-os", Name: "synapse-ingress-rendered",
	}, &got); err != nil {
		t.Fatalf("output ConfigMap not created: %v", err)
	}
	if got.Labels[ingressUpstreamsManagedLabel] != "true" {
		t.Errorf("missing managed label: %v", got.Labels)
	}
	body, ok := got.Data[UpstreamsKey]
	if !ok || !strings.Contains(body, "10.0.0.42:80") {
		t.Fatalf("upstreams.yaml missing or unresolved:\n%s", body)
	}
	if strings.Contains(body, "whoami.default.svc.cluster.local") {
		t.Errorf("FQDN should have been substituted with ClusterIP:\n%s", body)
	}
}

func TestRender_ConfigMapOutput_SecondRunIsNoOp(t *testing.T) {
	// Identical-content second render returns changed=false and does
	// not bump Generation by updating Data.
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{
				{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{
						Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}},
			},
		}},
	}}
	svc := svcWithIP("default", "whoami", "10.0.0.42", 80, "")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing, svc).Build()
	r := &IngressReconciler{
		Client:                   c,
		IngressClassName:         "synapse",
		ClusterDomain:            "cluster.local",
		UpstreamsOutConfigMap:    types.NamespacedName{Namespace: "ns", Name: "out"},
		ResolveBackendClusterIPs: true,
	}
	if changed, _, _, err := r.render(context.Background()); err != nil || !changed {
		t.Fatalf("first render must change: changed=%v err=%v", changed, err)
	}
	if changed, _, _, err := r.render(context.Background()); err != nil || changed {
		t.Fatalf("identical second render must be no-op: changed=%v err=%v", changed, err)
	}
}

func TestRender_FilePath_StillWorks(t *testing.T) {
	// Belt-and-suspenders for the existing sidecar mode: leaving
	// UpstreamsOutConfigMap empty must keep the file-write path.
	out := filepath.Join(t.TempDir(), "upstreams.yaml")
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{
				{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{
						Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}},
			},
		}},
	}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	r := &IngressReconciler{
		Client: c, IngressClassName: "synapse", UpstreamsOutPath: out, ClusterDomain: "cluster.local",
	}
	if changed, _, _, err := r.render(context.Background()); err != nil || !changed {
		t.Fatalf("render: changed=%v err=%v", changed, err)
	}
	body, _ := os.ReadFile(out)
	if !strings.Contains(string(body), "whoami.default.svc.cluster.local:80") {
		t.Fatalf("file output missing or unexpected:\n%s", body)
	}
}

// parseSize accepts bare bytes and k/m/g suffixes (case-insensitive,
// nginx-compatible). Malformed inputs return nil so callers can fall back
// to "no annotation set" — same contract as the rest of the parser.
func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want *uint64
	}{
		{"1048576", ptr[uint64](1048576)},
		{"50m", ptr[uint64](50 * 1024 * 1024)},
		{"50M", ptr[uint64](50 * 1024 * 1024)},
		{"1g", ptr[uint64](1024 * 1024 * 1024)},
		{"1G", ptr[uint64](1024 * 1024 * 1024)},
		{"1024k", ptr[uint64](1024 * 1024)},
		{"1024K", ptr[uint64](1024 * 1024)},
		{"  10m  ", ptr[uint64](10 * 1024 * 1024)}, // whitespace tolerated
		{"", nil},     // empty
		{"abc", nil},  // non-numeric
		{"50mb", nil}, // unsupported suffix
		{"-5m", nil},  // negative
	}
	for _, c := range cases {
		got := parseSize(c.in)
		if (got == nil) != (c.want == nil) {
			t.Fatalf("parseSize(%q): got %v want %v", c.in, got, c.want)
		}
		if got != nil && *got != *c.want {
			t.Fatalf("parseSize(%q): got %d want %d", c.in, *got, *c.want)
		}
	}
}

// proxy-body-size annotation: synapse-key wins over nginx-key. Either
// alone is enough.
func TestParseAnnotations_ProxyBodySize(t *testing.T) {
	// synapse-key takes precedence over nginx-key when both present
	a := parseAnnotations(map[string]string{
		annPrefix + "proxy-body-size":   "10m",
		nginxPrefix + "proxy-body-size": "1g", // ignored when synapse-key present
	})
	if a.maxBodySize == nil || *a.maxBodySize != 10*1024*1024 {
		t.Fatalf("synapse-key precedence: %+v", a.maxBodySize)
	}

	// nginx-key alone is the fallback path
	n := parseAnnotations(map[string]string{
		nginxPrefix + "proxy-body-size": "50m",
	})
	if n.maxBodySize == nil || *n.maxBodySize != 50*1024*1024 {
		t.Fatalf("nginx-compat fallback: %+v", n.maxBodySize)
	}

	// Invalid value silently drops to nil (matches rest of parser)
	bad := parseAnnotations(map[string]string{
		annPrefix + "proxy-body-size": "garbage",
	})
	if bad.maxBodySize != nil {
		t.Fatalf("invalid size must produce nil, got %d", *bad.maxBodySize)
	}
}

// server-alias annotation: synapse-key wins over nginx-key. Either alone
// is enough. csv() trims whitespace.
func TestParseAnnotations_ServerAlias(t *testing.T) {
	a := parseAnnotations(map[string]string{
		annPrefix + "server-alias": "www.example.com, alt.example.com",
	})
	if len(a.serverAliases) != 2 ||
		a.serverAliases[0] != "www.example.com" ||
		a.serverAliases[1] != "alt.example.com" {
		t.Fatalf("synapse-key csv parse: %+v", a.serverAliases)
	}
	// nginx-key fallback
	n := parseAnnotations(map[string]string{
		nginxPrefix + "server-alias": "x.example.com",
	})
	if len(n.serverAliases) != 1 || n.serverAliases[0] != "x.example.com" {
		t.Fatalf("nginx-compat fallback: %+v", n.serverAliases)
	}
	// synapse-key wins over nginx-key when both present
	both := parseAnnotations(map[string]string{
		annPrefix + "server-alias":   "synapse.example.com",
		nginxPrefix + "server-alias": "ignored.example.com",
	})
	if len(both.serverAliases) != 1 || both.serverAliases[0] != "synapse.example.com" {
		t.Fatalf("synapse-key precedence: %+v", both.serverAliases)
	}
}

// renderUpstreams emits each alias host as its own block with the same
// settings as the primary host. Verified by addRoute over alias hosts.
func TestRenderUpstreams_ServerAliasDuplicatesRoute(t *testing.T) {
	m := newRenderModel()
	a := annSettings{serverAliases: []string{"www.example.com", "alt.example.com"}}
	// Caller (ingress_controller.go) iterates aliases after the primary
	// addRoute; emulate that here.
	m.addRoute("example.com", "/",
		[]backend{{addr: "be:80"}}, a, nil, nil)
	for _, alias := range a.serverAliases {
		m.addRoute(alias, "/",
			[]backend{{addr: "be:80"}}, a, nil, nil)
	}
	out := renderUpstreams(m)
	for _, want := range []string{
		`"example.com":`,
		`"www.example.com":`,
		`"alt.example.com":`,
		`- "be:80"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

// ssl-passthrough annotation: synapse-key wins over nginx-key. Either
// alone is enough. False/missing leaves passthrough off.
func TestParseAnnotations_SslPassthrough(t *testing.T) {
	a := parseAnnotations(map[string]string{
		annPrefix + "ssl-passthrough": "true",
	})
	if !a.passthrough {
		t.Fatalf("synapse-key true: %+v", a.passthrough)
	}
	n := parseAnnotations(map[string]string{
		nginxPrefix + "ssl-passthrough": "true",
	})
	if !n.passthrough {
		t.Fatalf("nginx-compat fallback: %+v", n.passthrough)
	}
	z := parseAnnotations(map[string]string{
		annPrefix + "ssl-passthrough": "false",
	})
	if z.passthrough {
		t.Fatalf("false must stay off: %+v", z.passthrough)
	}
	if e := parseAnnotations(nil); e.passthrough {
		t.Fatalf("missing must stay off: %+v", e.passthrough)
	}
}

// addPassthroughHost FIRST-WRITER-WINS: a host already claimed by a
// terminate route can't be replaced; nor can two passthrough Ingresses
// share the same SNI. Both losers get false back so the caller can emit
// a RouteConflict Event + bump the counter.
func TestAddPassthroughHost_FirstWriterWins(t *testing.T) {
	// passthrough wins when claimed first
	m := newRenderModel()
	if !m.addPassthroughHost("a.example.com", "10.0.0.1:443") {
		t.Fatal("first passthrough must claim")
	}
	if m.addPassthroughHost("a.example.com", "10.0.0.2:443") {
		t.Fatal("duplicate passthrough must lose")
	}
	if m.addRoute("a.example.com", "/", []backend{{addr: "be:80"}}, annSettings{}, nil, nil) {
		t.Fatal("terminate route on passthrough host must lose")
	}
	// terminate wins when claimed first
	m2 := newRenderModel()
	if !m2.addRoute("b.example.com", "/", []backend{{addr: "be:80"}}, annSettings{}, nil, nil) {
		t.Fatal("first terminate route must claim")
	}
	if m2.addPassthroughHost("b.example.com", "10.0.0.5:443") {
		t.Fatal("passthrough on terminate host must lose")
	}
}

// renderUpstreamsV2: when at least one passthrough host is present,
// the whole file switches to v2 schema. Verifies passthrough block
// shape, terminate block shape with all v1-compat knobs threaded, and
// the top-level scaffolding (version, sticky, internal ACME).
func TestRenderUpstreamsV2_MixedTerminateAndPassthrough(t *testing.T) {
	m := newRenderModel()
	m.sticky = true
	m.acme = "10.0.0.99:8080" // ACME challenge backend
	m.addPassthroughHost("pt.example.com", "10.0.0.1:443")
	m.hostCert["app.example.com"] = "app.example.com"
	tru := true
	var ct uint64 = 7
	var bs uint64 = 10485760
	m.addRoute("app.example.com", "/",
		[]backend{{addr: "be:80"}},
		annSettings{
			ssl:              &tru,
			http2:            &tru,
			forceHTTPS:       &tru,
			disableAccessLog: &tru,
			connectTimeout:   &ct,
			maxBodySize:      &bs,
			reqHeaders:       []string{"X-A: 1"},
			respHeaders:      []string{"X-B: 2"},
		}, nil, nil)
	out := renderUpstreamsV2(m)
	for _, want := range []string{
		"version: 2",
		"sticky_sessions:\n    enabled: true",
		`"/.well-known/acme-challenge/*"`,
		`upstream: "10.0.0.99:8080"`,
		`"app.example.com":`,
		"      terminate:",
		`cert: "app.example.com"`,
		`"pt.example.com":`,
		"      passthrough: true",
		`upstream: "10.0.0.1:443"`,
		`upstream: "be:80"`,
		"ssl_enabled: true",
		"http2_enabled: true",
		"force_https: true",
		"disable_access_log: true",
		"max_body_size: 10485760",
		"connect: 7",
		`- "X-A: 1"`,
		`- "X-B: 2"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in v2 render:\n%s", want, out)
		}
	}
}

// permanent-redirect default is 301; permanent-redirect-code overrides.
// nginx-compat fallback keys are honored. permanent wins over temporal
// when both are set.
func TestParseAnnotations_Redirect(t *testing.T) {
	// permanent default 301
	a := parseAnnotations(map[string]string{
		annPrefix + "permanent-redirect": "https://example.com/new",
	})
	if a.redirectStatus == nil || *a.redirectStatus != 301 ||
		a.redirectLocation != "https://example.com/new" {
		t.Fatalf("permanent default 301: %+v / %q", a.redirectStatus, a.redirectLocation)
	}
	// permanent-redirect-code override
	a = parseAnnotations(map[string]string{
		annPrefix + "permanent-redirect":      "https://example.com/new",
		annPrefix + "permanent-redirect-code": "308",
	})
	if a.redirectStatus == nil || *a.redirectStatus != 308 {
		t.Fatalf("permanent-redirect-code override: %+v", a.redirectStatus)
	}
	// temporal default 302
	a = parseAnnotations(map[string]string{
		annPrefix + "temporal-redirect": "/somewhere",
	})
	if a.redirectStatus == nil || *a.redirectStatus != 302 ||
		a.redirectLocation != "/somewhere" {
		t.Fatalf("temporal default 302: %+v / %q", a.redirectStatus, a.redirectLocation)
	}
	// temporal-redirect-code override
	a = parseAnnotations(map[string]string{
		annPrefix + "temporal-redirect":      "/somewhere",
		annPrefix + "temporal-redirect-code": "307",
	})
	if a.redirectStatus == nil || *a.redirectStatus != 307 {
		t.Fatalf("temporal-redirect-code override: %+v", a.redirectStatus)
	}
	// permanent wins over temporal when both set
	a = parseAnnotations(map[string]string{
		annPrefix + "temporal-redirect":  "/old-temp",
		annPrefix + "permanent-redirect": "/new-perm",
	})
	if a.redirectStatus == nil || *a.redirectStatus != 301 ||
		a.redirectLocation != "/new-perm" {
		t.Fatalf("permanent must win over temporal: %+v / %q", a.redirectStatus, a.redirectLocation)
	}
	// nginx-compat fallback
	n := parseAnnotations(map[string]string{
		nginxPrefix + "permanent-redirect":      "/x",
		nginxPrefix + "permanent-redirect-code": "301",
	})
	if n.redirectStatus == nil || *n.redirectStatus != 301 || n.redirectLocation != "/x" {
		t.Fatalf("nginx-compat fallback: %+v / %q", n.redirectStatus, n.redirectLocation)
	}
}

// renderUpstreams emits a redirect: block on the route when set, both
// in v1 emission and v2 emission. Verified separately for each.
func TestRenderUpstreams_RedirectEmitted(t *testing.T) {
	st := uint64(301)
	m := newRenderModel()
	m.addRoute("example.com", "/old",
		[]backend{{addr: "be:80"}},
		annSettings{redirectStatus: &st, redirectLocation: "/new"}, nil, nil)
	out := renderUpstreams(m)
	for _, want := range []string{
		"redirect:",
		"status: 301",
		`location: "/new"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("v1 missing %q in:\n%s", want, out)
		}
	}

	// v2 emission (forced by adding a passthrough host)
	m2 := newRenderModel()
	m2.addPassthroughHost("pt.example.com", "10.0.0.1:443")
	m2.addRoute("example.com", "/old",
		[]backend{{addr: "be:80"}},
		annSettings{redirectStatus: &st, redirectLocation: "/new"}, nil, nil)
	out2 := renderUpstreamsV2(m2)
	for _, want := range []string{
		"redirect:",
		"status: 301",
		`location: "/new"`,
	} {
		if !strings.Contains(out2, want) {
			t.Fatalf("v2 missing %q in:\n%s", want, out2)
		}
	}
}

// renderUpstreams emits max_body_size: <bytes> when the route carries one,
// and omits the line otherwise (no regression for unannotated routes).
func TestRenderUpstreams_MaxBodySizeEmitted(t *testing.T) {
	m := newRenderModel()
	cap := uint64(52428800) // 50 MiB
	m.addRoute("app.example.com", "/",
		[]backend{{addr: "a:80"}},
		annSettings{maxBodySize: &cap}, nil, nil)
	out := renderUpstreams(m)
	if !strings.Contains(out, "max_body_size: 52428800") {
		t.Fatalf("max_body_size line missing:\n%s", out)
	}

	// Route without the annotation must not emit the line — unannotated
	// routes inherit synapse's no-cap default.
	m2 := newRenderModel()
	m2.addRoute("plain.example.com", "/",
		[]backend{{addr: "b:80"}},
		annSettings{}, nil, nil)
	out2 := renderUpstreams(m2)
	if strings.Contains(out2, "max_body_size") {
		t.Fatalf("unannotated route emitted max_body_size:\n%s", out2)
	}
}
