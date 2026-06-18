package controllers

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestRulePaths(t *testing.T) {
	rule := gwv1.HTTPRouteRule{Matches: []gwv1.HTTPRouteMatch{
		{Path: &gwv1.HTTPPathMatch{Value: ptr("/a")}},
		{Path: &gwv1.HTTPPathMatch{Value: ptr("/.well-known/acme-challenge/t")}},
	}}
	got, rx, warns := rulePaths(rule)
	if len(got) != 2 || got[0] != "/a" || got[1] != "/.well-known/acme-challenge/t" {
		t.Fatalf("rulePaths = %v", got)
	}
	if len(rx) != 0 {
		t.Fatalf("plain prefix paths must not produce regexes: %v", rx)
	}
	if len(warns) != 0 {
		t.Fatalf("plain prefix paths must not warn: %v", warns)
	}
	// No matches ⇒ match-all ⇒ ["/"] with no warning.
	if p, rx, w := rulePaths(gwv1.HTTPRouteRule{}); len(p) != 1 || p[0] != "/" || len(rx) != 0 || len(w) != 0 {
		t.Fatalf("empty rule must default to [/] no warn: %v %v %v", p, rx, w)
	}
	// Exact ⇒ used (approximated) + warning.
	p, rx, w := rulePaths(gwv1.HTTPRouteRule{Matches: []gwv1.HTTPRouteMatch{
		{Path: &gwv1.HTTPPathMatch{Type: ptr(gwv1.PathMatchExact), Value: ptr("/exact")}}}})
	if len(p) != 1 || p[0] != "/exact" || len(rx) != 0 || len(w) != 1 || !strings.Contains(w[0], "Exact") {
		t.Fatalf("Exact must be used + warn: p=%v rx=%v w=%v", p, rx, w)
	}
	// RegularExpression ⇒ emitted as a regex (NOT dropped, NO warning, NOT "/").
	p, rx, w = rulePaths(gwv1.HTTPRouteRule{Matches: []gwv1.HTTPRouteMatch{
		{Path: &gwv1.HTTPPathMatch{Type: ptr(gwv1.PathMatchRegularExpression), Value: ptr("/x.*")}}}})
	if len(p) != 0 || len(rx) != 1 || rx[0] != "/x.*" || len(w) != 0 {
		t.Fatalf("regex must be carried as a regex route (no drop/warn/'/' default): p=%v rx=%v w=%v", p, rx, w)
	}
	// Header-only match ⇒ default path "/" + header-dropped warning.
	p, rx, w = rulePaths(gwv1.HTTPRouteRule{Matches: []gwv1.HTTPRouteMatch{
		{Headers: []gwv1.HTTPHeaderMatch{{Name: "X-Env", Value: "canary"}}}}})
	if len(p) != 1 || p[0] != "/" || len(rx) != 0 || len(w) != 1 || !strings.Contains(w[0], "header") {
		t.Fatalf("header-only match ⇒ ['/'] + warn: p=%v rx=%v w=%v", p, rx, w)
	}
}

// A regex route renders as a match_expr block under a label key, in both
// the v1 and v2 schemas — proving regex Ingress/HTTPRoute paths survive.
func TestRegexRouteRender(t *testing.T) {
	m := newRenderModel()
	if !m.addRegexRoute("api.example.com", "^/api/runs/[^/]+/stream$",
		[]backend{{addr: "svc.ns.svc.cluster.local:8091"}}, annSettings{}, nil, nil) {
		t.Fatal("addRegexRoute should succeed on a fresh model")
	}
	// Duplicate regex on the same host ⇒ first-writer-wins (no second add).
	if m.addRegexRoute("api.example.com", "^/api/runs/[^/]+/stream$",
		[]backend{{addr: "other:1"}}, annSettings{}, nil, nil) {
		t.Fatal("duplicate regex route must be rejected (first-writer-wins)")
	}
	wantExpr := `match_expr: "http.request.path matches \"^/api/runs/[^/]+/stream$\""`
	for _, out := range []string{renderUpstreams(m), renderUpstreamsV2(m)} {
		if !strings.Contains(out, wantExpr) {
			t.Fatalf("rendered config missing match_expr:\nwant substring: %s\ngot:\n%s", wantExpr, out)
		}
		if !strings.Contains(out, "svc.ns.svc.cluster.local:8091") {
			t.Fatalf("rendered config missing backend:\n%s", out)
		}
	}
}

// Gateway API weight semantics: weight 0 ⇒ no traffic (excluded);
// unset-with-others ⇒ default 1; no weights at all ⇒ unweighted.
func TestRuleBackends_WeightSemantics(t *testing.T) {
	r := &IngressReconciler{ClusterDomain: "cluster.local"}
	rt := &gwv1.HTTPRoute{}
	rt.Namespace = "default"
	mkRef := func(name string, w *int32) gwv1.HTTPBackendRef {
		return gwv1.HTTPBackendRef{BackendRef: gwv1.BackendRef{Weight: w,
			BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(name), Port: ptr(gwv1.PortNumber(80))}}}
	}
	// Mixed weights: a=70, b=0 (excluded), c=unset→1.
	got := r.ruleBackends(context.Background(), rt, gwv1.HTTPRouteRule{BackendRefs: []gwv1.HTTPBackendRef{
		mkRef("a", ptr(int32(70))), mkRef("b", ptr(int32(0))), mkRef("c", nil)}})
	if len(got) != 2 {
		t.Fatalf("weight-0 backend must be excluded: %+v", got)
	}
	if got[0].addr != "a.default.svc.cluster.local:80" || got[0].weight != 70 {
		t.Fatalf("a weight: %+v", got[0])
	}
	if got[1].addr != "c.default.svc.cluster.local:80" || got[1].weight != 1 {
		t.Fatalf("unset-with-others must default to weight 1: %+v", got[1])
	}
	// No weights anywhere ⇒ unweighted (weight 0 ⇒ bare-string form).
	un := r.ruleBackends(context.Background(), rt, gwv1.HTTPRouteRule{BackendRefs: []gwv1.HTTPBackendRef{
		mkRef("x", nil), mkRef("y", nil)}})
	if len(un) != 2 || un[0].weight != 0 || un[1].weight != 0 {
		t.Fatalf("no weights ⇒ unweighted pool: %+v", un)
	}
	// All weight 0 ⇒ no servers (no traffic).
	if z := r.ruleBackends(context.Background(), rt, gwv1.HTTPRouteRule{BackendRefs: []gwv1.HTTPBackendRef{
		mkRef("a", ptr(int32(0)))}}); len(z) != 0 {
		t.Fatalf("all-zero-weight ⇒ empty pool: %+v", z)
	}
}

func TestGwBackend(t *testing.T) {
	r := &IngressReconciler{ClusterDomain: "cluster.local"}
	ok := gwv1.HTTPBackendRef{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
		Name: gwv1.ObjectName("solver"), Port: ptr(gwv1.PortNumber(8089))}}}
	if a, good := r.gwBackend("default", ok); !good || a != "solver.default.svc.cluster.local:8089" {
		t.Fatalf("gwBackend = %q good=%v", a, good)
	}
	noPort := gwv1.HTTPBackendRef{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
		Name: gwv1.ObjectName("x")}}}
	if _, good := r.gwBackend("default", noPort); good {
		t.Fatal("missing port must be rejected")
	}
	notSvc := gwv1.HTTPBackendRef{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
		Kind: ptr(gwv1.Kind("Secret")), Name: gwv1.ObjectName("x"), Port: ptr(gwv1.PortNumber(1))}}}
	if _, good := r.gwBackend("default", notSvc); good {
		t.Fatal("non-Service backend must be rejected")
	}
}

func TestHeaderFilters(t *testing.T) {
	filters := []gwv1.HTTPRouteFilter{
		{Type: gwv1.HTTPRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: &gwv1.HTTPHeaderFilter{
				Set: []gwv1.HTTPHeader{{Name: "X-Set", Value: "1"}},
				Add: []gwv1.HTTPHeader{{Name: "X-Add", Value: "2"}},
			}},
		{Type: gwv1.HTTPRouteFilterResponseHeaderModifier,
			ResponseHeaderModifier: &gwv1.HTTPHeaderFilter{
				Set: []gwv1.HTTPHeader{{Name: "X-R", Value: "3"}}}},
		{Type: gwv1.HTTPRouteFilterURLRewrite}, // no v1 equivalent → ignored
	}
	req, resp := headerFilters(filters)
	if len(req) != 2 || req[0] != "X-Set: 1" || req[1] != "X-Add: 2" {
		t.Fatalf("req headers = %v", req)
	}
	if len(resp) != 1 || resp[0] != "X-R: 3" {
		t.Fatalf("resp headers = %v", resp)
	}
}

func TestEnsureCond(t *testing.T) {
	r := &IngressReconciler{}
	var conds []metav1.Condition
	if !r.ensureCond(&conds, 1, "Accepted", "Accepted", "msg") {
		t.Fatal("first set must report changed")
	}
	if len(conds) != 1 || conds[0].Status != metav1.ConditionTrue {
		t.Fatalf("condition not set: %+v", conds)
	}
	if r.ensureCond(&conds, 1, "Accepted", "Accepted", "msg") {
		t.Fatal("idempotent set must report unchanged")
	}
	if !r.ensureCond(&conds, 2, "Accepted", "Accepted", "msg") {
		t.Fatal("new observedGeneration must report changed")
	}
	if len(conds) != 1 {
		t.Fatalf("same type must replace, not append: %+v", conds)
	}
}

// HTTPRoute attached to our GatewayClass: ACME path → model.acme;
// app path → weighted servers + header-filter injection.
func TestRenderGateways_SolverWeightsAndFilters(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "synapse-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("synapse")

	solver := &gwv1.HTTPRoute{}
	solver.Name, solver.Namespace = "cm-acme-http-solver-xyz", "default"
	solver.Spec.ParentRefs = []gwv1.ParentReference{{Name: "synapse-gw", Namespace: ptr(gwv1.Namespace("default"))}}
	solver.Spec.Hostnames = []gwv1.Hostname{"app.example.com"}
	solver.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches: []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{
			Type: ptr(gwv1.PathMatchExact), Value: ptr("/.well-known/acme-challenge/tok")}}},
		BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
			Name: "cm-acme-http-solver-svc", Port: ptr(gwv1.PortNumber(8089))}}}},
	}}

	app := &gwv1.HTTPRoute{}
	app.Name, app.Namespace = "app", "default"
	app.Spec.ParentRefs = []gwv1.ParentReference{{Name: "synapse-gw", Namespace: ptr(gwv1.Namespace("default"))}}
	app.Spec.Hostnames = []gwv1.Hostname{"app.example.com"}
	app.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches: []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Value: ptr("/")}}},
		BackendRefs: []gwv1.HTTPBackendRef{
			{BackendRef: gwv1.BackendRef{Weight: ptr(int32(70)), BackendObjectReference: gwv1.BackendObjectReference{Name: "v1", Port: ptr(gwv1.PortNumber(80))}}},
			{BackendRef: gwv1.BackendRef{Weight: ptr(int32(30)), BackendObjectReference: gwv1.BackendObjectReference{Name: "v2", Port: ptr(gwv1.PortNumber(80))}}},
		},
		Filters: []gwv1.HTTPRouteFilter{{Type: gwv1.HTTPRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: &gwv1.HTTPHeaderFilter{Set: []gwv1.HTTPHeader{{Name: "X-Canary", Value: "on"}}}}},
	}}

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(gc, gw, solver, app).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true}

	m := newRenderModel()
	if n := r.renderGateways(context.Background(), m); n < 2 {
		t.Fatalf("expected ≥2 programmed HTTPRoutes, got %d", n)
	}
	if m.acme != "cm-acme-http-solver-svc.default.svc.cluster.local:8089" {
		t.Fatalf("acme backend not set from Gateway solver HTTPRoute: %q", m.acme)
	}
	rc := m.hosts["app.example.com"]["/"]
	if rc == nil || len(rc.servers) != 2 ||
		rc.servers[0].addr != "v1.default.svc.cluster.local:80" || rc.servers[0].weight != 70 ||
		rc.servers[1].weight != 30 {
		t.Fatalf("weighted backends not mapped: %+v", rc)
	}
	if len(rc.reqHeaders) != 1 || rc.reqHeaders[0] != "X-Canary: on" {
		t.Fatalf("header filter not applied: %+v", rc)
	}
}

func TestRenderGateways_IgnoresForeignGatewayClass(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "other"
	gc.Spec.ControllerName = gwv1.GatewayController("example.com/other")
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "other-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("other")
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "r", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "other-gw"}}
	rt.Spec.Hostnames = []gwv1.Hostname{"x"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
		BackendObjectReference: gwv1.BackendObjectReference{Name: "x", Port: ptr(gwv1.PortNumber(1))}}}}}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(gc, gw, rt).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true}
	m := newRenderModel()
	if n := r.renderGateways(context.Background(), m); n != 0 || m.acme != "" || len(m.hosts) != 0 {
		t.Fatalf("foreign GatewayClass leaked: n=%d acme=%q hosts=%v", n, m.acme, m.hosts)
	}
}
