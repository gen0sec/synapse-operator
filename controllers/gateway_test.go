package controllers

import (
	"context"
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
	got := rulePaths(rule)
	if len(got) != 2 || got[0] != "/a" || got[1] != "/.well-known/acme-challenge/t" {
		t.Fatalf("rulePaths = %v", got)
	}
	if p := rulePaths(gwv1.HTTPRouteRule{}); len(p) != 1 || p[0] != "/" {
		t.Fatalf("empty rule must default to [/]: %v", p)
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

func TestEnsureCond(t *testing.T) {
	r := &IngressReconciler{}
	var conds []metav1.Condition
	if !r.ensureCond(&conds, 1, "Accepted", "Accepted", "msg") {
		t.Fatal("first set must report changed")
	}
	if len(conds) != 1 || conds[0].Status != metav1.ConditionTrue || conds[0].Type != "Accepted" {
		t.Fatalf("condition not set: %+v", conds)
	}
	if r.ensureCond(&conds, 1, "Accepted", "Accepted", "msg") {
		t.Fatal("idempotent set (same gen, already True) must report unchanged")
	}
	if r.ensureCond(&conds, 2, "Accepted", "Accepted", "msg") == false {
		t.Fatal("new observedGeneration must report changed")
	}
	if len(conds) != 1 {
		t.Fatalf("same type must replace, not append: %+v", conds)
	}
}

// renderGateways: an HTTPRoute attached to a Gateway of our
// GatewayClass, carrying the ACME challenge path, must surface as the
// acmeBackend (→ internal_paths override) — the cert-manager
// gatewayHTTPRoute solver path through synapse-as-ingress.
func TestRenderGateways_ACMESolverHTTPRoute(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)

	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "synapse-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("synapse")

	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "cm-acme-http-solver-xyz", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{
		Name: gwv1.ObjectName("synapse-gw"), Namespace: ptr(gwv1.Namespace("default")),
	}}
	rt.Spec.Hostnames = []gwv1.Hostname{"app.example.com"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches: []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{
			Type: ptr(gwv1.PathMatchExact), Value: ptr("/.well-known/acme-challenge/tok"),
		}}},
		BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
			BackendObjectReference: gwv1.BackendObjectReference{
				Name: gwv1.ObjectName("cm-acme-http-solver-svc"), Port: ptr(gwv1.PortNumber(8089)),
			}}}},
	}}

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(gc, gw, rt).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).
		Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true}

	hosts := map[string]map[string]string{}
	acme := ""
	n := r.renderGateways(context.Background(), hosts, &acme)
	if n < 1 {
		t.Fatalf("expected ≥1 programmed HTTPRoute, got %d", n)
	}
	if acme != "cm-acme-http-solver-svc.default.svc.cluster.local:8089" {
		t.Fatalf("acmeBackend not set from Gateway HTTPRoute solver: %q", acme)
	}
}

// An HTTPRoute whose parent Gateway is NOT of our GatewayClass must be
// ignored.
func TestRenderGateways_IgnoresForeignGatewayClass(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "other"
	gc.Spec.ControllerName = gwv1.GatewayController("example.com/other")
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "other-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("other")
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "r", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: gwv1.ObjectName("other-gw")}}
	rt.Spec.Hostnames = []gwv1.Hostname{"x"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
		BackendObjectReference: gwv1.BackendObjectReference{Name: "x", Port: ptr(gwv1.PortNumber(1))}}}}}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(gc, gw, rt).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true}
	hosts := map[string]map[string]string{}
	acme := ""
	if n := r.renderGateways(context.Background(), hosts, &acme); n != 0 || acme != "" || len(hosts) != 0 {
		t.Fatalf("foreign GatewayClass leaked: n=%d acme=%q hosts=%v", n, acme, hosts)
	}
}
