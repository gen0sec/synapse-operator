package controllers

import (
	"context"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestLeaderGate(t *testing.T) {
	var g LeaderGate
	if g.IsLeader() {
		t.Fatal("zero LeaderGate must not be leader")
	}
	g.flag.Store(true)
	if !g.IsLeader() {
		t.Fatal("flag set ⇒ IsLeader true")
	}

	// nil gate on the reconciler ⇒ always leader (unchanged behavior).
	if !(&IngressReconciler{}).leader() {
		t.Fatal("nil IsLeader ⇒ leader() must be true")
	}
	rNo := &IngressReconciler{IsLeader: func() bool { return false }}
	if rNo.leader() {
		t.Fatal("IsLeader=false ⇒ leader() false")
	}
}

// A non-leader replica must STILL render routes into the model
// (per-pod upstreams are never gated) but must NOT write shared
// cluster status (GatewayClass/Gateway/HTTPRoute).
func TestRenderGateways_NonLeaderSkipsStatusButStillRenders(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "synapse-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("synapse")
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "app", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "synapse-gw", Namespace: ptr(gwv1.Namespace("default"))}}
	rt.Spec.Hostnames = []gwv1.Hostname{"app.example.com"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Value: ptr("/")}}},
		BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "whoami", Port: ptr(gwv1.PortNumber(80))}}}},
	}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(gc, gw, rt).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true,
		IsLeader: func() bool { return false }} // NOT leader

	m := newRenderModel()
	if n := r.renderGateways(context.Background(), m); n < 1 {
		t.Fatalf("non-leader must still program routes, got n=%d", n)
	}
	if rc := m.hosts["app.example.com"]["/"]; rc == nil || len(rc.servers) != 1 {
		t.Fatalf("route must still be rendered into the model on a non-leader: %+v", rc)
	}
	// Shared status must be UNTOUCHED (no Status().Update happened).
	var got gwv1.HTTPRoute
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "app"}, &got)
	if len(got.Status.Parents) != 0 {
		t.Fatalf("non-leader must NOT write HTTPRoute status, got %+v", got.Status.Parents)
	}
	var gotGW gwv1.Gateway
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "synapse-gw"}, &gotGW)
	if len(gotGW.Status.Conditions) != 0 {
		t.Fatalf("non-leader must NOT write Gateway status, got %+v", gotGW.Status.Conditions)
	}

	// publishStatus is also leader-gated.
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	rNo := &IngressReconciler{Client: c, StatusAddresses: []string{"203.0.113.9"}, IsLeader: func() bool { return false }}
	rNo.publishStatus(context.Background(), []*networkingv1.Ingress{ing})
	if len(ing.Status.LoadBalancer.Ingress) != 0 {
		t.Fatalf("non-leader must NOT publish Ingress status, got %+v", ing.Status.LoadBalancer.Ingress)
	}
}
