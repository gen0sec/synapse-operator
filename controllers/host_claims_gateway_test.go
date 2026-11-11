package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// gatewayLedgerRig is a ledgerRig whose proxy also serves the Gateway API.
func gatewayLedgerRig(t *testing.T, objs ...client.Object) *ledgerRig {
	t.Helper()
	g := newLedgerRig(t)
	class := gwClass("edge", "synapse-os", "edge")
	g.r.GatewayAPI = true
	g.r.Client = fake.NewClientBuilder().WithScheme(proxyTestScheme(t)).
		WithObjects(append([]client.Object{g.proxy, classFor("public", g.proxy), &class}, objs...)...).
		WithStatusSubresource(&networkingv1.Ingress{}, &gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	return g
}

func (g *ledgerRig) listenerAccepted(ns, name, listener string) bool {
	g.t.Helper()
	var gw gwv1.Gateway
	if err := g.r.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &gw); err != nil {
		g.t.Fatal(err)
	}
	for _, ls := range gw.Status.Listeners {
		if string(ls.Name) == listener {
			return meta.IsStatusConditionTrue(ls.Conditions, "Accepted")
		}
	}
	g.t.Fatalf("no status for listener %s", listener)
	return false
}

func (g *ledgerRig) setHostname(gw *gwv1.Gateway, host string) {
	g.t.Helper()
	if err := g.r.Get(context.Background(), client.ObjectKeyFromObject(gw), gw); err != nil {
		g.t.Fatal(err)
	}
	gw.Spec.Listeners[0].Hostname = ptr(gwv1.Hostname(host))
	if err := g.r.Update(context.Background(), gw); err != nil {
		g.t.Fatal(err)
	}
}

func TestOwnership_AGatewayListenerOrRouteClaimGetsALedgerLine(t *testing.T) {
	gw := gateway("edge", "web", httpListener("named", 80, "listener.example.com"), httpListener("any", 81, ""))
	gw.CreationTimestamp = metav1.NewTime(t0.Add(-time.Hour))
	rt := httpRoute("shop", []string{"route.example.com"}, to("app"))
	rt.CreationTimestamp = metav1.NewTime(t0)
	rt.Spec.ParentRefs[0].SectionName = ptr(gwv1.SectionName("any"))
	g := gatewayLedgerRig(t, &gw, &rt)
	g.mustReconcile()

	cm, ok := g.ledger()
	if !ok {
		t.Fatal("no ledger written")
	}
	body := cm.Data["claims"]
	for _, want := range []string{
		// Claimed for the Gateway's namespace, aged by the Gateway ...
		"edge listener.example.com " + t0.Add(-time.Hour).Format(time.RFC3339Nano),
		// ... and a route by the later of itself and the Gateway.
		"edge route.example.com " + t0.Format(time.RFC3339Nano),
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("ledger lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "apps ") {
		t.Errorf("a route claim is recorded for the route's namespace:\n%s", body)
	}
}

func TestOwnership_AnOldGatewayEditedToNameAHostDoesNotTakeIt(t *testing.T) {
	gw := gateway("edge", "web", httpListener("http", 80, "other.example.com"))
	gw.CreationTimestamp = metav1.NewTime(t0.Add(-24 * time.Hour))
	g := gatewayLedgerRig(t, &gw, ownerIngress("shop", "keep", ownedHost, "/keep", t0))
	g.mustReconcile()

	g.setHostname(&gw, ownedHost)
	g.mustReconcile()
	if out := g.routes(); !strings.Contains(out, "/keep") {
		t.Errorf("an old Gateway edited to name the host took it from the Ingress:\n%s", out)
	}
	if g.listenerAccepted("edge", "web", "http") {
		t.Error("the listener for a host held by an older claim is accepted")
	}
}

func TestOwnership_AnOldRouteEditedToNameAHostDoesNotTakeIt(t *testing.T) {
	gw := gateway("edge", "web", httpListener("http", 80, ""))
	gw.CreationTimestamp = metav1.NewTime(t0.Add(-24 * time.Hour))
	rt := httpRoute("shop", []string{"other.example.com"}, to("app"))
	rt.CreationTimestamp = metav1.NewTime(t0.Add(-24 * time.Hour))
	g := gatewayLedgerRig(t, &gw, &rt, ownerIngress("shop", "keep", ownedHost, "/keep", t0))
	g.mustReconcile()

	if err := g.r.Get(context.Background(), client.ObjectKeyFromObject(&rt), &rt); err != nil {
		t.Fatal(err)
	}
	rt.Spec.Hostnames = []gwv1.Hostname{ownedHost}
	if err := g.r.Update(context.Background(), &rt); err != nil {
		t.Fatal(err)
	}
	g.mustReconcile()
	if out := g.routes(); !strings.Contains(out, "/keep") {
		t.Errorf("an old route edited to name the host took it from the Ingress:\n%s", out)
	}
}
