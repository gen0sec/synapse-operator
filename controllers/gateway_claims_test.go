package controllers

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func admitAll(*gwv1.Listener, bool) bool { return true }

// A route claims only through a listener that lets its namespace attach.
func TestGatewayClaims_ARouteTheListenerDoesNotAdmitClaimsNothing(t *testing.T) {
	w := newGwWorld()
	l := httpListener("http", 80, "")
	l.AllowedRoutes = &gwv1.AllowedRoutes{Namespaces: &gwv1.RouteNamespaces{From: ptr(gwv1.NamespacesFromSame)}}
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", l)}
	stranger := httpRoute("squat", []string{"shop.example.com"}, to("app"))
	w.in.routes = []gwv1.HTTPRoute{stranger}
	if got := w.in.claims(); len(got) != 0 {
		t.Errorf("claims = %v, want none from a namespace the listener does not admit", got)
	}
	own := httpRoute("own", []string{"shop.example.com"}, to("app"))
	own.Namespace = "edge"
	w.in.routes = []gwv1.HTTPRoute{own}
	if got := w.in.claims(); len(got) != 1 {
		t.Errorf("claims = %v, want one from the Gateway's own namespace", got)
	}
}

func TestGatewayClaims_AListenerIsAgedByItsGateway(t *testing.T) {
	gw := gateway("edge", "web", httpListener("a", 80, "a.example.com"), httpListener("b", 80, ""), httpListener("c", 80, "*.example.org"))
	gw.CreationTimestamp = metav1.NewTime(t0)
	got := listenerClaims(&gw)
	if len(got) != 2 {
		t.Fatalf("claims = %v, want the two listeners that name a host", got)
	}
	for _, c := range got {
		if c.ns != "edge" || !c.created.Equal(t0) || c.who != &gw {
			t.Errorf("claim %+v: want ns edge, created %v, made by the Gateway", c, t0)
		}
	}
	if got[0].host != "a.example.com" || got[1].host != "*.example.org" {
		t.Errorf("hosts = %q, %q", got[0].host, got[1].host)
	}
}

func TestGatewayClaims_ARouteClaimsAsTheGatewayAgedByTheLaterOfTheTwo(t *testing.T) {
	gw := gateway("edge", "web", httpListener("http", 80, ""))
	rt := httpRoute("shop", []string{"shop.example.com"}, to("app"))
	for _, c := range []struct {
		gateway, route, want time.Time
	}{
		{t0, t0.Add(time.Hour), t0.Add(time.Hour)},
		{t0.Add(time.Hour), t0, t0.Add(time.Hour)},
	} {
		gw.CreationTimestamp, rt.CreationTimestamp = metav1.NewTime(c.gateway), metav1.NewTime(c.route)
		got := routeClaims(&rt, &gw, admitAll)
		if len(got) != 1 {
			t.Fatalf("claims = %v, want one", got)
		}
		if got[0].ns != "edge" || got[0].host != "shop.example.com" || !got[0].created.Equal(c.want) || got[0].who != &rt {
			t.Errorf("claim %+v, want ns edge, created %v, made by the route", got[0], c.want)
		}
	}
}

func TestGatewayClaims_OneClaimPerParentRefToTheGateway(t *testing.T) {
	gw := gateway("edge", "web", httpListener("http", 80, ""), httpListener("alt", 8080, ""))
	rt := httpRoute("shop", []string{"shop.example.com"}, to("app"))
	rt.Spec.ParentRefs = []gwv1.ParentReference{
		{Namespace: ptr(gwv1.Namespace("edge")), Name: "web", SectionName: ptr(gwv1.SectionName("http"))},
		{Namespace: ptr(gwv1.Namespace("edge")), Name: "web", SectionName: ptr(gwv1.SectionName("alt"))},
		{Namespace: ptr(gwv1.Namespace("edge")), Name: "elsewhere"},
		{Namespace: ptr(gwv1.Namespace("edge")), Name: "web", Kind: ptr(gwv1.Kind("Service"))},
	}
	if got := routeClaims(&rt, &gw, admitAll); len(got) != 2 {
		t.Errorf("claims = %v, want one for each of the two parentRefs to the Gateway", got)
	}
	// A listener's host narrows what the route claims.
	gw = gateway("edge", "web", httpListener("http", 80, "*.example.org"))
	rt.Spec.ParentRefs = rt.Spec.ParentRefs[:1]
	rt.Spec.ParentRefs[0].SectionName = nil
	if got := routeClaims(&rt, &gw, admitAll); len(got) != 0 {
		t.Errorf("claims = %v, want none for a host no listener serves", got)
	}
}

// A Gateway whose class no proxy binds is not this proxy's: it claims nothing.
func TestGatewayClaims_AGatewayOfAnUnboundClassClaimsNothing(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", []string{"shop.example.com"}, to("app"))}
	if got := w.in.claims(); len(got) != 2 {
		t.Fatalf("claims = %v, want the listener's and the route's", got)
	}
	w.in.gateways[0].Spec.GatewayClassName = "elsewhere"
	if got := w.in.claims(); len(got) != 0 {
		t.Errorf("claims = %v, want none from a Gateway of a class the proxy does not bind", got)
	}
}

// A parentRef with a port reaches the listeners on that port, and claims
// through those alone.
func TestGatewayClaims_AParentRefPortNarrowsTheClaim(t *testing.T) {
	gw := gateway("edge", "web", httpListener("a", 80, "a.example.com"), httpListener("b", 8080, "b.example.com"))
	rt := httpRoute("shop", nil, to("app"))
	rt.Spec.ParentRefs[0].Port = ptr(gwv1.PortNumber(8080))
	got := routeClaims(&rt, &gw, admitAll)
	if len(got) != 1 || got[0].host != "b.example.com" {
		t.Errorf("claims = %v, want b.example.com alone", got)
	}
}
