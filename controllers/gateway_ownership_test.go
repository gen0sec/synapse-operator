package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// own makes the claims of the world's Gateways and routes, and the extra ones,
// the owners of its model.
func (w *gwWorld) own(extra ...hostClaim) {
	w.model.owners = resolveHostOwners(append(w.in.claims(), extra...))
}

func (w *gwWorld) gatewayCreated(at time.Time) {
	for i := range w.in.gateways {
		w.in.gateways[i].CreationTimestamp = metav1.NewTime(at)
	}
}

func TestGatewayOwnership_AListenerOwnedElsewhereIsNotAccepted(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.gatewayCreated(t0.Add(time.Hour))
	w.own(claim("shop.example.com", "shop", "ing", t0))
	m, st := w.translate()
	wantListener(t, st, "http", "Accepted", metav1.ConditionFalse, "HostnameConflict")
	wantListener(t, st, "http", "Programmed", metav1.ConditionFalse, "Invalid")
	gw := st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	for _, c := range gw.Conditions {
		if c.Type == "Programmed" && c.Status != metav1.ConditionFalse {
			t.Errorf("the Gateway is programmed with no listener to serve: %+v", c)
		}
	}
	if len(m.hosts) != 0 {
		t.Errorf("hosts = %v, want none", sortedKeys(m.hosts))
	}

	// The same listener, older than the claim, holds the host.
	w = newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.gatewayCreated(t0)
	w.own(claim("shop.example.com", "shop", "ing", t0.Add(time.Hour)))
	_, st = w.translate()
	wantListener(t, st, "http", "Accepted", metav1.ConditionTrue, "Accepted")
}

func TestGatewayOwnership_ARouteOwnedElsewhereIsNotAccepted(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, ""))}
	w.gatewayCreated(t0.Add(time.Hour))
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", []string{"shop.example.com"}, to("app"))}
	w.own(claim("shop.example.com", "shop", "ing", t0))
	m, st := w.translate()
	wantRoute(t, st, "shop", "Accepted", metav1.ConditionFalse, "HostnameConflict")
	if _, ok := m.hosts["shop.example.com"]; ok {
		t.Errorf("a host that belongs to another namespace is routed: %v", sortedKeys(m.hosts))
	}
	status := st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	if got := status.Listeners[0].AttachedRoutes; got != 0 {
		t.Errorf("%d routes attached to the listener, want 0", got)
	}
}

func TestGatewayOwnership_ARouteOwnedElsewhereInPartKeepsTheRest(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, ""))}
	w.gatewayCreated(t0.Add(time.Hour))
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", []string{"shop.example.com", "new.example.com"}, to("app"))}
	w.own(claim("shop.example.com", "shop", "ing", t0))
	m, st := w.translate()
	wantRoute(t, st, "shop", "Accepted", metav1.ConditionTrue, "Accepted")
	wantRoute(t, st, "shop", "PartiallyInvalid", metav1.ConditionTrue, "UnsupportedValue")
	if _, ok := m.hosts["shop.example.com"]; ok {
		t.Errorf("a host that belongs to another namespace is routed: %v", sortedKeys(m.hosts))
	}
	if got := server(t, m, "new.example.com", "GET", "/"); got != "10.0.0.1:80" {
		t.Errorf("the host that was free goes to %q", got)
	}
}

// Routes of two namespaces on one Gateway claim as the Gateway's namespace,
// so neither is refused for the other. An Ingress that comes later is.
func TestGatewayOwnership_RoutesOfTwoNamespacesKeepWorkingAndALaterIngressIsRefused(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.gatewayCreated(t0)
	a := httpRoute("a", []string{"shop.example.com"}, to("app", prefix("/a")))
	b := httpRoute("b", []string{"shop.example.com"}, to("app2", prefix("/b")))
	b.Namespace = "other"
	a.CreationTimestamp, b.CreationTimestamp = metav1.NewTime(t0.Add(time.Minute)), metav1.NewTime(t0.Add(2*time.Minute))
	w.service("other", "app2", "10.0.1.2")
	b.Spec.Rules[0].BackendRefs[0].Name = "app2"
	w.in.routes = []gwv1.HTTPRoute{a, b}
	late := ownerIngress("late", "shop", "shop.example.com", "/", t0.Add(time.Hour))
	w.own(ingressClaims(late, annSettings{})...)
	m, st := w.translate()
	wantRoute(t, st, "a", "Accepted", metav1.ConditionTrue, "Accepted")
	if got := server(t, m, "shop.example.com", "GET", "/a"); got != "10.0.0.1:80" {
		t.Errorf("/a goes to %q", got)
	}
	if parents := st.routes[types.NamespacedName{Namespace: "other", Name: "b"}]; len(parents) != 1 || parents[0].Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("the route of the second namespace is not accepted: %+v", parents)
	}
	if w.model.owners.allows("shop.example.com", "late") {
		t.Error("the later Ingress may use the host")
	}
	if !w.model.owners.allows("shop.example.com", "edge") {
		t.Error("the Gateway's namespace lost its host")
	}
}

func TestGatewayOwnership_AnOlderGatewayBeatsANewerIngress(t *testing.T) {
	newGateway := func() *gwWorld {
		w := newGwWorld()
		w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
		w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, to("app"))}
		return w
	}
	render := func(w *gwWorld, ing client.Object) (string, gatewayStatuses) {
		var st gatewayStatuses
		r := &IngressReconciler{IngressClassName: "synapse", HostOwnership: true,
			ExtraClaims: w.in.claims,
			RenderExtra: func(_ context.Context, m *renderModel) error {
				st = translateGateways(w.in, m)
				return nil
			}}
		return renderToFile(t, r, ing), st
	}

	w := newGateway()
	w.gatewayCreated(t0)
	newer := ingressIn("late", "shop", "shop.example.com", "/")
	newer.CreationTimestamp = metav1.NewTime(t0.Add(time.Hour))
	out, st := render(w, newer)
	if !strings.Contains(out, "10.0.0.1:80") || strings.Contains(out, "late.svc") {
		t.Errorf("the older Gateway does not hold its host:\n%s", out)
	}
	wantRoute(t, st, "shop", "Accepted", metav1.ConditionTrue, "Accepted")

	// An Ingress older than the Gateway keeps the host, and the Gateway's
	// listener and route are refused.
	w = newGateway()
	w.gatewayCreated(t0.Add(time.Hour))
	ing := ingressIn("late", "shop", "shop.example.com", "/")
	ing.CreationTimestamp = metav1.NewTime(t0)
	out, st = render(w, ing)
	if !strings.Contains(out, "late.svc") || strings.Contains(out, "10.0.0.1:80") {
		t.Errorf("the older Ingress does not hold its host:\n%s", out)
	}
	wantListener(t, st, "http", "Accepted", metav1.ConditionFalse, "HostnameConflict")
}

// An Ingress of the Gateway's own namespace is no foreign claim, and still
// beats the routes.
func TestGatewayOwnership_AnIngressOfTheGatewaysNamespaceStillBeatsItsRoutes(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, ""))}
	w.gatewayCreated(t0)
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", []string{"shop.example.com"}, to("app"))}
	w.model.addRoute("shop.example.com", "/", []backend{{addr: "ingress:80"}}, annSettings{}, nil, nil)
	w.own(claim("shop.example.com", "edge", "ing", t0.Add(time.Hour)))
	m, st := w.translate()
	if got := server(t, m, "shop.example.com", "GET", "/"); got != "ingress:80" {
		t.Errorf("the Ingress's host goes to %q", got)
	}
	wantRoute(t, st, "shop", "Accepted", metav1.ConditionFalse, "HostnameConflict")
}
