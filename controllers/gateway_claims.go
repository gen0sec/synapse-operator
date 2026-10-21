package controllers

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// The host claims of the Gateway API. They come from spec and metadata
// alone, as an Ingress's do: whether a listener is usable, or a route is
// allowed by it, does not move a host from one namespace to another.

// listenerClaims are the hostnames of a Gateway's listeners, claimed for the
// Gateway's namespace and aged by the Gateway.
func listenerClaims(gw *gwv1.Gateway) []hostClaim {
	var claims []hostClaim
	for i := range gw.Spec.Listeners {
		if h := gw.Spec.Listeners[i].Hostname; h != nil {
			claims = appendClaim(claims, string(*h), gw.Namespace, gw.CreationTimestamp.Time, gw)
		}
	}
	return claims
}

// routeClaims are the hosts a route is served on through each of its
// parentRefs to gw. They are claimed for the Gateway's namespace, so that
// routes of several namespaces on one Gateway do not refuse each other, and
// aged by the later of the route and the Gateway: a route cannot be older
// in a namespace than the Gateway that lets it in.
func routeClaims(rt *gwv1.HTTPRoute, gw *gwv1.Gateway) []hostClaim {
	created := gw.CreationTimestamp.Time
	if rt.CreationTimestamp.Time.After(created) {
		created = rt.CreationTimestamp.Time
	}
	gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
	var claims []hostClaim
	for _, ref := range rt.Spec.ParentRefs {
		if key, ok := parentGateway(rt, ref); !ok || key != gwKey {
			continue
		}
		for i := range gw.Spec.Listeners {
			l := &gw.Spec.Listeners[i]
			if (ref.SectionName != nil && *ref.SectionName != l.Name) || (ref.Port != nil && *ref.Port != l.Port) {
				continue
			}
			listener := ""
			if l.Hostname != nil {
				listener = strings.ToLower(string(*l.Hostname))
			}
			if len(rt.Spec.Hostnames) == 0 {
				claims = appendClaim(claims, listener, gw.Namespace, created, rt)
			}
			for _, h := range rt.Spec.Hostnames {
				claims = appendClaim(claims, hostIntersection(listener, strings.ToLower(string(h))), gw.Namespace, created, rt)
			}
		}
	}
	return claims
}

// appendClaim adds a claim for host unless Synapse would not take it as one.
func appendClaim(claims []hostClaim, host, ns string, created time.Time, who client.Object) []hostClaim {
	if host == "" || hostKeyProblem(host) != "" {
		return claims
	}
	return append(claims, hostClaim{host: host, ns: ns, created: created, who: who})
}

// claims are those of every Gateway handed to the proxy and of the routes
// that name one.
func (in gatewayInputs) claims() []hostClaim {
	bound := boundGatewayClasses(in.classes, in.proxy)
	var claims []hostClaim
	for i := range in.gateways {
		gw := &in.gateways[i]
		if !bound[string(gw.Spec.GatewayClassName)] {
			continue
		}
		claims = append(claims, listenerClaims(gw)...)
		for j := range in.routes {
			claims = append(claims, routeClaims(&in.routes[j], gw)...)
		}
	}
	return claims
}
