package controllers

import (
	"fmt"
	"sort"
	"strings"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A host belongs to the namespace whose object claimed it first, within one
// proxy. A claim is made by an object's spec and metadata alone, so that
// whether a Service exists, or the object is otherwise served, cannot move
// a host from one namespace to another.

// hostClaim is a claim of ns, made by who, to host.
type hostClaim struct {
	host    string
	ns      string
	created time.Time
	who     client.Object
}

// hostKey is the spelling of a host that claims are compared in. The file
// keeps the host as it was written.
func hostKey(host string) string {
	return strings.ToLower(strings.TrimRight(host, "."))
}

// hostOwners tells which namespace holds each claimed host. The zero value
// of a nil pointer holds nothing, and allows everything.
type hostOwners struct {
	byKey map[string]string
}

// resolveHostOwners takes the claims oldest first, by creation time, then
// namespace, name and kind, so that the result does not depend on the order
// they come in. A claim is refused when an older claim of another namespace
// holds the same host, or a wildcard that covers it. A wildcard does not
// take a host an older claim holds.
func resolveHostOwners(claims []hostClaim) *hostOwners {
	sorted := append([]hostClaim(nil), claims...)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		switch {
		case !a.created.Equal(b.created):
			return a.created.Before(b.created)
		case a.ns != b.ns:
			return a.ns < b.ns
		case a.who.GetName() != b.who.GetName():
			return a.who.GetName() < b.who.GetName()
		}
		return fmt.Sprintf("%T", a.who) < fmt.Sprintf("%T", b.who)
	})
	o := &hostOwners{byKey: map[string]string{}}
	for _, c := range sorted {
		key := hostKey(c.host)
		if key == "" {
			continue
		}
		if _, held := o.holder(key); !held {
			o.byKey[key] = c.ns
		}
	}
	return o
}

// holder is the namespace of the claim that holds host: the host itself, or
// else the wildcard that covers it.
func (o *hostOwners) holder(host string) (string, bool) {
	if o == nil {
		return "", false
	}
	key := sameOrCovering(hostKey(host), o.byKey)
	if key == "" {
		return "", false
	}
	return o.byKey[key], true
}

// owner is the namespace that holds host.
func (o *hostOwners) owner(host string) (string, bool) {
	return o.holder(host)
}

// allows reports whether ns may use host: it holds it, or nobody does.
func (o *hostOwners) allows(host, ns string) bool {
	owner, held := o.holder(host)
	return !held || owner == ns
}

// ingressClaims are the hosts an Ingress names: those of its rules, its
// accepted server-aliases and spec.tls. A name Synapse would not take as a
// host is no claim.
func ingressClaims(ing *networkingv1.Ingress, a annSettings) []hostClaim {
	var claims []hostClaim
	add := func(host string) {
		if host == "" || hostKeyProblem(host) != "" {
			return
		}
		claims = append(claims, hostClaim{host: host, ns: ing.Namespace, created: ing.CreationTimestamp.Time, who: ing})
	}
	for _, rule := range ing.Spec.Rules {
		add(rule.Host)
	}
	for _, alias := range a.serverAliases {
		add(alias)
	}
	for _, t := range ing.Spec.TLS {
		for _, h := range t.Hosts {
			add(h)
		}
	}
	return claims
}
