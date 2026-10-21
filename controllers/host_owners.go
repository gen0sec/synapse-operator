package controllers

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
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
	// spelled is the host of the claim that won each key, as it was written.
	spelled map[string]string
	// named are, per namespace, the hosts its own objects name: those of
	// its Ingresses, listeners and routes. A route of another namespace that
	// a Gateway admits claims as the Gateway's namespace, and is no object
	// of it. A name is recorded whether or not its claim won, so that it
	// fails closed.
	named map[string]map[string]bool
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
	o := &hostOwners{byKey: map[string]string{}, spelled: map[string]string{}, named: map[string]map[string]bool{}}
	for _, c := range sorted {
		key := hostKey(c.host)
		if key == "" {
			continue
		}
		if rt, ok := c.who.(*gwv1.HTTPRoute); !ok || rt.Namespace == c.ns {
			if o.named[c.ns] == nil {
				o.named[c.ns] = map[string]bool{}
			}
			o.named[c.ns][key] = true
		}
		if _, held := o.holder(key); !held {
			o.byKey[key] = c.ns
			o.spelled[key] = c.host
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

// namesMoreSpecifically reports whether ns, through its own objects, names
// host more narrowly than the listener hostname through which a route
// reaches it: the host itself, or a narrower wildcard that covers it.
func (o *hostOwners) namesMoreSpecifically(ns, host, listener string) bool {
	if o == nil {
		return false
	}
	key := sameOrCovering(hostKey(host), o.named[ns])
	return key != "" && hostSpecificity(key) > hostSpecificity(hostKey(listener))
}

// hostSpecificity orders hosts by how few names they cover: no host and `*`
// cover every name, a wildcard the more the shorter its suffix, a name one.
func hostSpecificity(key string) int {
	suffix, wild := strings.CutPrefix(key, "*")
	switch {
	case key == "" || wild && suffix == "":
		return 0
	case wild:
		return len(suffix)
	}
	return math.MaxInt
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

// holdOwnedHosts writes an entry with no paths for each held name that the
// file has no entry for and a wildcard of another namespace covers. Synapse
// matches the exact host first and answers 404 for it, where without the
// entry the wildcard would serve a name its namespace does not hold. A
// certificate bound to the name goes with the entry.
func (m *renderModel) holdOwnedHosts(logger logr.Logger) {
	if m.owners == nil {
		return
	}
	written := map[string]struct{}{}
	for h := range m.hosts {
		written[h] = struct{}{}
	}
	for h := range m.passthroughHosts {
		written[h] = struct{}{}
	}
	for h := range m.solverRoutes() {
		written[h] = struct{}{}
	}
	writtenKeys := map[string]struct{}{}
	for h := range written {
		writtenKeys[hostKey(h)] = struct{}{}
	}
	seen := map[string]bool{}
	for key := range m.owners.byKey {
		seen[key] = true
	}
	for _, names := range m.owners.named {
		for key := range names {
			seen[key] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ns, _ := m.owners.holder(key)
		if _, ok := writtenKeys[key]; ok {
			continue
		}
		w := sameOrCovering(key, written)
		if w == "" {
			continue
		}
		owner, _ := m.owners.holder(w)
		// The wildcard's own namespace holds a name it also names, when
		// only routes of other namespaces brought the wildcard in.
		if owner == ns && !(m.foreignWild[hostKey(w)] && m.owners.named[owner][key]) {
			continue
		}
		spelling := m.owners.spelled[key]
		if spelling == "" {
			spelling = key
		}
		m.hosts[spelling] = map[string]*routeCfg{}
		for h, stem := range m.hostCert {
			if hostKey(h) == key {
				m.hostCert[spelling] = stem
				break
			}
		}
		logger.Info("host held without routes: a wildcard of another namespace covers it",
			"host", spelling, "owner", ns, "wildcard", w)
	}
}
