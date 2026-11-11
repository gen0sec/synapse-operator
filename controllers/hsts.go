package controllers

import (
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
)

// Synapse refuses a file whose `hsts` has `preload` without a max age of a
// year and include_subdomains, so the operator never writes one.
const hstsPreloadMinAge = 31536000

// hstsPolicy is the `hsts` of a host or of the proxy: `off`, or a policy.
type hstsPolicy struct {
	off               bool
	maxAge            uint64
	includeSubdomains bool
	preload           bool
}

// hstsAnn is what an object's hsts annotations say. policy is nil when they
// say nothing usable. refused lists each annotation dropped, with why.
type hstsAnn struct {
	policy  *hstsPolicy
	refused []string
}

// DefaultHSTS is the proxy-wide `hsts`, from the spec of a SynapseProxy.
type DefaultHSTS struct {
	MaxAgeSeconds     int64
	IncludeSubdomains bool
	Preload           bool
}

// preloadProblem says why p cannot carry `preload`, or "" if it can.
func (p hstsPolicy) preloadProblem() string {
	switch {
	case p.maxAge < hstsPreloadMinAge:
		return fmt.Sprintf("preload needs a max age of at least %d (one year)", hstsPreloadMinAge)
	case !p.includeSubdomains:
		return "preload needs include-subdomains"
	}
	return ""
}

// parseHSTS reads the hsts, hsts-max-age, hsts-include-subdomains and
// hsts-preload annotations. A bad value is dropped alone. Without a usable
// max age there is no policy, so the other two flags have nothing to
// attach to and are dropped too.
func parseHSTS(ann map[string]string) hstsAnn {
	var out hstsAnn
	refuse := func(key, v, why string) {
		out.refused = append(out.refused, fmt.Sprintf("%s%s: %q (%s)", annPrefix, key, v, why))
	}
	flag := func(key string) (val, set, ok bool, raw string) {
		raw, set = ann[annPrefix+key]
		if !set {
			return false, false, false, ""
		}
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "true":
			return true, true, true, raw
		case "false":
			return false, true, true, raw
		}
		return false, true, false, raw
	}

	if v, ok := ann[annPrefix+"hsts"]; ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "off", "false":
			out.policy = &hstsPolicy{off: true}
			for _, k := range []string{"hsts-max-age", "hsts-include-subdomains", "hsts-preload"} {
				if v, ok := ann[annPrefix+k]; ok {
					refuse(k, v, "hsts is off")
				}
			}
			return out
		default:
			refuse("hsts", v, "only off is a value")
		}
	}

	ageRaw, hasAge := ann[annPrefix+"hsts-max-age"]
	var age uint64
	ageOK := false
	if hasAge {
		n, err := strconv.ParseUint(strings.TrimSpace(ageRaw), 10, 63)
		if err != nil {
			refuse("hsts-max-age", ageRaw, "not a number of seconds")
		} else {
			age, ageOK = n, true
		}
	}
	sub, subSet, subOK, subRaw := flag("hsts-include-subdomains")
	pre, preSet, preOK, preRaw := flag("hsts-preload")
	if !ageOK {
		if hasAge {
			// Already told: the max age is what the others hang on.
			return out
		}
		if subSet && sub {
			refuse("hsts-include-subdomains", subRaw, "needs hsts-max-age")
		}
		if preSet && pre {
			refuse("hsts-preload", preRaw, "needs hsts-max-age")
		}
		return out
	}
	p := &hstsPolicy{maxAge: age}
	if subSet && !subOK {
		refuse("hsts-include-subdomains", subRaw, "not true or false")
	}
	p.includeSubdomains = sub
	switch {
	case preSet && !preOK:
		refuse("hsts-preload", preRaw, "not true or false")
	case pre:
		if why := (hstsPolicy{maxAge: age, includeSubdomains: sub}).preloadProblem(); why != "" {
			refuse("hsts-preload", preRaw, why)
		} else {
			p.preload = true
		}
	}
	out.policy = p
	return out
}

// hstsPolicy turns the spec into a policy that Synapse takes, and says
// which part it dropped.
func (d *DefaultHSTS) policy() (p hstsPolicy, dropped string) {
	if d.MaxAgeSeconds < 1 {
		return hstsPolicy{}, "maxAgeSeconds must be at least 1"
	}
	p = hstsPolicy{maxAge: uint64(d.MaxAgeSeconds), includeSubdomains: d.IncludeSubdomains}
	if d.Preload {
		if why := p.preloadProblem(); why != "" {
			return p, why
		}
		p.preload = true
	}
	return p, ""
}

// hstsClaim is the policy a host has and the object it came from.
type hstsClaim struct {
	policy hstsPolicy
	from   string
}

// resolveDefaultHSTS gives the model the proxy-wide hsts.
func (r *IngressReconciler) resolveDefaultHSTS(m *renderModel) {
	if r.DefaultHSTS == nil {
		return
	}
	p, dropped := r.DefaultHSTS.policy()
	if dropped != "" {
		r.emit(r.EventsOn, corev1.EventTypeWarning, "InvalidHSTS", "spec.hsts: %s; it is not applied", dropped)
		if p.maxAge == 0 {
			return
		}
	}
	m.hsts = &p
}

// reportHSTS says which hsts annotations of obj were dropped.
func (r *IngressReconciler) reportHSTS(obj client.Object, a annSettings) {
	for _, refused := range a.hsts.refused {
		r.emit(obj, corev1.EventTypeWarning, "InvalidHSTS", "annotation %s is ignored", refused)
	}
}

// noteHostOwner records ns as the owner of host if no namespace has
// programmed a route on it yet. Call it when a route on host was added.
func (m *renderModel) noteHostOwner(host, ns string) {
	key := hostKey(host)
	if _, ok := m.hostOwner[key]; ok {
		return
	}
	if m.hostOwner == nil {
		m.hostOwner = map[string]string{}
	}
	m.hostOwner[key] = ns
}

// hostSettingOwner is the namespace that may set host-level settings on
// host: the owner that hostAllowed enforces under HostOwnership, else the
// namespace of the first object that programmed a route on it.
func (m *renderModel) hostSettingOwner(host string) (string, bool) {
	if owner, ok := m.owners.owner(host); ok {
		return owner, true
	}
	owner, ok := m.hostOwner[hostKey(host)]
	return owner, ok
}

// hostSettingAllowed says whether obj may set `what` on host. Only the
// owner namespace may; a refusal is told to obj once per host. claimNS is
// the namespace that obj's claim on host was made under when that is not its
// own: the Gateway's, for an HTTPRoute. Call it only after a route of obj on
// host was programmed.
func (r *IngressReconciler) hostSettingAllowed(m *renderModel, obj client.Object, claimNS, host, what string) bool {
	owner, ok := m.hostSettingOwner(host)
	if !ok || owner == obj.GetNamespace() || owner == claimNS {
		return true
	}
	r.hostSettingConflict(m, obj, host, what, "belongs to namespace "+owner+", which had it first")
	return false
}

// hostSettingConflict tells obj, once per host and setting, that its `what`
// for host is not applied.
func (r *IngressReconciler) hostSettingConflict(m *renderModel, obj client.Object, host, what, why string) {
	key := obj.GetNamespace() + "/" + obj.GetName() + "\x00" + hostKey(host) + "\x00" + what
	if m.settingSaid[key] {
		return
	}
	if m.settingSaid == nil {
		m.settingSaid = map[string]bool{}
	}
	m.settingSaid[key] = true
	r.emit(obj, corev1.EventTypeWarning, "HostSettingConflict",
		"host %s %s; the %s of this object do not apply to it", host, why, what)
}

// routeProgrammed is called when a route of obj on host was added. The first
// to do so owns the host's settings; obj's own are then claimed.
func (r *IngressReconciler) routeProgrammed(m *renderModel, obj client.Object, host string, a annSettings) {
	r.routeProgrammedUnder(m, obj, "", host, a)
}

// routeProgrammedUnder is routeProgrammed for an object whose claim on host
// is made under another namespace than its own, as an HTTPRoute's is under
// its Gateway's. claimNS is that namespace.
func (r *IngressReconciler) routeProgrammedUnder(m *renderModel, obj client.Object, claimNS, host string, a annSettings) {
	m.noteHostOwner(host, obj.GetNamespace())
	r.claimHSTS(m, obj, claimNS, host, a)
}

// claimHSTS gives host the hsts policy of obj, if obj's namespace owns the
// host. Within the owner namespace the first object to ask, in render order,
// keeps the host; a later one with another policy is told.
func (r *IngressReconciler) claimHSTS(m *renderModel, obj client.Object, claimNS, host string, a annSettings) {
	if a.hsts.policy == nil || !r.hostSettingAllowed(m, obj, claimNS, host, "hsts annotations") {
		return
	}
	from := obj.GetNamespace() + "/" + obj.GetName()
	have, ok := m.hostHSTS[host]
	if !ok {
		if m.hostHSTS == nil {
			m.hostHSTS = map[string]hstsClaim{}
		}
		m.hostHSTS[host] = hstsClaim{policy: *a.hsts.policy, from: from}
		return
	}
	if have.from == from || have.policy == *a.hsts.policy {
		return
	}
	r.hostSettingConflict(m, obj, host, "hsts annotations",
		"already has an HSTS setting from "+have.from+" (first-writer-wins)")
}

func writeHSTS(b *strings.Builder, indent string, p hstsPolicy) {
	if p.off {
		fmt.Fprintf(b, "%shsts: \"off\"\n", indent)
		return
	}
	fmt.Fprintf(b, "%shsts:\n%s  max_age: %d\n", indent, indent, p.maxAge)
	if p.includeSubdomains {
		fmt.Fprintf(b, "%s  include_subdomains: true\n", indent)
	}
	if p.preload {
		fmt.Fprintf(b, "%s  preload: true\n", indent)
	}
}
