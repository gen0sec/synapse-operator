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

// claimHSTS gives host the hsts policy of obj. The first object to ask,
// in render order, keeps the host; a later one with another policy is told.
func (r *IngressReconciler) claimHSTS(m *renderModel, obj client.Object, host string, a annSettings) {
	if a.hsts.policy == nil {
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
	r.emit(obj, corev1.EventTypeWarning, "HostSettingConflict",
		"host %s already has an HSTS setting from %s (first-writer-wins); the hsts annotations of this object do not apply to it", host, have.from)
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
