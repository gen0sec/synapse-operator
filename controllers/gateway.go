package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ControllerName is shared by the IngressClass controller and the
// GatewayClass controllerName. Only objects bound to this value are
// programmed.
const ControllerName = "gen0sec.com/synapse"

// renderGateways merges HTTPRoutes (attached to Gateways of a
// GatewayClass we control) into the same host/path map + acmeBackend
// the Ingress path uses, and sets the GatewayClass/Gateway/HTTPRoute
// status conditions cert-manager's gatewayHTTPRoute solver waits on.
// Tolerant of the Gateway API CRDs being absent (returns silently).
//
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gatewayclasses;gateways;httproutes,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gatewayclasses/status;gateways/status;httproutes/status,verbs=get;update;patch
func (r *IngressReconciler) renderGateways(ctx context.Context, m *renderModel) int {
	logger := ctrl.LoggerFrom(ctx).WithName("gateway")

	// GatewayClasses we control → mark Accepted.
	var gcList gwv1.GatewayClassList
	if err := r.List(ctx, &gcList); err != nil {
		// CRDs not installed / no permission → Gateway API disabled.
		return 0
	}
	ours := map[string]bool{} // gatewayclass name -> controlled by us
	for i := range gcList.Items {
		gc := &gcList.Items[i]
		if string(gc.Spec.ControllerName) != ControllerName {
			continue
		}
		ours[gc.Name] = true
		if r.ensureCond(&gc.Status.Conditions, gc.Generation, "Accepted", "Accepted", "synapse-operator") && r.leader() {
			_ = r.Status().Update(ctx, gc)
		}
	}
	if len(ours) == 0 {
		return 0
	}

	// Gateways bound to our classes → Accepted + Programmed.
	var gwList gwv1.GatewayList
	if err := r.List(ctx, &gwList); err != nil {
		return 0
	}
	ourGW := map[types.NamespacedName]bool{}
	for i := range gwList.Items {
		gw := &gwList.Items[i]
		if !ours[string(gw.Spec.GatewayClassName)] {
			continue
		}
		ourGW[types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}] = true
		// listener certificateRefs (Terminate) → project Secrets.
		for li := range gw.Spec.Listeners {
			l := &gw.Spec.Listeners[li]
			if l.TLS == nil {
				continue
			}
			if l.TLS.Mode != nil && *l.TLS.Mode != gwv1.TLSModeTerminate {
				continue
			}
			host := ""
			if l.Hostname != nil {
				host = string(*l.Hostname)
			}
			for _, cr := range l.TLS.CertificateRefs {
				if cr.Kind != nil && *cr.Kind != "Secret" {
					continue
				}
				ns := gw.Namespace
				if cr.Namespace != nil {
					ns = string(*cr.Namespace)
				}
				if stem, bound := certStem(host, ns, string(cr.Name)); bound {
					m.addCert(host, stem, ns, string(cr.Name))
				} else {
					m.addCert("", stem, ns, string(cr.Name))
				}
			}
		}
		changed := r.ensureCond(&gw.Status.Conditions, gw.Generation, "Accepted", "Accepted", "synapse-operator")
		changed = r.ensureCond(&gw.Status.Conditions, gw.Generation, "Programmed", "Programmed", "synapse-operator") || changed
		if changed && r.leader() {
			_ = r.Status().Update(ctx, gw)
		}
	}
	if len(ourGW) == 0 {
		return 0
	}

	// HTTPRoutes attached to our Gateways → program + accept.
	var rtList gwv1.HTTPRouteList
	if err := r.List(ctx, &rtList); err != nil {
		return 0
	}
	// Deterministic order so first-writer-wins is reproducible across
	// reconciles regardless of informer ordering.
	sort.Slice(rtList.Items, func(i, j int) bool {
		a, b := &rtList.Items[i], &rtList.Items[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	matched := 0
	for i := range rtList.Items {
		rt := &rtList.Items[i]
		var boundParent *gwv1.ParentReference
		for j := range rt.Spec.ParentRefs {
			p := &rt.Spec.ParentRefs[j]
			if p.Kind != nil && *p.Kind != "Gateway" {
				continue
			}
			ns := rt.Namespace
			if p.Namespace != nil {
				ns = string(*p.Namespace)
			}
			if ourGW[types.NamespacedName{Namespace: ns, Name: string(p.Name)}] {
				boundParent = p
				break
			}
		}
		if boundParent == nil {
			continue
		}
		matched++
		a := parseAnnotations(rt.Annotations)
		if a.sticky {
			m.sticky = true
		}
		hostnames := rt.Spec.Hostnames
		for _, rule := range rt.Spec.Rules {
			servers := r.ruleBackends(ctx, rt, rule)
			if len(servers) == 0 {
				continue
			}
			req, resp := headerFilters(rule.Filters)
			paths, warns := rulePaths(rule)
			for _, w := range warns {
				logger.Info("HTTPRoute match feature not representable in synapse v1 (best-effort)",
					"httproute", rt.Namespace+"/"+rt.Name, "detail", w)
				mUnsupportedMatch.Inc()
				r.emit(rt, corev1.EventTypeWarning, "UnsupportedMatch", "%s", w)
			}
			for _, path := range paths {
				for _, h := range hostnames {
					host := string(h)
					if strings.HasPrefix(path, acmeChallengePrefix) {
						if m.acme == "" {
							m.acme = servers[0].addr
						}
						continue
					}
					if !m.addRoute(host, path, servers, a, req, resp) {
						logger.Info("route conflict ignored (first-writer-wins; Ingress/earlier source kept)",
							"host", host, "path", path, "httproute", rt.Namespace+"/"+rt.Name)
						mRouteConflicts.Inc()
						r.emit(rt, corev1.EventTypeWarning, "RouteConflict",
							"host %s path %s already programmed by an earlier source (first-writer-wins); this rule is ignored", host, path)
					}
				}
			}
		}
		r.acceptRoute(ctx, rt, boundParent)
	}
	if matched > 0 {
		logger.Info("programmed HTTPRoutes", "count", matched)
	}
	return matched
}

// headerFilters maps HTTPRoute Request/ResponseHeaderModifier filters
// to synapse request_headers/response_headers injection lines
// ("Name: value"). Header `remove` and `URLRewrite`/`RequestMirror`
// have no equivalent in synapse's v1 per-path schema and are left
// unmodified (not silently faked).
func headerFilters(filters []gwv1.HTTPRouteFilter) (req, resp []string) {
	conv := func(f *gwv1.HTTPHeaderFilter) []string {
		if f == nil {
			return nil
		}
		var out []string
		for _, h := range f.Set {
			out = append(out, fmt.Sprintf("%s: %s", h.Name, h.Value))
		}
		for _, h := range f.Add {
			out = append(out, fmt.Sprintf("%s: %s", h.Name, h.Value))
		}
		return out
	}
	for i := range filters {
		switch filters[i].Type {
		case gwv1.HTTPRouteFilterRequestHeaderModifier:
			req = append(req, conv(filters[i].RequestHeaderModifier)...)
		case gwv1.HTTPRouteFilterResponseHeaderModifier:
			resp = append(resp, conv(filters[i].ResponseHeaderModifier)...)
		}
	}
	return req, resp
}

// ruleBackends resolves a rule's backendRefs into a synapse server
// pool, honoring Gateway API weight semantics:
//
//   - If NO backendRef sets a weight, the pool is unweighted (synapse
//     equal round-robin; rendered as bare "addr" strings).
//   - If ANY backendRef sets a weight, an unset weight defaults to 1
//     and weight 0 means "receive no traffic" — that backend is
//     EXCLUDED (Gateway API conformance; previously a 0-weight backend
//     was incorrectly treated as equal-weight).
func (r *IngressReconciler) ruleBackends(ctx context.Context, rt *gwv1.HTTPRoute, rule gwv1.HTTPRouteRule) []backend {
	weighted := false
	for i := range rule.BackendRefs {
		if rule.BackendRefs[i].Weight != nil {
			weighted = true
			break
		}
	}
	var servers []backend
	for _, br := range rule.BackendRefs {
		addr, ok := r.gwBackend(rt.Namespace, br)
		if !ok {
			mBackendUnresolved.Inc()
			r.emit(rt, corev1.EventTypeWarning, "BackendUnresolved",
				"backendRef %q could not be resolved (non-Service kind or missing port)", br.Name)
			continue
		}
		if !weighted {
			servers = append(servers, backend{addr: addr})
			continue
		}
		w := int32(1)
		if br.Weight != nil {
			w = *br.Weight
		}
		if w <= 0 {
			continue // weight 0 ⇒ no traffic (Gateway API)
		}
		servers = append(servers, backend{addr: addr, weight: uint32(w)})
	}
	return servers
}

// rulePaths extracts the path keys for a rule and reports any match
// features synapse's host+prefix v1 model cannot represent, so the
// caller warns instead of silently mis-routing:
//
//	PathPrefix           used as-is
//	Exact                used as a prefix (best-effort) + warning
//	RegularExpression    dropped + warning (no regex path support)
//	Headers/Method/Query path still used, constraint dropped + warning
//
// A rule with no matches means "match all" → ["/"]. A match with no
// Path defaults to PathPrefix "/" (Gateway API default).
func rulePaths(rule gwv1.HTTPRouteRule) (paths []string, warnings []string) {
	if len(rule.Matches) == 0 {
		return []string{"/"}, nil
	}
	for _, mt := range rule.Matches {
		if len(mt.Headers) > 0 || mt.Method != nil || len(mt.QueryParams) > 0 {
			warnings = append(warnings,
				"header/method/queryParam match conditions are ignored (synapse v1 routes on host+path only)")
		}
		pv := "/"
		pt := gwv1.PathMatchPathPrefix
		if mt.Path != nil {
			if mt.Path.Type != nil {
				pt = *mt.Path.Type
			}
			if mt.Path.Value != nil && *mt.Path.Value != "" {
				pv = *mt.Path.Value
			}
		}
		switch pt {
		case gwv1.PathMatchRegularExpression:
			warnings = append(warnings,
				fmt.Sprintf("RegularExpression path match %q dropped (synapse v1 has no regex path support)", pv))
			continue
		case gwv1.PathMatchExact:
			warnings = append(warnings,
				fmt.Sprintf("Exact path match %q is approximated as a prefix (synapse v1 matches longest-prefix)", pv))
		}
		paths = append(paths, pv)
	}
	return paths, warnings
}

func (r *IngressReconciler) gwBackend(routeNS string, br gwv1.HTTPBackendRef) (string, bool) {
	b := br.BackendRef.BackendObjectReference
	if b.Kind != nil && *b.Kind != "Service" {
		return "", false
	}
	ns := routeNS
	if b.Namespace != nil {
		ns = string(*b.Namespace)
	}
	if b.Port == nil {
		return "", false
	}
	cd := r.ClusterDomain
	if cd == "" {
		cd = "cluster.local"
	}
	return fmt.Sprintf("%s.%s.svc.%s:%d", b.Name, ns, cd, int32(*b.Port)), true
}

// ---- status helpers (enough for cert-manager to proceed) ----------

func (r *IngressReconciler) ensureCond(conds *[]metav1.Condition, gen int64, ctype, reason, msg string) bool {
	for i := range *conds {
		c := &(*conds)[i]
		if c.Type == ctype && c.Status == metav1.ConditionTrue && c.ObservedGeneration == gen {
			return false
		}
	}
	meta := metav1.Condition{
		Type:               ctype,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	replaced := false
	for i := range *conds {
		if (*conds)[i].Type == ctype {
			(*conds)[i] = meta
			replaced = true
			break
		}
	}
	if !replaced {
		*conds = append(*conds, meta)
	}
	return true
}

// acceptRoute sets the HTTPRoute parent status (Accepted +
// ResolvedRefs True) under our controller name — cert-manager's
// gatewayHTTPRoute solver blocks until the route is Accepted. Shared
// status: leader-gated so >1 replica does not churn it.
func (r *IngressReconciler) acceptRoute(ctx context.Context, rt *gwv1.HTTPRoute, parent *gwv1.ParentReference) {
	if !r.leader() {
		return
	}
	now := metav1.NewTime(time.Now())
	mk := func(t, reason string) metav1.Condition {
		return metav1.Condition{Type: t, Status: metav1.ConditionTrue, Reason: reason,
			Message: "synapse-operator", ObservedGeneration: rt.Generation, LastTransitionTime: now}
	}
	ps := gwv1.RouteParentStatus{
		ParentRef:      *parent,
		ControllerName: gwv1.GatewayController(ControllerName),
		Conditions:     []metav1.Condition{mk("Accepted", "Accepted"), mk("ResolvedRefs", "ResolvedRefs")},
	}
	out := rt.Status.Parents[:0]
	for _, p := range rt.Status.Parents {
		if p.ControllerName != gwv1.GatewayController(ControllerName) {
			out = append(out, p)
		}
	}
	rt.Status.Parents = append(out, ps)
	_ = r.Status().Update(ctx, rt)
}

// GatewayClass GVK helper kept for SetupWithManager wiring.
func gwGatewayClass() *gwv1.GatewayClass { return &gwv1.GatewayClass{} }
func gwGateway() *gwv1.Gateway           { return &gwv1.Gateway{} }
func gwHTTPRoute() *gwv1.HTTPRoute       { return &gwv1.HTTPRoute{} }
