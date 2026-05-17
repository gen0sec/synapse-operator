package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

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
func (r *IngressReconciler) renderGateways(ctx context.Context, hosts map[string]map[string]string, acmeBackend *string) int {
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
		if r.ensureCond(&gc.Status.Conditions, gc.Generation, "Accepted", "Accepted", "synapse-operator") {
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
		changed := r.ensureCond(&gw.Status.Conditions, gw.Generation, "Accepted", "Accepted", "synapse-operator")
		changed = r.ensureCond(&gw.Status.Conditions, gw.Generation, "Programmed", "Programmed", "synapse-operator") || changed
		if changed {
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
		hostnames := rt.Spec.Hostnames
		for _, rule := range rt.Spec.Rules {
			for _, br := range rule.BackendRefs {
				backend, ok := r.gwBackend(rt.Namespace, br)
				if !ok {
					continue
				}
				paths := rulePaths(rule)
				for _, h := range hostnames {
					host := string(h)
					for _, path := range paths {
						if strings.HasPrefix(path, acmeChallengePrefix) {
							*acmeBackend = backend
							continue
						}
						if hosts[host] == nil {
							hosts[host] = map[string]string{}
						}
						hosts[host][path] = backend
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

func rulePaths(rule gwv1.HTTPRouteRule) []string {
	var out []string
	for _, m := range rule.Matches {
		if m.Path != nil && m.Path.Value != nil && *m.Path.Value != "" {
			out = append(out, *m.Path.Value)
		}
	}
	if len(out) == 0 {
		out = []string{"/"}
	}
	return out
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
// gatewayHTTPRoute solver blocks until the route is Accepted.
func (r *IngressReconciler) acceptRoute(ctx context.Context, rt *gwv1.HTTPRoute, parent *gwv1.ParentReference) {
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
