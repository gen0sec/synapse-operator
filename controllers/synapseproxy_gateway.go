package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// GatewayAPIServed reports whether the cluster has the Gateway API kinds a
// SynapseProxy's Gateways are read from. They are CRDs, and a cluster may
// well not have them; a controller that watches a kind that is not there
// does not start.
//
// Not being able to tell is an error, and not a no: asked while the API
// server is unwell, a no would leave every Gateway unserved until the
// operator is next restarted.
func GatewayAPIServed(mapper meta.RESTMapper) (bool, error) {
	for _, kind := range []struct{ kind, version string }{
		{"GatewayClass", "v1"}, {"Gateway", "v1"}, {"HTTPRoute", "v1"}, {"ReferenceGrant", "v1beta1"},
	} {
		_, err := mapper.RESTMapping(schema.GroupKind{Group: gwv1.GroupName, Kind: kind.kind}, kind.version)
		switch {
		case err == nil:
		case meta.IsNoMatchError(err):
			return false, nil
		default:
			return false, fmt.Errorf("look up %s: %w", kind.kind, err)
		}
	}
	return true, nil
}

// CheckGatewayAccess says why Gateways cannot be served as whoever c acts
// as, or returns nil. It lists one object of each kind the route controller
// watches for them, in namespace when that is not empty.
//
// Like CheckProxyAccess, it is for use before the controllers start: one
// that may not read a kind it watches never starts, and stops the manager
// when its cache does not fill. That would take the Ingresses down with the
// Gateways.
func CheckGatewayAccess(ctx context.Context, c client.Reader, namespace string) error {
	for _, list := range []client.ObjectList{&gwv1.GatewayList{}, &gwv1.HTTPRouteList{}, &gwv1beta1.ReferenceGrantList{}} {
		if err := c.List(ctx, list, client.InNamespace(namespace), client.Limit(1)); err != nil {
			return fmt.Errorf("list %T: %w", list, err)
		}
	}
	for _, list := range []client.ObjectList{&gwv1.GatewayClassList{}, &corev1.NamespaceList{}} {
		if err := c.List(ctx, list, client.Limit(1)); err != nil {
			return fmt.Errorf("list %T: %w", list, err)
		}
	}
	return nil
}

// gatewayLists lists everything of the kinds a Gateway translation reads,
// and every SynapseProxy.
func (r *SynapseRouteReconciler) gatewayLists(ctx context.Context) (gatewayInputs, error) {
	in := gatewayInputs{
		clusterDomain: r.ClusterDomain, namespaces: map[string]map[string]string{},
		live: map[types.NamespacedName]bool{}, scope: r.Namespace,
	}
	var (
		classes    gwv1.GatewayClassList
		gateways   gwv1.GatewayList
		routes     gwv1.HTTPRouteList
		grants     gwv1beta1.ReferenceGrantList
		namespaces corev1.NamespaceList
		proxies    synapsev1alpha1.SynapseProxyList
	)
	for _, list := range []client.ObjectList{&classes, &gateways, &routes, &grants, &namespaces, &proxies} {
		if err := r.List(ctx, list); err != nil {
			return in, fmt.Errorf("list %T: %w", list, err)
		}
	}
	in.classes, in.gateways, in.routes, in.grants = classes.Items, gateways.Items, routes.Items, grants.Items
	for i := range namespaces.Items {
		in.namespaces[namespaces.Items[i].Name] = namespaces.Items[i].Labels
	}
	for i := range proxies.Items {
		// One that is going renders nothing more.
		if proxies.Items[i].DeletionTimestamp.IsZero() {
			in.live[client.ObjectKeyFromObject(&proxies.Items[i])] = true
		}
	}
	return in, nil
}

// gatewayInputs lists everything the translation of proxy's Gateways reads.
// The returned function reports a lookup that failed for another reason than
// the object not being there: what was translated with it is not to be
// trusted, and has to be done again.
func (r *SynapseRouteReconciler) gatewayInputs(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) (gatewayInputs, func() error, error) {
	in, err := r.gatewayLists(ctx)
	if err != nil {
		return in, nil, err
	}
	in.proxy = proxy

	var failed error
	get := func(namespace, name string, into client.Object) bool {
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, into)
		if err != nil && !apierrors.IsNotFound(err) && failed == nil {
			failed = fmt.Errorf("get %T %s/%s: %w", into, namespace, name, err)
		}
		return err == nil
	}
	in.service = func(namespace, name string) *corev1.Service {
		var svc corev1.Service
		if !get(namespace, name, &svc) {
			return nil
		}
		return &svc
	}
	in.secret = func(namespace, name string) *corev1.Secret {
		var sec corev1.Secret
		if !get(namespace, name, &sec) {
			return nil
		}
		return &sec
	}

	// Where the proxy is reached: what its own status says too.
	var svc corev1.Service
	if get(proxy.Namespace, proxy.Name, &svc) && metav1.IsControlledBy(&svc, proxy) {
		in.addresses = serviceAddresses(&svc)
	}
	return in, func() error { return failed }, nil
}

// mergeConditions returns the conditions an object should carry: those in
// want, each keeping the time of its last change from have when its status
// is the same. Conditions in have that want does not name are dropped: they
// are of an earlier state.
func mergeConditions(have, want []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, 0, len(want))
	now := metav1.Now()
	for _, c := range want {
		c.LastTransitionTime = now
		if old := meta.FindStatusCondition(have, c.Type); old != nil && old.Status == c.Status {
			c.LastTransitionTime = old.LastTransitionTime
		}
		out = append(out, c)
	}
	return out
}

// routeParents returns the parents a route's status should list: the ones
// it lists, with those that are this proxy's to write (mine) replaced by
// want, each where it stood, and the rest of want after them. One of mine
// that want does not name is of an earlier state, and goes.
//
// Where it stood, because the list is shared. Two proxies that each moved
// their own entries to the end would find the other's order wrong on every
// pass, and write the route for ever.
func routeParents(have, want []gwv1.RouteParentStatus, mine func(gwv1.RouteParentStatus) bool) []gwv1.RouteParentStatus {
	placed := make([]bool, len(want))
	var out []gwv1.RouteParentStatus
	for _, p := range have {
		if !mine(p) {
			out = append(out, p)
			continue
		}
		for i := range want {
			if !placed[i] && apiequality.Semantic.DeepEqual(want[i].ParentRef, p.ParentRef) {
				placed[i] = true
				w := want[i]
				w.Conditions = mergeConditions(p.Conditions, w.Conditions)
				out = append(out, w)
				break
			}
		}
	}
	for i := range want {
		if !placed[i] {
			w := want[i]
			w.Conditions = mergeConditions(nil, w.Conditions)
			out = append(out, w)
		}
	}
	return out
}

// writeGatewayStatuses records what the translation found on the objects it
// was about, and writes only those whose status that changes.
//
// It also takes back what was written about objects no proxy serves any
// more: a class that names a SynapseProxy that is gone, the Gateways of such
// a class, and a route's entry for a Gateway that is one of those or is
// gone itself. Left alone they would go on saying programmed, with the
// address of a proxy that is not there. With st empty that is all it does.
//
// A route's status is a list with an entry for each parent, and other
// controllers write theirs to it; so do other proxies of this one. An entry
// is this proxy's when it is ours and its parent is one of this proxy's
// Gateways, and those are the entries replaced, or removed.
func (r *SynapseRouteReconciler) writeGatewayStatuses(ctx context.Context, in gatewayInputs, st gatewayStatuses) error {
	orphanClasses, orphanGateways := unserved(in.classes, in.gateways, in.live, in.scope)
	for i := range in.classes {
		gc := &in.classes[i]
		want, ok := st.classes[gc.Name]
		if named, orphan := orphanClasses[gc.Name]; orphan {
			want, ok = []metav1.Condition{condition(string(gwv1.GatewayClassConditionStatusAccepted), false,
				string(gwv1.GatewayClassReasonInvalidParameters), "there is no SynapseProxy "+named.String(), gc.Generation)}, true
		}
		if !ok {
			continue
		}
		merged := mergeConditions(gc.Status.Conditions, want)
		if apiequality.Semantic.DeepEqual(gc.Status.Conditions, merged) {
			continue
		}
		gc = gc.DeepCopy()
		gc.Status.Conditions = merged
		if err := r.Status().Update(ctx, gc); err != nil {
			return fmt.Errorf("status of GatewayClass %s: %w", gc.Name, err)
		}
	}

	exists := map[types.NamespacedName]bool{}
	for i := range in.gateways {
		gw := &in.gateways[i]
		key := client.ObjectKeyFromObject(gw)
		exists[key] = true
		want, ok := st.gateways[key]
		if orphanGateways[key] {
			// As a Gateway is before any controller has looked at it.
			const message = "its GatewayClass names a SynapseProxy that does not exist"
			want, ok = gwv1.GatewayStatus{Conditions: []metav1.Condition{
				{Type: string(gwv1.GatewayConditionAccepted), Status: metav1.ConditionUnknown, Reason: string(gwv1.GatewayReasonPending), Message: message, ObservedGeneration: gw.Generation},
				{Type: string(gwv1.GatewayConditionProgrammed), Status: metav1.ConditionUnknown, Reason: string(gwv1.GatewayReasonPending), Message: message, ObservedGeneration: gw.Generation},
			}}, true
		}
		if !ok {
			continue
		}
		want.Conditions = mergeConditions(gw.Status.Conditions, want.Conditions)
		for j := range want.Listeners {
			var have []metav1.Condition
			for _, l := range gw.Status.Listeners {
				if l.Name == want.Listeners[j].Name {
					have = l.Conditions
				}
			}
			want.Listeners[j].Conditions = mergeConditions(have, want.Listeners[j].Conditions)
		}
		if apiequality.Semantic.DeepEqual(gw.Status, want) {
			continue
		}
		gw = gw.DeepCopy()
		gw.Status = want
		if err := r.Status().Update(ctx, gw); err != nil {
			return fmt.Errorf("status of Gateway %s/%s: %w", gw.Namespace, gw.Name, err)
		}
	}

	for i := range in.routes {
		rt := &in.routes[i]
		mine := func(p gwv1.RouteParentStatus) bool {
			gwKey, isGateway := parentGateway(rt, p.ParentRef)
			if string(p.ControllerName) != ControllerName || !isGateway {
				return false
			}
			_, thisProxys := st.gateways[gwKey]
			// One that is not there, where this operator would see it.
			gone := !exists[gwKey] && (in.scope == "" || gwKey.Namespace == in.scope)
			return thisProxys || orphanGateways[gwKey] || gone
		}
		parents := routeParents(rt.Status.Parents, st.routes[client.ObjectKeyFromObject(rt)], mine)
		if apiequality.Semantic.DeepEqual(rt.Status.Parents, parents) || (len(rt.Status.Parents) == 0 && len(parents) == 0) {
			continue
		}
		rt = rt.DeepCopy()
		rt.Status.Parents = parents
		if err := r.Status().Update(ctx, rt); err != nil {
			return fmt.Errorf("status of HTTPRoute %s/%s: %w", rt.Namespace, rt.Name, err)
		}
	}
	return nil
}

// sweepGatewayStatuses takes back the statuses of what no proxy serves any
// more, when there is no proxy to do it while writing its own: the one this
// was about has just gone.
func (r *SynapseRouteReconciler) sweepGatewayStatuses(ctx context.Context) error {
	in, err := r.gatewayLists(ctx)
	if err != nil {
		return err
	}
	return r.writeGatewayStatuses(ctx, in, gatewayStatuses{})
}
