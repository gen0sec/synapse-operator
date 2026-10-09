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
func GatewayAPIServed(mapper meta.RESTMapper) bool {
	for kind, version := range map[string]string{
		"GatewayClass": "v1", "Gateway": "v1", "HTTPRoute": "v1", "ReferenceGrant": "v1beta1",
	} {
		if _, err := mapper.RESTMapping(schema.GroupKind{Group: gwv1.GroupName, Kind: kind}, version); err != nil {
			return false
		}
	}
	return true
}

// gatewayInputs lists everything the translation of proxy's Gateways reads.
// The returned function reports a lookup that failed for another reason than
// the object not being there: what was translated with it is not to be
// trusted, and has to be done again.
func (r *SynapseRouteReconciler) gatewayInputs(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) (gatewayInputs, func() error, error) {
	in := gatewayInputs{proxy: proxy, clusterDomain: r.ClusterDomain, namespaces: map[string]map[string]string{}}
	var (
		classes    gwv1.GatewayClassList
		gateways   gwv1.GatewayList
		routes     gwv1.HTTPRouteList
		grants     gwv1beta1.ReferenceGrantList
		namespaces corev1.NamespaceList
	)
	for _, list := range []client.ObjectList{&classes, &gateways, &routes, &grants, &namespaces} {
		if err := r.List(ctx, list); err != nil {
			return in, nil, fmt.Errorf("list %T: %w", list, err)
		}
	}
	in.classes, in.gateways, in.routes, in.grants = classes.Items, gateways.Items, routes.Items, grants.Items
	for i := range namespaces.Items {
		in.namespaces[namespaces.Items[i].Name] = namespaces.Items[i].Labels
	}

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

// writeGatewayStatuses records what the translation found on the objects it
// was about, and writes only those whose status that changes.
//
// A route's status is a list with an entry for each parent, and other
// controllers write theirs to it; so do other proxies of this one. An entry
// is this proxy's when it is ours and its parent is one of this proxy's
// Gateways, and those are the entries replaced, or removed.
func (r *SynapseRouteReconciler) writeGatewayStatuses(ctx context.Context, in gatewayInputs, st gatewayStatuses) error {
	for i := range in.classes {
		gc := &in.classes[i]
		want, ok := st.classes[gc.Name]
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

	for i := range in.gateways {
		gw := &in.gateways[i]
		want, ok := st.gateways[client.ObjectKeyFromObject(gw)]
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
		var parents []gwv1.RouteParentStatus
		for _, p := range rt.Status.Parents {
			gwKey, isGateway := parentGateway(rt, p.ParentRef)
			_, thisProxys := st.gateways[gwKey]
			if string(p.ControllerName) == ControllerName && isGateway && thisProxys {
				continue
			}
			parents = append(parents, p)
		}
		for _, p := range st.routes[client.ObjectKeyFromObject(rt)] {
			var have []metav1.Condition
			for _, old := range rt.Status.Parents {
				if string(old.ControllerName) == ControllerName && apiequality.Semantic.DeepEqual(old.ParentRef, p.ParentRef) {
					have = old.Conditions
				}
			}
			p.Conditions = mergeConditions(have, p.Conditions)
			parents = append(parents, p)
		}
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
