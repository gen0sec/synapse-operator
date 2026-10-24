package controllers

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// Where what CheckAgentAccess finds missing comes from. The CRDs are in
// config/crd. config/agent-controller leaves them out because
// config/proxy-controller carries them, and both are applied together.
const (
	agentCRDInstall  = "config/crd"
	agentRoleInstall = "config/agent-controller"
)

// agentAPIKeySecretIndex indexes SynapseAgents by the Secret their API key
// comes from, so a change to that Secret finds the agents that read it.
const agentAPIKeySecretIndex = "spec.platform.apiKeySecretRef.name"

// SetupAgentControllers registers the controller that runs SynapseAgent
// resources.
func SetupAgentControllers(mgr ctrl.Manager) error {
	agents := &SynapseAgentReconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorderFor("synapse-agent"),
	}
	if err := agents.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("agent controller: %w", err)
	}
	return nil
}

// SetupWithManager registers the controller.
func (r *SynapseAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(context.Background(), &synapsev1alpha1.SynapseAgent{}, agentAPIKeySecretIndex,
		func(obj client.Object) []string {
			a := obj.(*synapsev1alpha1.SynapseAgent)
			if a.Spec.Platform == nil || a.Spec.Platform.APIKeySecretRef == nil {
				return nil
			}
			return []string{a.Spec.Platform.APIKeySecretRef.Name}
		})
	if err != nil {
		return err
	}
	// The Secret an API key comes from is the user's, not the agent's, so it
	// is found through the index rather than through ownership.
	readers := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, secret client.Object) []reconcile.Request {
		var agents synapsev1alpha1.SynapseAgentList
		if err := r.List(ctx, &agents, client.InNamespace(secret.GetNamespace()),
			client.MatchingFields{agentAPIKeySecretIndex: secret.GetName()}); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list SynapseAgents reading a Secret")
			return nil
		}
		return agentRequests(agents.Items, func(*synapsev1alpha1.SynapseAgent) bool { return true })
	})
	// Only an agent without a typed homeNet takes it from the cluster.
	followCluster := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		var agents synapsev1alpha1.SynapseAgentList
		if err := r.List(ctx, &agents); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list SynapseAgents following the cluster's addresses")
			return nil
		}
		return agentRequests(agents.Items, takesHomeNetFromCluster)
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("synapse-agent").
		// Only the spec: this controller's own status writes are not news.
		For(&synapsev1alpha1.SynapseAgent{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Secret{}).
		Watches(&corev1.Secret{}, readers).
		Watches(&corev1.Node{}, followCluster, builder.WithPredicates(agentNodePredicate())).
		Watches(&corev1.Service{}, followCluster, builder.WithPredicates(agentServicePredicate())).
		Complete(r)
}

// takesHomeNetFromCluster is true for an agent whose HOME_NET is not typed.
func takesHomeNetFromCluster(a *synapsev1alpha1.SynapseAgent) bool {
	return a.Spec.IDS == nil || len(a.Spec.IDS.HomeNet) == 0
}

func agentRequests(agents []synapsev1alpha1.SynapseAgent, keep func(*synapsev1alpha1.SynapseAgent) bool) []reconcile.Request {
	reqs := make([]reconcile.Request, 0, len(agents))
	for i := range agents {
		if keep(&agents[i]) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&agents[i])})
		}
	}
	return reqs
}

// agentNodePredicate passes the Node events that can change HOME_NET: a node
// that joins or leaves with an address or a pod CIDR, and a change to either.
// Heartbeats, labels and the rest of a node's status are filtered out.
func agentNodePredicate() predicate.Predicate {
	return homeNetInputsPredicate(func(obj client.Object) (string, bool) {
		n, ok := obj.(*corev1.Node)
		if !ok {
			return "", false
		}
		return homeNetFromCluster([]corev1.Node{*n}, nil), true
	})
}

// agentServicePredicate passes the Service events that can change HOME_NET:
// a change to the load-balancer addresses, externalIPs or loadBalancerIP.
func agentServicePredicate() predicate.Predicate {
	return homeNetInputsPredicate(func(obj client.Object) (string, bool) {
		s, ok := obj.(*corev1.Service)
		if !ok {
			return "", false
		}
		return homeNetFromCluster(nil, []corev1.Service{*s}), true
	})
}

// homeNetInputsPredicate compares what an object adds to HOME_NET before and
// after, so it can only disagree with homeNetFromCluster by being stricter
// about an object that adds nothing.
func homeNetInputsPredicate(contribution func(client.Object) (string, bool)) predicate.Predicate {
	nothing := homeNetFromCluster(nil, nil)
	adds := func(obj client.Object) bool {
		c, ok := contribution(obj)
		return ok && c != nothing
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return adds(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool { return adds(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			before, okOld := contribution(e.ObjectOld)
			after, okNew := contribution(e.ObjectNew)
			return okOld && okNew && before != after
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// CheckAgentAccess says why the SynapseAgent controller cannot run as whoever
// c acts as, or returns nil. It is for use before there is a manager: the
// controller indexes SynapseAgent resources, and a manager waits for the
// cache of an indexed kind before it starts anything. One that cannot read
// them starts no controller at all, and reports nothing but a failing watch
// in its log.
//
// Only that one read is checked. A controller that is refused anything else
// stops the manager with an error when its own cache does not fill.
func CheckAgentAccess(ctx context.Context, c client.Reader, namespace string) error {
	var agents synapsev1alpha1.SynapseAgentList
	err := c.List(ctx, &agents, client.InNamespace(namespace), client.Limit(1))
	switch {
	case err == nil:
		return nil
	case meta.IsNoMatchError(err):
		return fmt.Errorf("the SynapseAgent CRD is not installed; it is in %s: %w", agentCRDInstall, err)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("not allowed to read SynapseAgent resources; the role that allows it is in %s: %w", agentRoleInstall, err)
	}
	return err
}
