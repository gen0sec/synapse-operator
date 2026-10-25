package controllers

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// ReasonNoMatchingNodes is the Ready reason of an agent whose DaemonSet
// selects no node.
const ReasonNoMatchingNodes = "NoMatchingNodes"

// agentFieldOwner is the field manager the agent's objects are applied as. It
// is not the proxy's: an apply forced as the same manager could take over an
// object of a SynapseProxy of the same name, if the collision check missed it.
const agentFieldOwner = "synapse-operator-agent"

// SynapseAgentReconciler runs a SynapseAgent: it renders the config into a
// Secret and applies the ServiceAccount and the DaemonSet around it.
//
// Every object is applied server-side as one field manager, as the proxy's
// are. The config Secret keeps its name when its content changes, and the pod
// template carries only the RestartHash: a change to the settings Synapse
// reloads from a changed file (see RenderAgentConfig) reaches the running
// pods through kubelet's refresh of the mounted Secret, and any other change
// rolls them.
//
// What it needs to be allowed is the role in config/agent-controller. A change
// here that asks the API server for something new has to add it there.
type SynapseAgentReconciler struct {
	client.Client
	// Recorder emits Events on the SynapseAgent. May be nil.
	Recorder record.EventRecorder
	// RuleReader reads the ConfigMaps of spec.ids.rules. They are many and
	// not the agent's, so they are read live and not cached whole. Nil means
	// Client.
	RuleReader client.Reader
}

func (r *SynapseAgentReconciler) ruleReader() client.Reader {
	if r.RuleReader != nil {
		return r.RuleReader
	}
	return r.Client
}

// agentOutcome is what one reconcile concluded, to be written to status.
type agentOutcome struct {
	configValid metav1.Condition
	ready       metav1.Condition
	configHash  string
	requeue     time.Duration
}

func (o *agentOutcome) invalid(reason, message string) {
	o.configValid = metav1.Condition{Type: ConditionConfigValid, Status: metav1.ConditionFalse, Reason: reason, Message: message}
	o.notReady(ReasonConfigInvalid, message)
}

func (o *agentOutcome) notReady(reason, message string) {
	o.ready = metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message}
}

func (r *SynapseAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var agent synapsev1alpha1.SynapseAgent
	if err := r.Get(ctx, req.NamespacedName, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			mAgentReady.DeleteLabelValues(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !agent.DeletionTimestamp.IsZero() {
		// Everything the agent owns goes with it.
		return ctrl.Result{}, nil
	}

	outcome, err := r.reconcile(ctx, &agent)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.writeStatus(ctx, &agent, outcome); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: outcome.requeue}, nil
}

// reconcile brings the agent's objects in line with its spec, as far as it
// can. A returned error is one worth retrying; anything the user has to fix
// is reported through the outcome instead, with the running objects left as
// they are.
func (r *SynapseAgentReconciler) reconcile(ctx context.Context, agent *synapsev1alpha1.SynapseAgent) (*agentOutcome, error) {
	o := &agentOutcome{}

	var ref *synapsev1alpha1.SecretKeyReference
	if agent.Spec.Platform != nil {
		ref = agent.Spec.Platform.APIKeySecretRef
	}
	apiKey, reason, message, err := resolveAPIKey(ctx, r.Client, agent.Namespace, ref)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		o.invalid(reason, message)
		return o, nil
	}

	var ruleSources []synapsev1alpha1.AgentIDSRuleSource
	if agent.Spec.IDS != nil {
		ruleSources = agent.Spec.IDS.Rules
	}
	rules, reason, message, err := resolveAgentRules(ctx, ruleReaders{configMaps: r.ruleReader(), secrets: r.Client}, agent, ruleSources)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		o.invalid(reason, message)
		return o, nil
	}

	// A typed homeNet is rendered as it is written; the cluster is read only
	// when there is none.
	homeNet := ""
	if takesHomeNetFromCluster(agent) {
		if homeNet, err = r.clusterHomeNet(ctx); err != nil {
			return nil, err
		}
	}

	if message := validateAgentVolumes(&agent.Spec); message != "" {
		o.invalid(ReasonInvalidVolumes, message)
		return o, nil
	}

	cfg, err := RenderAgentConfig(AgentConfigInput{Agent: agent, APIKey: apiKey, HomeNet: homeNet, RuleFiles: agentRuleFileNames(rules)})
	if err != nil {
		o.invalid(ReasonInvalidConfig, err.Error())
		return o, nil
	}
	if reason, message := checkAgentSecretSize(cfg.YAML, rules); reason != "" {
		o.invalid(reason, message)
		return o, nil
	}
	o.configValid = metav1.Condition{Type: ConditionConfigValid, Status: metav1.ConditionTrue, Reason: ReasonRendered,
		Message: "spec renders to a Synapse configuration"}

	objects := []client.Object{
		buildAgentConfigSecret(agent, cfg.YAML, rules...),
		buildAgentServiceAccount(agent),
		buildAgentDaemonSet(agent, cfg.RestartHash),
	}
	// All of them, before any is written: half an agent is worse than none.
	// A SynapseProxy of the same name is found here too, through the
	// ServiceAccount it owns under that name.
	for _, obj := range objects {
		if inTheWay, err := objectInTheWay(ctx, r.Client, agent, "SynapseAgent", obj); err != nil {
			return nil, err
		} else if inTheWay != "" {
			o.notReady(ReasonNameCollision, inTheWay)
			o.requeue = collisionRetry
			return o, nil
		}
	}
	var appliedGeneration int64
	for _, obj := range objects {
		applied, err := applyObject(ctx, r.Client, agentFieldOwner, obj)
		if err != nil {
			return nil, fmt.Errorf("apply %T %s: %w", obj, obj.GetName(), err)
		}
		if _, isDaemonSet := obj.(*appsv1.DaemonSet); isDaemonSet {
			appliedGeneration = applied.GetGeneration()
		}
	}
	o.configHash = cfg.Hash

	var ds appsv1.DaemonSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(agent), &ds); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		// Applied a moment ago and not in the cache yet: its creation is
		// an event of its own.
		o.notReady(ReasonRollingOut, "waiting for the DaemonSet to be observed")
		return o, nil
	}
	reason, message = agentRolloutStateAt(&ds, appliedGeneration)
	if reason == ReasonApplied {
		o.ready = metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: reason, Message: message}
	} else {
		o.notReady(reason, message)
	}
	return o, nil
}

// clusterHomeNet is HOME_NET as the cluster's nodes and services make it.
func (r *SynapseAgentReconciler) clusterHomeNet(ctx context.Context) (string, error) {
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	var services corev1.ServiceList
	if err := r.List(ctx, &services); err != nil {
		return "", fmt.Errorf("list services: %w", err)
	}
	return homeNetFromCluster(nodes.Items, services.Items), nil
}

// agentRolloutStateAt is agentRolloutState for a DaemonSet read from a cache
// that may not hold the generation the operator just applied. Until it does,
// its status describes an older template.
func agentRolloutStateAt(ds *appsv1.DaemonSet, applied int64) (reason, message string) {
	if ds.Generation < applied {
		return ReasonRollingOut, "waiting for the DaemonSet to be observed"
	}
	return agentRolloutState(ds)
}

// agentRolloutState says whether the DaemonSet runs what it was last told to,
// the way `kubectl rollout status` decides it.
func agentRolloutState(ds *appsv1.DaemonSet) (reason, message string) {
	s := ds.Status
	switch {
	case ds.Generation > s.ObservedGeneration:
		return ReasonRollingOut, "waiting for the DaemonSet to be observed"
	case s.DesiredNumberScheduled == 0:
		return ReasonNoMatchingNodes, "no node matches the node selector and tolerations"
	case s.UpdatedNumberScheduled < s.DesiredNumberScheduled:
		return ReasonRollingOut, fmt.Sprintf("%d of %d pods run the current configuration and image", s.UpdatedNumberScheduled, s.DesiredNumberScheduled)
	case s.NumberAvailable < s.DesiredNumberScheduled:
		return ReasonRollingOut, fmt.Sprintf("%d of %d pods are available", s.NumberAvailable, s.DesiredNumberScheduled)
	}
	return ReasonApplied, "the pods run the current configuration and image"
}

// writeStatus records the outcome and what the live DaemonSet shows.
func (r *SynapseAgentReconciler) writeStatus(ctx context.Context, agent *synapsev1alpha1.SynapseAgent, o *agentOutcome) error {
	before := agent.DeepCopy()
	status := &agent.Status
	previousReady := meta.FindStatusCondition(status.Conditions, ConditionReady)
	previousReason := ""
	if previousReady != nil {
		previousReason = previousReady.Reason
	}

	status.ObservedGeneration = agent.Generation
	if o.configHash != "" {
		status.ConfigHash = o.configHash
	}
	for _, c := range []metav1.Condition{o.configValid, o.ready} {
		c.ObservedGeneration = agent.Generation
		meta.SetStatusCondition(&status.Conditions, c)
	}

	// The running pods are reported whatever the outcome: an invalid spec
	// does not stop what is already there.
	var ds appsv1.DaemonSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(agent), &ds); err == nil && metav1.IsControlledBy(&ds, agent) {
		status.DesiredNumberScheduled = ds.Status.DesiredNumberScheduled
		status.CurrentNumberScheduled = ds.Status.CurrentNumberScheduled
		status.UpdatedNumberScheduled = ds.Status.UpdatedNumberScheduled
		status.NumberReady = ds.Status.NumberReady
		status.NumberAvailable = ds.Status.NumberAvailable
		status.NumberMisscheduled = ds.Status.NumberMisscheduled
	} else if client.IgnoreNotFound(err) != nil {
		return err
	}

	ready := float64(0)
	if o.ready.Status == metav1.ConditionTrue {
		ready = 1
	}
	mAgentReady.WithLabelValues(agent.Namespace, agent.Name).Set(ready)

	if o.ready.Reason != previousReason && r.Recorder != nil {
		eventType := corev1.EventTypeNormal
		switch o.ready.Reason {
		case ReasonConfigInvalid, ReasonNameCollision:
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(agent, eventType, o.ready.Reason, o.ready.Message)
	}

	// A patch with nothing in it changes nothing on the server.
	return r.Status().Patch(ctx, agent, client.MergeFrom(before))
}
