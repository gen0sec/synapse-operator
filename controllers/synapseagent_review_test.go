package controllers

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// A cache that lags behind lets the collision check miss. The apply must then
// be refused by the API server, not take the object over.
func TestApplyObject_AgentDoesNotTakeOverAProxysServiceAccount(t *testing.T) {
	e := newAgentEnv(t)
	proxy := proxyForConfig()
	proxy.Namespace, proxy.Name = e.ns, "agent"
	e.create(proxy)
	agent := e.agent()

	serviceAccount := func(owner client.Object) *corev1.ServiceAccount {
		sa := &corev1.ServiceAccount{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "agent"},
		}
		if err := controllerutil.SetControllerReference(owner, sa, e.k8s.Scheme()); err != nil {
			t.Fatal(err)
		}
		return sa
	}
	if _, err := applyObject(e.ctx, e.k8s, proxyFieldOwner, serviceAccount(proxy)); err != nil {
		t.Fatalf("the proxy's apply: %v", err)
	}
	_, err := applyObject(e.ctx, e.k8s, agentFieldOwner, serviceAccount(agent))
	if err == nil || !strings.Contains(err.Error(), "Only one reference can have Controller set to true") {
		t.Fatalf("the agent's apply = %v, want it refused for a second controller", err)
	}
	var got corev1.ServiceAccount
	e.get("agent", &got)
	if ref := metav1.GetControllerOf(&got); ref == nil || ref.Kind != "SynapseProxy" {
		t.Errorf("the proxy's controller reference did not survive: %+v", got.OwnerReferences)
	}
}

func TestAgentRolloutStateAt(t *testing.T) {
	settled := func(generation int64) *appsv1.DaemonSet {
		return &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Generation: generation},
			Status: appsv1.DaemonSetStatus{ObservedGeneration: generation, DesiredNumberScheduled: 3,
				UpdatedNumberScheduled: 3, NumberAvailable: 3},
		}
	}
	for _, tc := range []struct {
		name    string
		ds      *appsv1.DaemonSet
		applied int64
		want    string
	}{
		{"cache holds the generation just applied", settled(4), 4, ReasonApplied},
		{"cache is behind the apply, though it looks settled", settled(3), 4, ReasonRollingOut},
		{"cache is ahead", settled(5), 4, ReasonApplied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reason, message := agentRolloutStateAt(tc.ds, tc.applied); reason != tc.want || message == "" {
				t.Errorf("= %q (%q), want %q with a message", reason, message, tc.want)
			}
		})
	}
}

// agentReadyValue reads the synapse_operator_agent_ready gauge of one agent.
func agentReadyValue(t *testing.T, ns, name string) (float64, bool) {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "synapse_operator_agent_ready" {
			continue
		}
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if got["namespace"] == ns && got["name"] == name {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestAgent_ReadyGaugeFollowsReadiness(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	if v, ok := agentReadyValue(t, e.ns, e.name); !ok || v != 0 {
		t.Errorf("gauge = %v (present %v) while rolling out, want 0", v, ok)
	}
	d := e.daemonSet()
	changeStatus(t, e.k8s, d, func() {
		d.Status = appsv1.DaemonSetStatus{ObservedGeneration: d.Generation, DesiredNumberScheduled: 2,
			CurrentNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberReady: 2, NumberAvailable: 2}
	})
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionTrue, ReasonApplied)
	if v, _ := agentReadyValue(t, e.ns, e.name); v != 1 {
		t.Errorf("gauge = %v when Ready, want 1", v)
	}
}

func TestAgent_EventOnlyWhenTheReasonChanges(t *testing.T) {
	e := newAgentEnv(t)
	e.running()
	first := e.events()
	if !strings.HasPrefix(first, "Normal "+ReasonRollingOut) || strings.Count(first, "\n") != 0 {
		t.Fatalf("first pass events = %q, want one Normal RollingOut", first)
	}
	e.reconcile(nil)
	if again := e.events(); again != "" {
		t.Errorf("an unchanged outcome emitted %q", again)
	}
}

func TestAgent_ProblemsAreWarnings(t *testing.T) {
	t.Run("ConfigInvalid", func(t *testing.T) {
		e := newAgentEnv(t)
		e.agent(withAPIKey) // its Secret does not exist
		e.reconcile(nil)
		if got := e.events(); !strings.HasPrefix(got, "Warning "+ReasonConfigInvalid) {
			t.Errorf("event = %q, want a Warning %s", got, ReasonConfigInvalid)
		}
	})
	t.Run("NameCollision", func(t *testing.T) {
		e := newAgentEnv(t)
		e.create(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "agent"}})
		e.agent()
		e.reconcile(nil)
		if got := e.events(); !strings.HasPrefix(got, "Warning "+ReasonNameCollision) {
			t.Errorf("event = %q, want a Warning %s", got, ReasonNameCollision)
		}
	})
}

// staleDaemonSets answers every Get of a DaemonSet with a copy taken earlier,
// as an informer cache does that has not seen the latest write.
type staleDaemonSets struct {
	client.Client
	stale *appsv1.DaemonSet
}

func (s staleDaemonSets) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if ds, ok := obj.(*appsv1.DaemonSet); ok {
		s.stale.DeepCopyInto(ds)
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

func TestAgent_ACacheBehindTheApplyIsRollingOut(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	d := e.daemonSet()
	changeStatus(t, e.k8s, d, func() {
		d.Status = appsv1.DaemonSetStatus{ObservedGeneration: d.Generation, DesiredNumberScheduled: 2,
			CurrentNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberReady: 2, NumberAvailable: 2}
	})
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionTrue, ReasonApplied)

	e.agents.Client = staleDaemonSets{Client: e.k8s, stale: e.daemonSet()}
	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Logging = &synapsev1alpha1.AgentLoggingSpec{Level: "debug"}
	})
	e.reconcile(a)
	if now := e.daemonSet(); now.Generation == e.agents.Client.(staleDaemonSets).stale.Generation {
		t.Fatal("the restart-field edit did not roll the template")
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
}
