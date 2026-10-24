package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	kptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

var agentTestRuns atomic.Int64

// agentEnv is one namespace on the local API server with a SynapseAgent
// controller to be stepped by hand. There is no DaemonSet controller behind
// it: nothing fills in a DaemonSet's status unless the test does.
type agentEnv struct {
	t        *testing.T
	ctx      context.Context
	k8s      client.Client
	ns       string
	name     string
	agents   *SynapseAgentReconciler
	recorder *record.FakeRecorder
}

func newAgentEnvIn(t *testing.T, ns, name string) *agentEnv {
	t.Helper()
	k8s, err := client.New(apiServer(t), client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(50)
	e := &agentEnv{t: t, ctx: context.Background(), k8s: k8s, ns: ns, name: name, recorder: rec,
		agents: &SynapseAgentReconciler{Client: k8s, Recorder: rec}}
	return e
}

func newAgentEnv(t *testing.T) *agentEnv {
	t.Helper()
	e := newAgentEnvIn(t, fmt.Sprintf("agent-test-%d", agentTestRuns.Add(1)), "agent")
	e.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e.ns}})
	return e
}

func (e *agentEnv) create(obj client.Object) {
	e.t.Helper()
	if err := e.k8s.Create(e.ctx, obj); err != nil {
		e.t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

func (e *agentEnv) key() client.ObjectKey { return client.ObjectKey{Namespace: e.ns, Name: e.name} }

// agent creates the SynapseAgent.
func (e *agentEnv) agent(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) *synapsev1alpha1.SynapseAgent {
	e.t.Helper()
	a := &synapsev1alpha1.SynapseAgent{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: e.name},
		Spec:       synapsev1alpha1.SynapseAgentSpec{Image: "example.test/synapse:1.0.0"},
	}
	for _, m := range mutate {
		m(&a.Spec)
	}
	e.create(a)
	return a
}

func (e *agentEnv) edit(a *synapsev1alpha1.SynapseAgent, mutate func(*synapsev1alpha1.SynapseAgentSpec)) {
	e.t.Helper()
	change(e.t, e.k8s, a, func() { mutate(&a.Spec) })
}

// reconcile steps the controller once and reloads the agent.
func (e *agentEnv) reconcile(a *synapsev1alpha1.SynapseAgent) ctrl.Result {
	e.t.Helper()
	res, err := e.agents.Reconcile(e.ctx, ctrl.Request{NamespacedName: e.key()})
	if err != nil {
		e.t.Fatalf("agent reconcile: %v", err)
	}
	if a != nil {
		if err := e.k8s.Get(e.ctx, e.key(), a); err != nil {
			e.t.Fatal(err)
		}
	}
	return res
}

func (e *agentEnv) running(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) *synapsev1alpha1.SynapseAgent {
	e.t.Helper()
	a := e.agent(mutate...)
	e.reconcile(a)
	return a
}

func (e *agentEnv) get(name string, into client.Object) bool {
	e.t.Helper()
	err := e.k8s.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: name}, into)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		e.t.Fatalf("get %T %s: %v", into, name, err)
	}
	return true
}

func (e *agentEnv) daemonSet() *appsv1.DaemonSet {
	e.t.Helper()
	var d appsv1.DaemonSet
	if !e.get(e.name, &d) {
		e.t.Fatal("no DaemonSet")
	}
	return &d
}

func (e *agentEnv) configSecret() *corev1.Secret {
	e.t.Helper()
	var s corev1.Secret
	if !e.get(e.name+"-config", &s) {
		e.t.Fatal("no config Secret")
	}
	return &s
}

func (e *agentEnv) config() string { return string(e.configSecret().Data[agentConfigKey]) }

func (e *agentEnv) apiKey(value string) {
	e.t.Helper()
	e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"},
		Data: map[string][]byte{"API_KEY": []byte(value)}})
}

func withAPIKey(s *synapsev1alpha1.SynapseAgentSpec) {
	s.Platform = &synapsev1alpha1.AgentPlatformSpec{
		APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
	}
}

func wantAgentCondition(t *testing.T, a *synapsev1alpha1.SynapseAgent, kind string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(a.Status.Conditions, kind)
	if c == nil {
		t.Fatalf("no %s condition; have %+v", kind, a.Status.Conditions)
	}
	if c.Status != status || c.Reason != reason {
		t.Errorf("%s = %s/%s (%s), want %s/%s", kind, c.Status, c.Reason, c.Message, status, reason)
	}
	return c
}

func (e *agentEnv) wantNothingApplied() {
	e.t.Helper()
	var d appsv1.DaemonSet
	var s corev1.Secret
	var sa corev1.ServiceAccount
	if e.get(e.name, &d) || e.get(e.name+"-config", &s) {
		e.t.Error("a DaemonSet or config Secret was created")
	}
	if e.get(e.name, &sa) {
		e.t.Error("a ServiceAccount was created")
	}
}

// agentReadySeries says whether synapse_operator_agent_ready has a series
// for the agent.
func agentReadySeries(t *testing.T, ns, name string) bool {
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
				return true
			}
		}
	}
	return false
}

func (e *agentEnv) events() string {
	var out []string
	for {
		select {
		case ev := <-e.recorder.Events:
			out = append(out, ev)
		default:
			return strings.Join(out, "\n")
		}
	}
}

func TestAgent_RendersAndApplies(t *testing.T) {
	e := newAgentEnv(t)
	e.apiKey("k-1")
	a := e.running(withAPIKey)

	ds, secret := e.daemonSet(), e.configSecret()
	var sa corev1.ServiceAccount
	if !e.get("agent", &sa) {
		t.Fatal("no ServiceAccount")
	}
	for _, o := range []client.Object{ds, secret, &sa} {
		if !metav1.IsControlledBy(o, a) {
			t.Errorf("%T %s is not controlled by the agent", o, o.GetName())
		}
	}
	if !strings.Contains(string(secret.Data[agentConfigKey]), "mode: agent") || !strings.Contains(string(secret.Data[agentConfigKey]), "k-1") {
		t.Errorf("config.yaml was not rendered:\n%s", secret.Data[agentConfigKey])
	}
	cfg, err := RenderAgentConfig(AgentConfigInput{Agent: a, APIKey: "k-1", HomeNet: homeNetOf(t, e.config())})
	if err != nil {
		t.Fatal(err)
	}
	if got := ds.Spec.Template.Annotations[agentRestartHashAnnotation]; got != cfg.RestartHash {
		t.Errorf("template annotation = %q, want the RestartHash %q", got, cfg.RestartHash)
	}
	if a.Status.ConfigHash != cfg.Hash {
		t.Errorf("status.configHash = %q, want %q", a.Status.ConfigHash, cfg.Hash)
	}
	if a.Status.ObservedGeneration != a.Generation {
		t.Errorf("observedGeneration = %d, want %d", a.Status.ObservedGeneration, a.Generation)
	}
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)

	for _, o := range []client.Object{ds, secret, &sa} {
		if !managedByAs(o, agentFieldOwner, metav1.ManagedFieldsOperationApply) {
			t.Errorf("%T %s is not applied by %q: %+v", o, o.GetName(), agentFieldOwner, o.GetManagedFields())
		}
	}

	// A second pass changes nothing anywhere.
	var fresh synapsev1alpha1.SynapseAgent
	e.get(e.name, &fresh)
	before := map[string]string{ds.Name: ds.ResourceVersion, "secret": secret.ResourceVersion, "sa": sa.ResourceVersion, "agent": fresh.ResourceVersion}
	e.reconcile(a)
	var sa2 corev1.ServiceAccount
	e.get("agent", &sa2)
	e.get(e.name, &fresh)
	after := map[string]string{ds.Name: e.daemonSet().ResourceVersion, "secret": e.configSecret().ResourceVersion, "sa": sa2.ResourceVersion, "agent": fresh.ResourceVersion}
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s: resourceVersion %s -> %s on an unchanged agent", k, v, after[k])
		}
	}
}

// homeNetOf returns the HOME_NET line's value of a rendered config, so a test
// can render the same config again.
func homeNetOf(t *testing.T, yaml string) string {
	t.Helper()
	for _, line := range strings.Split(yaml, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "HOME_NET:"); ok {
			return strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	t.Fatalf("no HOME_NET in\n%s", yaml)
	return ""
}

func TestAgent_SecretUpdatedInPlaceOnHotChange(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8"}}
	})
	secret, ds := e.configSecret(), e.daemonSet()
	hash := a.Status.ConfigHash

	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) { s.IDS.HomeNet = []string{"10.0.0.0/8", "192.0.2.0/24"} })
	e.reconcile(a)

	got := e.configSecret()
	if got.UID != secret.UID {
		t.Error("the config Secret was replaced")
	}
	if string(got.Data[agentConfigKey]) == string(secret.Data[agentConfigKey]) || !strings.Contains(string(got.Data[agentConfigKey]), "192.0.2.0/24") {
		t.Errorf("the Secret's data did not change:\n%s", got.Data[agentConfigKey])
	}
	if a.Status.ConfigHash == hash || a.Status.ConfigHash == "" {
		t.Errorf("status.configHash = %q, was %q", a.Status.ConfigHash, hash)
	}
	now := e.daemonSet()
	if now.Spec.Template.Annotations[agentRestartHashAnnotation] != ds.Spec.Template.Annotations[agentRestartHashAnnotation] {
		t.Error("a hot change changed the template annotation")
	}
	if now.ResourceVersion != ds.ResourceVersion {
		t.Error("a hot change touched the DaemonSet")
	}
}

func TestAgent_RestartFieldRollsTemplate(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	secret, ds := e.configSecret(), e.daemonSet()

	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) { s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"} })
	e.reconcile(a)

	now := e.daemonSet()
	if now.Spec.Template.Annotations[agentRestartHashAnnotation] == ds.Spec.Template.Annotations[agentRestartHashAnnotation] {
		t.Error("a logging.level change did not change the template annotation")
	}
	if e.configSecret().UID != secret.UID || !strings.Contains(e.config(), "level: debug") {
		t.Error("the Secret was not updated in place")
	}
}

func TestAgent_HostNetworkSwitch(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	on := e.daemonSet()
	if !on.Spec.Template.Spec.HostNetwork {
		t.Fatal("a new agent is not on the host network")
	}

	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) { s.HostNetwork = kptr.To(false) })
	e.reconcile(a)
	off := e.daemonSet()
	pod := off.Spec.Template.Spec
	if pod.HostNetwork || pod.DNSPolicy != corev1.DNSClusterFirst {
		t.Errorf("hostNetwork = %v, dnsPolicy = %q, want false and ClusterFirst", pod.HostNetwork, pod.DNSPolicy)
	}
	if v, m := bpffsOf(t, off); v || m {
		t.Errorf("the pod off the host network has the node's bpffs: volume=%v mount=%v", v, m)
	}
	if off.Generation <= on.Generation {
		t.Errorf("generation %d after %d: the switch did not roll the pods", off.Generation, on.Generation)
	}

	e.reconcile(a)
	if again := e.daemonSet(); again.ResourceVersion != off.ResourceVersion {
		t.Errorf("a second reconcile rewrote the DaemonSet: resourceVersion %s -> %s", off.ResourceVersion, again.ResourceVersion)
	}

	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) { s.HostNetwork = kptr.To(true) })
	e.reconcile(a)
	back := e.daemonSet()
	if !back.Spec.Template.Spec.HostNetwork || back.Spec.Template.Spec.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Errorf("hostNetwork = %v, dnsPolicy = %q, want true and ClusterFirstWithHostNet", back.Spec.Template.Spec.HostNetwork, back.Spec.Template.Spec.DNSPolicy)
	}
	if v, m := bpffsOf(t, back); !v || !m {
		t.Errorf("the bpffs did not come back: volume=%v mount=%v", v, m)
	}
}

func testNode(name, ip, podCIDR string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{PodCIDR: podCIDR, PodCIDRs: []string{podCIDR}},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}},
	}
}

func (e *agentEnv) createNode(n *corev1.Node) {
	e.t.Helper()
	status := n.Status
	e.create(n)
	e.t.Cleanup(func() { _ = e.k8s.Delete(context.Background(), n) })
	changeStatus(e.t, e.k8s, n, func() { n.Status = status })
}

func TestAgent_HomeNetFollowsNodes(t *testing.T) {
	e := newAgentEnv(t)
	e.createNode(testNode("agent-home-a", "192.0.2.77", "198.51.100.0/24"))
	a := e.running()
	cfg := e.config()
	for _, want := range []string{"192.0.2.77/32", "198.51.100.0/24"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("HOME_NET lacks %s:\n%s", want, cfg)
		}
	}

	// A typed list wins over what the nodes say.
	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"172.30.0.0/16"}}
	})
	e.reconcile(a)
	cfg = e.config()
	if !strings.Contains(cfg, "172.30.0.0/16") || strings.Contains(cfg, "192.0.2.77") {
		t.Errorf("a typed homeNet did not win:\n%s", cfg)
	}
}

func TestAgent_HomeNetFollowsLoadBalancers(t *testing.T) {
	e := newAgentEnv(t)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "lb"},
		Spec: corev1.ServiceSpec{
			Type:        corev1.ServiceTypeLoadBalancer,
			Ports:       []corev1.ServicePort{{Port: 80}},
			ExternalIPs: []string{"203.0.113.9"},
		},
	}
	e.create(svc)
	changeStatus(t, e.k8s, svc, func() {
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}}
	})
	e.running()
	cfg := e.config()
	for _, want := range []string{"203.0.113.9/32", "203.0.113.10/32"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("HOME_NET lacks %s:\n%s", want, cfg)
		}
	}
}

func TestAgentRolloutState(t *testing.T) {
	ds := func(gen, observed int64, desired, updated, available int32) *appsv1.DaemonSet {
		return &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Generation: gen},
			Status: appsv1.DaemonSetStatus{ObservedGeneration: observed, DesiredNumberScheduled: desired,
				UpdatedNumberScheduled: updated, NumberAvailable: available},
		}
	}
	for _, tc := range []struct {
		name string
		ds   *appsv1.DaemonSet
		want string
	}{
		{"not observed, even with nothing wanted", ds(2, 1, 0, 0, 0), ReasonRollingOut},
		{"not observed, all up to date", ds(2, 1, 3, 3, 3), ReasonRollingOut},
		{"no node matches", ds(1, 1, 0, 0, 0), ReasonNoMatchingNodes},
		{"some not updated", ds(1, 1, 3, 2, 3), ReasonRollingOut},
		{"some not available", ds(1, 1, 3, 3, 2), ReasonRollingOut},
		{"all updated and available", ds(1, 1, 3, 3, 3), ReasonApplied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reason, message := agentRolloutState(tc.ds); reason != tc.want || message == "" {
				t.Errorf("= %q (%q), want %q with a message", reason, message, tc.want)
			}
		})
	}
}

func TestAgent_StatusFollowsTheDaemonSet(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)

	step := func(status appsv1.DaemonSetStatus, reason string, ready metav1.ConditionStatus) {
		t.Helper()
		d := e.daemonSet()
		status.ObservedGeneration = d.Generation
		changeStatus(t, e.k8s, d, func() { d.Status = status })
		e.reconcile(a)
		wantAgentCondition(t, a, ConditionReady, ready, reason)
	}
	step(appsv1.DaemonSetStatus{}, ReasonNoMatchingNodes, metav1.ConditionFalse)
	step(appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, CurrentNumberScheduled: 3, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1},
		ReasonRollingOut, metav1.ConditionFalse)
	step(appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, CurrentNumberScheduled: 3, UpdatedNumberScheduled: 3, NumberReady: 3, NumberAvailable: 3, NumberMisscheduled: 1},
		ReasonApplied, metav1.ConditionTrue)
	s := a.Status
	if s.DesiredNumberScheduled != 3 || s.CurrentNumberScheduled != 3 || s.UpdatedNumberScheduled != 3 || s.NumberReady != 3 || s.NumberAvailable != 3 || s.NumberMisscheduled != 1 {
		t.Errorf("the DaemonSet's counters are not in status: %+v", s)
	}
	if !strings.Contains(e.events(), ReasonApplied) {
		t.Error("no event for the reason change to Applied")
	}
}

func TestAgent_NameCollision(t *testing.T) {
	foreign := map[string]func(ns string) client.Object{
		"DaemonSet": func(ns string) client.Object {
			labels := map[string]string{"theirs": "yes"}
			return &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agent", Labels: labels},
				Spec: appsv1.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: labels},
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "x"}}}},
				},
			}
		},
		"ServiceAccount": func(ns string) client.Object {
			return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agent", Labels: map[string]string{"theirs": "yes"}}}
		},
		"Secret": func(ns string) client.Object {
			return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agent-config", Labels: map[string]string{"theirs": "yes"}}}
		},
	}
	for kind, build := range foreign {
		t.Run(kind, func(t *testing.T) {
			e := newAgentEnv(t)
			e.create(build(e.ns))
			a := e.agent()
			res := e.reconcile(a)

			c := wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonNameCollision)
			if a.Status.ConfigHash != "" {
				t.Errorf("status.configHash = %q on an agent that was never applied", a.Status.ConfigHash)
			}
			if !strings.Contains(c.Message, kind) {
				t.Errorf("message does not say what is in the way: %s", c.Message)
			}
			if res.RequeueAfter <= 0 {
				t.Error("no retry is scheduled")
			}
			if !strings.Contains(e.events(), ReasonNameCollision) {
				t.Error("no event")
			}
			// Half an agent is worse than none, and the other object is left as it was.
			objs := 0
			var d appsv1.DaemonSet
			var s corev1.Secret
			var sa corev1.ServiceAccount
			for name, into := range map[string]client.Object{"agent": &d, "agent-config": &s} {
				if e.get(name, into) {
					objs++
				}
			}
			if e.get("agent", &sa) {
				objs++
			}
			if objs != 1 {
				t.Errorf("%d objects exist, want only the one in the way", objs)
			}
		})
	}

	t.Run("SynapseProxy", func(t *testing.T) {
		pe := newProxyEnv(t)
		pe.running()
		e := newAgentEnvIn(t, pe.ns, "edge")
		a := e.agent()
		e.reconcile(a)

		wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonNameCollision)
		var d appsv1.DaemonSet
		var s corev1.Secret
		if e.get("edge", &d) || e.get("edge-config", &s) {
			t.Error("the agent's objects were created beside a proxy of the same name")
		}
		var sa corev1.ServiceAccount
		e.get("edge", &sa)
		if len(sa.OwnerReferences) != 1 || sa.OwnerReferences[0].Kind != "SynapseProxy" {
			t.Errorf("the proxy's ServiceAccount was taken: %+v", sa.OwnerReferences)
		}
	})
}

func TestAgent_MissingAPIKeySecret(t *testing.T) {
	e := newAgentEnv(t)
	a := e.agent(withAPIKey)
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonSecretNotFound)
	if !strings.Contains(c.Message, `"creds"`) {
		t.Errorf("message: %s", c.Message)
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	e.wantNothingApplied()
	if !strings.Contains(e.events(), ReasonConfigInvalid) {
		t.Error("no event")
	}
}

func TestAgent_MissingAPIKeySecret_Key(t *testing.T) {
	e := newAgentEnv(t)
	e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"}, Data: map[string][]byte{"OTHER": []byte("x")}})
	a := e.agent(withAPIKey)
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonSecretKeyNotFound)
	if !strings.Contains(c.Message, `"API_KEY"`) {
		t.Errorf("message: %s", c.Message)
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	e.wantNothingApplied()
}

func TestAgent_InvalidRawConfig(t *testing.T) {
	e := newAgentEnv(t)
	a := e.agent(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(`{"mode":"proxy"}`)}
	})
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonInvalidConfig)
	if !strings.Contains(c.Message, "mode") {
		t.Errorf("message does not carry the render's error: %s", c.Message)
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	e.wantNothingApplied()
	if !strings.Contains(e.events(), ReasonConfigInvalid) {
		t.Error("no event")
	}

	// Once running, an invalid edit leaves what runs alone.
	e = newAgentEnv(t)
	a = e.running()
	secret, ds := e.configSecret(), e.daemonSet()
	hash := a.Status.ConfigHash
	e.edit(a, func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(`{"mode":"proxy"}`)}
	})
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonInvalidConfig)
	if e.configSecret().ResourceVersion != secret.ResourceVersion || e.daemonSet().ResourceVersion != ds.ResourceVersion {
		t.Error("the running objects were touched")
	}
	if a.Status.ConfigHash != hash {
		t.Errorf("configHash = %q, want the running config's %q", a.Status.ConfigHash, hash)
	}
}

func TestAgent_Gone(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	if !agentReadySeries(t, e.ns, e.name) {
		t.Fatal("no synapse_operator_agent_ready series while the agent exists")
	}
	if err := e.k8s.Delete(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	e.reconcile(nil)
	if agentReadySeries(t, e.ns, e.name) {
		t.Error("the series outlived the agent")
	}
}

func managedByAs(o client.Object, manager string, op metav1.ManagedFieldsOperationType) bool {
	for _, f := range o.GetManagedFields() {
		if f.Manager == manager && f.Operation == op {
			return true
		}
	}
	return false
}
