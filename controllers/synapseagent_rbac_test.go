package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// What the operator is granted to run the SynapseAgent controller. As for the
// proxy's role, nothing generates the file and nothing in the code refers to
// it: these tests hold the two together, with the grants helpers of
// synapseproxy_rbac_test.go.
const agentRoleManifest = "../config/agent-controller/rbac.yaml"

// runAgentController starts the agent controller as whoever cfg acts as, as
// the operator registers it: on the whole cluster, or on the one namespace
// given. The returned function stops it and gives the manager's error.
func runAgentController(t *testing.T, cfg *rest.Config, namespace ...string) (stop func() error) {
	t.Helper()
	options := ctrl.Options{
		Scheme:                 proxyTestScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	}
	for _, ns := range namespace {
		options.Cache.DefaultNamespaces = map[string]cache.Config{ns: {}}
	}
	mgr, err := ctrl.NewManager(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetupAgentControllers(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	var once sync.Once
	var stopErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			stopErr = <-stopped
		})
		return stopErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// agentStory takes one SynapseAgent through everything the controller does
// for it. The test plays the rest of the cluster: the user, the DaemonSet
// controller. Each step ends by waiting for what the controller owes in
// return.
type agentStory struct {
	t        *testing.T
	ctx      context.Context
	k8s      client.Client
	ns       string
	operator *asked
}

func newAgentStory(t *testing.T, admin client.Client, operator *asked) *agentStory {
	return &agentStory{
		t: t, ctx: context.Background(), k8s: admin, operator: operator,
		ns: fmt.Sprintf("agent-rbac-%d", rbacTestRuns.Add(1)),
	}
}

// wait polls until done returns "", and reports true; or until the operator
// has been refused something, and reports false.
func (s *agentStory) wait(what string, done func() string) bool {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if len(s.operator.refusals()) > 0 {
			return false
		}
		reason := done()
		if reason == "" {
			return true
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%s: %s", what, reason)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *agentStory) create(obj client.Object) {
	s.t.Helper()
	if err := s.k8s.Create(s.ctx, obj); err != nil {
		s.t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

// events counts the events of one reason about the agent, repeats included.
func (s *agentStory) events(reason string) int32 {
	s.t.Helper()
	var list corev1.EventList
	if err := s.k8s.List(s.ctx, &list, client.InNamespace(s.ns)); err != nil {
		s.t.Fatal(err)
	}
	var n int32
	for _, e := range list.Items {
		if e.InvolvedObject.Kind == "SynapseAgent" && e.InvolvedObject.Name == "edge" && e.Reason == reason {
			n += max(e.Count, 1)
		}
	}
	return n
}

// run reports whether the story reached its end. It stops early, with false,
// as soon as the operator is refused anything.
func (s *agentStory) run() bool {
	s.t.Helper()
	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: s.ns, Name: name} }
	get := func(name string, into client.Object) string {
		if err := s.k8s.Get(s.ctx, key(name), into); err != nil {
			return err.Error()
		}
		return ""
	}
	s.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s.ns}})

	// What HOME_NET is made of.
	node := testNode(fmt.Sprintf("%s-node", s.ns), "192.0.2.211", "198.51.100.0/24")
	status := node.Status
	s.create(node)
	s.t.Cleanup(func() { _ = s.k8s.Delete(context.Background(), node) })
	changeStatus(s.t, s.k8s, node, func() { node.Status = status })
	s.create(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "lb"},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 80}},
			ExternalIPs: []string{"203.0.113.88"}},
	})

	// The API key is not there yet: the agent says so.
	agent := &synapsev1alpha1.SynapseAgent{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "edge"},
		Spec:       synapsev1alpha1.SynapseAgentSpec{Image: "example.test/synapse:1.0.0"},
	}
	withAPIKey(&agent.Spec)
	s.create(agent)
	if !s.wait("a missing API key Secret is reported", func() string {
		if reason := get("edge", agent); reason != "" {
			return reason
		}
		if c := meta.FindStatusCondition(agent.Status.Conditions, ConditionReady); c == nil || c.Reason != ReasonConfigInvalid {
			return fmt.Sprintf("Ready is %+v", c)
		}
		return ""
	}) {
		return false
	}

	// Its arrival renders the configuration and applies everything.
	s.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "creds"},
		Data: map[string][]byte{"API_KEY": []byte("k-1")}})
	configHas := func(want string) func() string {
		return func() string {
			var cfg corev1.Secret
			if reason := get("edge-config", &cfg); reason != "" {
				return reason
			}
			if !strings.Contains(string(cfg.Data[agentConfigKey]), want) {
				return "the configuration lacks " + want
			}
			return ""
		}
	}
	if !s.wait("the agent is applied", func() string {
		var ds appsv1.DaemonSet
		var sa corev1.ServiceAccount
		for name, obj := range map[string]client.Object{"edge": &ds, "edge-config": &corev1.Secret{}} {
			if reason := get(name, obj); reason != "" {
				return reason
			}
		}
		if reason := get("edge", &sa); reason != "" {
			return reason
		}
		return configHas("k-1")()
	}) {
		return false
	}

	// A rotated key is written to the Secret it was applied to, and rolls the
	// pods: the DaemonSet's template changes with it.
	var creds corev1.Secret
	var ds appsv1.DaemonSet
	if reason := get("creds", &creds); reason != "" {
		s.t.Fatal(reason)
	}
	if reason := get("edge", &ds); reason != "" {
		s.t.Fatal(reason)
	}
	rolled := ds.Generation
	change(s.t, s.k8s, &creds, func() { creds.Data["API_KEY"] = []byte("k-2") })
	if !s.wait("a rotated API key is re-rendered, and rolls the pods", func() string {
		if reason := configHas("k-2")(); reason != "" {
			return reason
		}
		if reason := get("edge", &ds); reason != "" {
			return reason
		}
		if ds.Generation <= rolled {
			return "the DaemonSet's template is the one it was"
		}
		return ""
	}) {
		return false
	}

	// The DaemonSet controller reports, and the agent follows. The same
	// state twice over is the same event twice over.
	setStatus := func(updated int32) {
		var ds appsv1.DaemonSet
		if err := s.k8s.Get(s.ctx, key("edge"), &ds); err != nil {
			s.t.Fatal(err)
		}
		changeStatus(s.t, s.k8s, &ds, func() {
			ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation, DesiredNumberScheduled: 2,
				CurrentNumberScheduled: 2, UpdatedNumberScheduled: updated, NumberReady: 2, NumberAvailable: 2}
		})
	}
	readyIs := func(status metav1.ConditionStatus, reason string) func() string {
		return func() string {
			if r := get("edge", agent); r != "" {
				return r
			}
			if c := meta.FindStatusCondition(agent.Status.Conditions, ConditionReady); c == nil || c.Status != status || c.Reason != reason {
				return fmt.Sprintf("Ready is %+v", c)
			}
			return ""
		}
	}
	for round := 0; round < 2; round++ {
		setStatus(2)
		if !s.wait("a settled DaemonSet is Ready", readyIs(metav1.ConditionTrue, ReasonApplied)) {
			return false
		}
		setStatus(1)
		if !s.wait("a DaemonSet that stops being updated is rolling out", readyIs(metav1.ConditionFalse, ReasonRollingOut)) {
			return false
		}
	}
	return s.wait("the events are raised, and repeated", func() string {
		switch {
		case s.events(ReasonConfigInvalid) == 0:
			return "no event about the missing key"
		case s.events(ReasonApplied) < 2:
			return "the event about the settled DaemonSet was not repeated"
		case s.events(ReasonRollingOut) < 2:
			return "the event about the rollout was not repeated"
		}
		return ""
	})
}

// The role is handed to the ServiceAccount the operator runs as.
func TestAgentRole_IsBoundToTheOperator(t *testing.T) {
	role, binding := loadRole(t, agentRoleManifest)
	var operator corev1.ServiceAccount
	if err := json.Unmarshal(readManifest(t, operatorRoleManifest)["ServiceAccount"], &operator); err != nil {
		t.Fatalf("%s: ServiceAccount: %v", operatorRoleManifest, err)
	}
	if got, want := binding.RoleRef, (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name}); got != want {
		t.Errorf("the binding refers to %+v, want %+v", got, want)
	}
	want := []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: operator.Namespace, Name: operator.Name}}
	if !slices.Equal(binding.Subjects, want) {
		t.Errorf("the binding is to %+v, want the operator's ServiceAccount %+v", binding.Subjects, want)
	}
}

// Holding the role and nothing else, the operator is refused nothing; and
// there is nothing it may read that it does not.
func TestAgentRole_IsEnoughToRunAnAgent(t *testing.T) {
	role, _ := loadRole(t, agentRoleManifest)
	own, binding := ownCopy(t, role)
	admin := rbacAdmin(t)
	cfg, operator := operatorHolding(t, admin, own, binding)
	stop := runAgentController(t, cfg)

	finished := newAgentStory(t, admin, operator).run()
	if err := stop(); err != nil {
		t.Errorf("manager: %v", err)
	}
	if got := operator.refusals(); len(got) > 0 {
		t.Fatalf("the operator was refused:\n  %s", strings.Join(got, "\n  "))
	}
	if !finished {
		t.Fatal("the story stopped early")
	}
	for _, g := range grantsOf(t, role) {
		if g.verb == readGrant && !operator.didRead(g) {
			t.Errorf("the operator may %s and never did", g)
		}
	}
}

// Each write the role grants is one the controller makes: with any one of
// them taken away, the story cannot end, and the only thing the operator is
// refused is that.
//
// The reads are not tried one by one: an informer refused a read keeps
// retrying, and a manager that cannot read a resource it indexes never
// finishes starting. They are checked by IsEnough, which fails on a missing
// one and on one never used.
func TestAgentRole_GrantsNoWriteItDoesNotUse(t *testing.T) {
	if testing.Short() {
		t.Skip("starts the controller once for every grant")
	}
	role, _ := loadRole(t, agentRoleManifest)
	admin := rbacAdmin(t)
	grants := grantsOf(t, role)
	for i, without := range grants {
		if without.verb == readGrant {
			continue
		}
		t.Run(without.String(), func(t *testing.T) {
			name := fmt.Sprintf("agent-role-%d", rbacTestRuns.Add(1))
			rest := slices.Delete(slices.Clone(grants), i, i+1)
			cfg, operator := operatorHolding(t, admin, roleOf(name, rest), bindingOf(name), without)
			// On the story's namespace only, so that what is refused is
			// refused for this agent and not for one another test left.
			story := newAgentStory(t, admin, operator)
			stop := runAgentController(t, cfg, story.ns)
			finished := story.run()
			if err := stop(); err != nil {
				t.Errorf("manager: %v", err)
			}
			if finished {
				t.Errorf("the operator did everything it does for an agent without being allowed to %s", without)
			}
			// With one grant gone, that grant is all it can be refused for.
			// The finalizers one is the exception: it is asked for when an
			// owned object is created, whichever object that is.
			resource, subresource, _ := strings.Cut(without.resource, "/")
			for _, refused := range operator.refusals() {
				t.Log("refused: " + refused)
				switch {
				case !strings.Contains(refused, "/"+story.ns+"/"):
					t.Errorf("refused %s, which is not this story's", refused)
				case subresource == "finalizers":
				case !strings.Contains(refused+"/", "/"+resource+"/") || !strings.HasSuffix(refused, subresource):
					t.Errorf("refused %s, which is not about %s", refused, without)
				}
			}
		})
	}
}
