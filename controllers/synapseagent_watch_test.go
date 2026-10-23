package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

func TestAgentNodePredicate(t *testing.T) {
	node := func(mutate func(*corev1.Node)) *corev1.Node {
		n := testNode("n", "192.0.2.1", "198.51.100.0/24")
		n.Labels = map[string]string{"a": "b"}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
		if mutate != nil {
			mutate(n)
		}
		return n
	}
	p := agentNodePredicate()
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Node)
		want   bool
	}{
		{"an address changed", func(n *corev1.Node) { n.Status.Addresses[0].Address = "192.0.2.2" }, true},
		{"an external address appeared", func(n *corev1.Node) {
			n.Status.Addresses = append(n.Status.Addresses, corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "203.0.113.5"})
		}, true},
		{"the pod CIDR changed", func(n *corev1.Node) { n.Spec.PodCIDRs = []string{"198.51.101.0/24"} }, true},
		{"only a label changed", func(n *corev1.Node) { n.Labels["a"] = "c" }, false},
		{"only the heartbeat changed", func(n *corev1.Node) {
			n.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(time.Now())
		}, false},
		{"only the hostname address changed", func(n *corev1.Node) {
			n.Status.Addresses = append(n.Status.Addresses, corev1.NodeAddress{Type: corev1.NodeHostName, Address: "n1"})
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: node(nil), ObjectNew: node(tc.mutate)}); got != tc.want {
				t.Errorf("Update = %v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: node(nil)}) || !p.Delete(event.DeleteEvent{Object: node(nil)}) {
		t.Error("a node with addresses joining or leaving is filtered out")
	}
	if p.Create(event.CreateEvent{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "bare"}}}) {
		t.Error("a node with no address and no pod CIDR passes")
	}
}

func TestAgentServicePredicate(t *testing.T) {
	svc := func(mutate func(*corev1.Service)) *corev1.Service {
		s := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "s", Labels: map[string]string{"a": "b"}},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, ClusterIP: "10.96.0.1"},
		}
		if mutate != nil {
			mutate(s)
		}
		return s
	}
	p := agentServicePredicate()
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Service)
		want   bool
	}{
		{"an ingress address arrived", func(s *corev1.Service) {
			s.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.1"}}
		}, true},
		{"externalIPs changed", func(s *corev1.Service) { s.Spec.ExternalIPs = []string{"203.0.113.2"} }, true},
		{"loadBalancerIP changed", func(s *corev1.Service) { s.Spec.LoadBalancerIP = "203.0.113.3" }, true},
		{"only a label changed", func(s *corev1.Service) { s.Labels["a"] = "c" }, false},
		{"only a port changed", func(s *corev1.Service) { s.Spec.Ports = []corev1.ServicePort{{Port: 81}} }, false},
		{"only the cluster IP changed", func(s *corev1.Service) { s.Spec.ClusterIP = "10.96.0.2" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: svc(nil), ObjectNew: svc(tc.mutate)}); got != tc.want {
				t.Errorf("Update = %v, want %v", got, tc.want)
			}
		})
	}
	withVIP := svc(func(s *corev1.Service) { s.Spec.ExternalIPs = []string{"203.0.113.2"} })
	if !p.Create(event.CreateEvent{Object: withVIP}) || !p.Delete(event.DeleteEvent{Object: withVIP}) {
		t.Error("a Service with a VIP appearing or going is filtered out")
	}
	if p.Create(event.CreateEvent{Object: svc(nil)}) {
		t.Error("a Service with no VIP passes")
	}
}

// Like TestProxy_FollowsItsInputs: the controller runs in a manager, because
// what it is about is what the watches deliver.
func TestAgent_FollowsItsInputs(t *testing.T) {
	cfg := apiServer(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 proxyTestScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&SynapseAgentReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	e := newAgentEnv(t)
	configHas := func(what string, wants ...string) {
		t.Helper()
		eventually(t, what, func() string {
			var s corev1.Secret
			if err := e.k8s.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "agent-config"}, &s); err != nil {
				return err.Error()
			}
			for _, w := range wants {
				if !strings.Contains(string(s.Data[agentConfigKey]), w) {
					return "config lacks " + w
				}
			}
			return ""
		})
	}

	a := e.agent(withAPIKey)
	eventually(t, "a missing Secret is reported", func() string {
		if err := e.k8s.Get(ctx, e.key(), a); err != nil {
			return err.Error()
		}
		if c := meta.FindStatusCondition(a.Status.Conditions, ConditionConfigValid); c == nil || c.Reason != ReasonSecretNotFound {
			return fmt.Sprintf("ConfigValid is %+v", c)
		}
		return ""
	})
	e.apiKey("k-1")
	configHas("the Secret's arrival renders the config", "k-1")

	var creds corev1.Secret
	e.get("creds", &creds)
	change(t, e.k8s, &creds, func() { creds.Data["API_KEY"] = []byte("k-2") })
	configHas("a rotated API key is re-rendered", "k-2")

	e.createNode(testNode("agent-follow-node", "192.0.2.201", "198.51.100.0/24"))
	configHas("a node joining is followed", "192.0.2.201/32")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "lb"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 80}}, ExternalIPs: []string{"203.0.113.77"}},
	}
	e.create(svc)
	configHas("a Service with a VIP is followed", "203.0.113.77/32")

	// What the controller owns is watched: the DaemonSet's status, and the
	// Secret and the ServiceAccount it applied.
	setDaemonSetStatus := func(updated int32) {
		var d appsv1.DaemonSet
		if err := e.k8s.Get(ctx, e.key(), &d); err != nil {
			t.Fatal(err)
		}
		changeStatus(t, e.k8s, &d, func() {
			d.Status = appsv1.DaemonSetStatus{ObservedGeneration: d.Generation, DesiredNumberScheduled: 2,
				CurrentNumberScheduled: 2, UpdatedNumberScheduled: updated, NumberReady: 2, NumberAvailable: 2}
		})
	}
	readyIs := func(status metav1.ConditionStatus, reason string) func() string {
		return func() string {
			if err := e.k8s.Get(ctx, e.key(), a); err != nil {
				return err.Error()
			}
			if c := meta.FindStatusCondition(a.Status.Conditions, ConditionReady); c == nil || c.Status != status || c.Reason != reason {
				return fmt.Sprintf("Ready is %+v", c)
			}
			return ""
		}
	}
	// The reconciles still queued by the steps above may set Ready on their
	// own. Settling first absorbs them; the step after it can then only be
	// the DaemonSet's own event.
	setDaemonSetStatus(2)
	eventually(t, "a settled DaemonSet is Ready", readyIs(metav1.ConditionTrue, ReasonApplied))
	setDaemonSetStatus(1)
	eventually(t, "a DaemonSet that stops being updated reaches the agent", readyIs(metav1.ConditionFalse, ReasonRollingOut))
	for name, obj := range map[string]client.Object{"agent-config": &corev1.Secret{}, "agent": &corev1.ServiceAccount{}} {
		key := client.ObjectKey{Namespace: e.ns, Name: name}
		if err := e.k8s.Get(ctx, key, obj); err != nil {
			t.Fatal(err)
		}
		old := obj.GetUID()
		if err := e.k8s.Delete(ctx, obj); err != nil {
			t.Fatal(err)
		}
		eventually(t, name+" is recreated after it is deleted", func() string {
			fresh := obj.DeepCopyObject().(client.Object)
			if err := e.k8s.Get(ctx, key, fresh); err != nil {
				return err.Error()
			}
			if fresh.GetUID() == old {
				return "still the deleted object"
			}
			return ""
		})
	}
}

func TestTakesHomeNetFromCluster(t *testing.T) {
	typed := &synapsev1alpha1.SynapseAgent{Spec: synapsev1alpha1.SynapseAgentSpec{IDS: &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8"}}}}
	empty := &synapsev1alpha1.SynapseAgent{Spec: synapsev1alpha1.SynapseAgentSpec{IDS: &synapsev1alpha1.AgentIDSSpec{}}}
	if takesHomeNetFromCluster(typed) {
		t.Error("an agent with a typed homeNet follows the cluster")
	}
	if !takesHomeNetFromCluster(empty) || !takesHomeNetFromCluster(&synapsev1alpha1.SynapseAgent{}) {
		t.Error("an agent without a typed homeNet does not follow the cluster")
	}
}
