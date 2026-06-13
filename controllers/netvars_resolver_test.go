package controllers

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/yaml"
)

// netvarsWith builds a fake-client-backed reconciler pre-loaded with the
// given Nodes + the source ConfigMap, so Reconcile can run without envtest.
func netvarsWith(t *testing.T, cm *corev1.ConfigMap, nodes ...corev1.Node) *NetVarsResolverReconciler {
	t.Helper()
	return netvarsWithObjs(t, cm, nil, nodes...)
}

// netvarsWithObjs is netvarsWith plus extra runtime objects (e.g. Services).
func netvarsWithObjs(t *testing.T, cm *corev1.ConfigMap, extra []runtime.Object, nodes ...corev1.Node) *NetVarsResolverReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	objs := []runtime.Object{cm}
	for i := range nodes {
		objs = append(objs, &nodes[i])
	}
	objs = append(objs, extra...)
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return &NetVarsResolverReconciler{Client: c, Scheme: scheme}
}

func lbService(ns, name, ingressIP string) *corev1.Service {
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
	}
	s.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: ingressIP}}
	return s
}

func node(name, internalIP, externalIP, podCIDR string) corev1.Node {
	n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if internalIP != "" {
		n.Status.Addresses = append(n.Status.Addresses, corev1.NodeAddress{
			Type: corev1.NodeInternalIP, Address: internalIP})
	}
	if externalIP != "" {
		n.Status.Addresses = append(n.Status.Addresses, corev1.NodeAddress{
			Type: corev1.NodeExternalIP, Address: externalIP})
	}
	if podCIDR != "" {
		n.Spec.PodCIDR = podCIDR
	}
	return n
}

func srcCM(data string, labels, annotations map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "synapse-agent-config",
			Namespace:   "synapse-os",
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string]string{SynapseConfigKey: data},
	}
}

// readBackHomeNet reconciles, fetches the updated CM, and returns the
// resulting HOME_NET / EXTERNAL_NET strings.
func readBackNetVars(t *testing.T, r *NetVarsResolverReconciler) (string, string) {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "synapse-os", Name: "synapse-agent-config"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out corev1.ConfigMap
	if err := r.Get(context.Background(), req.NamespacedName, &out); err != nil {
		t.Fatalf("get cm: %v", err)
	}
	var root map[string]any
	if err := yaml.Unmarshal([]byte(out.Data[SynapseConfigKey]), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	av := nestedMap(root, "ids", "address_vars")
	if av == nil {
		return "", ""
	}
	home, _ := av["HOME_NET"].(string)
	ext, _ := av["EXTERNAL_NET"].(string)
	return home, ext
}

const labelOn = NetVarsSourceLabel

func TestNetVars_AutoFromNodes(t *testing.T) {
	cm := srcCM("ids:\n  enabled: true\n", map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm,
		node("n1", "10.0.0.30", "203.0.113.5", "10.244.0.0/24"),
		node("n2", "10.0.0.31", "", "10.244.1.0/24"),
	)
	home, ext := readBackNetVars(t, r)

	for _, want := range []string{
		"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", // supernets
		"203.0.113.5/32",   // node external IP as /32
		"10.244.0.0/24", "10.244.1.0/24", // pod CIDRs
	} {
		if !strings.Contains(home, want) {
			t.Errorf("HOME_NET missing %q; got %q", want, home)
		}
	}
	if ext != "!$HOME_NET" {
		t.Errorf("EXTERNAL_NET = %q, want !$HOME_NET", ext)
	}
}

func TestNetVars_LoadBalancerIPIsHome(t *testing.T) {
	// A source-rewriting edge LB makes the agent see the LB VIP as source for
	// all ingress; it MUST be HOME (never bannable), not EXTERNAL.
	cm := srcCM("ids:\n  enabled: true\n", map[string]string{labelOn: "true"}, nil)
	r := netvarsWithObjs(t, cm,
		[]runtime.Object{lbService("synapse-os", "synapse-proxy-lb", "203.0.113.200")},
		node("n1", "10.0.0.30", "", "10.244.0.0/24"),
	)
	home, _ := readBackNetVars(t, r)
	if !strings.Contains(home, "203.0.113.200/32") {
		t.Errorf("LoadBalancer VIP must be in HOME_NET; got %q", home)
	}
}

func TestNetVars_AnnotationOverride(t *testing.T) {
	cm := srcCM("ids:\n  enabled: true\n",
		map[string]string{labelOn: "true"},
		map[string]string{HomeNetOverrideAnnotation: "[10.1.2.0/24]"})
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "203.0.113.5", "10.244.0.0/24"))
	home, ext := readBackNetVars(t, r)
	if home != "[10.1.2.0/24]" {
		t.Errorf("annotation override ignored; HOME_NET=%q", home)
	}
	if ext != "!$HOME_NET" {
		t.Errorf("EXTERNAL_NET=%q", ext)
	}
}

func TestNetVars_ConfigManualRespected(t *testing.T) {
	// A non-"any" HOME_NET already in the config is treated as manual and
	// preserved verbatim; node auto-discovery is skipped.
	cm := srcCM("ids:\n  address_vars:\n    HOME_NET: \"[192.168.99.0/24]\"\n",
		map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "203.0.113.5", "10.244.0.0/24"))
	home, _ := readBackNetVars(t, r)
	if home != "[192.168.99.0/24]" {
		t.Errorf("manual HOME_NET not preserved; got %q", home)
	}
}

func TestNetVars_AnyIsAutoFilled(t *testing.T) {
	// HOME_NET: any means "not set" → auto-discovery kicks in.
	cm := srcCM("ids:\n  address_vars:\n    HOME_NET: any\n",
		map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "", "10.244.0.0/24"))
	home, _ := readBackNetVars(t, r)
	if home == "any" || !strings.Contains(home, "10.244.0.0/24") {
		t.Errorf("HOME_NET: any should be auto-filled; got %q", home)
	}
}

func TestNetVars_UnlabelledIgnored(t *testing.T) {
	cm := srcCM("ids:\n  address_vars:\n    HOME_NET: any\n", nil, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "", "10.244.0.0/24"))
	home, _ := readBackNetVars(t, r)
	if home != "any" {
		t.Errorf("unlabelled CM must not be modified; HOME_NET=%q", home)
	}
}

func TestNetVars_ExternalNetOverrideAnnotationAbsentDerives(t *testing.T) {
	cm := srcCM("ids:\n  enabled: true\n", map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "", ""))
	_, ext := readBackNetVars(t, r)
	if ext != "!$HOME_NET" {
		t.Errorf("EXTERNAL_NET should derive to !$HOME_NET; got %q", ext)
	}
}

func TestHostCIDR(t *testing.T) {
	if got := hostCIDR("10.0.0.1"); got != "10.0.0.1/32" {
		t.Errorf("v4 host cidr = %q", got)
	}
	if got := hostCIDR("fd00::1"); got != "fd00::1/128" {
		t.Errorf("v6 host cidr = %q", got)
	}
	if got := hostCIDR("not-an-ip"); got != "" {
		t.Errorf("invalid ip should be empty; got %q", got)
	}
}
