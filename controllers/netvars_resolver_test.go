package controllers

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
	if err := discoveryv1.AddToScheme(scheme); err != nil {
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

// readBackHomeNetFrom extracts HOME_NET/EXTERNAL_NET from an already-fetched CM.
func readBackHomeNetFrom(t *testing.T, cm *corev1.ConfigMap) (string, string) {
	t.Helper()
	var root map[string]any
	if err := yaml.Unmarshal([]byte(cm.Data[SynapseConfigKey]), &root); err != nil {
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
		"203.0.113.5/32",                 // node external IP as /32
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

func TestNetVars_AutoIsNotSticky_ReDerivesOnNodeChange(t *testing.T) {
	// Regression for bug A: after auto-fill writes a concrete HOME_NET, a later
	// reconcile must keep re-deriving (it must not mistake its own value for a
	// manual pin). The netvars-managed=auto marker disambiguates.
	cm := srcCM("ids:\n  enabled: true\n", map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "203.0.113.5", "10.244.0.0/24"))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "synapse-os", Name: "synapse-agent-config"}}

	// First reconcile: auto-fill.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	var after1 corev1.ConfigMap
	_ = r.Get(context.Background(), req.NamespacedName, &after1)
	if after1.Annotations[NetVarsManagedAnnotation] != managedAuto {
		t.Fatalf("expected netvars-managed=auto, got %q", after1.Annotations[NetVarsManagedAnnotation])
	}
	home1, _ := readBackHomeNetFrom(t, &after1)
	if !strings.Contains(home1, "203.0.113.5/32") {
		t.Fatalf("first auto fill missing node IP: %q", home1)
	}

	// Add a node, reconcile again: must re-derive and include the new node IP.
	n2 := node("n2", "10.0.0.31", "198.51.100.9", "10.244.1.0/24")
	if err := r.Create(context.Background(), &n2); err != nil {
		t.Fatalf("create node: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	var after2 corev1.ConfigMap
	_ = r.Get(context.Background(), req.NamespacedName, &after2)
	home2, _ := readBackHomeNetFrom(t, &after2)
	if !strings.Contains(home2, "198.51.100.9/32") {
		t.Errorf("auto did not re-derive on node change (sticky bug); HOME_NET=%q", home2)
	}
}

func TestNetVars_GenuineManualRespected_NoMarker(t *testing.T) {
	// A concrete HOME_NET with NO managed marker is a real user pin -> verbatim,
	// and the operator records managed=manual.
	cm := srcCM("ids:\n  address_vars:\n    HOME_NET: \"[192.168.99.0/24]\"\n",
		map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("n1", "10.0.0.30", "203.0.113.5", "10.244.0.0/24"))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "synapse-os", Name: "synapse-agent-config"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var out corev1.ConfigMap
	_ = r.Get(context.Background(), req.NamespacedName, &out)
	home, _ := readBackHomeNetFrom(t, &out)
	if home != "[192.168.99.0/24]" {
		t.Errorf("manual HOME_NET not preserved: %q", home)
	}
	if out.Annotations[NetVarsManagedAnnotation] != managedManual {
		t.Errorf("expected managed=manual, got %q", out.Annotations[NetVarsManagedAnnotation])
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

const cpLabel = "node-role.kubernetes.io/control-plane"

func kubernetesSvc(clusterIP string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "kubernetes", Namespace: "default"},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP},
	}
}

func kubernetesSlice(addrs ...string) *discoveryv1.EndpointSlice {
	eps := make([]discoveryv1.Endpoint, 0, len(addrs))
	for _, a := range addrs {
		eps = append(eps, discoveryv1.Endpoint{Addresses: []string{a}})
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kubernetes", Namespace: "default",
			Labels: map[string]string{discoveryv1.LabelServiceName: "kubernetes"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   eps,
	}
}

func cpNode(name, ip string) corev1.Node {
	n := node(name, ip, "", "")
	n.Labels = map[string]string{cpLabel: ""}
	return n
}

func readBackAddressVar(t *testing.T, r *NetVarsResolverReconciler, key string) (string, bool) {
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
	v, ok := nestedMap(root, "ids", "address_vars")[key].(string)
	return v, ok
}

func TestNetVars_ControlPlaneNet_ServiceEndpointsAndCPNodes(t *testing.T) {
	cm := srcCM("", map[string]string{labelOn: "true"}, nil)
	r := netvarsWithObjs(t, cm,
		[]runtime.Object{kubernetesSvc("10.96.0.1"), kubernetesSlice("192.0.2.10", "192.0.2.11")},
		cpNode("cp1", "10.0.0.5"), node("w1", "10.0.0.30", "", ""))
	got, ok := readBackAddressVar(t, r, "CONTROL_PLANE_NET")
	if !ok {
		t.Fatal("CONTROL_PLANE_NET absent")
	}
	for _, want := range []string{"10.96.0.1/32", "192.0.2.10/32", "192.0.2.11/32", "10.0.0.5/32"} {
		if !strings.Contains(got, want) {
			t.Errorf("CONTROL_PLANE_NET %q missing %s", got, want)
		}
	}
	if strings.Contains(got, "10.0.0.30") {
		t.Errorf("worker node leaked into CONTROL_PLANE_NET: %q", got)
	}
}

func TestNetVars_ControlPlaneNet_ManualSurvives(t *testing.T) {
	cm := srcCM("ids:\n  address_vars:\n    CONTROL_PLANE_NET: \"[198.51.100.0/24]\"\n",
		map[string]string{labelOn: "true"}, nil)
	r := netvarsWithObjs(t, cm, []runtime.Object{kubernetesSvc("10.96.0.1")}, cpNode("cp1", "10.0.0.5"))
	got, _ := readBackAddressVar(t, r, "CONTROL_PLANE_NET")
	if got != "[198.51.100.0/24]" {
		t.Errorf("manual CONTROL_PLANE_NET not preserved: %q", got)
	}
	// A second reconcile must not adopt it as auto either.
	got, _ = readBackAddressVar(t, r, "CONTROL_PLANE_NET")
	if got != "[198.51.100.0/24]" {
		t.Errorf("manual CONTROL_PLANE_NET lost on second reconcile: %q", got)
	}
}

func TestNetVars_ControlPlaneNet_AutoReDerives(t *testing.T) {
	cm := srcCM("", map[string]string{labelOn: "true"}, nil)
	r := netvarsWithObjs(t, cm, []runtime.Object{kubernetesSvc("10.96.0.1")})
	got, _ := readBackAddressVar(t, r, "CONTROL_PLANE_NET")
	if !strings.Contains(got, "10.96.0.1/32") {
		t.Fatalf("first derive: %q", got)
	}
	n := cpNode("cp2", "10.0.0.6")
	if err := r.Create(context.Background(), &n); err != nil {
		t.Fatal(err)
	}
	got, _ = readBackAddressVar(t, r, "CONTROL_PLANE_NET")
	if !strings.Contains(got, "10.0.0.6/32") {
		t.Errorf("auto value not re-derived after a control-plane node joined: %q", got)
	}
}

func TestNetVars_ControlPlaneNet_NoKubernetesService(t *testing.T) {
	cm := srcCM("", map[string]string{labelOn: "true"}, nil)
	r := netvarsWith(t, cm, node("w1", "10.0.0.30", "", ""))
	if got, ok := readBackAddressVar(t, r, "CONTROL_PLANE_NET"); ok {
		t.Errorf("CONTROL_PLANE_NET should be absent, got %q", got)
	}
}

func reconcileCM(t *testing.T, r *NetVarsResolverReconciler) corev1.ConfigMap {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "synapse-os", Name: "synapse-agent-config"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out corev1.ConfigMap
	if err := r.Get(context.Background(), req.NamespacedName, &out); err != nil {
		t.Fatalf("get cm: %v", err)
	}
	return out
}

func cpNetOf(t *testing.T, cm *corev1.ConfigMap) (string, bool) {
	t.Helper()
	var root map[string]any
	if err := yaml.Unmarshal([]byte(cm.Data[SynapseConfigKey]), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	v, ok := nestedMap(root, "ids", "address_vars")["CONTROL_PLANE_NET"].(string)
	return v, ok
}

func updateCM(t *testing.T, r *NetVarsResolverReconciler, cm *corev1.ConfigMap) {
	t.Helper()
	if err := r.Update(context.Background(), cm); err != nil {
		t.Fatalf("update cm: %v", err)
	}
}

func autoCPFixture(t *testing.T) (*NetVarsResolverReconciler, corev1.ConfigMap) {
	t.Helper()
	cm := srcCM("", map[string]string{labelOn: "true"}, nil)
	r := netvarsWithObjs(t, cm, []runtime.Object{kubernetesSvc("10.96.0.1")})
	out := reconcileCM(t, r)
	if out.Annotations[ControlPlaneNetManagedAnnotation] != managedAuto {
		t.Fatalf("fixture: expected auto marker, got %q", out.Annotations[ControlPlaneNetManagedAnnotation])
	}
	return r, out
}

func setCPNet(t *testing.T, cm *corev1.ConfigMap, value string) {
	t.Helper()
	cm.Data[SynapseConfigKey] = "ids:\n  address_vars:\n    CONTROL_PLANE_NET: \"" + value + "\"\n"
}

func TestNetVars_ControlPlaneNet_EditWhileAutoIsOverwritten(t *testing.T) {
	r, cm := autoCPFixture(t)
	setCPNet(t, &cm, "[198.51.100.0/24]")
	updateCM(t, r, &cm)
	out := reconcileCM(t, r)
	got, _ := cpNetOf(t, &out)
	if got != "[10.96.0.1/32]" {
		t.Errorf("edit under the auto marker should be re-derived, got %q", got)
	}
}

func TestNetVars_ControlPlaneNet_OverrideAnnotationKept(t *testing.T) {
	r, cm := autoCPFixture(t)
	cm.Annotations[ControlPlaneNetOverrideAnnotation] = "[198.51.100.0/24]"
	updateCM(t, r, &cm)
	out := reconcileCM(t, r)
	got, _ := cpNetOf(t, &out)
	if got != "[198.51.100.0/24]" {
		t.Errorf("override annotation not applied: %q", got)
	}
	if _, ok := out.Annotations[ControlPlaneNetManagedAnnotation]; ok {
		t.Errorf("auto marker should be dropped while pinned")
	}
	got, _ = cpNetOf(t, func() *corev1.ConfigMap { o := reconcileCM(t, r); return &o }())
	if got != "[198.51.100.0/24]" {
		t.Errorf("override lost on second reconcile: %q", got)
	}
}

func TestNetVars_ControlPlaneNet_ClearedIsReDerived(t *testing.T) {
	r, cm := autoCPFixture(t)
	cm.Data[SynapseConfigKey] = "ids:\n  address_vars:\n    CONTROL_PLANE_NET: \"\"\n"
	updateCM(t, r, &cm)
	out := reconcileCM(t, r)
	if got, _ := cpNetOf(t, &out); got != "[10.96.0.1/32]" {
		t.Errorf("cleared value should be re-derived, got %q", got)
	}
}

func TestNetVars_ControlPlaneNet_RemovingManagedAnnotationPins(t *testing.T) {
	r, cm := autoCPFixture(t)
	delete(cm.Annotations, ControlPlaneNetManagedAnnotation)
	updateCM(t, r, &cm)
	out := reconcileCM(t, r)
	if got, _ := cpNetOf(t, &out); got != "[10.96.0.1/32]" {
		t.Fatalf("pinned value changed: %q", got)
	}
	// A cluster change must not move the pin.
	n := cpNode("cp2", "10.0.0.6")
	if err := r.Create(context.Background(), &n); err != nil {
		t.Fatal(err)
	}
	out = reconcileCM(t, r)
	if got, _ := cpNetOf(t, &out); got != "[10.96.0.1/32]" {
		t.Errorf("pinned value was re-derived: %q", got)
	}
	if _, ok := out.Annotations[ControlPlaneNetManagedAnnotation]; ok {
		t.Errorf("auto marker came back on a pinned value")
	}
}

func TestNetVars_ControlPlaneNet_ServiceDisappearsRemovesAuto(t *testing.T) {
	r, _ := autoCPFixture(t)
	if err := r.Delete(context.Background(), kubernetesSvc("10.96.0.1")); err != nil {
		t.Fatal(err)
	}
	out := reconcileCM(t, r)
	if got, ok := cpNetOf(t, &out); ok {
		t.Errorf("auto value should be removed, got %q", got)
	}
	if _, ok := out.Annotations[ControlPlaneNetManagedAnnotation]; ok {
		t.Errorf("auto marker should be removed")
	}
}

func TestNetVars_ControlPlaneNet_IPv6Rendered(t *testing.T) {
	cm := srcCM("", map[string]string{labelOn: "true"}, nil)
	slice := kubernetesSlice("2001:db8::10")
	slice.AddressType = discoveryv1.AddressTypeIPv6
	n := cpNode("cp1", "2001:db8::5")
	r := netvarsWithObjs(t, cm, []runtime.Object{kubernetesSvc("fd00::1"), slice}, n)
	out := reconcileCM(t, r)
	got, _ := cpNetOf(t, &out)
	for _, want := range []string{"fd00::1/128", "2001:db8::10/128", "2001:db8::5/128"} {
		if !strings.Contains(got, want) {
			t.Errorf("CONTROL_PLANE_NET %q missing %s", got, want)
		}
	}
}

func TestKubernetesSlicePredicate(t *testing.T) {
	p := kubernetesSlicePredicate()
	mk := func(ns, svc string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: "x", Labels: map[string]string{discoveryv1.LabelServiceName: svc}}}
	}
	if !p.Generic(eventGeneric(mk("default", "kubernetes"))) {
		t.Error("default/kubernetes slice should pass")
	}
	for _, o := range []*discoveryv1.EndpointSlice{mk("other", "kubernetes"), mk("default", "api")} {
		if p.Generic(eventGeneric(o)) {
			t.Errorf("slice %s/%s should be filtered", o.Namespace, o.Labels[discoveryv1.LabelServiceName])
		}
	}
}

func eventGeneric(o *discoveryv1.EndpointSlice) event.GenericEvent {
	return event.GenericEvent{Object: o}
}
