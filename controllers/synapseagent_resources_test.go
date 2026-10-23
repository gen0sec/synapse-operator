package controllers

import (
	"maps"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	kptr "k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// nodeAgent is the SynapseAgent the builder tests start from. Like the
// objects a fake client stores, it carries no CRD defaults.
func nodeAgent(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) *synapsev1alpha1.SynapseAgent {
	a := agentForConfig(mutate...)
	a.UID = "uid-agent"
	return a
}

func agentPodSpec(d *appsv1.DaemonSet) *corev1.PodSpec { return &d.Spec.Template.Spec }

func agentContainer(t *testing.T, d *appsv1.DaemonSet) *corev1.Container {
	t.Helper()
	if n := len(d.Spec.Template.Spec.Containers); n != 1 {
		t.Fatalf("%d containers, want 1", n)
	}
	return &d.Spec.Template.Spec.Containers[0]
}

func agentVolume(t *testing.T, d *appsv1.DaemonSet, name string) corev1.VolumeSource {
	t.Helper()
	for _, v := range d.Spec.Template.Spec.Volumes {
		if v.Name == name {
			return v.VolumeSource
		}
	}
	t.Fatalf("no volume %q", name)
	return corev1.VolumeSource{}
}

func agentMountAt(t *testing.T, c *corev1.Container, mountPath string) corev1.VolumeMount {
	t.Helper()
	for _, m := range c.VolumeMounts {
		if m.MountPath == mountPath {
			return m
		}
	}
	t.Fatalf("nothing mounted at %s; mounts: %+v", mountPath, c.VolumeMounts)
	return corev1.VolumeMount{}
}

func TestAgentDaemonSet_Pod(t *testing.T) {
	a := nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
		s.NodeSelector = map[string]string{"pool": "edge"}
		s.PriorityClassName = "system-node-critical"
		s.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}
		s.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
	})
	d := buildAgentDaemonSet(a, "rh")
	pod := agentPodSpec(d)
	c := agentContainer(t, d)

	if d.Name != "node-agent" || d.Namespace != "synapse-os" {
		t.Errorf("name %s/%s", d.Namespace, d.Name)
	}
	if want := map[string]string{"synapse.gen0sec.com/agent": "node-agent"}; !maps.Equal(d.Spec.Selector.MatchLabels, want) {
		t.Errorf("selector = %v, want %v", d.Spec.Selector.MatchLabels, want)
	}
	wantLabels := map[string]string{
		"synapse.gen0sec.com/agent":    "node-agent",
		"app.kubernetes.io/component":  "agent",
		"app.kubernetes.io/instance":   "node-agent",
		"app.kubernetes.io/managed-by": "synapse-operator",
	}
	for _, got := range []map[string]string{d.Labels, d.Spec.Template.Labels} {
		if !maps.Equal(got, wantLabels) {
			t.Errorf("labels = %v, want %v", got, wantLabels)
		}
	}

	if !pod.HostNetwork || !pod.HostPID {
		t.Errorf("hostNetwork=%v hostPID=%v, want both", pod.HostNetwork, pod.HostPID)
	}
	if pod.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Errorf("dnsPolicy = %q", pod.DNSPolicy)
	}
	if pod.EnableServiceLinks == nil || *pod.EnableServiceLinks {
		t.Errorf("enableServiceLinks = %v, want false", pod.EnableServiceLinks)
	}
	if pod.ServiceAccountName != "node-agent" || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Errorf("service account %q, automount %v", pod.ServiceAccountName, pod.AutomountServiceAccountToken)
	}
	if want := map[string]string{"pool": "edge", "kubernetes.io/os": "linux"}; !maps.Equal(pod.NodeSelector, want) {
		t.Errorf("nodeSelector = %v, want %v", pod.NodeSelector, want)
	}
	if a.Spec.NodeSelector["kubernetes.io/os"] != "" {
		t.Errorf("the spec's nodeSelector was modified: %v", a.Spec.NodeSelector)
	}
	if pod.PriorityClassName != "system-node-critical" || len(pod.ImagePullSecrets) != 1 || pod.Affinity == nil {
		t.Errorf("scheduling fields not passed: %+v", pod)
	}
	if sc := pod.SecurityContext; sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 0 {
		t.Errorf("pod security context = %+v, want runAsUser 0", sc)
	}

	if c.Image != "example.test/synapse:1.0.0" {
		t.Errorf("image = %q", c.Image)
	}
	if !slices.Equal(c.Args, []string{"--config", "/etc/synapse/config/config.yaml"}) {
		t.Errorf("args = %v", c.Args)
	}
	if _, ok := c.Resources.Requests[corev1.ResourceCPU]; !ok {
		t.Errorf("resources not passed: %+v", c.Resources)
	}
	sc := c.SecurityContext
	if sc == nil || sc.Privileged == nil || !*sc.Privileged {
		t.Fatalf("security context = %+v, want privileged", sc)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeUnconfined {
		t.Errorf("seccomp = %+v", sc.SeccompProfile)
	}
	if sc.AppArmorProfile == nil || sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
		t.Errorf("appArmor = %+v", sc.AppArmorProfile)
	}
	if want := []corev1.Capability{"SYS_ADMIN", "NET_ADMIN", "BPF", "SYS_RESOURCE"}; !slices.Equal(sc.Capabilities.Add, want) {
		t.Errorf("caps add = %v", sc.Capabilities.Add)
	}
	if want := []corev1.Capability{"ALL"}; !slices.Equal(sc.Capabilities.Drop, want) {
		t.Errorf("caps drop = %v", sc.Capabilities.Drop)
	}

	if len(c.Env) != 2 || c.Env[0].Name != "SYNAPSE_LOG_FORMAT" || c.Env[0].Value != "json" ||
		c.Env[1].Name != "SYNAPSE_NODE_IP" || c.Env[1].ValueFrom.FieldRef.FieldPath != "status.hostIP" {
		t.Errorf("env = %+v", c.Env)
	}
	if c.StartupProbe != nil || c.ReadinessProbe != nil || c.LivenessProbe != nil {
		t.Errorf("probes set: startup=%v readiness=%v liveness=%v", c.StartupProbe, c.ReadinessProbe, c.LivenessProbe)
	}
	if len(c.Ports) != 0 {
		t.Errorf("ports = %+v, an agent listens on nothing", c.Ports)
	}

	// The config is a whole directory, so kubelet refreshes it in place.
	cfg := agentMountAt(t, c, "/etc/synapse/config")
	if cfg.Name != "config" || cfg.SubPath != "" || !cfg.ReadOnly {
		t.Errorf("config mount = %+v", cfg)
	}
	if s := agentVolume(t, d, "config").Secret; s == nil || s.SecretName != "node-agent-config" {
		t.Errorf("config volume = %+v", s)
	}
	if data := agentMountAt(t, c, "/var/lib/synapse"); data.SubPath != "" || data.ReadOnly {
		t.Errorf("data mount = %+v", data)
	}
	if agentVolume(t, d, "data").EmptyDir == nil {
		t.Errorf("data is not an emptyDir")
	}
	hostPaths := []struct {
		path     string
		readOnly bool
		bidi     bool
	}{
		{"/sys/fs/bpf", false, true},
		{"/sys/kernel/debug", false, false},
		{"/sys/kernel/btf", true, false},
		{"/lib/modules", true, false},
	}
	for _, h := range hostPaths {
		m := agentMountAt(t, c, h.path)
		if m.ReadOnly != h.readOnly {
			t.Errorf("%s readOnly = %v, want %v", h.path, m.ReadOnly, h.readOnly)
		}
		if bidi := m.MountPropagation != nil && *m.MountPropagation == corev1.MountPropagationBidirectional; bidi != h.bidi {
			t.Errorf("%s bidirectional = %v, want %v", h.path, bidi, h.bidi)
		}
		hp := agentVolume(t, d, m.Name).HostPath
		if hp == nil || hp.Path != h.path || hp.Type == nil || *hp.Type != corev1.HostPathDirectoryOrCreate {
			t.Errorf("%s volume = %+v", h.path, hp)
		}
	}
	if len(pod.Volumes) != 6 || len(c.VolumeMounts) != 6 {
		t.Errorf("%d volumes and %d mounts, want 6 each", len(pod.Volumes), len(c.VolumeMounts))
	}
}

func TestAgentDaemonSet_NodeSelectorWithoutSpecSelector(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(), "rh")
	if want := map[string]string{"kubernetes.io/os": "linux"}; !maps.Equal(agentPodSpec(d).NodeSelector, want) {
		t.Errorf("nodeSelector = %v, want %v", agentPodSpec(d).NodeSelector, want)
	}
}

func TestAgentDaemonSet_RestartHashAnnotation(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(), "restart-hash")
	if got := d.Spec.Template.Annotations["synapse.gen0sec.com/restart-config-hash"]; got != "restart-hash" {
		t.Errorf("annotation = %q, want the restart hash", got)
	}
	if len(d.Spec.Template.Annotations) != 1 {
		t.Errorf("annotations = %v, want only the restart hash", d.Spec.Template.Annotations)
	}
	if len(d.Annotations) != 0 {
		t.Errorf("the DaemonSet itself is annotated: %v", d.Annotations)
	}
}

func TestAgentDaemonSet_UpdateStrategyDefault(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(), "rh")
	us := d.Spec.UpdateStrategy
	if us.Type != appsv1.RollingUpdateDaemonSetStrategyType || us.RollingUpdate == nil ||
		us.RollingUpdate.MaxUnavailable == nil || *us.RollingUpdate.MaxUnavailable != intstr.FromInt32(1) {
		t.Errorf("update strategy = %+v, want RollingUpdate with maxUnavailable 1", us)
	}
}

func TestAgentDaemonSet_UpdateStrategyOverride(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.UpdateStrategy = &appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
	}), "rh")
	if got := d.Spec.UpdateStrategy; got.Type != appsv1.OnDeleteDaemonSetStrategyType || got.RollingUpdate != nil {
		t.Errorf("update strategy = %+v, want OnDelete", got)
	}

	d = buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.UpdateStrategy = &appsv1.DaemonSetUpdateStrategy{
			Type:          appsv1.RollingUpdateDaemonSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDaemonSet{MaxUnavailable: kptr.To(intstr.FromString("25%"))},
		}
	}), "rh")
	if got := d.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable; *got != intstr.FromString("25%") {
		t.Errorf("maxUnavailable = %v, want 25%%", got)
	}
}

func TestAgentDaemonSet_HostPIDOff(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) { s.HostPID = kptr.To(false) }), "rh")
	if agentPodSpec(d).HostPID {
		t.Error("an explicit hostPID: false was turned on")
	}
	d = buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) { s.HostPID = kptr.To(true) }), "rh")
	if !agentPodSpec(d).HostPID {
		t.Error("an explicit hostPID: true was turned off")
	}
}

func TestAgentDaemonSet_HostPIDUnsetIsOn(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(), "rh")
	if !agentPodSpec(d).HostPID {
		t.Error("unset hostPID gives false, want true")
	}
}

func TestAgentDaemonSet_TolerationsUnsetToleratesEverything(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(), "rh")
	got := agentPodSpec(d).Tolerations
	if len(got) != 1 || got[0].Operator != corev1.TolerationOpExists || got[0].Key != "" || got[0].Effect != "" {
		t.Errorf("tolerations = %+v, want one bare Exists", got)
	}
}

func TestAgentDaemonSet_TolerationsEmptyStaysEmpty(t *testing.T) {
	d := buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Tolerations = []corev1.Toleration{}
	}), "rh")
	if got := agentPodSpec(d).Tolerations; len(got) != 0 {
		t.Errorf("tolerations = %+v, an explicit empty list means none", got)
	}
}

func TestAgentDaemonSet_TolerationsFromSpec(t *testing.T) {
	want := []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "edge", Effect: corev1.TaintEffectNoSchedule}}
	d := buildAgentDaemonSet(nodeAgent(func(s *synapsev1alpha1.SynapseAgentSpec) { s.Tolerations = want }), "rh")
	if got := agentPodSpec(d).Tolerations; !slices.Equal(got, want) {
		t.Errorf("tolerations = %+v, want %+v", got, want)
	}
}

func TestAgentConfigSecret_IsMutable(t *testing.T) {
	s := buildAgentConfigSecret(nodeAgent(), []byte("mode: agent\n"))
	if s.Immutable != nil && *s.Immutable {
		t.Error("the config Secret is immutable: a hot change could not be written in place")
	}
	if s.Name != "node-agent-config" || s.Namespace != "synapse-os" {
		t.Errorf("name %s/%s", s.Namespace, s.Name)
	}
	if string(s.Data["config.yaml"]) != "mode: agent\n" || len(s.Data) != 1 {
		t.Errorf("data = %v", s.Data)
	}
}

func TestAgentServiceAccount(t *testing.T) {
	sa := buildAgentServiceAccount(nodeAgent())
	if sa.Name != "node-agent" || sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Errorf("service account = %s, automount %v", sa.Name, sa.AutomountServiceAccountToken)
	}
}

func agentObjects(a *synapsev1alpha1.SynapseAgent) []client.Object {
	return []client.Object{
		buildAgentConfigSecret(a, []byte("x")),
		buildAgentServiceAccount(a),
		buildAgentDaemonSet(a, "rh"),
	}
}

func TestAgentObjects_OwnerRefs(t *testing.T) {
	a := nodeAgent()
	for _, obj := range agentObjects(a) {
		ref := metav1.GetControllerOf(obj)
		if ref == nil {
			t.Errorf("%T %s has no controller owner reference", obj, obj.GetName())
			continue
		}
		if ref.Kind != "SynapseAgent" || ref.Name != "node-agent" || ref.UID != "uid-agent" ||
			ref.APIVersion != "synapse.gen0sec.com/v1alpha1" ||
			ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
			t.Errorf("%T %s owner = %+v", obj, obj.GetName(), ref)
		}
		if !metav1.IsControlledBy(obj, a) {
			t.Errorf("%T %s is not controlled by the agent", obj, obj.GetName())
		}
	}
}

// The Helm chart's workloads and the operator's config controller select by
// app.kubernetes.io/name=synapse, the operator's default --label-selector
// (main.go, README "Flags"). The chart is not vendored here, so its agent
// selector labels are taken to be that label plus an instance label.
func TestAgentObjects_DoNotMatchLegacySelectors(t *testing.T) {
	a := nodeAgent()
	selectors := []labels.Selector{
		labels.SelectorFromSet(labels.Set{"app.kubernetes.io/name": "synapse"}),
		labels.SelectorFromSet(labels.Set{"app.kubernetes.io/name": "synapse", "app.kubernetes.io/instance": "node-agent"}),
		labels.SelectorFromSet(labels.Set{"app.kubernetes.io/name": "synapse", "app.kubernetes.io/component": "agent"}),
	}
	podLabels := labels.Set(buildAgentDaemonSet(a, "rh").Spec.Template.Labels)
	for _, obj := range agentObjects(a) {
		if _, ok := obj.GetLabels()["app.kubernetes.io/name"]; ok {
			t.Errorf("%T %s carries app.kubernetes.io/name", obj, obj.GetName())
		}
		for _, sel := range selectors {
			if sel.Matches(labels.Set(obj.GetLabels())) {
				t.Errorf("%q matches %T %s", sel, obj, obj.GetName())
			}
		}
	}
	for _, sel := range selectors {
		if sel.Matches(podLabels) {
			t.Errorf("%q matches the pod labels", sel)
		}
	}
}

func TestOwnedByManagedResource_SynapseAgent(t *testing.T) {
	for _, obj := range agentObjects(nodeAgent()) {
		if !ownedByManagedResource(obj) {
			t.Errorf("%T %s controlled by a SynapseAgent is not left alone by the ConfigMap controller", obj, obj.GetName())
		}
	}
}
