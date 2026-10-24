package controllers

import (
	"path"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kptr "k8s.io/utils/ptr"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The objects a SynapseAgent owns: the rendered config, a ServiceAccount and
// the DaemonSet. An agent has no Service, since it listens on nothing.

const (
	// agentLabel carries the agent's name on everything it owns. It is the
	// whole pod selector, which can never change once the DaemonSet exists.
	agentLabel = "synapse.gen0sec.com/agent"

	// agentRestartHashAnnotation holds the hash of the config minus the
	// settings Synapse reloads on its own. Only a change to the rest rolls
	// the pods.
	agentRestartHashAnnotation = "synapse.gen0sec.com/restart-config-hash"

	agentConfigDir = "/etc/synapse/config"
	agentConfigKey = "config.yaml"
	// agentDataDir is Synapse's working directory; see agentconfig_defaults.yaml.
	agentDataDir = "/var/lib/synapse"
)

// agentLabels are set on every object an agent owns. app.kubernetes.io/name is
// left out on purpose: the Helm agent and the config controller select by it,
// and must not pick these up.
func agentLabels(a *synapsev1alpha1.SynapseAgent) map[string]string {
	return map[string]string{
		agentLabel:                     a.Name,
		"app.kubernetes.io/instance":   a.Name,
		"app.kubernetes.io/component":  "agent",
		"app.kubernetes.io/managed-by": "synapse-operator",
	}
}

func agentSelector(a *synapsev1alpha1.SynapseAgent) map[string]string {
	return map[string]string{agentLabel: a.Name}
}

func agentObjectMeta(a *synapsev1alpha1.SynapseAgent, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: a.Namespace,
		Name:      name,
		Labels:    agentLabels(a),
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(a, synapsev1alpha1.GroupVersion.WithKind("SynapseAgent")),
		},
	}
}

// agentConfigSecretName is the one config Secret of an agent. Its name does
// not follow the content: a hot change is written in place and kubelet
// refreshes the mounted files, so the pods are not replaced.
func agentConfigSecretName(a *synapsev1alpha1.SynapseAgent) string {
	return a.Name + "-config"
}

// buildAgentConfigSecret holds the rendered config. It is mutable on purpose.
func buildAgentConfigSecret(a *synapsev1alpha1.SynapseAgent, yaml []byte) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: agentObjectMeta(a, agentConfigSecretName(a)),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{agentConfigKey: yaml},
	}
}

func buildAgentServiceAccount(a *synapsev1alpha1.SynapseAgent) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: agentObjectMeta(a, a.Name),
		// The agent never talks to the Kubernetes API.
		AutomountServiceAccountToken: kptr.To(false),
	}
}

// buildAgentDaemonSet builds the DaemonSet that runs the agent on every
// selected node. restartHash is the RestartHash of the rendered config.
//
// The pod runs as the chart ran it: root and privileged, on the host network,
// because packet capture, the kernel firewall and the IDS need it. It has no
// probes, since an agent serves nothing to probe.
func buildAgentDaemonSet(a *synapsev1alpha1.SynapseAgent, restartHash string) *appsv1.DaemonSet {
	spec := &a.Spec

	container := corev1.Container{
		Name:  "synapse",
		Image: spec.Image,
		Args:  []string{"--config", path.Join(agentConfigDir, agentConfigKey)},
		// Synapse reads un-prefixed environment variables as overrides of
		// the config file, so nothing else belongs here.
		Env: []corev1.EnvVar{
			{Name: "SYNAPSE_LOG_FORMAT", Value: "json"},
			{Name: "SYNAPSE_NODE_IP", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"},
			}},
		},
		Resources: spec.Resources,
		SecurityContext: &corev1.SecurityContext{
			Privileged:      kptr.To(true),
			SeccompProfile:  &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined},
			AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined},
			Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"SYS_ADMIN", "NET_ADMIN", "BPF", "SYS_RESOURCE"},
				Drop: []corev1.Capability{"ALL"},
			},
		},
		// Whole directories, never a subPath: a subPath mount is not
		// refreshed, so a hot change would never reach the pod.
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: agentConfigDir, ReadOnly: true},
			{Name: "data", MountPath: agentDataDir},
			{Name: "bpffs", MountPath: "/sys/fs/bpf", MountPropagation: kptr.To(corev1.MountPropagationBidirectional)},
			{Name: "debugfs", MountPath: "/sys/kernel/debug"},
			{Name: "modules", MountPath: "/lib/modules", ReadOnly: true},
		},
	}

	hostPath := func(name, p string) corev1.Volume {
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: p,
			Type: kptr.To(corev1.HostPathDirectoryOrCreate),
		}}}
	}

	return &appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
		ObjectMeta: agentObjectMeta(a, a.Name),
		Spec: appsv1.DaemonSetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: agentSelector(a)},
			UpdateStrategy: agentUpdateStrategy(spec),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      agentLabels(a),
					Annotations: map[string]string{agentRestartHashAnnotation: restartHash},
				},
				Spec: corev1.PodSpec{
					HostNetwork: agentHostNetwork(spec),
					HostPID:     agentHostPID(spec),
					DNSPolicy:   agentDNSPolicy(spec),

					ServiceAccountName:           a.Name,
					AutomountServiceAccountToken: kptr.To(false),
					// A Service named `internal-services` would otherwise
					// inject INTERNAL_SERVICES_PORT, which Synapse reads.
					EnableServiceLinks: kptr.To(false),
					ImagePullSecrets:   spec.ImagePullSecrets,
					NodeSelector:       agentNodeSelector(spec),
					Tolerations:        agentTolerations(spec),
					Affinity:           spec.Affinity,
					PriorityClassName:  spec.PriorityClassName,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:    kptr.To(int64(0)),
						RunAsGroup:   kptr.To(int64(0)),
						RunAsNonRoot: kptr.To(false),
					},
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: agentConfigSecretName(a)},
						}},
						{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						hostPath("bpffs", "/sys/fs/bpf"),
						hostPath("debugfs", "/sys/kernel/debug"),
						hostPath("modules", "/lib/modules"),
					},
				},
			},
		},
	}
}

// agentHostPID is true unless the spec says otherwise: the agent sees the
// processes of its node by default.
func agentHostPID(spec *synapsev1alpha1.SynapseAgentSpec) bool {
	return spec.HostPID == nil || *spec.HostPID
}

// agentHostNetwork is true unless the spec says otherwise: the agent sees the
// traffic of its node by default.
func agentHostNetwork(spec *synapsev1alpha1.SynapseAgentSpec) bool {
	return spec.HostNetwork == nil || *spec.HostNetwork
}

// agentDNSPolicy keeps cluster DNS working for a pod on the host network,
// which would otherwise resolve through the node.
func agentDNSPolicy(spec *synapsev1alpha1.SynapseAgentSpec) corev1.DNSPolicy {
	if agentHostNetwork(spec) {
		return corev1.DNSClusterFirstWithHostNet
	}
	return ""
}

// agentTolerations tolerates every taint when the spec sets none. An empty
// list that the spec does set means no toleration at all, so only nil is
// unset. The API server applies the same default; a client that does not
// would otherwise leave the agent off tainted nodes.
func agentTolerations(spec *synapsev1alpha1.SynapseAgentSpec) []corev1.Toleration {
	if spec.Tolerations == nil {
		return []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	}
	return spec.Tolerations
}

// agentNodeSelector is the spec's selector plus the Linux requirement.
func agentNodeSelector(spec *synapsev1alpha1.SynapseAgentSpec) map[string]string {
	sel := make(map[string]string, len(spec.NodeSelector)+1)
	for k, v := range spec.NodeSelector {
		sel[k] = v
	}
	sel["kubernetes.io/os"] = "linux"
	return sel
}

// agentUpdateStrategy replaces the pods one node at a time unless the spec
// chooses another strategy.
func agentUpdateStrategy(spec *synapsev1alpha1.SynapseAgentSpec) appsv1.DaemonSetUpdateStrategy {
	if spec.UpdateStrategy != nil {
		return *spec.UpdateStrategy.DeepCopy()
	}
	return appsv1.DaemonSetUpdateStrategy{
		Type: appsv1.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDaemonSet{
			MaxUnavailable: kptr.To(intstr.FromInt32(1)),
		},
	}
}
