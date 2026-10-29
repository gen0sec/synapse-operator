package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SynapseAgent is a Synapse agent run by the operator on every node it
// selects. The operator renders its configuration and creates the DaemonSet
// for it.
//
// The name is capped below the 63-character limit of a label value to leave
// room for the suffixes of the objects derived from it.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=synagent,categories=synapse
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.status.desiredNumberScheduled`
// +kubebuilder:printcolumn:name="Ready-Pods",type=integer,JSONPath=`.status.numberReady`
// +kubebuilder:printcolumn:name="Updated",type=integer,JSONPath=`.status.updatedNumberScheduled`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`,priority=1
// +kubebuilder:printcolumn:name="Config",type=string,JSONPath=`.status.configHash`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 48",message="metadata.name must be at most 48 characters"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a DNS-1035 label: lower-case letters, digits and '-', starting with a letter"
type SynapseAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of the agent.
	Spec   SynapseAgentSpec   `json:"spec"`
	Status SynapseAgentStatus `json:"status,omitempty"`
}

// SynapseAgentList is a list of SynapseAgent.
//
// +kubebuilder:object:root=true
type SynapseAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SynapseAgent `json:"items"`
}

// SynapseAgentSpec is the desired state of a SynapseAgent.
type SynapseAgentSpec struct {
	// Image is the Synapse container image, tag or digest included.
	//
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// ImagePullSecrets are the Secrets used to pull Image.
	//
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Resources are the compute resources of the agent container.
	//
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// NodeSelector restricts the nodes the agent pods run on. The agent runs
	// on Linux nodes only, so the kubernetes.io/os label, when present, has
	// to be linux.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="!('kubernetes.io/os' in self) || self['kubernetes.io/os'] == 'linux'",message="agents run on Linux nodes only"
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations are the taints the agent pods tolerate. Unset, the agent
	// tolerates every taint, so it runs on every selected node. An explicit
	// empty list means it tolerates none.
	//
	// +optional
	// +kubebuilder:default={{operator: Exists}}
	Tolerations []corev1.Toleration `json:"tolerations"`

	// Affinity is the scheduling affinity of the agent pods.
	//
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PriorityClassName is the priority class of the agent pods.
	//
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// UpdateStrategy is the update strategy of the DaemonSet. Empty means a
	// rolling update that takes one node down at a time.
	//
	// +optional
	UpdateStrategy *appsv1.DaemonSetUpdateStrategy `json:"updateStrategy,omitempty"`

	// HostPID runs the agent pods in the process namespace of their node, so
	// the agent can see the host's processes. Unset means true on Linux.
	//
	// +optional
	HostPID *bool `json:"hostPID,omitempty"`

	// HostNetwork runs the agent pods in the network namespace of their node.
	// Unset means true. With false the pod has its own network namespace and
	// no bpffs of the node, so it inspects and protects only the traffic of
	// its own pod. XDP does not attach there: capture uses AF_PACKET and the
	// firewall nftables or iptables. Switching an existing agent leaves the
	// earlier pod's XDP link, tc programs and pinned maps on the node; the
	// README says how to remove them.
	//
	// +optional
	HostNetwork *bool `json:"hostNetwork,omitempty"`

	// OS is the operating system of the nodes the agent runs on.
	//
	// +optional
	// +kubebuilder:default=linux
	// +kubebuilder:validation:Enum=linux
	OS string `json:"os,omitempty"`

	// Volumes are added to the agent pod after the ones the operator sets up.
	// A name the operator uses (config, data, bpffs, debugfs, modules,
	// eve-log) is refused.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// VolumeMounts mount the volumes above into the Synapse container. Each
	// names a volume of spec.volumes and has an absolute mountPath that does
	// not equal, contain or sit inside a path the operator mounts. subPath and
	// subPathExpr are refused, since kubelet does not refresh such a mount.
	//
	// +optional
	// +listType=map
	// +listMapKey=mountPath
	// +kubebuilder:validation:MaxItems=16
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// Logging configures the agent's own logs.
	//
	// +optional
	Logging *AgentLoggingSpec `json:"logging,omitempty"`

	// Platform connects the agent to the Gen0Sec platform.
	//
	// +optional
	Platform *AgentPlatformSpec `json:"platform,omitempty"`

	// Capture configures the packet capture.
	//
	// +optional
	Capture *AgentCaptureSpec `json:"capture,omitempty"`

	// IDS configures the intrusion detection of the agent.
	//
	// +optional
	IDS *AgentIDSSpec `json:"ids,omitempty"`

	// Config holds Synapse settings the fields above do not model, in the
	// shape of Synapse's own configuration file. A key that a field above or
	// the operator itself sets is refused, not overridden.
	//
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// AgentPlatformSpec connects the agent to the Gen0Sec platform.
type AgentPlatformSpec struct {
	// APIKeySecretRef names the Secret key holding the platform API key.
	//
	// +optional
	APIKeySecretRef *SecretKeyReference `json:"apiKeySecretRef,omitempty"`

	// Identity configures the identity the agent reports to the platform.
	//
	// +optional
	Identity *AgentIdentitySpec `json:"identity,omitempty"`
}

// AgentIdentitySpec configures the identity the agent reports.
type AgentIdentitySpec struct {
	// Enabled turns the identity reporting on or off. Empty leaves Synapse's
	// default.
	//
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Cluster is the name the agent reports for this cluster.
	//
	// +optional
	Cluster string `json:"cluster,omitempty"`
}

// AgentCaptureSpec configures the packet capture.
type AgentCaptureSpec struct {
	// Interfaces are the network interfaces the agent captures on. Empty
	// leaves Synapse's default.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	Interfaces []string `json:"interfaces,omitempty"`
}

// AgentLoggingSpec configures the agent's own logs and its EVE log.
type AgentLoggingSpec struct {
	// Level is the minimum severity logged. Empty leaves Synapse's default.
	//
	// +optional
	// +kubebuilder:validation:Enum=error;warn;info;debug;trace
	Level string `json:"level,omitempty"`

	// Eve writes Synapse's EVE log: alert, http and stats events as JSON
	// lines. Unset or disabled, nothing is written.
	//
	// +optional
	Eve *EveLogSpec `json:"eve,omitempty"`
}

// EveLogSpec configures where the EVE log goes.
//
// +kubebuilder:validation:XValidation:rule="!has(self.hostPath) || self.output == 'file'",message="hostPath is only for output file"
// +kubebuilder:validation:XValidation:rule="!has(self.hostPath) || (self.hostPath.matches('^/var/log/[^/]+(/[^/]+)*$') && !self.hostPath.split('/').exists(s, s == '.' || s == '..'))",message="hostPath must be an absolute path under /var/log/, with a directory name and no empty, . or .. segment"
type EveLogSpec struct {
	// Enabled turns the EVE log on.
	Enabled bool `json:"enabled"`

	// Output is where the log goes: the pod's stdout (the default), its
	// stderr, or a file on the node. stdout carries little else by default.
	// stderr carries Synapse's own logs and its block events. Filter on
	// event_type. Nothing rotates the file.
	//
	// +optional
	// +kubebuilder:default=stdout
	// +kubebuilder:validation:Enum=stdout;stderr;file
	Output string `json:"output,omitempty"`

	// HostPath is the node directory that holds eve.json when output is
	// file. Unset means /var/log/synapse. It is a guard against accidents,
	// not a security boundary: the agent pod is privileged.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=200
	HostPath string `json:"hostPath,omitempty"`

	// EventTypes selects the events written. Empty means all of them.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:items:Enum=alert;http;stats
	EventTypes []string `json:"eventTypes,omitempty"`
}

// AgentIDSSpec configures the intrusion detection of the agent.
type AgentIDSSpec struct {
	// Enabled turns the intrusion detection on or off. Empty means true.
	//
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// EnforceBlock makes the agent block what it detects, not only report it.
	// Empty leaves Synapse's default.
	//
	// +optional
	EnforceBlock *bool `json:"enforceBlock,omitempty"`

	// HomeNet lists the CIDRs that count as the protected network. Empty
	// means the operator computes it from the cluster.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	HomeNet []string `json:"homeNet,omitempty"`

	// Rules lists ConfigMaps and Secrets whose IDS rule files the agent loads
	// next to the default rules and the ones managed on the platform. The
	// operator copies each selected key into the agent's configuration
	// Secret. With the IDS on, a change to a rule reloads in the running pods
	// and rolls none of them; with it off, a change rolls every pod. At most
	// 256 files and 900 KiB of rules.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=16
	Rules []AgentIDSRuleSource `json:"rules,omitempty"`
}

// AgentIDSRuleSource names a ConfigMap or a Secret in the agent's namespace
// that holds IDS rule files.
//
// +kubebuilder:validation:XValidation:rule="has(self.configMapRef) != has(self.secretRef)",message="set exactly one of configMapRef and secretRef"
type AgentIDSRuleSource struct {
	// ConfigMapRef names the ConfigMap that holds the rules.
	//
	// +optional
	ConfigMapRef *ObjectNameReference `json:"configMapRef,omitempty"`

	// SecretRef names the Secret that holds the rules.
	//
	// +optional
	SecretRef *ObjectNameReference `json:"secretRef,omitempty"`

	// Keys lists the keys to load, in order. A key the object lacks is an
	// error. Empty means every key that ends in ".rules".
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	Keys []string `json:"keys,omitempty"`
}

// ObjectNameReference names an object in the namespace of the resource that
// refers to it.
type ObjectNameReference struct {
	// Name is the name of the object.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// SynapseAgentStatus is the observed state of a SynapseAgent.
type SynapseAgentStatus struct {
	// ObservedGeneration is the generation the status was computed from.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the state of the agent.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ConfigHash identifies the rendered configuration the pods are meant to
	// run.
	//
	// +optional
	ConfigHash string `json:"configHash,omitempty"`

	// DesiredNumberScheduled is the number of nodes that should run an agent
	// pod.
	//
	// +optional
	DesiredNumberScheduled int32 `json:"desiredNumberScheduled,omitempty"`

	// CurrentNumberScheduled is the number of nodes running an agent pod.
	//
	// +optional
	CurrentNumberScheduled int32 `json:"currentNumberScheduled,omitempty"`

	// UpdatedNumberScheduled is the number of nodes running an agent pod of
	// the current template.
	//
	// +optional
	UpdatedNumberScheduled int32 `json:"updatedNumberScheduled,omitempty"`

	// NumberReady is the number of nodes whose agent pod is ready.
	//
	// +optional
	NumberReady int32 `json:"numberReady,omitempty"`

	// NumberAvailable is the number of nodes whose agent pod is available.
	//
	// +optional
	NumberAvailable int32 `json:"numberAvailable,omitempty"`

	// NumberMisscheduled is the number of nodes running an agent pod that
	// should not.
	//
	// +optional
	NumberMisscheduled int32 `json:"numberMisscheduled,omitempty"`
}

func init() {
	SchemeBuilder.Register(&SynapseAgent{}, &SynapseAgentList{})
}
