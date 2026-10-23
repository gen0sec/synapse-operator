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
	// tolerates every taint, so it runs on every selected node.
	//
	// +optional
	// +kubebuilder:default={{operator: Exists}}
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

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
	// the agent can see the host's processes.
	//
	// +optional
	// +kubebuilder:default=true
	HostPID *bool `json:"hostPID,omitempty"`

	// OS is the operating system of the nodes the agent runs on.
	//
	// +optional
	// +kubebuilder:default=linux
	// +kubebuilder:validation:Enum=linux
	OS string `json:"os,omitempty"`

	// Logging configures the agent's own logs.
	//
	// +optional
	Logging *LoggingSpec `json:"logging,omitempty"`

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

	// IPVersion selects the IP versions captured: both, ipv4 or ipv6. Empty
	// leaves Synapse's default.
	//
	// +optional
	// +kubebuilder:validation:Enum=both;ipv4;ipv6
	IPVersion string `json:"ipVersion,omitempty"`
}

// AgentIDSSpec configures the intrusion detection of the agent.
type AgentIDSSpec struct {
	// Enabled turns the intrusion detection on or off. Empty leaves Synapse's
	// default.
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
