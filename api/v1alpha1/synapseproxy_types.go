package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SynapseProxy is a Synapse reverse proxy run by the operator. The operator
// renders its configuration and creates the Deployment and Service for it.
//
// The name is capped below the 63-character limit of a Service name to leave
// room for the suffixes of the objects derived from it.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=synproxy,categories=synapse
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Address",type=string,JSONPath=`.status.addresses[0]`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`,priority=1
// +kubebuilder:printcolumn:name="Config",type=string,JSONPath=`.status.configHash`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 48",message="metadata.name must be at most 48 characters"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a DNS-1035 label: lower-case letters, digits and '-', starting with a letter"
type SynapseProxy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SynapseProxySpec   `json:"spec,omitempty"`
	Status SynapseProxyStatus `json:"status,omitempty"`
}

// SynapseProxyList is a list of SynapseProxy.
//
// +kubebuilder:object:root=true
type SynapseProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SynapseProxy `json:"items"`
}

// SynapseProxySpec is the desired state of a SynapseProxy.
type SynapseProxySpec struct {
	// Image is the Synapse container image, tag or digest included.
	//
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// ImagePullSecrets are the Secrets used to pull Image.
	//
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Replicas is the number of proxy pods.
	//
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources are the compute resources of the proxy container.
	//
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// NodeSelector restricts the nodes the proxy pods may run on.
	//
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations are the taints the proxy pods tolerate.
	//
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity is the scheduling affinity of the proxy pods.
	//
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Service describes the Service in front of the proxy. Its ports follow
	// Listeners.
	//
	// +optional
	Service *ProxyServiceSpec `json:"service,omitempty"`

	// Listeners are the endpoints the proxy binds. Order matters: an
	// HTTP-to-HTTPS redirect targets the first TLS listener. At least one
	// HTTP listener is required, because the health probes use it.
	//
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:XValidation:rule="self.all(l, self.exists_one(m, m.port == l.port))",message="listener ports must be unique"
	// +kubebuilder:validation:XValidation:rule="self.exists(l, l.protocol == 'HTTP')",message="at least one HTTP listener is required"
	Listeners []Listener `json:"listeners"`

	// TLS configures TLS termination on the TLS listeners.
	//
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`

	// TrustedProxies are the CIDRs of the load balancers and proxies in front
	// of this one. Only from these sources are the forwarded-for headers
	// believed.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	TrustedProxies []string `json:"trustedProxies,omitempty"`

	// Logging configures the proxy's own logs.
	//
	// +optional
	Logging *LoggingSpec `json:"logging,omitempty"`

	// Platform connects the proxy to the Gen0Sec platform.
	//
	// +optional
	Platform *PlatformSpec `json:"platform,omitempty"`

	// Config holds Synapse settings the fields above do not model, in the
	// shape of Synapse's own configuration file. A key that a field above or
	// the operator itself sets is refused, not overridden.
	//
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// ProxyServiceSpec describes the Service in front of the proxy.
//
// +kubebuilder:validation:XValidation:rule="!has(self.externalTrafficPolicy) || (has(self.type) && self.type != 'ClusterIP')",message="externalTrafficPolicy needs type NodePort or LoadBalancer"
type ProxyServiceSpec struct {
	// Type is the Service type. Empty means ClusterIP.
	//
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	Type corev1.ServiceType `json:"type,omitempty"`

	// Annotations are added to the Service.
	//
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// ExternalTrafficPolicy is Local to keep the client source address, or
	// Cluster. It applies to NodePort and LoadBalancer Services only.
	//
	// +optional
	// +kubebuilder:validation:Enum=Cluster;Local
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicy `json:"externalTrafficPolicy,omitempty"`
}

// Listener is one endpoint the proxy binds.
type Listener struct {
	// Name identifies the listener. It is also the name of the container and
	// Service port, hence the port-name length limit.
	//
	// +kubebuilder:validation:MaxLength=15
	// +kubebuilder:validation:Pattern=`^[a-z]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// Port is the port the Service exposes for this listener.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// Protocol is HTTP for plaintext, or TLS for a listener that terminates
	// or passes through TLS.
	//
	// +kubebuilder:validation:Enum=HTTP;TLS
	Protocol string `json:"protocol"`
}

// TLSSpec configures TLS termination.
type TLSSpec struct {
	// Grade selects the protocol versions and cipher suites offered. Empty
	// leaves Synapse's default.
	//
	// +optional
	// +kubebuilder:validation:Enum=high;medium
	Grade string `json:"grade,omitempty"`
}

// LoggingSpec configures the proxy's own logs.
type LoggingSpec struct {
	// Level is the minimum severity logged. Empty leaves Synapse's default.
	//
	// +optional
	// +kubebuilder:validation:Enum=error;warn;info;debug;trace
	Level string `json:"level,omitempty"`
}

// PlatformSpec connects the proxy to the Gen0Sec platform.
type PlatformSpec struct {
	// APIKeySecretRef names the Secret key holding the platform API key.
	//
	// +optional
	APIKeySecretRef *SecretKeyReference `json:"apiKeySecretRef,omitempty"`
}

// SecretKeyReference names one key of a Secret in the same namespace.
type SecretKeyReference struct {
	// Name is the name of the Secret.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is the key within the Secret.
	//
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// SynapseProxyStatus is the observed state of a SynapseProxy.
type SynapseProxyStatus struct {
	// ObservedGeneration is the generation the status was computed from.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the state of the proxy.
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

	// Replicas is the number of proxy pods.
	Replicas int32 `json:"replicas"`

	// ReadyReplicas is the number of proxy pods that are ready.
	//
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// UpdatedReplicas is the number of proxy pods running the current
	// configuration and image.
	//
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// Selector is the label selector of the proxy pods, in the string form
	// the scale subresource expects.
	//
	// +optional
	Selector string `json:"selector,omitempty"`

	// Addresses are the addresses the proxy is reachable on.
	//
	// +optional
	Addresses []string `json:"addresses,omitempty"`
}

func init() {
	SchemeBuilder.Register(&SynapseProxy{}, &SynapseProxyList{})
}
