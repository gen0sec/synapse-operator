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

// The objects a SynapseProxy owns besides its routes and certificates: the
// rendered config, a ServiceAccount, a Service and the Deployment.

const (
	// proxyLabel carries the proxy's name on everything it owns. It is the
	// whole pod selector, which can never change once the Deployment exists.
	proxyLabel = "synapse.gen0sec.com/proxy"
	// proxyPartLabel tells the owned objects of one proxy apart.
	proxyPartLabel  = "synapse.gen0sec.com/part"
	proxyPartConfig = "config"

	proxyConfigDir = "/etc/synapse/config"
	proxyConfigKey = "config.yaml"
	// proxyDataDir is Synapse's working directory and where it downloads the
	// threat feed and GeoIP database; see proxyconfig_defaults.yaml.
	proxyDataDir = "/var/lib/synapse"

	// proxyStopDelaySeconds is how long a stopping pod keeps serving, so it
	// is out of the Service's endpoints before Synapse gets SIGTERM. Synapse
	// exits on SIGTERM without waiting for requests in flight.
	proxyStopDelaySeconds = 15
)

// proxyLabels are set on every object a proxy owns. app.kubernetes.io/name is
// left out on purpose: installs that predate SynapseProxy select workloads and
// config by it, and must not pick these up.
func proxyLabels(p *synapsev1alpha1.SynapseProxy) map[string]string {
	return map[string]string{
		proxyLabel:                     p.Name,
		"app.kubernetes.io/instance":   p.Name,
		"app.kubernetes.io/component":  "proxy",
		"app.kubernetes.io/managed-by": "synapse-operator",
	}
}

func proxySelector(p *synapsev1alpha1.SynapseProxy) map[string]string {
	return map[string]string{proxyLabel: p.Name}
}

func proxyObjectMeta(p *synapsev1alpha1.SynapseProxy, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: p.Namespace,
		Name:      name,
		Labels:    proxyLabels(p),
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(p, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy")),
		},
	}
}

// proxyConfigSecretName names the config Secret after its content. A config
// change is then a new Secret, which the pod template references and so
// rolls out, while the pods of the previous ReplicaSet keep the one they
// started with.
func proxyConfigSecretName(p *synapsev1alpha1.SynapseProxy, hash string) string {
	return p.Name + "-config-" + hash[:min(len(hash), 10)]
}

func buildProxyConfigSecret(p *synapsev1alpha1.SynapseProxy, cfg ProxyConfigOutput) *corev1.Secret {
	meta := proxyObjectMeta(p, proxyConfigSecretName(p, cfg.Hash))
	meta.Labels[proxyPartLabel] = proxyPartConfig
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: meta,
		Immutable:  kptr.To(true),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{proxyConfigKey: cfg.YAML},
	}
}

func buildProxyServiceAccount(p *synapsev1alpha1.SynapseProxy) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: proxyObjectMeta(p, p.Name),
		// The proxy never talks to the Kubernetes API.
		AutomountServiceAccountToken: kptr.To(false),
	}
}

func buildProxyService(p *synapsev1alpha1.SynapseProxy) *corev1.Service {
	meta := proxyObjectMeta(p, p.Name)
	spec := corev1.ServiceSpec{
		Type:     corev1.ServiceTypeClusterIP,
		Selector: proxySelector(p),
	}
	if s := p.Spec.Service; s != nil {
		meta.Annotations = s.Annotations
		if s.Type != "" {
			spec.Type = s.Type
		}
		spec.ExternalTrafficPolicy = s.ExternalTrafficPolicy
	}
	for _, l := range p.Spec.Listeners {
		spec.Ports = append(spec.Ports, corev1.ServicePort{
			Name:       l.Name,
			Port:       l.Port,
			TargetPort: intstr.FromString(l.Name),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta,
		Spec:       spec,
	}
}

// buildProxyDeployment builds the Deployment that runs the proxy with the
// config Secret of the given name.
//
// The pod runs as the chart ran it: root and privileged, because packet
// capture, the kernel firewall and the IDS are on by default.
func buildProxyDeployment(p *synapsev1alpha1.SynapseProxy, configSecret string) *appsv1.Deployment {
	spec := &p.Spec

	var ports []corev1.ContainerPort
	probePort := intstr.IntOrString{}
	for _, l := range spec.Listeners {
		ports = append(ports, corev1.ContainerPort{Name: l.Name, ContainerPort: l.Port, Protocol: corev1.ProtocolTCP})
		// Probes speak plain HTTP. The API requires an HTTP listener.
		if l.Protocol == "HTTP" && probePort.StrVal == "" {
			probePort = intstr.FromString(l.Name)
		}
	}
	httpGet := func(urlPath string) corev1.ProbeHandler {
		return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: urlPath, Port: probePort}}
	}

	container := corev1.Container{
		Name:  "synapse",
		Image: spec.Image,
		Args:  []string{"--config", path.Join(proxyConfigDir, proxyConfigKey)},
		Ports: ports,
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
			Privileged: kptr.To(true),
			Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"SYS_ADMIN", "NET_ADMIN", "BPF", "SYS_RESOURCE"},
				Drop: []corev1.Capability{"ALL"},
			},
		},
		// /health answers once the routes have loaded, which is what a start
		// has to wait for; with a slow platform that can take minutes.
		StartupProbe: &corev1.Probe{ProbeHandler: httpGet("/health"), PeriodSeconds: 2, FailureThreshold: 180},
		// "/" with the pod's address as Host is answered before access rules
		// and the WAF. /health passes through both, and a rule that caught
		// the node's address there would take healthy pods out.
		ReadinessProbe: &corev1.Probe{ProbeHandler: httpGet("/"), PeriodSeconds: 10},
		LivenessProbe:  &corev1.Probe{ProbeHandler: httpGet("/"), PeriodSeconds: 20},
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: proxyStopDelaySeconds}},
		},
		// Whole directories, never a subPath: a subPath mount is not
		// refreshed, so routes and certificates would never change.
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: proxyConfigDir, ReadOnly: true},
			{Name: "upstreams", MountPath: path.Dir(proxyUpstreamsFile), ReadOnly: true},
			{Name: "certs", MountPath: proxyCertsDir, ReadOnly: true},
			{Name: "data", MountPath: proxyDataDir},
		},
	}

	meta := proxyObjectMeta(p, p.Name)
	return &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: meta,
		Spec: appsv1.DeploymentSpec{
			Replicas: spec.Replicas,
			Selector: &metav1.LabelSelector{MatchLabels: proxySelector(p)},
			// A new pod must be serving before an old one goes.
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: kptr.To(intstr.FromInt32(0)),
					MaxSurge:       kptr.To(intstr.FromInt32(1)),
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: proxyLabels(p)},
				Spec: corev1.PodSpec{
					ServiceAccountName:           p.Name,
					AutomountServiceAccountToken: kptr.To(false),
					// A Service named `internal-services` would otherwise
					// inject INTERNAL_SERVICES_PORT, which Synapse reads.
					EnableServiceLinks:            kptr.To(false),
					TerminationGracePeriodSeconds: kptr.To(int64(proxyStopDelaySeconds + 30)),
					ImagePullSecrets:              spec.ImagePullSecrets,
					NodeSelector:                  spec.NodeSelector,
					Tolerations:                   spec.Tolerations,
					Affinity:                      spec.Affinity,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:    kptr.To(int64(0)),
						RunAsGroup:   kptr.To(int64(0)),
						RunAsNonRoot: kptr.To(false),
						FSGroup:      kptr.To(int64(0)),
					},
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: configSecret},
						}},
						{Name: "upstreams", VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: proxyUpstreamsName(p).Name},
							},
						}},
						{Name: "certs", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: proxyCertsName(p).Name},
						}},
						{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}
