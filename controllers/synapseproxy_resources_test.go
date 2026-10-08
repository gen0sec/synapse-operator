package controllers

import (
	"path"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// edgeProxy is the SynapseProxy the builder tests start from: the usual pair
// of listeners, TLS first so that "the first HTTP listener" is not simply the
// first one.
func edgeProxy(mutate ...func(*synapsev1alpha1.SynapseProxySpec)) *synapsev1alpha1.SynapseProxy {
	replicas := int32(2)
	p := &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "synapse-os", Name: "edge", UID: "uid-edge"},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:    "example.test/synapse:1.0.0",
			Replicas: &replicas,
			Listeners: []synapsev1alpha1.Listener{
				{Name: "https", Port: 443, Protocol: "TLS"},
				{Name: "http", Port: 80, Protocol: "HTTP"},
			},
		},
	}
	for _, m := range mutate {
		m(&p.Spec)
	}
	return p
}

func proxyContainer(t *testing.T, d *appsv1.Deployment) *corev1.Container {
	t.Helper()
	if n := len(d.Spec.Template.Spec.Containers); n != 1 {
		t.Fatalf("%d containers, want 1", n)
	}
	return &d.Spec.Template.Spec.Containers[0]
}

func volumeNamed(t *testing.T, d *appsv1.Deployment, name string) corev1.VolumeSource {
	t.Helper()
	for _, v := range d.Spec.Template.Spec.Volumes {
		if v.Name == name {
			return v.VolumeSource
		}
	}
	t.Fatalf("no volume %q", name)
	return corev1.VolumeSource{}
}

func mountAt(t *testing.T, c *corev1.Container, mountPath string) corev1.VolumeMount {
	t.Helper()
	for _, m := range c.VolumeMounts {
		if m.MountPath == mountPath {
			return m
		}
	}
	t.Fatalf("nothing mounted at %s; mounts: %+v", mountPath, c.VolumeMounts)
	return corev1.VolumeMount{}
}

func TestProxyDeployment_Workload(t *testing.T) {
	p := edgeProxy()
	d := buildProxyDeployment(p, "edge-config-0123456789")

	if d.Name != "edge" || d.Namespace != "synapse-os" {
		t.Errorf("name %s/%s", d.Namespace, d.Name)
	}
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 2 {
		t.Errorf("replicas = %v", d.Spec.Replicas)
	}
	// The selector can never change once the Deployment exists, so it is one
	// label of the operator's own and nothing a user or a chart also sets.
	if d.Spec.Selector == nil {
		t.Fatal("no selector")
	}
	want := map[string]string{"synapse.gen0sec.com/proxy": "edge"}
	if got := d.Spec.Selector.MatchLabels; len(got) != 1 || got["synapse.gen0sec.com/proxy"] != "edge" {
		t.Errorf("selector = %v, want %v", got, want)
	}
	if d.Spec.Template.Labels["synapse.gen0sec.com/proxy"] != "edge" {
		t.Errorf("pod labels %v do not satisfy the selector", d.Spec.Template.Labels)
	}

	// A new pod must be serving before an old one goes.
	ru := d.Spec.Strategy.RollingUpdate
	if d.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || ru == nil ||
		ru.MaxUnavailable == nil || *ru.MaxUnavailable != intstr.FromInt32(0) ||
		ru.MaxSurge == nil || *ru.MaxSurge != intstr.FromInt32(1) {
		t.Errorf("strategy = %+v", d.Spec.Strategy)
	}
}

func TestProxyDeployment_Pod(t *testing.T) {
	p := edgeProxy(func(s *synapsev1alpha1.SynapseProxySpec) {
		s.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
		s.NodeSelector = map[string]string{"edge": "true"}
		s.Tolerations = []corev1.Toleration{{Key: "edge", Operator: corev1.TolerationOpExists}}
		s.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
		s.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}
	})
	d := buildProxyDeployment(p, "edge-config-0123456789")
	pod := d.Spec.Template.Spec

	if pod.ServiceAccountName != "edge" {
		t.Errorf("service account %q", pod.ServiceAccountName)
	}
	// The proxy never talks to the Kubernetes API.
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("the service account token is mounted")
	}
	// Synapse reads un-prefixed environment variables as overrides. With
	// service links on, a Service named `internal-services` would inject
	// INTERNAL_SERVICES_PORT and move a port.
	if pod.EnableServiceLinks == nil || *pod.EnableServiceLinks {
		t.Error("service links are enabled")
	}
	if pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 45 {
		t.Errorf("termination grace period = %v", pod.TerminationGracePeriodSeconds)
	}
	if len(pod.ImagePullSecrets) != 1 || pod.ImagePullSecrets[0].Name != "registry" {
		t.Errorf("image pull secrets = %v", pod.ImagePullSecrets)
	}
	if pod.NodeSelector["edge"] != "true" || len(pod.Tolerations) != 1 || pod.Affinity == nil || pod.Affinity.PodAntiAffinity == nil {
		t.Errorf("scheduling not copied: %v %v %v", pod.NodeSelector, pod.Tolerations, pod.Affinity)
	}

	c := proxyContainer(t, d)
	if c.Image != "example.test/synapse:1.0.0" {
		t.Errorf("image %q", c.Image)
	}
	if !slices.Equal(c.Args, []string{"--config", "/etc/synapse/config/config.yaml"}) {
		t.Errorf("args = %v", c.Args)
	}
	if got := c.Resources.Requests[corev1.ResourceCPU]; got.String() != "500m" {
		t.Errorf("resources not copied: %v", c.Resources)
	}
}

// Packet capture, the kernel firewall and the IDS are on by default in a
// SynapseProxy, and they need what the chart gave them.
func TestProxyDeployment_RunsPrivileged(t *testing.T) {
	d := buildProxyDeployment(edgeProxy(), "edge-config-0123456789")

	psc := d.Spec.Template.Spec.SecurityContext
	if psc == nil || psc.RunAsUser == nil || *psc.RunAsUser != 0 || psc.RunAsNonRoot == nil || *psc.RunAsNonRoot {
		t.Errorf("pod security context = %+v, want root", psc)
	}
	sc := proxyContainer(t, d).SecurityContext
	if sc == nil || sc.Privileged == nil || !*sc.Privileged {
		t.Fatalf("container security context = %+v, want privileged", sc)
	}
	for _, c := range []corev1.Capability{"SYS_ADMIN", "NET_ADMIN", "BPF", "SYS_RESOURCE"} {
		if !slices.Contains(sc.Capabilities.Add, c) {
			t.Errorf("capability %s not added: %v", c, sc.Capabilities.Add)
		}
	}
}

func TestProxyDeployment_Ports(t *testing.T) {
	c := proxyContainer(t, buildProxyDeployment(edgeProxy(), "edge-config-0123456789"))

	want := []corev1.ContainerPort{
		{Name: "https", ContainerPort: 443, Protocol: corev1.ProtocolTCP},
		{Name: "http", ContainerPort: 80, Protocol: corev1.ProtocolTCP},
	}
	if !slices.Equal(c.Ports, want) {
		t.Errorf("ports = %+v, want %+v", c.Ports, want)
	}
}

func TestProxyDeployment_Environment(t *testing.T) {
	c := proxyContainer(t, buildProxyDeployment(edgeProxy(), "edge-config-0123456789"))

	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	if env["SYNAPSE_LOG_FORMAT"].Value != "json" {
		t.Errorf("SYNAPSE_LOG_FORMAT = %+v", env["SYNAPSE_LOG_FORMAT"])
	}
	node := env["SYNAPSE_NODE_IP"].ValueFrom
	if node == nil || node.FieldRef == nil || node.FieldRef.FieldPath != "status.hostIP" {
		t.Errorf("SYNAPSE_NODE_IP = %+v, want the node's address", env["SYNAPSE_NODE_IP"])
	}
	// Anything else would be read as a config override; see the renderer.
	if len(c.Env) != 2 {
		t.Errorf("%d environment variables, want 2: %+v", len(c.Env), c.Env)
	}
}

func TestProxyDeployment_Probes(t *testing.T) {
	c := proxyContainer(t, buildProxyDeployment(edgeProxy(), "edge-config-0123456789"))

	get := func(name string, p *corev1.Probe) *corev1.HTTPGetAction {
		t.Helper()
		if p == nil || p.HTTPGet == nil {
			t.Fatalf("no %s HTTP probe", name)
		}
		// Probes speak plain HTTP, so they go to the first HTTP listener even
		// when a TLS one is declared before it.
		if p.HTTPGet.Port != intstr.FromString("http") || p.HTTPGet.Scheme != "" && p.HTTPGet.Scheme != corev1.URISchemeHTTP {
			t.Errorf("%s probe goes to %v %s", name, p.HTTPGet.Port, p.HTTPGet.Scheme)
		}
		return p.HTTPGet
	}

	// /health answers once the routes have loaded, which is what a start-up
	// has to wait for. It can take minutes when the platform is slow.
	if get("startup", c.StartupProbe).Path != "/health" {
		t.Errorf("startup probe path %q", c.StartupProbe.HTTPGet.Path)
	}
	if budget := c.StartupProbe.PeriodSeconds * c.StartupProbe.FailureThreshold; budget < 300 {
		t.Errorf("startup probe gives up after %ds, want at least 300", budget)
	}
	// "/" with the pod address as Host is answered before access rules and
	// the WAF. /health goes through both, and a rule that caught the node's
	// address there would restart healthy pods.
	if get("readiness", c.ReadinessProbe).Path != "/" {
		t.Errorf("readiness probe path %q", c.ReadinessProbe.HTTPGet.Path)
	}
	if get("liveness", c.LivenessProbe).Path != "/" {
		t.Errorf("liveness probe path %q", c.LivenessProbe.HTTPGet.Path)
	}
}

// Synapse exits on SIGTERM without waiting for requests in flight. The sleep
// lets the endpoint be withdrawn before that happens.
func TestProxyDeployment_Shutdown(t *testing.T) {
	d := buildProxyDeployment(edgeProxy(), "edge-config-0123456789")
	c := proxyContainer(t, d)

	if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Sleep == nil || c.Lifecycle.PreStop.Sleep.Seconds != 15 {
		t.Fatalf("preStop = %+v, want a 15 second sleep", c.Lifecycle)
	}
	if grace := *d.Spec.Template.Spec.TerminationGracePeriodSeconds; grace <= c.Lifecycle.PreStop.Sleep.Seconds {
		t.Errorf("grace period %ds does not outlast the preStop sleep", grace)
	}
}

func TestProxyDeployment_Volumes(t *testing.T) {
	p := edgeProxy()
	d := buildProxyDeployment(p, "edge-config-0123456789")
	c := proxyContainer(t, d)

	config := mountAt(t, c, "/etc/synapse/config")
	if src := volumeNamed(t, d, config.Name); !config.ReadOnly || src.Secret == nil || src.Secret.SecretName != "edge-config-0123456789" {
		t.Errorf("config: mount %+v, source %+v", config, src)
	}
	upstreams := mountAt(t, c, "/etc/synapse/upstreams")
	if src := volumeNamed(t, d, upstreams.Name); !upstreams.ReadOnly || src.ConfigMap == nil || src.ConfigMap.Name != proxyUpstreamsName(p).Name {
		t.Errorf("upstreams: mount %+v, source %+v", upstreams, src)
	}
	certs := mountAt(t, c, "/etc/synapse/certs")
	if src := volumeNamed(t, d, certs.Name); !certs.ReadOnly || src.Secret == nil || src.Secret.SecretName != proxyCertsName(p).Name {
		t.Errorf("certs: mount %+v, source %+v", certs, src)
	}
	data := mountAt(t, c, "/var/lib/synapse")
	if src := volumeNamed(t, d, data.Name); data.ReadOnly || src.EmptyDir == nil {
		t.Errorf("data: mount %+v, source %+v", data, src)
	}

	// A subPath mount is never refreshed, so routes and certificates would
	// stay as they were when the pod started.
	for _, m := range c.VolumeMounts {
		if m.SubPath != "" || m.SubPathExpr != "" {
			t.Errorf("mount %s uses a subPath", m.MountPath)
		}
	}
}

// The rendered config tells Synapse where to look; the pod decides what is
// there. The two are written in different files and have to agree.
func TestProxyDeployment_MountsWhatTheConfigPointsAt(t *testing.T) {
	p := edgeProxy()
	tree := renderedTree(t, ProxyConfigInput{Proxy: p})
	c := proxyContainer(t, buildProxyDeployment(p, "edge-config-0123456789"))

	conf, _ := at(tree, "proxy.upstream.conf")
	confPath, _ := conf.(string)
	mountAt(t, c, path.Dir(confPath))
	if path.Base(confPath) != UpstreamsKey {
		t.Errorf("Synapse reads %s, but the routes are written under the key %s", confPath, UpstreamsKey)
	}

	certs, _ := at(tree, "proxy.certificates")
	certsPath, _ := certs.(string)
	mountAt(t, c, certsPath)

	// Everything Synapse writes by default goes to one directory, which the
	// defaults file names in several places and the pod mounts once.
	writable := map[string]string{}
	for _, key := range []string{"daemon.working_directory", "platform.threat.path"} {
		v, _ := at(tree, key)
		writable[key], _ = v.(string)
	}
	geoip, _ := at(tree, "platform.geoip.country.path")
	geoipPath, _ := geoip.(string)
	writable["platform.geoip.country.path"] = path.Dir(geoipPath)
	for key, dir := range writable {
		if dir == "" || dir == "." {
			t.Errorf("%s is not set by the defaults", key)
			continue
		}
		if mountAt(t, c, dir).ReadOnly {
			t.Errorf("%s points into %s, which is mounted read-only", key, dir)
		}
	}

	if got := c.Args[len(c.Args)-1]; path.Base(got) != proxyConfigKey {
		t.Errorf("Synapse is started with %s, but the config is written under the key %s", got, proxyConfigKey)
	}
}

func TestProxyService(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		svc := buildProxyService(edgeProxy())
		if svc.Name != "edge" || svc.Namespace != "synapse-os" {
			t.Errorf("name %s/%s", svc.Namespace, svc.Name)
		}
		if svc.Spec.Type != corev1.ServiceTypeClusterIP {
			t.Errorf("type = %q, want ClusterIP", svc.Spec.Type)
		}
		if got := svc.Spec.Selector; len(got) != 1 || got["synapse.gen0sec.com/proxy"] != "edge" {
			t.Errorf("selector = %v", got)
		}
		want := []corev1.ServicePort{
			{Name: "https", Port: 443, TargetPort: intstr.FromString("https"), Protocol: corev1.ProtocolTCP},
			{Name: "http", Port: 80, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
		}
		if !slices.Equal(svc.Spec.Ports, want) {
			t.Errorf("ports = %+v, want %+v", svc.Spec.Ports, want)
		}
		if svc.Spec.ExternalTrafficPolicy != "" {
			t.Errorf("externalTrafficPolicy = %q on a ClusterIP Service", svc.Spec.ExternalTrafficPolicy)
		}
	})

	t.Run("as specified", func(t *testing.T) {
		svc := buildProxyService(edgeProxy(func(s *synapsev1alpha1.SynapseProxySpec) {
			s.Service = &synapsev1alpha1.ProxyServiceSpec{
				Type:                  corev1.ServiceTypeLoadBalancer,
				Annotations:           map[string]string{"example.test/scheme": "internet-facing"},
				ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			}
		}))
		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer || svc.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal {
			t.Errorf("type %q, policy %q", svc.Spec.Type, svc.Spec.ExternalTrafficPolicy)
		}
		if svc.Annotations["example.test/scheme"] != "internet-facing" {
			t.Errorf("annotations = %v", svc.Annotations)
		}
	})
}

func TestProxyServiceAccount(t *testing.T) {
	sa := buildProxyServiceAccount(edgeProxy())
	if sa.Name != "edge" || sa.Namespace != "synapse-os" {
		t.Errorf("name %s/%s", sa.Namespace, sa.Name)
	}
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Error("the token is mounted by default")
	}
}

func TestProxyConfigSecret(t *testing.T) {
	p := edgeProxy()
	cfg := ProxyConfigOutput{YAML: []byte("mode: proxy\n"), Hash: "0123456789abcdef0123456789abcdef"}
	sec := buildProxyConfigSecret(p, cfg)

	// Named by content: a config change is a new Secret and so a rollout,
	// and the pods of the previous ReplicaSet keep the one they started with.
	if sec.Name != "edge-config-0123456789" || sec.Name != proxyConfigSecretName(p, cfg.Hash) {
		t.Errorf("name %q", sec.Name)
	}
	if sec.Immutable == nil || !*sec.Immutable {
		t.Error("the Secret is not immutable")
	}
	if string(sec.Data[proxyConfigKey]) != "mode: proxy\n" || len(sec.Data) != 1 {
		t.Errorf("data keys %v", keysOf(sec.Data))
	}
}

// Older installs select workloads and config by app.kubernetes.io/name. An
// object carrying one of those values would be picked up by them.
func TestProxyObjects_DoNotMatchLegacySelectors(t *testing.T) {
	p := edgeProxy()
	d := buildProxyDeployment(p, "edge-config-0123456789")
	sets := map[string]map[string]string{
		"Deployment":     d.Labels,
		"pod template":   d.Spec.Template.Labels,
		"Service":        buildProxyService(p).Labels,
		"ServiceAccount": buildProxyServiceAccount(p).Labels,
		"config Secret":  buildProxyConfigSecret(p, ProxyConfigOutput{Hash: "0123456789abcdef"}).Labels,
	}
	for name, labels := range sets {
		if labels["synapse.gen0sec.com/proxy"] != "edge" {
			t.Errorf("%s: no proxy label in %v", name, labels)
		}
		if v := labels["app.kubernetes.io/name"]; v == "synapse" || v == "synapse-proxy" || v == "synapse-agent" {
			t.Errorf("%s: app.kubernetes.io/name=%s matches a legacy selector", name, v)
		}
	}
}
