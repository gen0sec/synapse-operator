//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// The sidecar layout of --ingress-mode: one pod, Synapse beside the operator,
// the two sharing the routes file in an emptyDir. An initContainer renders
// the file once, so that Synapse's blocking first read finds it; the sidecar
// keeps it current and signals Synapse when it changes.
const (
	sidecarName  = "sidecar"
	sidecarClass = "e2e-sidecar"
	// The name of the ClusterRole and its binding, which no other part of the
	// install shares.
	sidecarRole = "e2e-ingress-mode"

	aliasHost   = "alias.e2e.test"
	noProbeHost = "noprobe.e2e.test"
	lateHost    = "late.e2e.test"
	// A host a server-alias adds, beside the refused one.
	extraAliasHost = "extra-alias.e2e.test"

	routesDir  = "/etc/synapse/upstreams"
	routesFile = routesDir + "/upstreams.yaml"
	// Where the file reader of the pod listens: not 8080 or 8081, which are
	// the operator's.
	readerPort = "8082"

	// A regular expression the Go engine reads one way and Synapse's another:
	// \Q...\E exists in the first only. Written to the file as it is, it
	// would be refused, and the file with it.
	quotedRegexPath = `/api/\Qv1.0\E/.*`
)

// The config of Synapse as a SynapseProxy renders it, with one HTTP listener
// and the routes file where the pod mounts it.
const sidecarSynapseConfig = `daemon:
  enabled: false
  working_directory: /var/lib/synapse
ids:
  enabled: true
  enforce_block: true
  rule_paths:
  - /etc/synapse/rules/*.rules
logging:
  file_logging_enabled: false
mode: proxy
platform:
  geoip:
    country:
      path: /var/lib/synapse/ipinfo_lite.mmdb
      url: https://github.com/gen0sec/geoip-databases/raw/download/ipinfo_lite.mmdb
  threat:
    path: /var/lib/synapse
    url: https://download.gen0sec.com/v1
proxy:
  captcha:
    cache_ttl: 300
    token_ttl: 7200
  certificates: /etc/synapse/certs
  default_certificate: default
  h2c: true
  internal_services:
    enabled: true
  listeners:
  - address: 0.0.0.0:80
    mode: http
    name: http
  upstream:
    conf: ` + routesFile + `
`

// operatorFlags are the flags of the initContainer and of the sidecar. They
// are the same, and the initContainer adds --render-once.
func operatorFlags(extra ...string) []string {
	return append([]string{
		"--ingress-mode",
		"--ingress-class=" + sidecarClass,
		"--upstreams-out=" + routesFile,
		// The sidecar binds nothing Synapse binds.
		"--metrics-bind-address=0",
		"--health-probe-bind-address=0",
	}, extra...)
}

// ingressModeRBAC is what --ingress-mode reads and writes, on its own role.
func ingressModeRBAC() (*rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding) {
	read := []string{"get", "list", "watch"}
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: sidecarRole},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"services", "secrets", "configmaps", "namespaces", "pods"}, Verbs: read},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
			{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"ingresses", "ingressclasses"}, Verbs: read},
			{APIGroups: []string{"discovery.k8s.io"}, Resources: []string{"endpointslices"}, Verbs: read},
		},
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: sidecarRole},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: sidecarRole},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: sidecarName, Namespace: proxyNamespace}},
	}
	return role, binding
}

// sidecarDeployment is Synapse with the operator beside it. The pod shares
// its process namespace, so that the sidecar finds Synapse's process to
// signal, and the sidecar runs as root and unconfined for the same reason
// (the README's "Deployment requirements"). A fourth container serves the
// routes file back to the test, since neither image has a shell.
func (c *cluster) sidecarDeployment(operatorImage string) []client.Object {
	labels := map[string]string{"app": sidecarName}
	routes := corev1.VolumeMount{Name: "routes", MountPath: routesDir}
	operator := func(name string, args []string) corev1.Container {
		return corev1.Container{
			Name:            name,
			Image:           operatorImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Args:            args,
			VolumeMounts:    []corev1.VolumeMount{routes},
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:       ptrTo(int64(0)),
				RunAsNonRoot:    ptrTo(false),
				AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined},
			},
		}
	}
	httpGet := func(path string) corev1.ProbeHandler {
		return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("http")}}
	}
	synapse := corev1.Container{
		Name:            "synapse",
		Image:           c.synapseImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            []string{"--config", "/etc/synapse/config/config.yaml"},
		Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 80}},
		Env: []corev1.EnvVar{
			{Name: "SYNAPSE_LOG_FORMAT", Value: "json"},
			{Name: "SYNAPSE_NODE_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptrTo(true),
			Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"SYS_ADMIN", "NET_ADMIN", "BPF", "SYS_RESOURCE"},
				Drop: []corev1.Capability{"ALL"},
			},
		},
		StartupProbe:   &corev1.Probe{ProbeHandler: httpGet("/health"), PeriodSeconds: 2, FailureThreshold: 180},
		ReadinessProbe: &corev1.Probe{ProbeHandler: httpGet("/"), PeriodSeconds: 5},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: "/etc/synapse/config", ReadOnly: true},
			{Name: "routes", MountPath: routesDir, ReadOnly: true},
			{Name: "certs", MountPath: "/etc/synapse/certs", ReadOnly: true},
			{Name: "data", MountPath: "/var/lib/synapse"},
		},
	}
	reader := corev1.Container{
		Name:            "reader",
		Image:           c.backendImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "NAME", Value: "reader"},
			{Name: "PORT", Value: readerPort},
			{Name: "SERVE_DIR", Value: routesDir},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "routes", MountPath: routesDir, ReadOnly: true}},
	}
	empty := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	return []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: sidecarName}},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: sidecarName + "-config"},
			Data:       map[string]string{"config.yaml": sidecarSynapseConfig},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: sidecarName},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						ServiceAccountName:    sidecarName,
						ShareProcessNamespace: ptrTo(true),
						EnableServiceLinks:    ptrTo(false),
						InitContainers:        []corev1.Container{operator("render-once", operatorFlags("--render-once"))},
						Containers:            []corev1.Container{synapse, operator("ingress", operatorFlags()), reader},
						Volumes: []corev1.Volume{
							{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: sidecarName + "-config"},
							}}},
							{Name: "routes", VolumeSource: empty},
							{Name: "certs", VolumeSource: empty},
							{Name: "data", VolumeSource: empty},
						},
					},
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: sidecarName},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeLoadBalancer,
				Selector: labels,
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("http")}},
			},
		},
	}
}

// sidecarPod is the pod of the sidecar Deployment, once there is one that is
// not on its way out.
func (c *cluster) sidecarPod() (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := c.k8s.List(c.ctx, &pods, client.InNamespace(proxyNamespace), client.MatchingLabels{"app": sidecarName}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			return &pods.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no pod of %s yet", sidecarName)
}

// containerLog is what a container of the sidecar pod has logged.
func (c *cluster) containerLog(cs kubernetes.Interface, container string) (string, error) {
	pod, err := c.sidecarPod()
	if err != nil {
		return "", err
	}
	raw, err := cs.CoreV1().Pods(proxyNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container}).DoRaw(c.ctx)
	return string(raw), err
}

// sidecarReady reports "" when the pod is ready: the routes file was
// rendered, and Synapse loaded it and answers.
func (c *cluster) sidecarReady() string {
	var d appsv1.Deployment
	if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: sidecarName}, &d); err != nil {
		return err.Error()
	}
	if d.Status.ReadyReplicas != 1 {
		return fmt.Sprintf("%d ready replicas, want 1", d.Status.ReadyReplicas)
	}
	return ""
}

// routesDocument is the part of the routes file the test reads.
type routesDocument struct {
	Version int `json:"version"`
	Hosts   map[string]struct {
		Paths map[string]struct {
			HealthCheck *struct {
				Type string `json:"type"`
			} `json:"health_check"`
		} `json:"paths"`
	} `json:"hosts"`
}

// routes reads the routes file out of the pod, as the file Synapse reads.
func (c *cluster) routes(cs kubernetes.Interface) (raw string, doc routesDocument, err error) {
	pod, err := c.sidecarPod()
	if err != nil {
		return "", doc, err
	}
	body, err := cs.CoreV1().Pods(proxyNamespace).ProxyGet("http", pod.Name, readerPort, "/file/upstreams.yaml", nil).DoRaw(c.ctx)
	if err != nil {
		return "", doc, err
	}
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return string(body), doc, fmt.Errorf("the routes file is not YAML: %w", err)
	}
	return string(body), doc, nil
}

// answers reports "" when a request for the path on host gets a 200 from
// backend.
func (c *cluster) answers(host, path, backend string) string {
	a, err := c.request(http.MethodGet, "http", host, path)
	switch {
	case err != nil:
		return err.Error()
	case a.status != http.StatusOK || a.backend != backend:
		return fmt.Sprintf("http://%s%s answered %d from backend %q, want 200 from %q", host, path, a.status, a.backend, backend)
	}
	return ""
}

// TestIngressMode runs the operator as a sidecar of Synapse, as the README's
// sidecar layout has it, and checks what a user sees: the file the operator
// writes is a v2 file, and the Synapse that reads it serves.
func TestIngressMode(t *testing.T) {
	c := newCluster(t)
	operatorImage := env(t, "E2E_OPERATOR_IMAGE")
	// The cluster's own kubeconfig, as newCluster has it: never kubectl's
	// default context.
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	step := func(name string, run func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, run) {
			t.FailNow()
		}
	}

	role, binding := ingressModeRBAC()
	removeCluster := func() {
		for _, obj := range []client.Object{binding, role} {
			if err := c.k8s.Delete(c.ctx, obj); client.IgnoreNotFound(err) != nil {
				t.Errorf("remove %s: %v", obj.GetName(), err)
			}
		}
	}
	removeCluster()
	t.Cleanup(removeCluster)

	step("Synapse starts from the file the initContainer rendered", func(t *testing.T) {
		c.clear(t)
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("shop")...)
		c.create(t, c.backend("api")...)
		c.create(t, c.backend("plain")...)

		regexed := ingress("regex", sidecarClass, regexHost, "api")
		regexed.Annotations = map[string]string{"synapse.gen0sec.com/use-regex": "true"}
		regexed.Spec.Rules[0].HTTP.Paths[0].Path = quotedRegexPath
		regexed.Spec.Rules[0].HTTP.Paths[0].PathType = ptrTo(networkingv1.PathTypeImplementationSpecific)

		// A catch-all alias: no host name Synapse's v2 file takes. Beside it
		// one that is a host, which is kept.
		aliased := ingress("alias", sidecarClass, aliasHost, "plain")
		aliased.Annotations = map[string]string{"synapse.gen0sec.com/server-alias": "*, " + extraAliasHost}

		unprobed := ingress("noprobe", sidecarClass, noProbeHost, "plain")
		unprobed.Annotations = map[string]string{"synapse.gen0sec.com/healthcheck": "false"}

		c.create(t, role, binding)
		c.create(t, ingress("shop", sidecarClass, shopHost, "shop"), regexed, aliased, unprobed)
		c.create(t, c.sidecarDeployment(operatorImage)...)
		// Given long: the first start of a cluster pulls its load balancer's
		// image.
		eventually(t, afterASync, "the pod becomes ready", c.sidecarReady)
	})

	step("the initContainer rendered the file before Synapse started", func(t *testing.T) {
		log, err := c.containerLog(cs, "render-once")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(log, "render-once: ingresses=4 ") {
			t.Errorf("the initContainer did not render the four Ingresses:\n%s", log)
		}
	})

	step("the routes file is a v2 file", func(t *testing.T) {
		raw, doc, err := c.routes(cs)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Version != 2 {
			t.Errorf("the routes file says version %d, want 2:\n%s", doc.Version, raw)
		}
		for _, host := range []string{shopHost, regexHost, aliasHost, noProbeHost} {
			if _, ok := doc.Hosts[host]; !ok {
				t.Errorf("the routes file has no host %s:\n%s", host, raw)
			}
		}
		// A v1 file's route says it has TLS to its backend or not, in a
		// field a v2 file has too; what tells them apart is the version, and
		// that a plain-HTTP backend is not written as TLS.
		if strings.Contains(raw, "ssl_enabled: true") {
			t.Errorf("a plain-HTTP backend is written as TLS:\n%s", raw)
		}
	})

	step("an Ingress host answers through Synapse", func(t *testing.T) {
		eventually(t, soon, "the shop host answers", func() string { return c.routed("http", shopHost, "shop") })
	})

	step("a use-regex path answers, as Synapse reads it", func(t *testing.T) {
		raw, _, err := c.routes(cs)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(raw, `\Q`) {
			t.Errorf("the regular expression is written as the user wrote it, not as Synapse reads it:\n%s", raw)
		}
		eventually(t, soon, "the regex path answers", func() string { return c.answers(regexHost, "/api/v1.0/x", "api") })
		// And only what it matches: the dot is a dot.
		a, err := c.request(http.MethodGet, "http", regexHost, "/api/v1x0/x")
		if err != nil {
			t.Fatal(err)
		}
		if a.backend == "api" {
			t.Errorf("/api/v1x0/x was served by the regex route of /api/v1.0/, answered %d", a.status)
		}
		// And from the start of the path, as a user's regular expression is.
		a, err = c.request(http.MethodGet, "http", regexHost, "/zz/api/v1.0/x")
		if err != nil {
			t.Fatal(err)
		}
		if a.backend == "api" {
			t.Errorf("/zz/api/v1.0/x was served by a regex route that is anchored at /api/, answered %d", a.status)
		}
	})

	step("a server-alias of * is refused with an event and the file still loads", func(t *testing.T) {
		raw, doc, err := c.routes(cs)
		if err != nil {
			t.Fatal(err)
		}
		for host := range doc.Hosts {
			if strings.Contains(host, "*") {
				t.Errorf("the routes file has the host %q, which Synapse does not take:\n%s", host, raw)
			}
		}
		eventually(t, soon, "InvalidServerAlias is raised on the Ingress", func() string {
			var events corev1.EventList
			if err := c.k8s.List(c.ctx, &events, client.InNamespace(appsNamespace)); err != nil {
				return err.Error()
			}
			for _, e := range events.Items {
				if e.Reason == "InvalidServerAlias" && e.InvolvedObject.Name == "alias" && e.Type == corev1.EventTypeWarning {
					return ""
				}
			}
			return fmt.Sprintf("%d events, none InvalidServerAlias on the Ingress alias", len(events.Items))
		})
		// The Ingress's own host is served, and so is every other: the file
		// was loaded, not dropped. So is the alias that is a host.
		eventually(t, soon, "the valid alias answers", func() string { return c.answers(extraAliasHost, "/", "plain") })
		eventually(t, soon, "the host of the refused alias answers", func() string { return c.answers(aliasHost, "/", "plain") })
		eventually(t, soon, "the shop host still answers", func() string { return c.routed("http", shopHost, "shop") })
		pod, err := c.sidecarPod()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range pod.Status.ContainerStatuses {
			if s.RestartCount != 0 {
				t.Errorf("container %s restarted %d times", s.Name, s.RestartCount)
			}
		}
	})

	step("a backend with healthcheck false is served, and not probed", func(t *testing.T) {
		eventually(t, soon, "the unprobed host answers", func() string { return c.answers(noProbeHost, "/", "plain") })
		raw, doc, err := c.routes(cs)
		if err != nil {
			t.Fatal(err)
		}
		paths := doc.Hosts[noProbeHost].Paths
		if len(paths) == 0 {
			t.Fatalf("no routes for %s:\n%s", noProbeHost, raw)
		}
		for path, route := range paths {
			if route.HealthCheck == nil || route.HealthCheck.Type != "none" {
				t.Errorf("%s%s is probed, want health_check type none:\n%s", noProbeHost, path, raw)
			}
		}
	})

	step("an Ingress added after start is served after the reload", func(t *testing.T) {
		c.create(t, ingress("late", sidecarClass, lateHost, "api"))
		eventually(t, soon, "the late host answers", func() string { return c.routed("http", lateHost, "api") })
		raw, doc, err := c.routes(cs)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Version != 2 {
			t.Errorf("the routes file says version %d after the change, want 2:\n%s", doc.Version, raw)
		}
		// The sidecar signalled Synapse itself, rather than leaving the
		// reload to Synapse's file watch.
		eventually(t, soon, "the sidecar sends SIGHUP to Synapse", func() string {
			log, err := c.containerLog(cs, "ingress")
			if err != nil {
				return err.Error()
			}
			for _, bad := range []string{"SIGHUP failed", "no target process found"} {
				if strings.Contains(log, bad) {
					return "the signal did not reach Synapse (" + bad + "):\n" + log
				}
			}
			if !strings.Contains(log, "SIGHUP → reload") {
				return "no \"SIGHUP → reload\" in the sidecar's log"
			}
			return ""
		})
		// The first hosts are served still.
		eventually(t, soon, "the shop host answers after the reload", func() string { return c.routed("http", shopHost, "shop") })
	})

	step("an Ingress removed is no longer served", func(t *testing.T) {
		late := ingress("late", sidecarClass, lateHost, "api")
		if err := c.k8s.Delete(c.ctx, late); err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		eventually(t, soon, "the late host stops answering", func() string {
			a, err := c.get("http", lateHost)
			if err != nil {
				return err.Error()
			}
			if a.status == http.StatusOK && a.backend == "api" {
				return "the late host still answers"
			}
			return ""
		})
		// Synapse is up, not gone: the others are served still.
		eventually(t, soon, "the shop host answers after the removal", func() string { return c.routed("http", shopHost, "shop") })
	})
}
