//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The central layout with config-sync, as the README's "Migrating an existing
// deployment" has it: one operator renders the routes and the CA files into a
// ConfigMap, and the Synapse pod reads that ConfigMap through a config-sync
// sidecar into an emptyDir at /shared. The operator is told, with
// --upstreams-ca-dir, that the pod reads the CA files from there and not
// from the ConfigMap mount.
const (
	csName      = "cs"
	csClass     = "e2e-cs"
	csCentral   = "cs-central"
	csRole      = "e2e-cs-central"
	csSharedDir = "/shared"
	csRoutesCM  = "cs-upstreams"

	csDefaultOperator = "cs-default"
	csDefaultClass    = "e2e-cs-default"
	csDefaultCM       = "cs-default-upstreams"

	csBaseHost   = "csbase.e2e.test"
	csCAHost     = "csca.e2e.test"
	csRotateHost = "csrotate.e2e.test"

	// A change reaches the proxy through the operator's write, the
	// sidecar's watch and a reload, and not through the kubelet's mount sync
	// of about a minute: a route is served well inside this.
	csFast = 10 * time.Second
)

var csCAFile = regexp.MustCompile(`ca: "` + csSharedDir + `/(ca-[0-9a-f]{16}\.pem)"`)

func csSyncArgs(once bool, syncKeys string) []string {
	args := []string{"--config-sync", "--watch-configmap=" + proxyNamespace + "/" + csRoutesCM, "--out-dir=" + csSharedDir}
	if once {
		args = append(args, "--sync-once", "--ensure-files=upstreams.yaml")
	}
	if syncKeys != "" {
		args = append(args, "--sync-keys="+syncKeys)
	}
	return args
}

func csRBAC() []client.Object {
	read := []string{"get", "list", "watch"}
	write := []string{"get", "list", "watch", "create", "update", "patch"}
	return []client.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: csRole},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: write},
				{APIGroups: []string{""}, Resources: []string{"services", "secrets", "namespaces", "pods"}, Verbs: read},
				{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
				{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"ingresses", "ingressclasses"}, Verbs: read},
				{APIGroups: []string{"discovery.k8s.io"}, Resources: []string{"endpointslices"}, Verbs: read},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: csRole},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: csRole},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: csCentral, Namespace: proxyNamespace}},
		},
	}
}

// csCentralOperator is the central operator: it renders every Ingress of the
// class into the ConfigMap and names /shared as the directory of the CA files.
func (c *cluster) csCentralOperator(operatorImage string) []client.Object {
	return append(
		[]client.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csCentral}}},
		c.csOperator(operatorImage, csCentral, csClass, csRoutesCM, "--upstreams-ca-dir="+csSharedDir)...)
}

// csOperator is a central operator with the flags it is given beside its own.
func (c *cluster) csOperator(operatorImage, name, class, configMap string, extra ...string) []client.Object {
	labels := map[string]string{"app": name}
	return []client.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: name},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						ServiceAccountName: csCentral,
						Containers: []corev1.Container{{
							Name:            "operator",
							Image:           operatorImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args: append([]string{
								"--ingress-mode",
								"--ingress-class=" + class,
								"--upstreams-out-configmap=" + proxyNamespace + "/" + configMap,
								"--metrics-bind-address=0",
								"--health-probe-bind-address=0",
							}, extra...),
						}},
					},
				},
			},
		},
	}
}

// csSynapse is Synapse with the config-sync layout around it.
func (c *cluster) csSynapse(operatorImage string) []client.Object {
	labels := map[string]string{"app": csName}
	shared := corev1.VolumeMount{Name: "shared", MountPath: csSharedDir}
	sync := func(name string, once bool) corev1.Container {
		return corev1.Container{
			Name:            name,
			Image:           operatorImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Args:            csSyncArgs(once, ""),
			VolumeMounts:    []corev1.VolumeMount{shared},
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
			shared,
			{Name: "certs", MountPath: "/etc/synapse/certs", ReadOnly: true},
			{Name: "data", MountPath: "/var/lib/synapse"},
		},
	}
	// Neither image has a shell: a container serves /shared back to the test.
	reader := corev1.Container{
		Name:            "reader",
		Image:           c.backendImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "NAME", Value: "reader"},
			{Name: "PORT", Value: readerPort},
			{Name: "SERVE_DIR", Value: csSharedDir},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "shared", MountPath: csSharedDir, ReadOnly: true}},
	}
	empty := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	config := strings.Replace(sidecarSynapseConfig, routesFile, csSharedDir+"/upstreams.yaml", 1)
	return []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName}},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch"}},
			},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: csName},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: csName, Namespace: proxyNamespace}},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName + "-config"},
			Data:       map[string]string{"config.yaml": config},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				// A new pod replaces the old one, which cannot go on serving
				// from the files it has while the new one is looked at.
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						ServiceAccountName:    csName,
						ShareProcessNamespace: ptrTo(true),
						EnableServiceLinks:    ptrTo(false),
						InitContainers:        []corev1.Container{sync("sync-once", true)},
						Containers:            []corev1.Container{synapse, sync("sync", false), reader},
						Volumes: []corev1.Volume{
							{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: csName + "-config"},
							}}},
							{Name: "shared", VolumeSource: empty},
							{Name: "certs", VolumeSource: empty},
							{Name: "data", VolumeSource: empty},
						},
					},
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: csName},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeLoadBalancer,
				Selector: labels,
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("http")}},
			},
		},
	}
}

// csPod is the pod of the Synapse Deployment that is not on its way out.
func (c *cluster) csPod() (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := c.k8s.List(c.ctx, &pods, client.InNamespace(proxyNamespace), client.MatchingLabels{"app": csName}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			return &pods.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no pod of %s yet", csName)
}

// csFile reads a file of /shared in the Synapse pod, as the pod has it.
func (c *cluster) csFile(cs kubernetes.Interface, name string) (string, error) {
	pod, err := c.csPod()
	if err != nil {
		return "", err
	}
	body, err := cs.CoreV1().Pods(proxyNamespace).ProxyGet("http", pod.Name, readerPort, "/file/"+name, nil).DoRaw(c.ctx)
	return string(body), err
}

// csRoutes is the routes file the pod reads from /shared.
func (c *cluster) csRoutes(cs kubernetes.Interface) (string, error) {
	return c.csFile(cs, "upstreams.yaml")
}

// csReady reports "" when the Deployment has its one replica ready.
func (c *cluster) csReady() string {
	var d appsv1.Deployment
	if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: csName}, &d); err != nil {
		return err.Error()
	}
	if d.Status.ReadyReplicas != 1 || d.Status.UpdatedReplicas != 1 {
		return fmt.Sprintf("%d ready, %d updated replicas, want 1", d.Status.ReadyReplicas, d.Status.UpdatedReplicas)
	}
	return ""
}

// csSetSyncKeys gives both config-sync containers a --sync-keys, or none.
func (c *cluster) csSetSyncKeys(t *testing.T, keys string) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var d appsv1.Deployment
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: csName}, &d); err != nil {
			return err
		}
		spec := &d.Spec.Template.Spec
		spec.InitContainers[0].Args = csSyncArgs(true, keys)
		for i := range spec.Containers {
			if spec.Containers[i].Name == "sync" {
				spec.Containers[i].Args = csSyncArgs(false, keys)
			}
		}
		return c.k8s.Update(c.ctx, &d)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// csRoutesCMData is the routes ConfigMap the operator renders.
func (c *cluster) csRoutesCMData() (map[string]string, error) {
	return c.csRoutesCMDataOf(csRoutesCM)
}

func (c *cluster) csRoutesCMDataOf(name string) (map[string]string, error) {
	var cm corev1.ConfigMap
	if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: name}, &cm); err != nil {
		return nil, err
	}
	return cm.Data, nil
}

// neverServed fails the test when host answers 200 from backend at any time
// in the window.
func (c *cluster) neverServed(t *testing.T, host, backend string, window time.Duration) {
	t.Helper()
	for end := time.Now().Add(window); time.Now().Before(end); time.Sleep(time.Second) {
		if a, err := c.get("http", host); err == nil && a.status == http.StatusOK && a.backend == backend {
			t.Fatalf("%s is served by %s, want it refused", host, backend)
		}
	}
}

func TestUpstreamsCADir(t *testing.T) {
	c := newCluster(t)
	operatorImage := env(t, "E2E_OPERATOR_IMAGE")
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

	rbac := csRBAC()
	removeCluster := func() {
		for i := len(rbac) - 1; i >= 0; i-- {
			if err := c.k8s.Delete(c.ctx, rbac[i]); client.IgnoreNotFound(err) != nil {
				t.Errorf("remove %s: %v", rbac[i].GetName(), err)
			}
		}
	}
	removeCluster()
	t.Cleanup(removeCluster)

	right := newAuthority(t, "cs CA right")
	wrong := newAuthority(t, "cs CA wrong")
	var firstCA string

	step("Synapse reads the routes the central operator renders, through config-sync", func(t *testing.T) {
		c.clear(t)
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("plain")...)
		certPEM, keyPEM := right.issue(t, "tlscs"+backendDomain)
		c.create(t, c.tlsBackend("tlscs", certPEM, keyPEM)...)
		c.create(t, ingress("csbase", csClass, csBaseHost, "plain"))
		c.create(t, rbac...)
		c.create(t, c.csCentralOperator(operatorImage)...)
		eventually(t, afterASync, "the operator renders the routes ConfigMap", func() string {
			data, err := c.csRoutesCMData()
			switch {
			case err != nil:
				return err.Error()
			case !strings.Contains(data["upstreams.yaml"], csBaseHost):
				return "the routes do not name " + csBaseHost
			}
			return ""
		})
		c.create(t, c.csSynapse(operatorImage)...)
		eventually(t, afterASync, "the Synapse pod becomes ready", c.csReady)
		eventually(t, soon, "the base host is served", func() string { return c.routed("http", csBaseHost, "plain") })
	})

	step("a relative --upstreams-ca-dir stops the operator at startup", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "cs-relative"},
			Spec: corev1.PodSpec{
				RestartPolicy:      corev1.RestartPolicyNever,
				ServiceAccountName: csCentral,
				Containers: []corev1.Container{{
					Name:            "operator",
					Image:           operatorImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Args: []string{"--ingress-mode", "--ingress-class=cs-relative", "--upstreams-out-configmap=" + proxyNamespace + "/cs-relative",
						"--upstreams-ca-dir=shared", "--metrics-bind-address=0", "--health-probe-bind-address=0"},
				}},
			},
		}
		c.create(t, pod)
		eventually(t, soon, "the operator exits", func() string {
			if err := c.k8s.Get(c.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
				return err.Error()
			}
			if len(pod.Status.ContainerStatuses) == 0 || pod.Status.ContainerStatuses[0].State.Terminated == nil {
				return "still running"
			}
			if code := pod.Status.ContainerStatuses[0].State.Terminated.ExitCode; code == 0 {
				return "exited 0"
			}
			return ""
		})
		raw, err := cs.CoreV1().Pods(proxyNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(c.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), "must be an absolute path") {
			t.Errorf("the log does not say the directory must be absolute:\n%s", raw)
		}
		if _, err := c.csRoutesCMDataOf("cs-relative"); err == nil {
			t.Error("a routes ConfigMap was written")
		}
	})

	step("without --upstreams-ca-dir the routes name the directory of the SynapseProxy mount", func(t *testing.T) {
		c.create(t, caSecret(appsNamespace, "cs-default-ca", right.pem))
		c.create(t, withClass(httpsIngress("csdefault", "csdefault.e2e.test", "tlscs", map[string]string{"upstream-tls-ca": "cs-default-ca"}), csDefaultClass))
		c.create(t, c.csOperator(operatorImage, csDefaultOperator, csDefaultClass, csDefaultCM)...)
		var routes string
		eventually(t, afterASync, "the operator renders the routes", func() string {
			data, err := c.csRoutesCMDataOf(csDefaultCM)
			if err != nil {
				return err.Error()
			}
			routes = data["upstreams.yaml"]
			if !strings.Contains(routes, "csdefault.e2e.test") {
				return "the routes do not name the host"
			}
			return ""
		})
		if !regexp.MustCompile(`ca: "/etc/synapse/upstreams/ca-[0-9a-f]{16}\.pem"`).MatchString(routes) {
			t.Errorf("the routes do not name a CA file in /etc/synapse/upstreams:\n%s", routes)
		}
	})

	step("an Ingress with upstream-tls-ca added to a running Synapse is served within seconds", func(t *testing.T) {
		c.create(t, caSecret(appsNamespace, "cs-ca", right.pem))
		c.create(t, withClass(httpsIngress("csca", csCAHost, "tlscs", map[string]string{"upstream-tls-ca": "cs-ca"}), csClass))
		took := eventually(t, csFast, "the CA-backed host is served", func() string { return c.routed("http", csCAHost, "tlscs") })
		t.Logf("served after %s", took)
	})

	step("the routes name the CA file under the directory of the flag, and the file is there", func(t *testing.T) {
		data, err := c.csRoutesCMData()
		if err != nil {
			t.Fatal(err)
		}
		m := csCAFile.FindStringSubmatch(data["upstreams.yaml"])
		if m == nil {
			t.Fatalf("the routes ConfigMap names no CA file under %s:\n%s", csSharedDir, data["upstreams.yaml"])
		}
		firstCA = m[1]
		if strings.Contains(data["upstreams.yaml"], "/etc/synapse/upstreams") {
			t.Errorf("the routes still name the mounted directory:\n%s", data["upstreams.yaml"])
		}
		if data[firstCA] != string(right.pem) {
			t.Errorf("the ConfigMap key %s holds %q, want the CA of the Secret", firstCA, data[firstCA])
		}
		raw, err := c.csRoutes(cs)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(raw, `"`+csSharedDir+`/`+firstCA+`"`) {
			t.Errorf("the routes file in the pod does not name %s/%s:\n%s", csSharedDir, firstCA, raw)
		}
		if body, err := c.csFile(cs, firstCA); err != nil || body != string(right.pem) {
			t.Errorf("the pod's %s/%s holds %q (error %v), want the CA of the Secret", csSharedDir, firstCA, body, err)
		}
	})

	step("a rotated CA is served within seconds", func(t *testing.T) {
		c.create(t, caSecret(appsNamespace, "cs-rotating-ca", wrong.pem))
		c.create(t, withClass(httpsIngress("csrotate", csRotateHost, "tlscs", map[string]string{"upstream-tls-ca": "cs-rotating-ca"}), csClass))
		var wrongFile string
		eventually(t, csFast, "the wrong CA is a file in the pod", func() string {
			raw, err := c.csRoutes(cs)
			if err != nil {
				return err.Error()
			}
			for _, m := range csCAFile.FindAllStringSubmatch(raw, -1) {
				if m[1] != firstCA {
					wrongFile = m[1]
				}
			}
			if wrongFile == "" {
				return "the routes in the pod name no second CA file"
			}
			if _, err := c.csFile(cs, wrongFile); err != nil {
				return err.Error()
			}
			return ""
		})
		// The Secret holds a CA that did not sign the back-end's certificate.
		c.neverServed(t, csRotateHost, "tlscs", 6*time.Second)
		c.setSecret(t, appsNamespace, "cs-rotating-ca", right.pem)
		took := eventually(t, csFast, "the host of the rotated CA is served", func() string { return c.routed("http", csRotateHost, "tlscs") })
		t.Logf("served after %s", took)
		raw, err := c.csRoutes(cs)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(raw, wrongFile) {
			t.Errorf("the routes still name the CA file %s of the replaced CA:\n%s", wrongFile, raw)
		}
		eventually(t, csFast, "the first CA-backed host is served still", func() string { return c.routed("http", csCAHost, "tlscs") })
	})

	step("--sync-keys=upstreams.yaml leaves the CA files out, and the route is not served", func(t *testing.T) {
		old, err := c.csPod()
		if err != nil {
			t.Fatal(err)
		}
		c.csSetSyncKeys(t, "upstreams.yaml")
		// The new pod, with its containers running.
		eventually(t, afterASync, "a new pod runs with the new arguments", func() string {
			pod, err := c.csPod()
			switch {
			case err != nil:
				return err.Error()
			case pod.UID == old.UID:
				return "still the old pod"
			case len(pod.Status.InitContainerStatuses) == 0 || pod.Status.InitContainerStatuses[0].State.Terminated == nil:
				return "the initContainer has not finished"
			}
			for _, s := range pod.Status.ContainerStatuses {
				if s.State.Running == nil {
					return "container " + s.Name + " is not running"
				}
			}
			return ""
		})
		eventually(t, soon, "the routes file is in the new pod", func() string {
			raw, err := c.csRoutes(cs)
			if err != nil {
				return err.Error()
			}
			if !csCAFile.MatchString(raw) {
				return "the routes file names no CA file under " + csSharedDir
			}
			return ""
		})
		listing, err := c.csFile(cs, "")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(listing, "ca-") {
			t.Errorf("a CA file is in %s with --sync-keys=upstreams.yaml:\n%s", csSharedDir, listing)
		}
		c.neverServed(t, csCAHost, "tlscs", 15*time.Second)
	})

	step("without --sync-keys the CA files are projected again, and the route is served", func(t *testing.T) {
		c.csSetSyncKeys(t, "")
		eventually(t, afterASync, "the Synapse pod becomes ready", c.csReady)
		eventually(t, soon, "the CA-backed host is served", func() string { return c.routed("http", csCAHost, "tlscs") })
		eventually(t, soon, "the rotated host is served", func() string { return c.routed("http", csRotateHost, "tlscs") })
	})
}

func withClass(ing *networkingv1.Ingress, class string) *networkingv1.Ingress {
	ing.Spec.IngressClassName = &class
	return ing
}
