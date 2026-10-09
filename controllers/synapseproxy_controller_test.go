package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

var proxyTestRuns atomic.Int64

// proxyEnv is one namespace on the local API server, with both of a
// SynapseProxy's controllers ready to be stepped by hand. There is no
// Deployment controller behind it: nothing creates ReplicaSets or pods, and
// nothing fills in a Deployment's status unless the test does.
type proxyEnv struct {
	t       *testing.T
	ctx     context.Context
	k8s     client.Client
	ns      string
	routes  *SynapseRouteReconciler
	proxies *SynapseProxyReconciler
}

func newProxyEnv(t *testing.T) *proxyEnv {
	t.Helper()
	k8s, err := client.New(apiServer(t), client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	e := &proxyEnv{
		t: t, ctx: context.Background(), k8s: k8s,
		ns:      fmt.Sprintf("proxy-test-%d", proxyTestRuns.Add(1)),
		routes:  &SynapseRouteReconciler{Client: k8s, ClusterDomain: "cluster.local"},
		proxies: &SynapseProxyReconciler{Client: k8s},
	}
	e.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e.ns}})
	return e
}

func (e *proxyEnv) create(obj client.Object) {
	e.t.Helper()
	if err := e.k8s.Create(e.ctx, obj); err != nil {
		e.t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

// proxy creates the SynapseProxy "edge" with one HTTP listener.
func (e *proxyEnv) proxy(mutate ...func(*synapsev1alpha1.SynapseProxySpec)) *synapsev1alpha1.SynapseProxy {
	e.t.Helper()
	p := &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "edge"},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:     "example.test/synapse:1.0.0",
			Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
		},
	}
	for _, m := range mutate {
		m(&p.Spec)
	}
	e.create(p)
	return p
}

// edit changes the proxy's spec the way a user would.
func (e *proxyEnv) edit(p *synapsev1alpha1.SynapseProxy, mutate func(*synapsev1alpha1.SynapseProxySpec)) {
	e.t.Helper()
	change(e.t, e.k8s, p, func() { mutate(&p.Spec) })
}

func (e *proxyEnv) renderRoutes(p *synapsev1alpha1.SynapseProxy) {
	e.t.Helper()
	if _, err := e.routes.Reconcile(e.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(p)}); err != nil {
		e.t.Fatalf("route reconcile: %v", err)
	}
}

// reconcile steps the proxy controller once and reloads the proxy.
func (e *proxyEnv) reconcile(p *synapsev1alpha1.SynapseProxy) ctrl.Result {
	e.t.Helper()
	res, err := e.proxies.Reconcile(e.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(p)})
	if err != nil {
		e.t.Fatalf("proxy reconcile: %v", err)
	}
	if err := e.k8s.Get(e.ctx, client.ObjectKeyFromObject(p), p); err != nil {
		e.t.Fatal(err)
	}
	return res
}

// running creates the proxy and takes it to the point where its objects exist.
func (e *proxyEnv) running(mutate ...func(*synapsev1alpha1.SynapseProxySpec)) *synapsev1alpha1.SynapseProxy {
	e.t.Helper()
	p := e.proxy(mutate...)
	e.renderRoutes(p)
	e.reconcile(p)
	return p
}

func (e *proxyEnv) get(name string, into client.Object) bool {
	e.t.Helper()
	err := e.k8s.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: name}, into)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		e.t.Fatalf("get %T %s: %v", into, name, err)
	}
	return true
}

func (e *proxyEnv) deployment() *appsv1.Deployment {
	e.t.Helper()
	var d appsv1.Deployment
	if !e.get("edge", &d) {
		e.t.Fatal("no Deployment")
	}
	return &d
}

// configSecretName is the Secret the Deployment's pods mount their config from.
func (e *proxyEnv) configSecretName() string {
	e.t.Helper()
	return volumeNamed(e.t, e.deployment(), "config").Secret.SecretName
}

func (e *proxyEnv) configSecrets() []string {
	e.t.Helper()
	var list corev1.SecretList
	if err := e.k8s.List(e.ctx, &list, client.InNamespace(e.ns), client.MatchingLabels{proxyPartLabel: proxyPartConfig}); err != nil {
		e.t.Fatal(err)
	}
	var names []string
	for _, s := range list.Items {
		names = append(names, s.Name)
	}
	return names
}

func wantCondition(t *testing.T, p *synapsev1alpha1.SynapseProxy, kind string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(p.Status.Conditions, kind)
	if c == nil {
		t.Fatalf("no %s condition; have %+v", kind, p.Status.Conditions)
	}
	if c.Status != status || c.Reason != reason {
		t.Errorf("%s = %s/%s (%s), want %s/%s", kind, c.Status, c.Reason, c.Message, status, reason)
	}
	return c
}

func TestProxy_CreatesItsObjects(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()

	d := e.deployment()
	var svc corev1.Service
	var sa corev1.ServiceAccount
	var cfg corev1.Secret
	if !e.get("edge", &svc) || !e.get("edge", &sa) || !e.get(e.configSecretName(), &cfg) {
		t.Fatal("the Service, the ServiceAccount or the config Secret is missing")
	}
	for name, obj := range map[string]metav1.Object{"Deployment": d, "Service": &svc, "ServiceAccount": &sa, "config Secret": &cfg} {
		if !metav1.IsControlledBy(obj, p) {
			t.Errorf("%s is not controlled by the proxy: %v", name, obj.GetOwnerReferences())
		}
	}

	rendered := string(cfg.Data[proxyConfigKey])
	if !strings.Contains(rendered, "mode: proxy") {
		t.Errorf("config Secret does not hold the rendered config:\n%s", rendered)
	}
	if cfg.Immutable == nil || !*cfg.Immutable {
		t.Error("config Secret is not immutable")
	}

	wantCondition(t, p, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	// Nothing runs pods here, so the rollout cannot have finished.
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
	if p.Status.ObservedGeneration != p.Generation {
		t.Errorf("observedGeneration = %d, generation %d", p.Status.ObservedGeneration, p.Generation)
	}
	if p.Status.Selector != "synapse.gen0sec.com/proxy=edge" {
		t.Errorf("selector = %q", p.Status.Selector)
	}
	if p.Status.ConfigHash == "" || !strings.HasSuffix(cfg.Name, p.Status.ConfigHash[:10]) {
		t.Errorf("configHash %q does not name the config Secret %q", p.Status.ConfigHash, cfg.Name)
	}
	if len(p.Status.Addresses) != 1 || p.Status.Addresses[0] != svc.Spec.ClusterIP {
		t.Errorf("addresses = %v, want the Service's cluster IP %s", p.Status.Addresses, svc.Spec.ClusterIP)
	}
}

// Synapse does not start watching a routes file that was missing when it
// started, and the pod mounts the certificates Secret, so neither may be
// absent when the first pod is created.
func TestProxy_WaitsForRoutes(t *testing.T) {
	e := newProxyEnv(t)
	p := e.proxy()

	e.reconcile(p)
	wantCondition(t, p, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonWaitingForRoutes)
	var d appsv1.Deployment
	if e.get("edge", &d) {
		t.Fatal("a Deployment was created before its routes existed")
	}

	e.renderRoutes(p)
	e.reconcile(p)
	e.deployment()
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
}

// Objects that merely carry the expected names are not the route
// controller's output. Each of the two is checked on its own: the other one
// is the proxy's, so only the foreign one can be what holds the proxy back.
func TestProxy_RoutesMustBeItsOwn(t *testing.T) {
	for _, foreign := range []string{"edge-upstreams", "edge-certs"} {
		t.Run(foreign, func(t *testing.T) {
			e := newProxyEnv(t)
			p := e.proxy()
			meta := func(name string) metav1.ObjectMeta {
				m := metav1.ObjectMeta{Namespace: e.ns, Name: name}
				if name != foreign {
					m.OwnerReferences = []metav1.OwnerReference{
						*metav1.NewControllerRef(p, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy")),
					}
				}
				return m
			}
			e.create(&corev1.ConfigMap{ObjectMeta: meta("edge-upstreams"), Data: map[string]string{UpstreamsKey: "upstreams:\n"}})
			e.create(&corev1.Secret{ObjectMeta: meta("edge-certs")})

			e.reconcile(p)
			c := wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonWaitingForRoutes)
			if !strings.Contains(c.Message, foreign) {
				t.Errorf("message does not name %s: %s", foreign, c.Message)
			}
			var d appsv1.Deployment
			if e.get("edge", &d) {
				t.Fatal("a Deployment was created on an object that is not the proxy's")
			}
		})
	}
}

// The routes ConfigMap being there is not enough: Synapse reads one file from
// it, and a pod started without that file never looks for it again.
func TestProxy_RoutesMustHoldTheRoutesFile(t *testing.T) {
	e := newProxyEnv(t)
	p := e.proxy()
	owned := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: e.ns, Name: name, OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(p, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy")),
		}}
	}
	e.create(&corev1.ConfigMap{ObjectMeta: owned("edge-upstreams"), Data: map[string]string{"other.yaml": "x"}})
	e.create(&corev1.Secret{ObjectMeta: owned("edge-certs")})

	e.reconcile(p)
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonWaitingForRoutes)
	var d appsv1.Deployment
	if e.get("edge", &d) {
		t.Fatal("a Deployment was created without a routes file to mount")
	}
}

func TestProxy_ReconcilingAgainChangesNothing(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()

	versions := func() map[string]string {
		var svc corev1.Service
		var sa corev1.ServiceAccount
		var cfg corev1.Secret
		e.get("edge", &svc)
		e.get("edge", &sa)
		e.get(e.configSecretName(), &cfg)
		return map[string]string{
			"Deployment": e.deployment().ResourceVersion, "Service": svc.ResourceVersion,
			"ServiceAccount": sa.ResourceVersion, "config Secret": cfg.ResourceVersion, "SynapseProxy": p.ResourceVersion,
		}
	}
	before := versions()
	e.reconcile(p)
	e.reconcile(p)
	for name, v := range versions() {
		if v != before[name] {
			t.Errorf("%s was written again by a reconcile with nothing to do", name)
		}
	}
}

func TestProxy_ConfigChangeRollsThePods(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()
	first := e.configSecretName()

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"} })
	e.reconcile(p)

	second := e.configSecretName()
	if second == first {
		t.Fatal("the pod template still mounts the old config Secret")
	}
	var cfg corev1.Secret
	if !e.get(second, &cfg) || !strings.Contains(string(cfg.Data[proxyConfigKey]), "level: debug") {
		t.Errorf("the new config Secret does not hold the new config")
	}
	// No ReplicaSet holds on to the old one here.
	if got := e.configSecrets(); len(got) != 1 || got[0] != second {
		t.Errorf("config Secrets = %v, want only %s", got, second)
	}
}

// Replicas, image and Service are not configuration: changing them must not
// produce a new config, or the pods would restart to read the same file.
func TestProxy_OtherChangesKeepTheConfig(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()
	before := e.configSecretName()

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) {
		replicas := int32(3)
		s.Replicas = &replicas
		s.Image = "example.test/synapse:2.0.0"
		s.Service = &synapsev1alpha1.ProxyServiceSpec{Annotations: map[string]string{"example.test/a": "b"}}
	})
	e.reconcile(p)

	d := e.deployment()
	if e.configSecretName() != before {
		t.Error("the config Secret changed")
	}
	if *d.Spec.Replicas != 3 || proxyContainer(t, d).Image != "example.test/synapse:2.0.0" {
		t.Errorf("Deployment not updated: replicas %d, image %s", *d.Spec.Replicas, proxyContainer(t, d).Image)
	}
	var svc corev1.Service
	e.get("edge", &svc)
	if svc.Annotations["example.test/a"] != "b" {
		t.Errorf("Service annotations = %v", svc.Annotations)
	}
}

// While a rollout is in progress the previous ReplicaSet's pods still mount
// the previous config. One of them may restart, and it must find its file.
func TestProxy_KeepsConfigSecretsThatAreStillMounted(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()
	first := e.configSecretName()

	one := int32(1)
	old := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "edge-old", Labels: proxySelector(p)},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{proxyLabel: "edge", "pod-template-hash": "old"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{proxyLabel: "edge", "pod-template-hash": "old"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "synapse", Image: "example.test/synapse:1.0.0"}},
					Volumes: []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: first},
					}}},
				},
			},
		},
	}
	e.create(old)

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"} })
	e.reconcile(p)
	if got := e.configSecrets(); len(got) != 2 {
		t.Fatalf("config Secrets = %v, want the old one kept while its ReplicaSet has pods", got)
	}

	zero := int32(0)
	old.Spec.Replicas = &zero
	if err := e.k8s.Update(e.ctx, old); err != nil {
		t.Fatal(err)
	}
	e.reconcile(p)
	if got := e.configSecrets(); len(got) != 1 || got[0] == first {
		t.Errorf("config Secrets = %v, want the old one gone once its ReplicaSet is empty", got)
	}
}

func TestProxy_APIKey(t *testing.T) {
	withKey := func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Platform = &synapsev1alpha1.PlatformSpec{
			APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
		}
	}

	t.Run("no Secret", func(t *testing.T) {
		e := newProxyEnv(t)
		p := e.proxy(withKey)
		e.renderRoutes(p)
		e.reconcile(p)
		c := wantCondition(t, p, ConditionConfigValid, metav1.ConditionFalse, ReasonSecretNotFound)
		if !strings.Contains(c.Message, `"creds"`) {
			t.Errorf("message does not name the Secret: %s", c.Message)
		}
		wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
		var d appsv1.Deployment
		if e.get("edge", &d) {
			t.Error("a Deployment was created without its API key")
		}
	})

	t.Run("no such key", func(t *testing.T) {
		e := newProxyEnv(t)
		e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"}, Data: map[string][]byte{"OTHER": []byte("x")}})
		p := e.proxy(withKey)
		e.renderRoutes(p)
		e.reconcile(p)
		c := wantCondition(t, p, ConditionConfigValid, metav1.ConditionFalse, ReasonSecretKeyNotFound)
		if !strings.Contains(c.Message, `"API_KEY"`) {
			t.Errorf("message does not name the key: %s", c.Message)
		}
	})

	// An empty key would start the proxy without the platform and say nothing.
	t.Run("an empty key", func(t *testing.T) {
		e := newProxyEnv(t)
		e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"}, Data: map[string][]byte{"API_KEY": {}}})
		p := e.proxy(withKey)
		e.renderRoutes(p)
		e.reconcile(p)
		wantCondition(t, p, ConditionConfigValid, metav1.ConditionFalse, ReasonSecretKeyNotFound)
		var d appsv1.Deployment
		if e.get("edge", &d) {
			t.Error("a Deployment was created with an empty API key")
		}
	})

	t.Run("the key reaches the config and a new key rolls the pods", func(t *testing.T) {
		e := newProxyEnv(t)
		creds := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "creds"}, Data: map[string][]byte{"API_KEY": []byte("k-1")}}
		e.create(creds)
		p := e.running(withKey)

		first := e.configSecretName()
		var cfg corev1.Secret
		e.get(first, &cfg)
		if !strings.Contains(string(cfg.Data[proxyConfigKey]), "api_key: k-1") {
			t.Fatalf("the API key is not in the rendered config:\n%s", cfg.Data[proxyConfigKey])
		}

		creds.Data["API_KEY"] = []byte("k-2")
		if err := e.k8s.Update(e.ctx, creds); err != nil {
			t.Fatal(err)
		}
		e.reconcile(p)
		if e.configSecretName() == first {
			t.Error("a rotated API key did not produce a new config Secret")
		}
	})
}

// A spec that cannot be rendered must not take down what is running.
func TestProxy_InvalidSpecLeavesTheRunningProxyAlone(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()
	before := e.deployment().ResourceVersion
	hash := p.Status.ConfigHash

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(`{"mode": "agent"}`)}
	})
	e.reconcile(p)

	c := wantCondition(t, p, ConditionConfigValid, metav1.ConditionFalse, ReasonInvalidConfig)
	if !strings.Contains(c.Message, "spec.config.mode") {
		t.Errorf("message does not name the offending key: %s", c.Message)
	}
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	if e.deployment().ResourceVersion != before {
		t.Error("the Deployment was written despite the invalid spec")
	}
	if p.Status.ConfigHash != hash {
		t.Errorf("configHash moved to %q although nothing new was rendered", p.Status.ConfigHash)
	}
	if p.Status.ObservedGeneration != p.Generation {
		t.Error("the invalid generation is not marked as observed")
	}
}

func TestProxy_NameCollision(t *testing.T) {
	e := newProxyEnv(t)
	theirs := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "edge", Labels: map[string]string{"theirs": "yes"}},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 9}}},
	}
	e.create(theirs)

	p := e.proxy()
	e.renderRoutes(p)
	res := e.reconcile(p)

	c := wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonNameCollision)
	if !strings.Contains(c.Message, "Service") || !strings.Contains(c.Message, `"edge"`) {
		t.Errorf("message does not say what is in the way: %s", c.Message)
	}
	// Nothing watches an object the proxy does not own, so it has to look again.
	if res.RequeueAfter <= 0 {
		t.Error("no retry is scheduled")
	}

	var svc corev1.Service
	e.get("edge", &svc)
	if svc.Labels["theirs"] != "yes" || len(svc.OwnerReferences) != 0 || svc.Spec.Ports[0].Port != 9 {
		t.Errorf("the Service in the way was changed: %+v", svc)
	}
	// Half a proxy is worse than none.
	var d appsv1.Deployment
	var sa corev1.ServiceAccount
	if e.get("edge", &d) || e.get("edge", &sa) || len(e.configSecrets()) != 0 {
		t.Error("some of the proxy's objects were created although one name is taken")
	}
}

func TestProxy_RemovedListenerIsRemovedEverywhere(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running(func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Listeners = append(s.Listeners, synapsev1alpha1.Listener{Name: "https", Port: 443, Protocol: "TLS"})
	})
	var svc corev1.Service
	e.get("edge", &svc)
	if len(svc.Spec.Ports) != 2 || len(proxyContainer(t, e.deployment()).Ports) != 2 {
		t.Fatal("the second listener was not applied")
	}

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Listeners = s.Listeners[:1] })
	e.reconcile(p)

	e.get("edge", &svc)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Name != "http" {
		t.Errorf("Service ports = %+v", svc.Spec.Ports)
	}
	if ports := proxyContainer(t, e.deployment()).Ports; len(ports) != 1 || ports[0].Name != "http" {
		t.Errorf("container ports = %+v", ports)
	}
}

// `kubectl rollout restart` works by annotating the pod template. The next
// reconcile must not undo it, or the restart is cancelled half way.
func TestProxy_LeavesOtherWritersFieldsAlone(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()

	d := e.deployment()
	patch := client.MergeFrom(d.DeepCopy())
	d.Spec.Template.Annotations = map[string]string{"kubectl.kubernetes.io/restartedAt": "2024-01-01T00:00:00Z"}
	if err := e.k8s.Patch(e.ctx, d, patch, client.FieldOwner("kubectl-rollout")); err != nil {
		t.Fatal(err)
	}

	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Image = "example.test/synapse:2.0.0" })
	e.reconcile(p)

	d = e.deployment()
	if proxyContainer(t, d).Image != "example.test/synapse:2.0.0" {
		t.Fatal("the reconcile did not apply the new image")
	}
	if d.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Error("the restart annotation was removed")
	}
}

func TestProxy_StatusFollowsTheDeployment(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running(func(s *synapsev1alpha1.SynapseProxySpec) {
		replicas := int32(2)
		s.Replicas = &replicas
	})

	report := func(mutate func(*appsv1.DeploymentStatus)) {
		t.Helper()
		d := e.deployment()
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation}
		mutate(&d.Status)
		if err := e.k8s.Status().Update(e.ctx, d); err != nil {
			t.Fatal(err)
		}
		e.reconcile(p)
	}
	available := func(status corev1.ConditionStatus) appsv1.DeploymentCondition {
		return appsv1.DeploymentCondition{Type: appsv1.DeploymentAvailable, Status: status, Reason: "MinimumReplicas",
			LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()}
	}

	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 1, 1, 1
		s.UnavailableReplicas = 1
		s.Conditions = []appsv1.DeploymentCondition{available(corev1.ConditionFalse)}
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
	wantCondition(t, p, ConditionAvailable, metav1.ConditionFalse, "MinimumReplicas")
	if p.Status.Replicas != 2 || p.Status.UpdatedReplicas != 1 || p.Status.ReadyReplicas != 1 {
		t.Errorf("replica counts = %d/%d/%d", p.Status.Replicas, p.Status.UpdatedReplicas, p.Status.ReadyReplicas)
	}

	// Scaling up: every pod there is runs the current spec, but there are
	// fewer of them than asked for.
	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 1, 1, 1, 1
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)

	// Updated but an old pod is still shutting down.
	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 3, 2, 3, 3
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)

	// Updated, but not all available yet.
	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 2, 1, 1
		s.UnavailableReplicas = 1
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)

	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 2, 2, 2
		s.Conditions = []appsv1.DeploymentCondition{available(corev1.ConditionTrue)}
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionTrue, ReasonApplied)
	wantCondition(t, p, ConditionAvailable, metav1.ConditionTrue, "MinimumReplicas")

	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas = 2, 1
		s.UnavailableReplicas = 1
		s.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded", Message: "stuck", LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonProgressDeadlineExceeded)

	// A status from before the last change says nothing about it, however
	// complete it looks. Change the pod template without reporting again.
	report(func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 2, 2, 2
	})
	wantCondition(t, p, ConditionReady, metav1.ConditionTrue, ReasonApplied)
	e.edit(p, func(s *synapsev1alpha1.SynapseProxySpec) { s.Image = "example.test/synapse:2.0.0" })
	e.reconcile(p)
	if d := e.deployment(); d.Generation <= d.Status.ObservedGeneration {
		t.Fatalf("the Deployment's generation did not move: %d, observed %d", d.Generation, d.Status.ObservedGeneration)
	}
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
}

func TestProxy_AddressesComeFromTheLoadBalancer(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running(func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Service = &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer}
	})

	var svc corev1.Service
	e.get("edge", &svc)
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}, {Hostname: "lb.example.test"}}
	if err := e.k8s.Status().Update(e.ctx, &svc); err != nil {
		t.Fatal(err)
	}
	e.reconcile(p)

	if got := p.Status.Addresses; len(got) != 2 || got[0] != "203.0.113.10" || got[1] != "lb.example.test" {
		t.Errorf("addresses = %v", got)
	}
}

func TestProxy_Gone(t *testing.T) {
	e := newProxyEnv(t)
	gone := &synapsev1alpha1.SynapseProxy{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "never-existed"}}
	if _, err := e.proxies.Reconcile(e.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gone)}); err != nil {
		t.Errorf("reconciling a proxy that does not exist: %v", err)
	}
}
