package controllers

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

func proxyTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := testScheme(t)
	if err := synapsev1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func testProxy(namespace, name string) *synapsev1alpha1.SynapseProxy {
	return &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID("uid-" + namespace + "-" + name)},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:     "example.test/synapse:1.0.0",
			Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
		},
	}
}

// classFor is an IngressClass of ours that hands its Ingresses to proxy.
func classFor(name string, proxy *synapsev1alpha1.SynapseProxy) *networkingv1.IngressClass {
	ic := ourIngressClass(name, false)
	ic.Spec.Parameters = &networkingv1.IngressClassParametersReference{
		APIGroup:  ptr("synapse.gen0sec.com"),
		Kind:      "SynapseProxy",
		Name:      proxy.Name,
		Scope:     ptr("Namespace"),
		Namespace: ptr(proxy.Namespace),
	}
	return ic
}

func newRouteReconciler(t *testing.T, objs ...client.Object) *SynapseRouteReconciler {
	t.Helper()
	return &SynapseRouteReconciler{
		Client: fake.NewClientBuilder().WithScheme(proxyTestScheme(t)).WithObjects(objs...).
			WithStatusSubresource(&networkingv1.Ingress{}).Build(),
		ClusterDomain: "cluster.local",
	}
}

func reconcileRoutes(t *testing.T, r *SynapseRouteReconciler, proxy *synapsev1alpha1.SynapseProxy) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(proxy)})
	return err
}

func mustReconcileRoutes(t *testing.T, r *SynapseRouteReconciler, proxy *synapsev1alpha1.SynapseProxy) {
	t.Helper()
	if err := reconcileRoutes(t, r, proxy); err != nil {
		t.Fatalf("reconcile %s/%s: %v", proxy.Namespace, proxy.Name, err)
	}
}

// upstreamsOf returns the routes rendered for proxy.
func upstreamsOf(t *testing.T, r *SynapseRouteReconciler, proxy *synapsev1alpha1.SynapseProxy) string {
	t.Helper()
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: proxy.Namespace, Name: proxy.Name + "-upstreams"}
	if err := r.Get(context.Background(), key, &cm); err != nil {
		t.Fatalf("upstreams ConfigMap of %s: %v", proxy.Name, err)
	}
	return cm.Data[UpstreamsKey]
}

func certsOf(t *testing.T, r *SynapseRouteReconciler, proxy *synapsev1alpha1.SynapseProxy) *corev1.Secret {
	t.Helper()
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proxy.Namespace, Name: proxy.Name + "-certs"}
	if err := r.Get(context.Background(), key, &sec); err != nil {
		t.Fatalf("certs Secret of %s: %v", proxy.Name, err)
	}
	return &sec
}

// Each proxy gets the Ingresses of the classes bound to it and nobody else's.
func TestRoutes_EachProxyRendersItsOwnClasses(t *testing.T) {
	edge, internal := testProxy("synapse-os", "edge"), testProxy("synapse-os", "internal")
	r := newRouteReconciler(t, edge, internal,
		classFor("public", edge), classFor("partners", edge), classFor("private", internal),
		routedIngress("shop", ptr("public"), "shop.example.com"),
		routedIngress("b2b", ptr("partners"), "b2b.example.com"),
		routedIngress("admin", ptr("private"), "admin.example.com"),
		routedIngress("elsewhere", ptr("nginx"), "nginx.example.com"),
	)
	mustReconcileRoutes(t, r, edge)
	mustReconcileRoutes(t, r, internal)

	wantHosts(t, upstreamsOf(t, r, edge),
		[]string{"shop.example.com", "b2b.example.com"},
		[]string{"admin.example.com", "nginx.example.com"})
	wantHosts(t, upstreamsOf(t, r, internal),
		[]string{"admin.example.com"},
		[]string{"shop.example.com", "b2b.example.com", "nginx.example.com"})
}

// A proxy nobody has bound a class to still gets both outputs: Synapse will
// not start its route watcher without an upstreams file, and the pod mounts
// the certificates Secret whether or not it holds anything.
func TestRoutes_UnboundProxyGetsEmptyOutputs(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	r := newRouteReconciler(t, edge,
		ourIngressClass("synapse", true), // ours, and the default, but bound to no proxy
		routedIngress("named", ptr("synapse"), "named.example.com"),
		routedIngress("empty", ptr(""), "empty.example.com"),
		routedIngress("classless", nil, "classless.example.com"),
	)
	mustReconcileRoutes(t, r, edge)

	if got, floor := upstreamsOf(t, r, edge), proxyFloor(); got != floor {
		t.Errorf("upstreams =\n%s\nwant the empty document\n%s", got, floor)
	}
	if sec := certsOf(t, r, edge); len(sec.Data) != 0 {
		t.Errorf("certs Secret holds %d keys, want none", len(sec.Data))
	}
}

// Which proxy receives a class is decided by the cluster-scoped IngressClass
// alone, and it has to name the proxy exactly.
func TestRoutes_ClassBinding(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	ing := routedIngress("shop", ptr("public"), "shop.example.com")

	bound := func(mutate func(*networkingv1.IngressClass)) string {
		t.Helper()
		ic := classFor("public", edge)
		if mutate != nil {
			mutate(ic)
		}
		r := newRouteReconciler(t, edge, ic, ing)
		mustReconcileRoutes(t, r, edge)
		return upstreamsOf(t, r, edge)
	}

	wantHosts(t, bound(nil), []string{"shop.example.com"}, nil)

	notBound := map[string]func(*networkingv1.IngressClass){
		"another controller's class":   func(ic *networkingv1.IngressClass) { ic.Spec.Controller = "example.test/other" },
		"no parameters":                func(ic *networkingv1.IngressClass) { ic.Spec.Parameters = nil },
		"another API group":            func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.APIGroup = ptr("example.test") },
		"no API group":                 func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.APIGroup = nil },
		"another kind":                 func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Kind = "ConfigMap" },
		"another name":                 func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Name = "internal" },
		"same name, another namespace": func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Namespace = ptr("elsewhere") },
		"cluster scope":                func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Scope = ptr("Cluster") },
		"no scope":                     func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Scope = nil },
		"no namespace":                 func(ic *networkingv1.IngressClass) { ic.Spec.Parameters.Namespace = nil },
	}
	for name, mutate := range notBound {
		t.Run(name, func(t *testing.T) {
			wantHosts(t, bound(mutate), nil, []string{"shop.example.com"})
		})
	}
}

func TestRoutes_DefaultClass(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	ic := classFor("public", edge)
	ic.Annotations = map[string]string{"ingressclass.kubernetes.io/is-default-class": "true"}

	r := newRouteReconciler(t, edge, ic, routedIngress("classless", nil, "classless.example.com"))
	mustReconcileRoutes(t, r, edge)
	wantHosts(t, upstreamsOf(t, r, edge), []string{"classless.example.com"}, nil)
}

func TestRoutes_OutputsBelongToTheProxy(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	r := newRouteReconciler(t, edge)
	mustReconcileRoutes(t, r, edge)

	var cm corev1.ConfigMap
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "synapse-os", Name: "edge-upstreams"}, &cm); err != nil {
		t.Fatal(err)
	}
	for name, obj := range map[string]metav1.Object{"upstreams ConfigMap": &cm, "certs Secret": certsOf(t, r, edge)} {
		ref := metav1.GetControllerOf(obj)
		if ref == nil || ref.UID != edge.UID || ref.Kind != "SynapseProxy" || ref.APIVersion != "synapse.gen0sec.com/v1alpha1" || ref.Name != "edge" {
			t.Errorf("%s: controller = %+v, want the SynapseProxy", name, ref)
		}
	}
}

// The proxy's certificates Secret is where TLS keys from other namespaces
// end up. That copy is the reason binding is the IngressClass's to make.
func TestRoutes_ProjectsCertificatesAcrossNamespaces(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	ing := routedIngress("shop", ptr("public"), "shop.example.com")
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"shop.example.com"}, SecretName: "shop-tls"}}

	r := newRouteReconciler(t, edge, classFor("public", edge), ing, tlsSecret("default", "shop-tls", "CRT", "KEY"))
	mustReconcileRoutes(t, r, edge)

	sec := certsOf(t, r, edge)
	if string(sec.Data["shop.example.com.crt"]) != "CRT" || string(sec.Data["shop.example.com.key"]) != "KEY" {
		t.Errorf("certificate not projected; keys %v", keysOf(sec.Data))
	}
	if got := parseV2(t, upstreamsOf(t, r, edge)).Hosts["shop.example.com"].TLS.Terminate; got == nil || got.Cert != "shop.example.com" {
		t.Errorf("host not bound to its certificate:\n%s", upstreamsOf(t, r, edge))
	}
}

// proxyFloor is the routes file of a proxy that has nothing to route.
func proxyFloor() string {
	m := newRenderModel()
	m.sameAsV1 = true
	return renderUpstreamsV2(m)
}

// A proxy's routes are written in Synapse's v2 schema, whatever is in them,
// and say what the v1 file they used to be written in said: see
// renderUpstreamsV2.
func TestRoutes_AreAV2File(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	plain := routedIngress("plain", ptr("public"), "plain.example.com")
	versioned := routedIngress("versioned", ptr("public"), "api.example.com")
	versioned.Annotations = map[string]string{"synapse.gen0sec.com/use-regex": "true"}
	versioned.Spec.Rules[0].HTTP.Paths[0].Path = "/v[0-9]+/items"
	versioned.Spec.Rules[0].HTTP.Paths[0].PathType = ptr(networkingv1.PathTypeImplementationSpecific)

	r := newRouteReconciler(t, edge, classFor("public", edge), plain, versioned)
	mustReconcileRoutes(t, r, edge)
	rendered := upstreamsOf(t, r, edge)
	doc := parseV2(t, rendered)

	// A host with no certificate, which a v2 file has to say.
	host, ok := doc.Hosts["plain.example.com"]
	if !ok || host.TLS.Terminate == nil || host.TLS.Terminate.Cert != "" {
		t.Fatalf("plain.example.com is %+v in\n%s", host, rendered)
	}
	route, ok := host.Paths["/"]
	if !ok || route.Upstream != "app.default.svc.cluster.local:80" {
		t.Errorf("its route is %+v", route)
	}
	if route.SSLEnabled == nil || *route.SSLEnabled {
		t.Errorf("how its backend is reached is not said: %+v", route.SSLEnabled)
	}
	if got := doc.Timeouts.Read; got == nil || *got != v1ReadTimeoutSeconds {
		t.Errorf("the file's read timeout is %v", got)
	}
	// A route chosen by an expression, which a v1 file was needed for.
	var expressions int
	for _, route := range doc.Hosts["api.example.com"].Paths {
		if route.MatchExpr != "" {
			expressions++
		}
	}
	if expressions != 1 {
		t.Errorf("api.example.com has %d routes chosen by an expression, want 1:\n%s", expressions, rendered)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The proxy's pods read the routes through a volume that the kubelet
// refreshes on its own schedule, so a pod address rendered here would be
// stale for about a minute after every rollout of the backend. The Service's
// cluster IP does not move.
func TestRoutes_BackendsAreAddressedByClusterIP(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	r := newRouteReconciler(t, edge, classFor("public", edge),
		routedIngress("shop", ptr("public"), "shop.example.com"),
		svcWithIP("default", "app", "10.0.0.42", 80, ""))
	mustReconcileRoutes(t, r, edge)

	if out := upstreamsOf(t, r, edge); !strings.Contains(out, `"10.0.0.42:80"`) {
		t.Errorf("backend not addressed by cluster IP:\n%s", out)
	}
}

func TestRoutes_RefusesOutputsItDoesNotControl(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	theirs := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "synapse-os", Name: "edge-upstreams"},
		Data:       map[string]string{"theirs": "untouched"},
	}
	r := newRouteReconciler(t, edge, theirs)

	if err := reconcileRoutes(t, r, edge); err == nil {
		t.Fatal("reconcile succeeded, want an error so it is retried")
	}
	var cm corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(theirs), &cm); err != nil {
		t.Fatal(err)
	}
	if _, written := cm.Data[UpstreamsKey]; written || cm.Data["theirs"] != "untouched" {
		t.Errorf("the ConfigMap was written to: %v", cm.Data)
	}
}

// Ingresses report where they can be reached: the address of the Service in
// front of the proxy that serves them.
func TestRoutes_PublishesTheProxyAddressOnItsIngresses(t *testing.T) {
	ctx := context.Background()
	edge := testProxy("synapse-os", "edge")
	ing := routedIngress("shop", ptr("public"), "shop.example.com")

	service := func(controlled bool) *corev1.Service {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "synapse-os", Name: "edge"}}
		if controlled {
			svc.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(edge, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy"))}
		}
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}, {Hostname: "lb.example.test"}}
		return svc
	}
	published := func(r *SynapseRouteReconciler) []networkingv1.IngressLoadBalancerIngress {
		t.Helper()
		mustReconcileRoutes(t, r, edge)
		var got networkingv1.Ingress
		if err := r.Get(ctx, client.ObjectKeyFromObject(ing), &got); err != nil {
			t.Fatal(err)
		}
		return got.Status.LoadBalancer.Ingress
	}

	got := published(newRouteReconciler(t, edge, classFor("public", edge), ing.DeepCopy(), service(true)))
	if len(got) != 2 || got[0].IP != "203.0.113.10" || got[1].Hostname != "lb.example.test" {
		t.Errorf("status = %+v, want the Service's two addresses", got)
	}

	// A Service that merely shares the proxy's name is not the proxy's.
	if got := published(newRouteReconciler(t, edge, classFor("public", edge), ing.DeepCopy(), service(false))); len(got) != 0 {
		t.Errorf("status = %+v from a Service the proxy does not control", got)
	}
	if got := published(newRouteReconciler(t, edge, classFor("public", edge), ing.DeepCopy())); len(got) != 0 {
		t.Errorf("status = %+v with no Service at all", got)
	}
}

func TestRoutes_DeletedProxy(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	r := newRouteReconciler(t, edge, classFor("public", edge), routedIngress("shop", ptr("public"), "shop.example.com"))
	mustReconcileRoutes(t, r, edge)
	if got := gaugeValue(t, mProxyHosts.WithLabelValues("synapse-os", "edge")); got != 1 {
		t.Fatalf("hosts gauge = %v before deletion, want 1", got)
	}

	mProxyRenderErrors.WithLabelValues("synapse-os", "edge").Inc()

	if err := r.Delete(context.Background(), edge); err != nil {
		t.Fatal(err)
	}
	mustReconcileRoutes(t, r, edge)

	// The series must be gone, not zero. Reading one through WithLabelValues
	// would recreate it, so count what the vectors actually hold.
	if n := proxySeries(t, "synapse-os", "edge"); n != 0 {
		t.Errorf("%d per-proxy series left after the proxy was deleted", n)
	}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// proxySeries counts the per-proxy gauge series that exist for one proxy.
func proxySeries(t *testing.T, namespace, name string) int {
	t.Helper()
	n := 0
	for _, vec := range []prometheus.Collector{mProxyHosts, mProxyRoutes, mProxyCerts, mProxyRenderErrors} {
		ch := make(chan prometheus.Metric)
		go func() {
			vec.Collect(ch)
			close(ch)
		}()
		for m := range ch {
			var d dto.Metric
			if err := m.Write(&d); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{}
			for _, l := range d.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["namespace"] == namespace && labels["name"] == name {
				n++
			}
		}
	}
	return n
}
