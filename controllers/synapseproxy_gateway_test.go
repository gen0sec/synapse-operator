package controllers

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

var gatewayTestRuns atomic.Int64

// The translation is tested on its own, object by object. This runs the
// controllers on a real API server for what that leaves out: that each kind
// of object, when it changes, is looked at again; that what is found is
// written to the objects' status and then left alone; and that a route's
// status is shared with whoever else serves it.
func TestProxyGateway_FollowsItsInputs(t *testing.T) {
	if !gatewayAPIServed(t) {
		t.Skip("the test API server is too old for the Gateway API's CRDs")
	}
	cfg := apiServer(t)
	scheme := proxyTestScheme(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SetupProxyControllers(mgr, "cluster.local"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	// The proxy and its Gateway in one namespace, the route in another, its
	// backend in a third: every reference crosses.
	ns := fmt.Sprintf("gateway-watch-%d", gatewayTestRuns.Add(1))
	apps, backends := ns+"-apps", ns+"-backends"
	create := func(obj client.Object) {
		t.Helper()
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}
	for _, name := range []string{ns, apps, backends} {
		create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	far := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: backends, Name: "far"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}},
	}
	create(far)
	edge := testProxy(ns, "edge")
	edge.UID = ""
	create(edge)
	class := gwClass(ns, ns, "edge")
	class.Generation = 0
	create(&class)
	listener := httpListener("http", 80, "gw.example.com")
	listener.AllowedRoutes.Namespaces = &gwv1.RouteNamespaces{
		From: ptr(gwv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"gateway": ns}},
	}
	gw := gateway(ns, "web", listener)
	gw.Generation, gw.Spec.GatewayClassName = 0, gwv1.ObjectName(ns)
	create(&gw)
	rule := gwv1.HTTPRouteRule{}
	rule.BackendRefs = []gwv1.HTTPBackendRef{backendRef(backends, "far")}
	route := httpRoute("shop", nil, rule)
	route.Generation, route.CreationTimestamp, route.Namespace = 0, metav1.Time{}, apps
	route.Spec.ParentRefs = []gwv1.ParentReference{{Namespace: ptr(gwv1.Namespace(ns)), Name: "web"}}
	create(&route)

	// ours returns the route's parent status that this proxy's Gateway has.
	ours := func() *gwv1.RouteParentStatus {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&route), &route); err != nil {
			t.Fatal(err)
		}
		for i := range route.Status.Parents {
			p := &route.Status.Parents[i]
			if string(p.ControllerName) == ControllerName && p.ParentRef.Name == "web" {
				return p
			}
		}
		return nil
	}
	routeIs := func(what, kind string, status metav1.ConditionStatus, reason string) {
		t.Helper()
		eventually(t, what, func() string {
			p := ours()
			if p == nil {
				return "the route has no status of ours"
			}
			c := meta.FindStatusCondition(p.Conditions, kind)
			if c == nil || c.Status != status || c.Reason != reason {
				return fmt.Sprintf("%s is %+v, want %s/%s", kind, c, status, reason)
			}
			return ""
		})
	}
	routes := func() string {
		var cm corev1.ConfigMap
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "edge-upstreams"}, &cm); err != nil {
			return ""
		}
		return cm.Data[UpstreamsKey]
	}

	routeIs("a route from a namespace the listener does not select is turned away", "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")

	// The namespace gets the label: nothing else changed.
	var appsNS corev1.Namespace
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := k8s.Get(ctx, client.ObjectKey{Name: apps}, &appsNS); err != nil {
			return err
		}
		appsNS.Labels = map[string]string{"gateway": ns}
		return k8s.Update(ctx, &appsNS)
	}); err != nil {
		t.Fatal(err)
	}
	routeIs("a label on its namespace lets the route attach", "Accepted", metav1.ConditionTrue, "Accepted")
	routeIs("a backend in another namespace is not its to use", "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	if strings.Contains(routes(), "gw.example.com") {
		t.Errorf("the route is rendered without leave to use its backend:\n%s", routes())
	}

	// That namespace says the route may.
	grant := &gwv1beta1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: backends, Name: "from-apps"}}
	grant.Spec.From = []gwv1beta1.ReferenceGrantFrom{{Group: gwv1.GroupName, Kind: "HTTPRoute", Namespace: gwv1.Namespace(apps)}}
	grant.Spec.To = []gwv1beta1.ReferenceGrantTo{{Kind: "Service"}}
	create(grant)
	routeIs("a ReferenceGrant that appears is acted on", "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	eventually(t, "the route is rendered", func() string {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(far), far); err != nil {
			return err.Error()
		}
		if body := routes(); !strings.Contains(body, `"gw.example.com"`) || !strings.Contains(body, far.Spec.ClusterIP+":80") {
			return "routes are\n" + body
		}
		return ""
	})

	// The Gateway and its class are told too.
	eventually(t, "the Gateway reports its listener, the route and an address", func() string {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&gw), &gw); err != nil {
			return err.Error()
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&class), &class); err != nil {
			return err.Error()
		}
		switch {
		case !meta.IsStatusConditionTrue(class.Status.Conditions, "Accepted"):
			return "the class is not accepted"
		case !meta.IsStatusConditionTrue(gw.Status.Conditions, "Programmed"):
			return fmt.Sprintf("the Gateway's conditions are %+v", gw.Status.Conditions)
		case len(gw.Status.Listeners) != 1 || gw.Status.Listeners[0].AttachedRoutes != 1:
			return fmt.Sprintf("the listeners are %+v", gw.Status.Listeners)
		case len(gw.Status.Addresses) == 0:
			return "the Gateway has no address"
		}
		return ""
	})

	// Told once. A status written again on every pass would be seen by
	// every watcher of these objects, this controller among them.
	versions := func() string {
		_ = ours()
		_ = k8s.Get(ctx, client.ObjectKeyFromObject(&gw), &gw)
		_ = k8s.Get(ctx, client.ObjectKeyFromObject(&class), &class)
		return route.ResourceVersion + " " + gw.ResourceVersion + " " + class.ResourceVersion
	}
	settled := versions()
	time.Sleep(2 * time.Second)
	if now := versions(); now != settled {
		t.Errorf("the objects were written again with nothing changed: versions %s, were %s", now, settled)
	}

	// The Gateway alone changes, then its class alone: each is noticed.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&gw), &gw); err != nil {
			return err
		}
		gw.Spec.Listeners[0].Hostname = ptr(gwv1.Hostname("moved.example.com"))
		return k8s.Update(ctx, &gw)
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a Gateway that changes is rendered again", func() string {
		if body := routes(); !strings.Contains(body, `"moved.example.com"`) || strings.Contains(body, "gw.example.com") {
			return "routes are\n" + body
		}
		return ""
	})
	rebind := func(proxy string) {
		t.Helper()
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := k8s.Get(ctx, client.ObjectKeyFromObject(&class), &class); err != nil {
				return err
			}
			class.Spec.ParametersRef.Name = proxy
			return k8s.Update(ctx, &class)
		}); err != nil {
			t.Fatal(err)
		}
	}
	rebind("another-proxy")
	eventually(t, "a class that names another proxy takes its Gateways along", func() string {
		if body := routes(); strings.Contains(body, "moved.example.com") {
			return "routes are\n" + body
		}
		return ""
	})
	rebind("edge")
	eventually(t, "a class that names the proxy again brings them back", func() string {
		if body := routes(); !strings.Contains(body, `"moved.example.com"`) {
			return "routes are\n" + body
		}
		return ""
	})

	// Somebody else's controller serves the route as well, and says so.
	theirs := gwv1.RouteParentStatus{
		ParentRef:      gwv1.ParentReference{Name: "theirs"},
		ControllerName: "example.com/another",
		Conditions: []metav1.Condition{{
			Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", LastTransitionTime: metav1.Now(),
		}},
	}
	// So does another SynapseProxy, through a Gateway of its own: the same
	// controller name, and not this proxy's entry to touch.
	anotherProxys := theirs
	anotherProxys.ParentRef = gwv1.ParentReference{Name: "another-proxys-gateway"}
	anotherProxys.ControllerName = ControllerName
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_ = ours()
		route.Status.Parents = append(route.Status.Parents, theirs, anotherProxys)
		return k8s.Status().Update(ctx, &route)
	}); err != nil {
		t.Fatal(err)
	}

	// The route leaves the Gateway: its routes go, and so does what was
	// said of it here. What the other controller said stays.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_ = ours()
		route.Spec.ParentRefs = []gwv1.ParentReference{{Name: "theirs"}}
		return k8s.Update(ctx, &route)
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a route that left is no longer rendered or spoken for", func() string {
		if p := ours(); p != nil {
			return fmt.Sprintf("the route still carries %+v", *p)
		}
		if len(route.Status.Parents) != 2 || route.Status.Parents[0].ControllerName != "example.com/another" ||
			route.Status.Parents[1].ParentRef.Name != "another-proxys-gateway" {
			return fmt.Sprintf("what the others said of the route is now %+v", route.Status.Parents)
		}
		if body := routes(); strings.Contains(body, "moved.example.com") {
			return "routes are\n" + body
		}
		return ""
	})

	for _, obj := range []client.Object{&route, &gw, &class, edge} {
		if err := k8s.Delete(ctx, obj); err != nil {
			t.Errorf("delete %T %s: %v", obj, obj.GetName(), err)
		}
	}
}

// A condition keeps the time it last changed for as long as its status is
// the same. Given a new time on every pass, no status would ever compare
// equal to the one already there, and each would be written again.
func TestMergeConditions(t *testing.T) {
	then := metav1.NewTime(time.Unix(1000, 0))
	have := []metav1.Condition{
		{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", LastTransitionTime: then},
		{Type: "Programmed", Status: metav1.ConditionFalse, Reason: "Invalid", LastTransitionTime: then},
		{Type: "Gone", Status: metav1.ConditionTrue, Reason: "Old", LastTransitionTime: then},
	}
	got := mergeConditions(have, []metav1.Condition{
		{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", Message: "reworded"},
		{Type: "Programmed", Status: metav1.ConditionTrue, Reason: "Programmed"},
		{Type: "ResolvedRefs", Status: metav1.ConditionTrue, Reason: "ResolvedRefs"},
	})
	if len(got) != 3 {
		t.Fatalf("got %d conditions, want the three asked for and not the one that is gone: %+v", len(got), got)
	}
	if !got[0].LastTransitionTime.Equal(&then) || got[0].Message != "reworded" {
		t.Errorf("a condition whose status did not change is %+v, want its old time and its new message", got[0])
	}
	for _, c := range got[1:] {
		if c.LastTransitionTime.Equal(&then) || c.LastTransitionTime.IsZero() {
			t.Errorf("a condition that changed, or is new, has the time %v", c.LastTransitionTime)
		}
	}
}

// Whether the Gateway API is there is asked of the API server in two ways
// that have to agree: the one the operator uses, and a plain listing of what
// the server serves. A wrong "no" would be silent twice over: the operator
// would serve no Gateway, and every test of it here would skip itself.
func TestGatewayAPIServed(t *testing.T) {
	cfg := apiServer(t)
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	served := func(groupVersion string, kinds ...string) bool {
		list, err := dc.ServerResourcesForGroupVersion(groupVersion)
		if err != nil {
			return false
		}
		for _, kind := range kinds {
			if !slices.ContainsFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Kind == kind }) {
				return false
			}
		}
		return true
	}
	want := served(gwv1.GroupName+"/v1", "GatewayClass", "Gateway", "HTTPRoute") && served(gwv1.GroupName+"/v1beta1", "ReferenceGrant")
	if got := gatewayAPIServed(t); got != want {
		t.Errorf("GatewayAPIServed = %v, and the API server serves the kinds = %v", got, want)
	}
	// The version the tests are pinned to takes the CRDs. If it does not,
	// something is wrong with installing them, not with the server.
	if os.Getenv("ENVTEST_K8S_VERSION") == "" && os.Getenv("KUBEBUILDER_ASSETS") == "" && !want {
		t.Errorf("the test API server has no Gateway API, so every test of it is skipped")
	}
}

// The address a Gateway is given is the proxy's own Service's. One of the
// same name that the proxy does not control is somebody else's.
func TestGatewayInputs_AddressesAreOfTheProxysOwnService(t *testing.T) {
	proxy := testProxy("edge", "edge")
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "edge"}}
	svc.Spec.ClusterIP = "10.0.0.9"
	for name, tc := range map[string]struct {
		owned bool
		want  []string
	}{"its own": {true, []string{"10.0.0.9"}}, "another's": {false, nil}} {
		t.Run(name, func(t *testing.T) {
			s := svc.DeepCopy()
			if tc.owned {
				s.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(proxy, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy"))}
			}
			r := newRouteReconciler(t, proxy, s)
			in, failed, err := r.gatewayInputs(context.Background(), proxy)
			if err != nil || failed() != nil {
				t.Fatal(err, failed())
			}
			if !slices.Equal(in.addresses, tc.want) {
				t.Errorf("addresses are %v, want %v", in.addresses, tc.want)
			}
		})
	}
}
