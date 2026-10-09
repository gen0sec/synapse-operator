package controllers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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
	if err := SetupProxyControllers(context.Background(), mgr, "cluster.local", ""); err != nil {
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

	// A second proxy serves the route as well, through a Gateway of its
	// own, and so does somebody else's controller. A route's status is one
	// list, and all three write to it.
	other := testProxy(ns, "other")
	other.UID = ""
	create(other)
	otherClass := gwClass(ns+"-other", ns, "other")
	otherClass.Generation = 0
	create(&otherClass)
	side := gateway(apps, "side", httpListener("http", 80, "side.example.com"))
	side.Generation, side.Spec.GatewayClassName = 0, gwv1.ObjectName(ns+"-other")
	create(&side)
	change(t, k8s, &route, func() {
		route.Spec.ParentRefs = []gwv1.ParentReference{{Namespace: ptr(gwv1.Namespace(ns)), Name: "web"}, {Name: "side"}}
	})
	// entry returns what a controller says of the route for one parent.
	entry := func(controller, parent string) *gwv1.RouteParentStatus {
		_ = ours()
		for i := range route.Status.Parents {
			if p := &route.Status.Parents[i]; string(p.ControllerName) == controller && string(p.ParentRef.Name) == parent {
				return p
			}
		}
		return nil
	}
	eventually(t, "each proxy speaks for its own Gateway", func() string {
		for _, parent := range []string{"web", "side"} {
			p := entry(ControllerName, parent)
			if p == nil || !meta.IsStatusConditionTrue(p.Conditions, "Accepted") {
				return fmt.Sprintf("for %s the route says %+v", parent, p)
			}
		}
		return ""
	})
	theirs := gwv1.RouteParentStatus{
		ParentRef:      gwv1.ParentReference{Name: "theirs"},
		ControllerName: "example.com/another",
		Conditions: []metav1.Condition{{
			Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", LastTransitionTime: metav1.Now(),
		}},
	}
	// In the middle of the list, where neither proxy would have put it.
	changeStatus(t, k8s, &route, func() {
		route.Status.Parents = slices.Insert(route.Status.Parents, 1, theirs)
	})
	order := func() string {
		_ = ours()
		var names []string
		for _, p := range route.Status.Parents {
			names = append(names, string(p.ParentRef.Name))
		}
		return strings.Join(names, ",")
	}
	// Every proxy looks again, at something that changes nothing for the
	// route. Each leaves the list as it found it: one that put its own
	// entries last would have the other do the same, on every pass.
	listed, version := order(), route.ResourceVersion
	for i := range 3 {
		change(t, k8s, far, func() { far.Annotations = map[string]string{"looked-at": strconv.Itoa(i)} })
		time.Sleep(700 * time.Millisecond)
	}
	if now := order(); now != listed || route.ResourceVersion != version {
		t.Errorf("the route's status was written again with nothing changed: parents %s at %s, were %s at %s",
			now, route.ResourceVersion, listed, version)
	}

	// The route leaves the first proxy's Gateway: its routes go, and so
	// does what that proxy said of it. What the others said stays.
	change(t, k8s, &route, func() {
		route.Spec.ParentRefs = []gwv1.ParentReference{{Name: "side"}, {Name: "theirs"}}
	})
	eventually(t, "a route that left is no longer rendered or spoken for", func() string {
		if now := order(); now != "theirs,side" {
			return "the route's parents are " + now
		}
		if body := routes(); strings.Contains(body, "moved.example.com") {
			return "routes are\n" + body
		}
		return ""
	})

	// The other proxy's Gateway is deleted. Nobody has it among their
	// Gateways any more, and what was said about it would stay for good.
	if err := k8s.Delete(ctx, &side); err != nil {
		t.Fatal(err)
	}
	eventually(t, "what was said for a Gateway that is gone is taken back", func() string {
		if now := order(); now != "theirs" {
			return "the route's parents are " + now
		}
		return ""
	})

	// A proxy is deleted under its class. Nothing else changes, and there
	// is no proxy left whose turn it would be to notice: it is noticed
	// because the proxy went.
	if err := k8s.Delete(ctx, other); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the class of a proxy that was deleted says it has none", func() string {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&otherClass), &otherClass); err != nil {
			return err.Error()
		}
		if c := meta.FindStatusCondition(otherClass.Status.Conditions, "Accepted"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "InvalidParameters" {
			return fmt.Sprintf("the class says %+v", otherClass.Status.Conditions)
		}
		return ""
	})

	// A class is pointed at a proxy that does not exist. Its Gateway is
	// served by nothing, and would go on saying programmed, at an address.
	change(t, k8s, &route, func() {
		route.Spec.ParentRefs = []gwv1.ParentReference{{Namespace: ptr(gwv1.Namespace(ns)), Name: "web"}}
	})
	routeIs("the route is back on the first Gateway", "Accepted", metav1.ConditionTrue, "Accepted")
	rebind("nobody")
	eventually(t, "what was said of Gateways no proxy serves is taken back", func() string {
		if p := ours(); p != nil {
			return fmt.Sprintf("the route still carries %+v", *p)
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&gw), &gw); err != nil {
			return err.Error()
		}
		if c := meta.FindStatusCondition(gw.Status.Conditions, "Programmed"); c == nil || c.Status != metav1.ConditionUnknown {
			return fmt.Sprintf("the Gateway still says %+v", gw.Status.Conditions)
		}
		if len(gw.Status.Addresses) != 0 || len(gw.Status.Listeners) != 0 {
			return fmt.Sprintf("the Gateway still has addresses %v and listeners %v", gw.Status.Addresses, gw.Status.Listeners)
		}
		for _, gc := range []*gwv1.GatewayClass{&class, &otherClass} {
			if err := k8s.Get(ctx, client.ObjectKeyFromObject(gc), gc); err != nil {
				return err.Error()
			}
			if c := meta.FindStatusCondition(gc.Status.Conditions, "Accepted"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "InvalidParameters" {
				return fmt.Sprintf("class %s says %+v", gc.Name, gc.Status.Conditions)
			}
		}
		return ""
	})
	rebind("edge")
	routeIs("a class that names its proxy again is served again", "Accepted", metav1.ConditionTrue, "Accepted")

	for _, obj := range []client.Object{&route, &gw, &class, &otherClass, edge} {
		if err := k8s.Delete(ctx, obj); err != nil {
			t.Errorf("delete %T %s: %v", obj, obj.GetName(), err)
		}
	}
}

// The parents of a route are a list that several write to. A proxy's own
// entries are replaced where they stand.
func TestRouteParents(t *testing.T) {
	entry := func(controller, parent, reason string) gwv1.RouteParentStatus {
		return gwv1.RouteParentStatus{
			ParentRef: gwv1.ParentReference{Name: gwv1.ObjectName(parent)}, ControllerName: gwv1.GatewayController(controller),
			Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, Reason: reason}},
		}
	}
	names := func(ps []gwv1.RouteParentStatus) string {
		var out []string
		for _, p := range ps {
			out = append(out, string(p.ParentRef.Name)+":"+p.Conditions[0].Reason)
		}
		return strings.Join(out, " ")
	}
	mine := func(parents ...string) func(gwv1.RouteParentStatus) bool {
		return func(p gwv1.RouteParentStatus) bool {
			return string(p.ControllerName) == ControllerName && slices.Contains(parents, string(p.ParentRef.Name))
		}
	}
	have := []gwv1.RouteParentStatus{
		entry(ControllerName, "a", "Old"), entry("example.com/another", "x", "Theirs"),
		entry(ControllerName, "b", "Other"), entry(ControllerName, "gone", "Old"),
	}
	// This proxy has the Gateways a, gone and new; b is another proxy's.
	got := routeParents(have, []gwv1.RouteParentStatus{entry(ControllerName, "new", "New"), entry(ControllerName, "a", "New")}, mine("a", "gone", "new"))
	if want := "a:New x:Theirs b:Other new:New"; names(got) != want {
		t.Errorf("parents are %q, want %q", names(got), want)
	}
	// Written by one proxy and then by the other, the list is as it was.
	again := routeParents(got, []gwv1.RouteParentStatus{entry(ControllerName, "b", "Other")}, mine("b"))
	if names(again) != names(got) {
		t.Errorf("the other proxy turns %q into %q", names(got), names(again))
	}
	if again := routeParents(again, []gwv1.RouteParentStatus{entry(ControllerName, "new", "New"), entry(ControllerName, "a", "New")}, mine("a", "gone", "new")); names(again) != names(got) {
		t.Errorf("and the first one turns it into %q", names(again))
	}
	if got := routeParents(have, nil, mine("a", "gone", "new")); names(got) != "x:Theirs b:Other" {
		t.Errorf("with nothing to say, the parents are %q", names(got))
	}
}

func TestUnserved(t *testing.T) {
	classes := []gwv1.GatewayClass{
		gwClass("served", "edge", "edge"), gwClass("gone", "edge", "nobody"), gwClass("elsewhere", "far", "edge"),
		// Ours without a proxy to name, and somebody else's: neither is
		// for a SynapseProxy to serve, and neither is missing one.
		{ObjectMeta: metav1.ObjectMeta{Name: "bare"}, Spec: gwv1.GatewayClassSpec{ControllerName: ControllerName}},
		{ObjectMeta: metav1.ObjectMeta{Name: "theirs"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/another"}},
	}
	var gateways []gwv1.Gateway
	for _, class := range []string{"served", "gone", "elsewhere", "bare", "theirs"} {
		gw := gateway("apps", "of-"+class)
		gw.Spec.GatewayClassName = gwv1.ObjectName(class)
		gateways = append(gateways, gw)
	}
	live := map[types.NamespacedName]bool{{Namespace: "edge", Name: "edge"}: true}

	orphanClasses, orphanGateways := unserved(classes, gateways, live, "")
	if got := sortedKeys(orphanClasses); !reflect.DeepEqual(got, []string{"elsewhere", "gone"}) {
		t.Errorf("classes without a proxy are %v", got)
	}
	if got := sortedKeys(orphanGateways); len(got) != 2 || got[0].Name != "of-elsewhere" || got[1].Name != "of-gone" {
		t.Errorf("Gateways without a proxy are %v", got)
	}
	// Limited to a namespace, the operator does not see far/edge whether it
	// is there or not.
	orphanClasses, _ = unserved(classes, gateways, live, "edge")
	if got := sortedKeys(orphanClasses); !reflect.DeepEqual(got, []string{"gone"}) {
		t.Errorf("limited to a namespace, classes without a proxy are %v", got)
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

// failingMapper answers every lookup of a kind with err.
type failingMapper struct {
	meta.RESTMapper
	err error
}

func (f failingMapper) RESTMapping(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
	return nil, f.err
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
	// Not being able to tell is not a no.
	unwell := failingMapper{err: apierrors.NewServiceUnavailable("discovery is down")}
	if served, err := GatewayAPIServed(unwell); served || err == nil {
		t.Errorf("asked of an API server that cannot answer: served %v, error %v; want an error", served, err)
	}
	missing := failingMapper{err: &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: gwv1.GroupName, Kind: "Gateway"}}}
	if served, err := GatewayAPIServed(missing); served || err != nil {
		t.Errorf("asked of an API server without the kinds: served %v, error %v; want a plain no", served, err)
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
