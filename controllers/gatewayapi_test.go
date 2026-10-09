package controllers

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// gwWorld is a small cluster for the translation to read: the proxy
// edge/edge listening on 80 and 443, the class "edge" that names it, and two
// Services in the namespace apps.
type gwWorld struct {
	in       gatewayInputs
	services map[types.NamespacedName]*corev1.Service
	secrets  map[types.NamespacedName]*corev1.Secret
	// model is what is rendered already when the Gateways are looked at.
	model *renderModel
}

func newGwWorld() *gwWorld {
	proxy := testProxy("edge", "edge")
	proxy.Spec.Listeners = []synapsev1alpha1.Listener{
		{Name: "http", Port: 80, Protocol: "HTTP"}, {Name: "https", Port: 443, Protocol: "TLS"},
	}
	w := &gwWorld{
		services: map[types.NamespacedName]*corev1.Service{},
		secrets:  map[types.NamespacedName]*corev1.Secret{},
		model:    newRenderModel(),
	}
	w.in = gatewayInputs{
		proxy:      proxy,
		classes:    []gwv1.GatewayClass{gwClass("edge", "edge", "edge")},
		namespaces: map[string]map[string]string{"edge": {}, "apps": {"team": "a"}, "other": {"team": "b"}},
		service: func(ns, name string) *corev1.Service {
			return w.services[types.NamespacedName{Namespace: ns, Name: name}]
		},
		secret: func(ns, name string) *corev1.Secret {
			return w.secrets[types.NamespacedName{Namespace: ns, Name: name}]
		},
		addresses: []string{"203.0.113.10"},
	}
	w.service("apps", "app", "10.0.0.1")
	w.service("apps", "app2", "10.0.0.2")
	w.service("apps", "app3", "10.0.0.3")
	w.service("apps", "app4", "10.0.0.4")
	return w
}

func (w *gwWorld) service(ns, name, clusterIP string) {
	s := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	s.Spec.ClusterIP = clusterIP
	w.services[types.NamespacedName{Namespace: ns, Name: name}] = s
}

func (w *gwWorld) tlsSecret(ns, name string) {
	w.secrets[types.NamespacedName{Namespace: ns, Name: name}] = tlsSecret(ns, name, "CRT", "KEY")
}

func (w *gwWorld) translate() (*renderModel, gatewayStatuses) {
	return w.model, translateGateways(w.in, w.model)
}

func gwClass(name, proxyNamespace, proxyName string) gwv1.GatewayClass {
	gc := gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 3}}
	gc.Spec.ControllerName = ControllerName
	gc.Spec.ParametersRef = &gwv1.ParametersReference{
		Group: "synapse.gen0sec.com", Kind: "SynapseProxy", Name: proxyName, Namespace: ptr(gwv1.Namespace(proxyNamespace)),
	}
	return gc
}

func gateway(ns, name string, listeners ...gwv1.Listener) gwv1.Gateway {
	gw := gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 5}}
	gw.Spec.GatewayClassName = "edge"
	gw.Spec.Listeners = listeners
	return gw
}

// httpListener is a listener for host, which may be "", that lets routes
// from every namespace attach.
func httpListener(name string, port int32, host string) gwv1.Listener {
	l := gwv1.Listener{Name: gwv1.SectionName(name), Port: gwv1.PortNumber(port), Protocol: gwv1.HTTPProtocolType}
	if host != "" {
		l.Hostname = ptr(gwv1.Hostname(host))
	}
	l.AllowedRoutes = &gwv1.AllowedRoutes{Namespaces: &gwv1.RouteNamespaces{From: ptr(gwv1.NamespacesFromAll)}}
	return l
}

func httpsListener(name string, host string, secrets ...gwv1.SecretObjectReference) gwv1.Listener {
	l := httpListener(name, 443, host)
	l.Protocol = gwv1.HTTPSProtocolType
	l.TLS = &gwv1.ListenerTLSConfig{CertificateRefs: secrets}
	return l
}

func secretRef(ns, name string) gwv1.SecretObjectReference {
	ref := gwv1.SecretObjectReference{Name: gwv1.ObjectName(name)}
	if ns != "" {
		ref.Namespace = ptr(gwv1.Namespace(ns))
	}
	return ref
}

// httpRoute is a route in apps attached to the Gateway edge/web.
func httpRoute(name string, hosts []string, rules ...gwv1.HTTPRouteRule) gwv1.HTTPRoute {
	rt := gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace: "apps", Name: name, Generation: 7, CreationTimestamp: metav1.NewTime(time.Unix(1000, 0)),
	}}
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Namespace: ptr(gwv1.Namespace("edge")), Name: "web"}}
	for _, h := range hosts {
		rt.Spec.Hostnames = append(rt.Spec.Hostnames, gwv1.Hostname(h))
	}
	rt.Spec.Rules = rules
	return rt
}

// to is a rule sending the given matches to a Service in the route's
// namespace, on port 80.
func to(service string, matches ...gwv1.HTTPRouteMatch) gwv1.HTTPRouteRule {
	rule := gwv1.HTTPRouteRule{Matches: matches}
	rule.BackendRefs = []gwv1.HTTPBackendRef{backendRef("", service)}
	return rule
}

func backendRef(ns, service string) gwv1.HTTPBackendRef {
	var ref gwv1.HTTPBackendRef
	ref.Name, ref.Port = gwv1.ObjectName(service), ptr(gwv1.PortNumber(80))
	if ns != "" {
		ref.Namespace = ptr(gwv1.Namespace(ns))
	}
	return ref
}

func pathMatch(kind gwv1.PathMatchType, value string) gwv1.HTTPRouteMatch {
	return gwv1.HTTPRouteMatch{Path: &gwv1.HTTPPathMatch{Type: &kind, Value: &value}}
}

func onMethod(method string, m gwv1.HTTPRouteMatch) gwv1.HTTPRouteMatch {
	m.Method = ptr(gwv1.HTTPMethod(method))
	return m
}

var (
	prefix = func(v string) gwv1.HTTPRouteMatch { return pathMatch(gwv1.PathMatchPathPrefix, v) }
	exact  = func(v string) gwv1.HTTPRouteMatch { return pathMatch(gwv1.PathMatchExact, v) }
	regex  = func(v string) gwv1.HTTPRouteMatch { return pathMatch(gwv1.PathMatchRegularExpression, v) }
)

// routeCondition is a condition of a route's first parent of ours.
func routeCondition(t *testing.T, st gatewayStatuses, name, kind string) *metav1.Condition {
	t.Helper()
	parents := st.routes[types.NamespacedName{Namespace: "apps", Name: name}]
	if len(parents) == 0 {
		t.Fatalf("route %s has no status; statuses are for %v", name, sortedKeys(st.routes))
	}
	return meta.FindStatusCondition(parents[0].Conditions, kind)
}

func wantRoute(t *testing.T, st gatewayStatuses, name, kind string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := routeCondition(t, st, name, kind)
	if c == nil || c.Status != status || c.Reason != reason {
		t.Errorf("route %s: %s is %+v, want %s/%s", name, kind, c, status, reason)
	}
}

func listenerCondition(t *testing.T, st gatewayStatuses, listener, kind string) *metav1.Condition {
	t.Helper()
	for _, l := range st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}].Listeners {
		if string(l.Name) == listener {
			return meta.FindStatusCondition(l.Conditions, kind)
		}
	}
	t.Fatalf("no status for listener %s", listener)
	return nil
}

func wantListener(t *testing.T, st gatewayStatuses, listener, kind string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := listenerCondition(t, st, listener, kind)
	if c == nil || c.Status != status || c.Reason != reason {
		t.Errorf("listener %s: %s is %+v, want %s/%s", listener, kind, c, status, reason)
	}
}

// server returns where a request ends up, by the rules Synapse applies to
// what was rendered for the host: a plain path that is the request's path,
// then the expressions, then the longest plain path that is a prefix. The
// expressions are evaluated here, by evalRouteExpr; more than one of them
// being true for a request fails the test, because Synapse tries them in no
// particular order.
func server(t *testing.T, m *renderModel, host, method, path string) string {
	t.Helper()
	routes := m.hosts[host]
	first := func(rc *routeCfg) string {
		if len(rc.servers) == 0 {
			return ""
		}
		return rc.servers[0].addr
	}
	if rc, ok := routes[path]; ok && rc.matchExpr == "" {
		return first(rc)
	}
	var hits []string
	for label, rc := range routes {
		if rc.matchExpr != "" && evalRouteExpr(t, rc.matchExpr, method, path) {
			hits = append(hits, label+" -> "+first(rc))
		}
	}
	switch len(hits) {
	case 0:
	case 1:
		return strings.SplitN(hits[0], " -> ", 2)[1]
	default:
		t.Fatalf("%s %s on %s is claimed by more than one expression: %v", method, path, host, hits)
	}
	for p := path; ; {
		i := strings.LastIndex(p, "/")
		if i < 0 {
			return ""
		}
		p = p[:i]
		if rc, ok := routes[cmpOr(p, "/")]; ok && rc.matchExpr == "" {
			return first(rc)
		}
		if p == "" {
			return ""
		}
	}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// evalRouteExpr evaluates the subset of Synapse's route expressions the
// operator writes: `eq` and `matches` on the path and the method, `and`,
// `or`, `not` and parentheses.
func evalRouteExpr(t *testing.T, expr, method, path string) bool {
	t.Helper()
	tokens := regexp.MustCompile(`"(?:[^"\\]|\\.)*"|[()]|[A-Za-z_.]+`).FindAllString(expr, -1)
	pos := 0
	next := func() string {
		if pos >= len(tokens) {
			t.Fatalf("expression ends early: %s", expr)
		}
		pos++
		return tokens[pos-1]
	}
	peek := func() string {
		if pos < len(tokens) {
			return tokens[pos]
		}
		return ""
	}
	var or func() bool
	unary := func() bool { return false }
	unary = func() bool {
		switch tok := next(); tok {
		case "not":
			return !unary()
		case "(":
			v := or()
			if next() != ")" {
				t.Fatalf("unbalanced parentheses: %s", expr)
			}
			return v
		case "http.request.path", "http.request.method":
			field := path
			if tok == "http.request.method" {
				field = method
			}
			op := next()
			lit, err := strconv.Unquote(next())
			if err != nil {
				t.Fatalf("bad string in %s: %v", expr, err)
			}
			switch op {
			case "eq":
				return field == lit
			case "matches":
				return regexp.MustCompile(lit).MatchString(field)
			}
			t.Fatalf("unknown operator %q in %s", op, expr)
		default:
			t.Fatalf("unexpected %q in %s", tok, expr)
		}
		return false
	}
	and := func() bool {
		v := unary()
		for peek() == "and" {
			next()
			r := unary()
			v = v && r
		}
		return v
	}
	or = func() bool {
		v := and()
		for peek() == "or" {
			next()
			r := and()
			v = v || r
		}
		return v
	}
	v := or()
	if pos != len(tokens) {
		t.Fatalf("left over after %d tokens: %s", pos, expr)
	}
	return v
}

func TestGateway_ClassHandsItsGatewaysToTheProxyItNames(t *testing.T) {
	for name, tc := range map[string]struct {
		class  gwv1.GatewayClass
		served bool
	}{
		"this proxy":        {gwClass("edge", "edge", "edge"), true},
		"another proxy":     {gwClass("edge", "edge", "other"), false},
		"another namespace": {gwClass("edge", "elsewhere", "edge"), false},
		"another controller": {func() gwv1.GatewayClass {
			c := gwClass("edge", "edge", "edge")
			c.Spec.ControllerName = "example.com/x"
			return c
		}(), false},
		"no parameters": {func() gwv1.GatewayClass { c := gwClass("edge", "edge", "edge"); c.Spec.ParametersRef = nil; return c }(), false},
		"no namespace": {func() gwv1.GatewayClass {
			c := gwClass("edge", "edge", "edge")
			c.Spec.ParametersRef.Namespace = nil
			return c
		}(), false},
		"another kind": {func() gwv1.GatewayClass {
			c := gwClass("edge", "edge", "edge")
			c.Spec.ParametersRef.Kind = "ConfigMap"
			return c
		}(), false},
		"another group": {func() gwv1.GatewayClass {
			c := gwClass("edge", "edge", "edge")
			c.Spec.ParametersRef.Group = "example.com"
			return c
		}(), false},
	} {
		t.Run(name, func(t *testing.T) {
			w := newGwWorld()
			w.in.classes = []gwv1.GatewayClass{tc.class}
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, to("app"))}
			m, st := w.translate()
			if got := server(t, m, "shop.example.com", "GET", "/") != ""; got != tc.served {
				t.Errorf("served = %v, want %v", got, tc.served)
			}
			if got := len(st.classes)+len(st.gateways)+len(st.routes) > 0; got != tc.served {
				t.Errorf("statuses %+v: an object that is not this proxy's is given a status, or one that is, is not", st)
			}
			if tc.served {
				c := meta.FindStatusCondition(st.classes["edge"], "Accepted")
				if c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != 3 {
					t.Errorf("the class is %+v, want Accepted for generation 3", c)
				}
			}
		})
	}
}

func TestGateway_ListenersTheProxyCanServe(t *testing.T) {
	w := newGwWorld()
	tcp := httpListener("tcp", 80, "")
	tcp.Protocol = gwv1.TCPProtocolType
	onHTTPPort := httpsListener("https-on-80", "a.example.com", secretRef("", "cert"))
	onHTTPPort.Port = 80
	passthrough := httpsListener("passthrough", "p.example.com", secretRef("", "cert"))
	passthrough.TLS.Mode = ptr(gwv1.TLSModePassthrough)
	grpcOnly := httpListener("grpc-only", 80, "g.example.com")
	grpcOnly.AllowedRoutes.Kinds = []gwv1.RouteGroupKind{{Kind: "GRPCRoute"}}
	w.tlsSecret("edge", "cert")
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web",
		httpListener("http", 80, "shop.example.com"), httpListener("other-port", 8080, ""), tcp, onHTTPPort, passthrough, grpcOnly,
		httpsListener("https", "shop.example.com", secretRef("", "cert")),
	)}
	_, st := w.translate()

	wantListener(t, st, "http", "Accepted", metav1.ConditionTrue, "Accepted")
	wantListener(t, st, "http", "Programmed", metav1.ConditionTrue, "Programmed")
	wantListener(t, st, "https", "Programmed", metav1.ConditionTrue, "Programmed")
	wantListener(t, st, "other-port", "Accepted", metav1.ConditionFalse, "PortUnavailable")
	wantListener(t, st, "other-port", "Programmed", metav1.ConditionFalse, "Invalid")
	wantListener(t, st, "tcp", "Accepted", metav1.ConditionFalse, "UnsupportedProtocol")
	wantListener(t, st, "https-on-80", "Accepted", metav1.ConditionFalse, "PortUnavailable")
	wantListener(t, st, "passthrough", "Accepted", metav1.ConditionFalse, "UnsupportedValue")
	// It allows a kind that is not served: said, and still a listener.
	wantListener(t, st, "grpc-only", "ResolvedRefs", metav1.ConditionFalse, "InvalidRouteKinds")
	wantListener(t, st, "grpc-only", "Accepted", metav1.ConditionTrue, "Accepted")

	gw := st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	accepted := meta.FindStatusCondition(gw.Conditions, "Accepted")
	if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.Reason != "ListenersNotValid" || accepted.ObservedGeneration != 5 {
		t.Errorf("Gateway Accepted is %+v, want True/ListenersNotValid for generation 5", accepted)
	}
	if len(gw.Addresses) != 1 || gw.Addresses[0].Value != "203.0.113.10" || *gw.Addresses[0].Type != gwv1.IPAddressType {
		t.Errorf("addresses are %+v", gw.Addresses)
	}

	// With no listener the proxy can serve, the Gateway is not accepted.
	w = newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("other-port", 8080, ""))}
	_, st = w.translate()
	gw = st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	if c := meta.FindStatusCondition(gw.Conditions, "Accepted"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "ListenersNotValid" {
		t.Errorf("Gateway Accepted is %+v, want False/ListenersNotValid", c)
	}
	if c := meta.FindStatusCondition(gw.Conditions, "Programmed"); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("Gateway Programmed is %+v, want False", c)
	}
}

func TestGateway_AddressesAreTheProxys(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.in.addresses = []string{"lb.example.net"}
	_, st := w.translate()
	gw := st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	if len(gw.Addresses) != 1 || *gw.Addresses[0].Type != gwv1.HostnameAddressType {
		t.Errorf("addresses are %+v, want one hostname", gw.Addresses)
	}

	w.in.addresses = nil
	_, st = w.translate()
	gw = st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}]
	if c := meta.FindStatusCondition(gw.Conditions, "Programmed"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "AddressNotAssigned" {
		t.Errorf("Programmed is %+v, want False/AddressNotAssigned", c)
	}
}

func TestGateway_WhichRoutesMayAttach(t *testing.T) {
	sameOnly := httpListener("http", 80, "shop.example.com")
	sameOnly.AllowedRoutes = nil
	selected := func(team string) gwv1.Listener {
		l := httpListener("http", 80, "shop.example.com")
		l.AllowedRoutes.Namespaces = &gwv1.RouteNamespaces{
			From: ptr(gwv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": team}},
		}
		return l
	}
	noSelector := selected("a")
	noSelector.AllowedRoutes.Namespaces.Selector = nil
	otherKind := httpListener("http", 80, "shop.example.com")
	otherKind.AllowedRoutes.Kinds = []gwv1.RouteGroupKind{{Kind: "GRPCRoute"}}
	bothKinds := httpListener("http", 80, "shop.example.com")
	bothKinds.AllowedRoutes.Kinds = []gwv1.RouteGroupKind{{Kind: "GRPCRoute"}, {Kind: "HTTPRoute"}}

	for name, tc := range map[string]struct {
		listener gwv1.Listener
		parent   func(*gwv1.ParentReference)
		reason   string
	}{
		"from every namespace":               {httpListener("http", 80, "shop.example.com"), nil, "Accepted"},
		"from the Gateway's own, by default": {sameOnly, nil, "NotAllowedByListeners"},
		"from namespaces with a label":       {selected("a"), nil, "Accepted"},
		"not from one without it":            {selected("b"), nil, "NotAllowedByListeners"},
		"a selector that is not there":       {noSelector, nil, "NotAllowedByListeners"},
		"only routes of another kind":        {otherKind, nil, "NotAllowedByListeners"},
		"routes of this kind among others":   {bothKinds, nil, "Accepted"},
		"the listener it names":              {httpListener("http", 80, "shop.example.com"), func(p *gwv1.ParentReference) { p.SectionName = ptr(gwv1.SectionName("http")) }, "Accepted"},
		"a listener that is not there":       {httpListener("http", 80, "shop.example.com"), func(p *gwv1.ParentReference) { p.SectionName = ptr(gwv1.SectionName("nope")) }, "NoMatchingParent"},
		"the port it names":                  {httpListener("http", 80, "shop.example.com"), func(p *gwv1.ParentReference) { p.Port = ptr(gwv1.PortNumber(80)) }, "Accepted"},
		"a port nothing listens on":          {httpListener("http", 80, "shop.example.com"), func(p *gwv1.ParentReference) { p.Port = ptr(gwv1.PortNumber(81)) }, "NoMatchingParent"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newGwWorld()
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", tc.listener)}
			rt := httpRoute("shop", nil, to("app"))
			if tc.parent != nil {
				tc.parent(&rt.Spec.ParentRefs[0])
			}
			w.in.routes = []gwv1.HTTPRoute{rt}
			m, st := w.translate()
			want := metav1.ConditionFalse
			if tc.reason == "Accepted" {
				want = metav1.ConditionTrue
			}
			wantRoute(t, st, "shop", "Accepted", want, tc.reason)
			if got := server(t, m, "shop.example.com", "GET", "/") != ""; got != (tc.reason == "Accepted") {
				t.Errorf("served = %v with the route %s", got, tc.reason)
			}
			attached := st.gateways[types.NamespacedName{Namespace: "edge", Name: "web"}].Listeners[0].AttachedRoutes
			if (attached == 1) != (tc.reason == "Accepted") {
				t.Errorf("the listener counts %d attached routes with the route %s", attached, tc.reason)
			}
		})
	}

	// A route in the Gateway's own namespace needs no permission.
	w := newGwWorld()
	w.service("edge", "app", "10.0.9.9")
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", sameOnly)}
	rt := httpRoute("shop", nil, to("app"))
	rt.Namespace = "edge"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "web"}}
	w.in.routes = []gwv1.HTTPRoute{rt}
	if m, _ := w.translate(); server(t, m, "shop.example.com", "GET", "/") != "10.0.9.9:80" {
		t.Errorf("a route in the Gateway's namespace is not served")
	}
}

// A route whose parent is somebody else's Gateway is none of our business:
// writing to its status would fight whoever serves it.
func TestGateway_LeavesOtherPeoplesRoutesAlone(t *testing.T) {
	w := newGwWorld()
	theirs := gateway("edge", "theirs", httpListener("http", 80, "shop.example.com"))
	theirs.Spec.GatewayClassName = "somebody-elses"
	// A Gateway of ours beside it: a parent that is a Service of the same
	// name is not that Gateway.
	w.in.gateways = []gwv1.Gateway{theirs, gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	rt := httpRoute("shop", nil, to("app"))
	rt.Spec.ParentRefs[0].Name = "theirs"
	notAGateway := httpRoute("mesh", nil, to("app"))
	notAGateway.Spec.ParentRefs[0].Kind = ptr(gwv1.Kind("Service"))
	otherGroup := httpRoute("other-group", nil, to("app"))
	otherGroup.Spec.ParentRefs[0].Group = ptr(gwv1.Group("example.com"))
	w.in.routes = []gwv1.HTTPRoute{rt, notAGateway, otherGroup}
	m, st := w.translate()
	if len(st.routes) != 0 || len(st.gateways) != 1 || len(m.hosts) != 0 {
		t.Errorf("statuses %+v and hosts %v for objects that are not this proxy's", st, sortedKeys(m.hosts))
	}
}

func TestGateway_HostsARouteIsServedOn(t *testing.T) {
	for name, tc := range map[string]struct {
		listener string
		route    []string
		want     []string
		reason   string
	}{
		"the listener's, when the route names none":   {"shop.example.com", nil, []string{"shop.example.com"}, "Accepted"},
		"the route's, when the listener names none":   {"", []string{"shop.example.com"}, []string{"shop.example.com"}, "Accepted"},
		"the one they share":                          {"shop.example.com", []string{"shop.example.com", "b.example.com"}, []string{"shop.example.com"}, "Accepted"},
		"the route's, under the listener's wildcard":  {"*.example.com", []string{"shop.example.com", "example.com", "x.example.org"}, []string{"shop.example.com"}, "Accepted"},
		"the listener's, under the route's wildcard":  {"shop.example.com", []string{"*.example.com"}, []string{"shop.example.com"}, "Accepted"},
		"the narrower of two wildcards":               {"*.example.com", []string{"*.eu.example.com"}, []string{"*.eu.example.com"}, "Accepted"},
		"a wildcard, as it is":                        {"*.example.com", nil, []string{"*.example.com"}, "Accepted"},
		"whatever the case":                           {"Shop.Example.com", []string{"SHOP.example.COM"}, []string{"shop.example.com"}, "Accepted"},
		"none, when they share none":                  {"a.example.com", []string{"b.example.com"}, nil, "NoMatchingListenerHostname"},
		"none, when a wildcard is met by its own top": {"*.example.com", []string{"example.com"}, nil, "NoMatchingListenerHostname"},
		"none, when neither names one":                {"", nil, nil, "UnsupportedValue"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newGwWorld()
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, tc.listener))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", tc.route, to("app"))}
			m, st := w.translate()
			if got := sortedKeys(m.hosts); !reflect.DeepEqual(got, append([]string{}, tc.want...)) {
				t.Errorf("hosts are %v, want %v", got, tc.want)
			}
			want := metav1.ConditionFalse
			if tc.reason == "Accepted" {
				want = metav1.ConditionTrue
			}
			wantRoute(t, st, "shop", "Accepted", want, tc.reason)
		})
	}
}

func TestGateway_Backends(t *testing.T) {
	grant := func(fromNamespace, toKind, toName string) gwv1beta1.ReferenceGrant {
		g := gwv1beta1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "g"}}
		g.Spec.From = []gwv1beta1.ReferenceGrantFrom{{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Namespace: gwv1.Namespace(fromNamespace)}}
		to := gwv1beta1.ReferenceGrantTo{Kind: gwv1.Kind(toKind)}
		if toName != "" {
			to.Name = ptr(gwv1.ObjectName(toName))
		}
		g.Spec.To = []gwv1beta1.ReferenceGrantTo{to}
		return g
	}
	elsewhere := func(g ...gwv1beta1.ReferenceGrant) func(*gwWorld) gwv1.HTTPRouteRule {
		return func(w *gwWorld) gwv1.HTTPRouteRule {
			w.service("other", "far", "10.0.5.5")
			w.in.grants = g
			rule := gwv1.HTTPRouteRule{}
			rule.BackendRefs = []gwv1.HTTPBackendRef{backendRef("other", "far")}
			return rule
		}
	}
	misplaced := grant("apps", "Service", "")
	misplaced.Namespace = "apps"

	for name, tc := range map[string]struct {
		rule   func(*gwWorld) gwv1.HTTPRouteRule
		want   string
		reason string
	}{
		"a Service, by its cluster IP": {func(*gwWorld) gwv1.HTTPRouteRule { return to("app") }, "10.0.0.1:80", "ResolvedRefs"},
		"a headless one, by its name": {func(w *gwWorld) gwv1.HTTPRouteRule {
			w.service("apps", "headless", corev1.ClusterIPNone)
			return to("headless")
		}, "headless.apps.svc.cluster.local:80", "ResolvedRefs"},
		"an IPv6 one, in brackets": {func(w *gwWorld) gwv1.HTTPRouteRule {
			w.service("apps", "six", "fd00::1")
			return to("six")
		}, "[fd00::1]:80", "ResolvedRefs"},
		"one that does not exist": {func(*gwWorld) gwv1.HTTPRouteRule { return to("nope") }, "", "BackendNotFound"},
		"something that is not a Service": {func(*gwWorld) gwv1.HTTPRouteRule {
			r := to("app")
			r.BackendRefs[0].Kind = ptr(gwv1.Kind("Bucket"))
			return r
		}, "", "InvalidKind"},
		"one without a port": {func(*gwWorld) gwv1.HTTPRouteRule {
			r := to("app")
			r.BackendRefs[0].Port = nil
			return r
		}, "", "UnsupportedValue"},
		"another namespace's, unasked":          {elsewhere(), "", "RefNotPermitted"},
		"another namespace's, granted":          {elsewhere(grant("apps", "Service", "")), "10.0.5.5:80", "ResolvedRefs"},
		"granted by name":                       {elsewhere(grant("apps", "Service", "far")), "10.0.5.5:80", "ResolvedRefs"},
		"granted to another name":               {elsewhere(grant("apps", "Service", "near")), "", "RefNotPermitted"},
		"granted to another namespace's routes": {elsewhere(grant("edge", "Service", "")), "", "RefNotPermitted"},
		"granted for another kind":              {elsewhere(grant("apps", "Secret", "")), "", "RefNotPermitted"},
		"granted from the wrong side":           {elsewhere(misplaced), "", "RefNotPermitted"},
		"one of two, when the other does not exist": {func(*gwWorld) gwv1.HTTPRouteRule {
			r := to("nope")
			r.BackendRefs = append(r.BackendRefs, backendRef("", "app"))
			return r
		}, "10.0.0.1:80", "BackendNotFound"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newGwWorld()
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, tc.rule(w))}
			m, st := w.translate()
			if got := server(t, m, "shop.example.com", "GET", "/"); got != tc.want {
				t.Errorf("requests go to %q, want %q", got, tc.want)
			}
			want := metav1.ConditionFalse
			if tc.reason == "ResolvedRefs" {
				want = metav1.ConditionTrue
			}
			wantRoute(t, st, "shop", "ResolvedRefs", want, tc.reason)
			// A backend that cannot be used is not a route that is refused.
			wantRoute(t, st, "shop", "Accepted", metav1.ConditionTrue, "Accepted")
		})
	}
}

func TestGateway_BackendWeights(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	rule := to("app")
	rule.BackendRefs[0].Weight = ptr(int32(3))
	rule.BackendRefs = append(rule.BackendRefs, backendRef("", "app2"), backendRef("", "app3"))
	rule.BackendRefs[2].Weight = ptr(int32(0))
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, rule)}
	m, _ := w.translate()
	got := m.hosts["shop.example.com"]["/"].servers
	want := []backend{{addr: "10.0.0.1:80", weight: 3}, {addr: "10.0.0.2:80", weight: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("servers are %+v, want %+v: a weight not given is 1 once any is, and 0 gets nothing", got, want)
	}

	// Without any weight, none is written.
	w = newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, to("app"))}
	m, _ = w.translate()
	if got := m.hosts["shop.example.com"]["/"].servers; !reflect.DeepEqual(got, []backend{{addr: "10.0.0.1:80"}}) {
		t.Errorf("servers are %+v, want one without a weight", got)
	}
}

func TestGateway_Certificates(t *testing.T) {
	grant := gwv1beta1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: "certs", Name: "g"}}
	grant.Spec.From = []gwv1beta1.ReferenceGrantFrom{{Group: "gateway.networking.k8s.io", Kind: "Gateway", Namespace: "edge"}}
	grant.Spec.To = []gwv1beta1.ReferenceGrantTo{{Kind: "Secret"}}

	for name, tc := range map[string]struct {
		ref    gwv1.SecretObjectReference
		setup  func(*gwWorld)
		reason string
	}{
		"in the Gateway's namespace": {secretRef("", "cert"), func(w *gwWorld) { w.tlsSecret("edge", "cert") }, "ResolvedRefs"},
		"in another, unasked":        {secretRef("certs", "cert"), func(w *gwWorld) { w.tlsSecret("certs", "cert") }, "RefNotPermitted"},
		"in another, granted":        {secretRef("certs", "cert"), func(w *gwWorld) { w.tlsSecret("certs", "cert"); w.in.grants = []gwv1beta1.ReferenceGrant{grant} }, "ResolvedRefs"},
		"that does not exist":        {secretRef("", "cert"), func(*gwWorld) {}, "InvalidCertificateRef"},
		"that is not a TLS Secret": {secretRef("", "cert"), func(w *gwWorld) {
			w.tlsSecret("edge", "cert")
			w.secrets[types.NamespacedName{Namespace: "edge", Name: "cert"}].Type = corev1.SecretTypeOpaque
		}, "InvalidCertificateRef"},
		"that has no key": {secretRef("", "cert"), func(w *gwWorld) {
			w.tlsSecret("edge", "cert")
			delete(w.secrets[types.NamespacedName{Namespace: "edge", Name: "cert"}].Data, corev1.TLSPrivateKeyKey)
		}, "InvalidCertificateRef"},
		"that is not a Secret": {func() gwv1.SecretObjectReference {
			r := secretRef("", "cert")
			r.Kind = ptr(gwv1.Kind("ConfigMap"))
			return r
		}(), func(w *gwWorld) { w.tlsSecret("edge", "cert") }, "InvalidCertificateRef"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newGwWorld()
			tc.setup(w)
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpsListener("https", "shop.example.com", tc.ref))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, to("app"))}
			m, st := w.translate()
			ok := tc.reason == "ResolvedRefs"
			want := metav1.ConditionFalse
			if ok {
				want = metav1.ConditionTrue
			}
			wantListener(t, st, "https", "ResolvedRefs", want, tc.reason)
			if got := listenerCondition(t, st, "https", "Programmed").Status == metav1.ConditionTrue; got != ok {
				t.Errorf("the listener is programmed = %v with its certificate %s", got, tc.reason)
			}
			if _, projected := m.certProjections["shop.example.com"]; projected != ok || (m.hostCert["shop.example.com"] != "") != ok {
				t.Errorf("certificates are %+v, bound %v, with the certificate %s", m.certProjections, m.hostCert, tc.reason)
			}
			// Without its certificate the listener serves nothing.
			if got := server(t, m, "shop.example.com", "GET", "/") != ""; got != ok {
				t.Errorf("served = %v with the certificate %s", got, tc.reason)
			}
		})
	}

	// A listener with no certificate at all, and one with two of which one
	// is missing: neither is programmed, and the good one is not used.
	w := newGwWorld()
	w.tlsSecret("edge", "cert")
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web",
		httpsListener("none", "a.example.com"),
		httpsListener("half", "b.example.com", secretRef("", "cert"), secretRef("", "missing")))}
	m, st := w.translate()
	wantListener(t, st, "none", "ResolvedRefs", metav1.ConditionFalse, "InvalidCertificateRef")
	wantListener(t, st, "half", "Programmed", metav1.ConditionFalse, "Invalid")
	if len(m.certProjections) != 0 {
		t.Errorf("certificates %+v are projected for listeners that are not programmed", m.certProjections)
	}
}

// What a request reaches has to be what the API says it reaches, whatever
// order Synapse looks at a host's routes in.
func TestGateway_WhichRuleARequestMatches(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, ""))}
	w.in.routes = []gwv1.HTTPRoute{
		// Nothing but prefixes: plain paths, resolved by the longest.
		httpRoute("plain", []string{"plain.example.com"}, to("app", prefix("/")), to("app2", prefix("/a")), to("app3", prefix("/a/b/"))),
		// Everything else: an exact path, a method, a regular expression.
		httpRoute("mixed", []string{"mixed.example.com"},
			to("app", prefix("/")),
			to("app2", prefix("/a")),
			to("app3", exact("/a")),
			to("app4", onMethod("POST", prefix("/a"))),
			to("app", regex("/files/[0-9]+$")),
		),
		// A method is enough to need expressions: plain paths have none.
		httpRoute("methods", []string{"methods.example.com"}, to("app", prefix("/")), to("app2", onMethod("POST", prefix("/")))),
	}
	m, st := w.translate()
	wantRoute(t, st, "plain", "Accepted", metav1.ConditionTrue, "Accepted")
	wantRoute(t, st, "mixed", "Accepted", metav1.ConditionTrue, "Accepted")
	for _, rc := range m.hosts["plain.example.com"] {
		if rc.matchExpr != "" {
			t.Errorf("a host with nothing but prefixes got the expression %s", rc.matchExpr)
		}
	}
	for label, rc := range m.hosts["mixed.example.com"] {
		if rc.matchExpr == "" {
			t.Errorf("%s on a host with an exact match is a plain path: Synapse would try it in an order of its own", label)
		}
	}

	const app, app2, app3, app4 = "10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80", "10.0.0.4:80"
	for _, tc := range []struct{ host, method, path, want string }{
		{"plain.example.com", "GET", "/", app},
		{"plain.example.com", "GET", "/x", app},
		{"plain.example.com", "GET", "/a", app2},
		{"plain.example.com", "GET", "/a/x", app2},
		{"plain.example.com", "GET", "/ab", app},
		{"plain.example.com", "GET", "/a/b", app3},
		{"plain.example.com", "GET", "/a/b/c", app3},

		{"mixed.example.com", "GET", "/", app},
		{"mixed.example.com", "GET", "/x", app},
		{"mixed.example.com", "GET", "/a", app3},    // exact, over the prefix of the same path
		{"mixed.example.com", "POST", "/a", app3},   // and over the method
		{"mixed.example.com", "GET", "/a/x", app2},  // the prefix
		{"mixed.example.com", "POST", "/a/x", app4}, // the method, over the same prefix without one
		{"mixed.example.com", "POST", "/x", app},    // a method match is for its own path only
		{"mixed.example.com", "GET", "/ab", app},    // a prefix ends at a path segment
		{"mixed.example.com", "GET", "/files/12", app},
		{"mixed.example.com", "GET", "/files/x", app},

		{"methods.example.com", "GET", "/x", app},
		{"methods.example.com", "POST", "/x", app2},
	} {
		if got := server(t, m, tc.host, tc.method, tc.path); got != tc.want {
			t.Errorf("%s %s%s goes to %q, want %q", tc.method, tc.host, tc.path, got, tc.want)
		}
	}
}

// Between two routes that match equally well, the older one has the request;
// and a match written twice is one route, not two that both claim it.
func TestGateway_TheOlderRouteWins(t *testing.T) {
	for _, exactPath := range []bool{false, true} {
		w := newGwWorld()
		w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
		match := prefix("/a")
		if exactPath {
			match = exact("/a")
		}
		newer := httpRoute("a-newer", nil, to("app2", match))
		newer.CreationTimestamp = metav1.NewTime(time.Unix(2000, 0))
		older := httpRoute("z-older", nil, to("app", match))
		w.in.routes = []gwv1.HTTPRoute{newer, older}
		m, _ := w.translate()
		if got := server(t, m, "shop.example.com", "GET", "/a"); got != "10.0.0.1:80" {
			t.Errorf("exact=%v: the request goes to %q, want the older route's 10.0.0.1:80", exactPath, got)
		}
		if n := len(m.hosts["shop.example.com"]); n != 1 {
			t.Errorf("exact=%v: %d routes are rendered for one match; the second can never be reached", exactPath, n)
		}
	}
}

func TestGateway_WhatCannotBeHonouredIsNotProgrammed(t *testing.T) {
	header := prefix("/h")
	header.Headers = []gwv1.HTTPHeaderMatch{{Name: "X-Env", Value: "canary"}}
	query := prefix("/q")
	query.QueryParams = []gwv1.HTTPQueryParamMatch{{Name: "v", Value: "2"}}
	filtered := func(f gwv1.HTTPRouteFilter) gwv1.HTTPRouteRule {
		r := to("app2", prefix("/f"))
		r.Filters = []gwv1.HTTPRouteFilter{f}
		return r
	}
	backendFilter := to("app2", prefix("/f"))
	backendFilter.BackendRefs[0].Filters = []gwv1.HTTPRouteFilter{{Type: gwv1.HTTPRouteFilterRequestHeaderModifier}}
	timeouts := to("app2", prefix("/f"))
	timeouts.Timeouts = &gwv1.HTTPRouteTimeouts{}
	noBackend := gwv1.HTTPRouteRule{Matches: []gwv1.HTTPRouteMatch{prefix("/f")}}

	for name, rule := range map[string]gwv1.HTTPRouteRule{
		"a match on a header":                  to("app2", header),
		"a match on a query parameter":         to("app2", query),
		"a redirect":                           filtered(gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterRequestRedirect, RequestRedirect: &gwv1.HTTPRequestRedirectFilter{}}),
		"a rewrite":                            filtered(gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterURLRewrite, URLRewrite: &gwv1.HTTPURLRewriteFilter{}}),
		"a mirror":                             filtered(gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterRequestMirror}),
		"a header taken off a request":         filtered(gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterRequestHeaderModifier, RequestHeaderModifier: &gwv1.HTTPHeaderFilter{Remove: []string{"X"}}}),
		"a header taken off a response":        filtered(gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterResponseHeaderModifier, ResponseHeaderModifier: &gwv1.HTTPHeaderFilter{Remove: []string{"X"}}}),
		"a filter on a backend":                backendFilter,
		"a timeout":                            timeouts,
		"a rule with no backend":               noBackend,
		"a regular expression that is not one": to("app2", regex("/a(")),
	} {
		t.Run(name, func(t *testing.T) {
			// Beside a rule that can be: the route is accepted, in part.
			w := newGwWorld()
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, to("app", prefix("/ok")), rule)}
			m, st := w.translate()
			wantRoute(t, st, "shop", "Accepted", metav1.ConditionTrue, "Accepted")
			wantRoute(t, st, "shop", "PartiallyInvalid", metav1.ConditionTrue, "UnsupportedValue")
			if got := server(t, m, "shop.example.com", "GET", "/ok"); got != "10.0.0.1:80" {
				t.Errorf("the rule that can be honoured goes to %q", got)
			}
			for _, path := range []string{"/h", "/q", "/f", "/a("} {
				if got := server(t, m, "shop.example.com", "GET", path); got != "" {
					t.Errorf("%s reaches %s without what was asked for it", path, got)
				}
			}

			// On its own: nothing of the route is programmed, and it says so.
			w = newGwWorld()
			w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
			w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, rule)}
			m, st = w.translate()
			wantRoute(t, st, "shop", "Accepted", metav1.ConditionFalse, "UnsupportedValue")
			if c := routeCondition(t, st, "shop", "PartiallyInvalid"); c != nil {
				t.Errorf("a route with nothing programmed is called partially invalid: %+v", c)
			}
			if len(m.hosts) != 0 {
				t.Errorf("hosts %v are rendered for a route that is not accepted", sortedKeys(m.hosts))
			}
		})
	}
}

func TestGateway_HeadersSetAndAdded(t *testing.T) {
	w := newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	rule := to("app")
	rule.Filters = []gwv1.HTTPRouteFilter{
		{Type: gwv1.HTTPRouteFilterRequestHeaderModifier, RequestHeaderModifier: &gwv1.HTTPHeaderFilter{
			Set: []gwv1.HTTPHeader{{Name: "X-Set", Value: "1"}}, Add: []gwv1.HTTPHeader{{Name: "X-Add", Value: "2"}},
		}},
		{Type: gwv1.HTTPRouteFilterResponseHeaderModifier, ResponseHeaderModifier: &gwv1.HTTPHeaderFilter{
			Set: []gwv1.HTTPHeader{{Name: "X-Resp", Value: "3"}},
		}},
	}
	// On a host of plain paths and on one of expressions alike.
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", []string{"shop.example.com"}, rule)}
	m, st := w.translate()
	wantRoute(t, st, "shop", "Accepted", metav1.ConditionTrue, "Accepted")
	rc := m.hosts["shop.example.com"]["/"]
	if !reflect.DeepEqual(rc.reqHeaders, []string{"X-Set: 1", "X-Add: 2"}) || !reflect.DeepEqual(rc.respHeaders, []string{"X-Resp: 3"}) {
		t.Errorf("headers are %v and %v", rc.reqHeaders, rc.respHeaders)
	}

	rule.Matches = []gwv1.HTTPRouteMatch{exact("/x")}
	w = newGwWorld()
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, "shop.example.com"))}
	w.in.routes = []gwv1.HTTPRoute{httpRoute("shop", nil, rule)}
	m, _ = w.translate()
	for _, rc := range m.hosts["shop.example.com"] {
		if !reflect.DeepEqual(rc.reqHeaders, []string{"X-Set: 1", "X-Add: 2"}) || !reflect.DeepEqual(rc.respHeaders, []string{"X-Resp: 3"}) {
			t.Errorf("on an expression route the headers are %v and %v", rc.reqHeaders, rc.respHeaders)
		}
	}
}

// An Ingress is rendered first. A host it routes stays its own: the two
// kinds of route are matched in ways that do not combine.
func TestGateway_AHostAnIngressRoutesIsTaken(t *testing.T) {
	w := newGwWorld()
	w.model.addRoute("shop.example.com", "/", []backend{{addr: "ingress:80"}}, annSettings{}, nil, nil)
	w.in.gateways = []gwv1.Gateway{gateway("edge", "web", httpListener("http", 80, ""))}
	w.in.routes = []gwv1.HTTPRoute{
		httpRoute("only", []string{"shop.example.com"}, to("app")),
		httpRoute("also", []string{"shop.example.com", "new.example.com"}, to("app2")),
	}
	m, st := w.translate()
	if got := server(t, m, "shop.example.com", "GET", "/"); got != "ingress:80" {
		t.Errorf("the Ingress's host goes to %q", got)
	}
	if len(m.hosts["shop.example.com"]) != 1 {
		t.Errorf("routes were added to the Ingress's host: %v", sortedKeys(m.hosts["shop.example.com"]))
	}
	wantRoute(t, st, "only", "Accepted", metav1.ConditionFalse, "HostnameConflict")
	wantRoute(t, st, "also", "Accepted", metav1.ConditionTrue, "Accepted")
	wantRoute(t, st, "also", "PartiallyInvalid", metav1.ConditionTrue, "UnsupportedValue")
	if got := server(t, m, "new.example.com", "GET", "/"); got != "10.0.0.2:80" {
		t.Errorf("the host that was free goes to %q", got)
	}
}

// The result may not depend on the order things are listed in: it is written
// to a ConfigMap, and a different order would roll nothing but still rewrite
// it, and statuses, on every pass.
func TestGateway_TranslationIsAFunctionOfItsInputs(t *testing.T) {
	build := func(seed int64) (string, gatewayStatuses) {
		w := newGwWorld()
		w.tlsSecret("edge", "cert")
		w.in.classes = append(w.in.classes, gwClass("other", "edge", "other"))
		w.in.gateways = []gwv1.Gateway{
			gateway("edge", "web", httpListener("http", 80, ""), httpsListener("https", "shop.example.com", secretRef("", "cert"))),
			gateway("edge", "second", httpListener("http", 80, "*.example.org")),
		}
		for i := range 6 {
			rt := httpRoute(fmt.Sprintf("r%d", i), []string{"shop.example.com", fmt.Sprintf("h%d.example.com", i%2)},
				to("app", prefix("/")), to("app2", exact(fmt.Sprintf("/e%d", i%3))), to("app3", onMethod("POST", prefix("/a"))))
			w.in.routes = append(w.in.routes, rt)
		}
		r := rand.New(rand.NewSource(seed))
		r.Shuffle(len(w.in.routes), func(i, j int) { w.in.routes[i], w.in.routes[j] = w.in.routes[j], w.in.routes[i] })
		r.Shuffle(len(w.in.gateways), func(i, j int) { w.in.gateways[i], w.in.gateways[j] = w.in.gateways[j], w.in.gateways[i] })
		r.Shuffle(len(w.in.classes), func(i, j int) { w.in.classes[i], w.in.classes[j] = w.in.classes[j], w.in.classes[i] })
		m, st := w.translate()
		return renderUpstreams(m), st
	}
	wantYAML, wantStatus := build(1)
	if !strings.Contains(wantYAML, "match_expr") || len(wantStatus.routes) != 6 {
		t.Fatalf("the example renders too little to show anything:\n%s", wantYAML)
	}
	for seed := int64(2); seed < 12; seed++ {
		gotYAML, gotStatus := build(seed)
		if gotYAML != wantYAML {
			t.Fatalf("listed in another order, the routes come out differently:\n%s\n---\n%s", wantYAML, gotYAML)
		}
		if !reflect.DeepEqual(gotStatus, wantStatus) {
			t.Fatalf("listed in another order, the statuses come out differently")
		}
	}
}
