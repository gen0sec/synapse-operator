package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

// v2File is as much of Synapse's v2 upstreams schema as the operator writes.
type v2File struct {
	Version int `json:"version"`
	Proxy   struct {
		Fingerprints struct {
			Forward *bool `json:"forward"`
		} `json:"fingerprints"`
		StickySessions struct {
			Enabled bool `json:"enabled"`
		} `json:"sticky_sessions"`
	} `json:"proxy"`
	Internal []struct {
		Path     string `json:"path"`
		Upstream string `json:"upstream"`
	} `json:"internal"`
	Timeouts v2Timeouts `json:"timeouts"`
	Hosts    map[string]struct {
		TLS struct {
			Terminate *struct {
				Cert string `json:"cert"`
			} `json:"terminate"`
			Passthrough bool `json:"passthrough"`
		} `json:"tls"`
		Upstream string             `json:"upstream"`
		Paths    map[string]v2Route `json:"paths"`
	} `json:"hosts"`
}

type v2Timeouts struct {
	Connect *uint64 `json:"connect"`
	Read    *uint64 `json:"read"`
	Write   *uint64 `json:"write"`
	Idle    *uint64 `json:"idle"`
}

type v2Route struct {
	Upstream    string `json:"upstream"`
	Upstreams   []any  `json:"upstreams"`
	MatchExpr   string `json:"match_expr"`
	SSLEnabled  *bool  `json:"ssl_enabled"`
	HTTP2       *bool  `json:"http2_enabled"`
	ForceHTTPS  *bool  `json:"force_https"`
	HealthCheck *struct {
		Type string `json:"type"`
	} `json:"health_check"`
	DisableAccessLog *bool   `json:"disable_access_log"`
	MaxBodySize      *uint64 `json:"max_body_size"`
	Redirect         *struct {
		Status   int    `json:"status"`
		Location string `json:"location"`
	} `json:"redirect"`
	Timeouts v2Timeouts `json:"timeouts"`
	Headers  struct {
		Request  []string `json:"request"`
		Response []string `json:"response"`
	} `json:"headers"`
	Transforms struct {
		RequestHeaders  []v2HeaderRule `json:"request_headers"`
		ResponseHeaders []v2HeaderRule `json:"response_headers"`
	} `json:"transforms"`
}

type v2HeaderRule struct {
	Remove []string            `json:"remove"`
	Add    map[string][]string `json:"add"`
}

// parseV2 reads a rendered v2 file strictly: a key Synapse's schema does not
// have is an error there, and the file is refused as a whole.
func parseV2(t *testing.T, rendered string) v2File {
	t.Helper()
	var doc v2File
	if err := yaml.UnmarshalStrict([]byte(rendered), &doc); err != nil {
		t.Fatalf("rendered file: %v\n%s", err, rendered)
	}
	if doc.Version != 2 {
		t.Fatalf("version is %d:\n%s", doc.Version, rendered)
	}
	return doc
}

// A host that terminates and has no certificate of its own, one that is
// only reached in plain HTTP for one, says `terminate: {}`. A `terminate:`
// with nothing after it is no `tls:` block to Synapse, and a host without
// one is refused, and the file with it.
func TestRenderUpstreamsV2_AHostWithoutACertificate(t *testing.T) {
	m := newRenderModel()
	servers := []backend{{addr: "10.0.0.1:80"}}
	m.addRoute("plain.example.com", "/", servers, annSettings{}, nil, nil)
	m.addRoute("tls.example.com", "/", servers, annSettings{}, nil, nil)
	m.addCert("tls.example.com", "tls-stem", "ns", "tls")
	rendered := renderUpstreamsV2(m)
	doc := parseV2(t, rendered)

	if got := doc.Hosts["plain.example.com"].TLS.Terminate; got == nil || got.Cert != "" {
		t.Errorf("a host without a certificate: terminate is %+v, want an empty block\n%s", got, rendered)
	}
	if !strings.Contains(rendered, "terminate: {}") {
		t.Errorf("no `terminate: {}` in:\n%s", rendered)
	}
	if got := doc.Hosts["tls.example.com"].TLS.Terminate; got == nil || got.Cert != "tls-stem" {
		t.Errorf("a host with a certificate: terminate is %+v", got)
	}
}

// With nothing to route the file is still a v2 file, and one Synapse loads.
func TestRenderUpstreamsV2_WithNothingInIt(t *testing.T) {
	for _, sameAsV1 := range []bool{false, true} {
		m := newRenderModel()
		m.sameAsV1 = sameAsV1
		if doc := parseV2(t, renderUpstreamsV2(m)); len(doc.Hosts) != 0 {
			t.Errorf("hosts are %v", doc.Hosts)
		}
	}
}

// v2Routes renders the routes build puts into a model and returns those of
// the host h.example.com, with the file they are in.
func v2Routes(t *testing.T, sameAsV1 bool, build func(m *renderModel)) (map[string]v2Route, v2File) {
	t.Helper()
	m := newRenderModel()
	m.sameAsV1 = sameAsV1
	build(m)
	doc := parseV2(t, renderUpstreamsV2(m))
	return doc.Hosts["h.example.com"].Paths, doc
}

// A SynapseProxy's routes were written as a v1 file. As a v2 file they say
// what Synapse made of that one, where the two schemas do not mean the same
// by saying nothing.

// How a backend is reached: plain HTTP unless the route asks for TLS. Left
// unsaid in a v2 file, Synapse goes by the port, and a Service on 443 that
// speaks plain HTTP would be sent TLS.
func TestRenderUpstreamsV2_TLSToBackendsIsSaid(t *testing.T) {
	servers := []backend{{addr: "10.0.0.1:443"}}
	build := func(m *renderModel) {
		m.addRoute("h.example.com", "/", servers, annSettings{}, nil, nil)
		m.addRoute("h.example.com", "/tls", servers, annSettings{ssl: ptr(true)}, nil, nil)
		m.addExprRoute("h.example.com", "gateway:00000", `http.request.method eq "POST"`, servers, nil, nil)
	}
	routes, _ := v2Routes(t, true, build)
	for key, want := range map[string]bool{"/": false, "/tls": true, "gateway:00000": false} {
		if got := routes[key].SSLEnabled; got == nil || *got != want {
			t.Errorf("route %q: ssl_enabled is %v, want %v", key, got, want)
		}
	}
	// The older modes leave it to Synapse, as they did.
	routes, _ = v2Routes(t, false, build)
	if got := routes["/"].SSLEnabled; got != nil {
		t.Errorf("without it being asked for, ssl_enabled is %v", *got)
	}
}

// Whether a route's backends are probed. In a v1 file a route is probed
// unless it says `healthcheck: false`, and saying `true` changes nothing.
// In a v2 file a route without a `health_check:` block is probed in the
// same way, and `type: none` is the way to say it is not.
func TestRenderUpstreamsV2_ProbesAreWhatTheV1FileAskedFor(t *testing.T) {
	servers := []backend{{addr: "10.0.0.1:8080"}}
	build := func(m *renderModel) {
		m.addRoute("h.example.com", "/", servers, annSettings{}, nil, nil)
		m.addRoute("h.example.com", "/probed", servers, annSettings{healthcheck: ptr(true)}, nil, nil)
		m.addRoute("h.example.com", "/unprobed", servers, annSettings{healthcheck: ptr(false)}, nil, nil)
		m.addRoute("h.example.com", "/unprobed-tls", servers, annSettings{healthcheck: ptr(false), ssl: ptr(true)}, nil, nil)
		m.addRegexRoute("h.example.com", "/v[0-9]+", servers, annSettings{healthcheck: ptr(true)}, nil, nil)
		m.addRegexRoute("h.example.com", "/w[0-9]+", servers, annSettings{healthcheck: ptr(false)}, nil, nil)
	}
	routes, _ := v2Routes(t, true, build)
	if len(routes) != 6 {
		t.Fatalf("%d routes, want 6", len(routes))
	}
	for key, route := range routes {
		kind := ""
		if route.HealthCheck != nil {
			kind = route.HealthCheck.Type
		}
		switch {
		case route.MatchExpr != "":
			// Synapse does not probe a route chosen by an expression, and
			// refuses a file that asks it to.
			if kind != "" {
				t.Errorf("expression route %q has a health check of type %q", key, kind)
			}
			if route.SSLEnabled == nil || *route.SSLEnabled {
				t.Errorf("expression route %q: ssl_enabled is %v, want it said, false", key, route.SSLEnabled)
			}
		case strings.HasPrefix(key, "/unprobed"):
			if kind != "none" {
				t.Errorf("route %q is not probed in the v1 file, and here its health check is %q", key, kind)
			}
			// A backend that is never probed is reached by its port,
			// whatever the route says of TLS, in a v1 file too. A v2 file
			// that says otherwise beside `type: none` is refused.
			if route.SSLEnabled != nil {
				t.Errorf("route %q says ssl_enabled: %v beside a health check of type none", key, *route.SSLEnabled)
			}
		default:
			if kind != "" {
				t.Errorf("route %q is probed with the global settings in the v1 file, and here by %q", key, kind)
			}
		}
	}

	// The older modes write what they wrote.
	routes, _ = v2Routes(t, false, build)
	if hc := routes["/probed"].HealthCheck; hc == nil || hc.Type != "tcp" {
		t.Errorf("the older modes' health check for a probed route is %+v", hc)
	}
	if hc := routes["/unprobed"].HealthCheck; hc != nil {
		t.Errorf("the older modes' health check for a route that is not probed is %+v", hc)
	}
	// Except on a route chosen by an expression, where Synapse takes a
	// health check for an error.
	for key, route := range routes {
		if route.MatchExpr != "" && route.HealthCheck != nil {
			t.Errorf("the older modes' expression route %q has a health check: %+v", key, route.HealthCheck)
		}
	}
}

// How long a backend may take to send. A route that sets no read timeout
// has 120 seconds in a v1 file, and would have 60 in a v2 file that sets
// none either.
func TestRenderUpstreamsV2_TheReadTimeoutIsTheV1Files(t *testing.T) {
	servers := []backend{{addr: "10.0.0.1:8080"}}
	build := func(m *renderModel) {
		m.addRoute("h.example.com", "/", servers, annSettings{}, nil, nil)
		m.addRoute("h.example.com", "/slow", servers, annSettings{readTimeout: ptr(uint64(600)), connectTimeout: ptr(uint64(3))}, nil, nil)
	}
	routes, doc := v2Routes(t, true, build)
	if got := doc.Timeouts.Read; got == nil || *got != 120 {
		t.Errorf("the file's read timeout is %v, want 120", got)
	}
	// The other three are the same in both schemas, and are not said.
	if doc.Timeouts.Connect != nil || doc.Timeouts.Write != nil || doc.Timeouts.Idle != nil {
		t.Errorf("the file's timeouts are %+v", doc.Timeouts)
	}
	if got := routes["/"].Timeouts; got != (v2Timeouts{}) {
		t.Errorf("a route that sets no timeout has %+v", got)
	}
	slow := routes["/slow"].Timeouts
	if slow.Read == nil || *slow.Read != 600 || slow.Connect == nil || *slow.Connect != 3 {
		t.Errorf("a route's own timeouts are %+v", slow)
	}

	_, doc = v2Routes(t, false, build)
	if doc.Timeouts != (v2Timeouts{}) {
		t.Errorf("the older modes' file sets timeouts: %+v", doc.Timeouts)
	}
}

// What a backend is told about the client. A v2 file has the client's
// fingerprints sent to the backend in request headers unless it says not
// to, and a v1 file does not send them.
func TestRenderUpstreamsV2_FingerprintsAreNotSentToBackends(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		_, doc := v2Routes(t, true, func(m *renderModel) { m.sticky = sticky })
		if got := doc.Proxy.Fingerprints.Forward; got == nil || *got {
			t.Errorf("sticky %v: fingerprints.forward is %v, want it said, false", sticky, got)
		}
		if doc.Proxy.StickySessions.Enabled != sticky {
			t.Errorf("sticky sessions are %v, want %v", doc.Proxy.StickySessions.Enabled, sticky)
		}
		// The older modes write what they wrote.
		_, doc = v2Routes(t, false, func(m *renderModel) { m.sticky = sticky })
		if got := doc.Proxy.Fingerprints.Forward; got != nil {
			t.Errorf("the older modes say fingerprints.forward: %v", *got)
		}
		if doc.Proxy.StickySessions.Enabled != sticky {
			t.Errorf("the older modes' sticky sessions are %v, want %v", doc.Proxy.StickySessions.Enabled, sticky)
		}
	}
}

// The older modes write a v2 file only beside a passthrough host, and write
// it as they did: what a proxy's file says of TLS to backends, of the read
// timeout and of fingerprints is left unsaid in theirs.
func TestRender_TheOlderModesV2FileIsAsItWas(t *testing.T) {
	out := filepath.Join(t.TempDir(), "u.yaml")
	plain := routedIngress("plain", ptr("synapse"), "plain.example.com")
	raw := routedIngress("raw", ptr("synapse"), "raw.example.com")
	raw.Annotations = map[string]string{"synapse.gen0sec.com/ssl-passthrough": "true"}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(plain, raw).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	doc := parseV2(t, string(rendered))
	if !doc.Hosts["raw.example.com"].TLS.Passthrough {
		t.Fatalf("raw.example.com is not passed through:\n%s", rendered)
	}
	route, ok := doc.Hosts["plain.example.com"].Paths["/"]
	if !ok {
		t.Fatalf("plain.example.com has no route:\n%s", rendered)
	}
	if route.SSLEnabled != nil {
		t.Errorf("ssl_enabled is said: %v", *route.SSLEnabled)
	}
	if doc.Timeouts != (v2Timeouts{}) {
		t.Errorf("the file sets timeouts: %+v", doc.Timeouts)
	}
	if got := doc.Proxy.Fingerprints.Forward; got != nil {
		t.Errorf("the file says fingerprints.forward: %v", *got)
	}
}

// cert-manager answers an HTTP-01 challenge from a solver it puts behind an
// Ingress of its own. In a v2 file Synapse has no list of paths outside the
// hosts that it routes by, so the solver's path is a route of its host: a
// plain path, which Synapse tries before the host's expressions.
func TestRenderUpstreamsV2_ASolverIsARouteOfItsHost(t *testing.T) {
	const token = "/.well-known/acme-challenge/tok"
	m := newRenderModel()
	m.sameAsV1 = true
	servers := []backend{{addr: "10.0.0.1:80"}}
	m.addRoute("plain.example.com", "/", servers, annSettings{}, nil, nil)
	m.addExprRoute("expr.example.com", "gateway:00000", `http.request.path matches "^/"`, servers, nil, nil)
	for _, host := range []string{"plain.example.com", "expr.example.com", "new.example.com"} {
		m.addSolver(host, token, "solver.ns.svc:8089")
	}
	m.acme = "solver.ns.svc:8089"
	rendered := renderUpstreamsV2(m)
	doc := parseV2(t, rendered)
	if len(doc.Internal) != 0 {
		t.Errorf("the file has a list of internal paths, which Synapse reads and does not route by:\n%s", rendered)
	}
	for host, others := range map[string]int{"plain.example.com": 1, "expr.example.com": 1, "new.example.com": 0} {
		paths := doc.Hosts[host].Paths
		route, ok := paths[token]
		switch {
		case !ok || route.Upstream != "solver.ns.svc:8089" || route.MatchExpr != "":
			t.Errorf("%s: the solver's route is %+v\n%s", host, route, rendered)
		case route.SSLEnabled == nil || *route.SSLEnabled:
			t.Errorf("%s: the solver is not said to be reached in plain HTTP", host)
		case len(paths) != others+1:
			t.Errorf("%s has %d routes, want %d", host, len(paths), others+1)
		}
	}
	if got := doc.Hosts["new.example.com"].TLS.Terminate; got == nil || got.Cert != "" {
		t.Errorf("a host that has only a solver: terminate is %+v", got)
	}
	// A solver does not take its host: what else may route it is decided
	// by what is in the model's hosts.
	if _, taken := m.hosts["new.example.com"]; taken {
		t.Error("a solver made its host one that is routed")
	}

	// The older modes write what they wrote.
	m.sameAsV1 = false
	doc = parseV2(t, renderUpstreamsV2(m))
	if len(doc.Internal) != 1 || doc.Internal[0].Upstream != "solver.ns.svc:8089" {
		t.Errorf("the older modes' internal paths are %+v", doc.Internal)
	}
	if _, ok := doc.Hosts["plain.example.com"].Paths[token]; ok {
		t.Error("the older modes' file has the solver as a route")
	}
}

// v2Server is where the rendered file sends a request for name, as Synapse
// resolves it: the host of that name, or else the wildcard host with the
// longest suffix that covers it; on that host a plain path that is the
// request's, then the expression routes in the order of their keys, then the
// longest plain path that is a prefix. host is the host it was served as.
func v2Server(t *testing.T, doc v2File, name, path string) (host, upstream string) {
	t.Helper()
	if _, ok := doc.Hosts[name]; ok {
		host = name
	} else {
		for h := range doc.Hosts {
			if suffix, wild := strings.CutPrefix(h, "*"); wild && len(name) > len(suffix) &&
				strings.HasSuffix(name, suffix) && len(h) > len(host) {
				host = h
			}
		}
	}
	routes := doc.Hosts[host].Paths
	if rc, ok := routes[path]; ok && rc.MatchExpr == "" {
		return host, rc.Upstream
	}
	for _, key := range sortedKeys(routes) {
		if rc := routes[key]; rc.MatchExpr != "" && evalRouteExpr(t, rc.MatchExpr, gwRequest{method: "GET", path: path}) {
			return host, rc.Upstream
		}
	}
	for p := path; p != ""; {
		p = p[:strings.LastIndex(p, "/")]
		if rc, ok := routes[cmpOr(p, "/")]; ok && rc.MatchExpr == "" {
			return host, rc.Upstream
		}
	}
	return host, ""
}

// Synapse serves a request as one host, the one of its name before any
// wildcard, and looks no further. A host written for a solver alone would
// take every other request for that name from the wildcard host that was
// serving it, for as long as the challenge is open. So the solver's path is
// a path of the host that serves its name.
func TestRenderUpstreamsV2_ASolverDoesNotTakeItsNameFromAWildcardHost(t *testing.T) {
	const token = "/.well-known/acme-challenge/tok"
	m := newRenderModel()
	m.sameAsV1 = true
	m.addRoute("*.example.com", "/", []backend{{addr: "10.0.0.1:80"}}, annSettings{}, nil, nil)
	m.addRoute("*.a.example.com", "/", []backend{{addr: "10.0.0.2:80"}}, annSettings{}, nil, nil)
	m.addSolver("foo.example.com", token, "solver.ns.svc:8089")
	m.addSolver("x.a.example.com", token+"2", "solver.ns.svc:8089")
	m.addSolver("other.test", token, "solver.ns.svc:8089")
	// Whichever of the two wildcards is looked at first.
	for range 64 {
		if host, ok := m.solverHost("x.a.example.com"); host != "*.a.example.com" || !ok {
			t.Fatalf("x.a.example.com is served as %q (%v)", host, ok)
		}
	}
	rendered := renderUpstreamsV2(m)
	doc := parseV2(t, rendered)

	if want := []string{"*.a.example.com", "*.example.com", "other.test"}; !slices.Equal(sortedKeys(doc.Hosts), want) {
		t.Fatalf("the hosts are %q, want %q\n%s", sortedKeys(doc.Hosts), want, rendered)
	}
	for _, c := range []struct{ name, path, host, upstream string }{
		{"foo.example.com", "/", "*.example.com", "10.0.0.1:80"},
		{"foo.example.com", token, "*.example.com", "solver.ns.svc:8089"},
		// The wildcard with the longest suffix is the one that serves.
		{"x.a.example.com", "/", "*.a.example.com", "10.0.0.2:80"},
		{"x.a.example.com", token + "2", "*.a.example.com", "solver.ns.svc:8089"},
		{"x.a.example.com", token, "*.a.example.com", "10.0.0.2:80"},
		// No host covers the name: it is written for the solver.
		{"other.test", token, "other.test", "solver.ns.svc:8089"},
	} {
		if host, upstream := v2Server(t, doc, c.name, c.path); host != c.host || upstream != c.upstream {
			t.Errorf("%s%s is served as %q by %q, want as %q by %q", c.name, c.path, host, upstream, c.host, c.upstream)
		}
	}
}

// Synapse finds a plain path by walking up the request's path, which never
// arrives at a key that ends in a slash; and it tries a host's expressions
// before any plain path that is only a prefix. A solver for the whole
// challenge directory is reached all the same.
func TestRenderUpstreamsV2_ASolverForTheWholeDirectory(t *testing.T) {
	const dir, token = "/.well-known/acme-challenge", "/.well-known/acme-challenge/tok"
	m := newRenderModel()
	m.sameAsV1 = true
	servers := []backend{{addr: "10.0.0.1:80"}}
	m.addRoute("plain.example.com", "/", servers, annSettings{}, nil, nil)
	m.addRegexRoute("regex.example.com", "/.*", servers, annSettings{}, nil, nil)
	m.addExprRoute("gw.example.com", "gateway:00000", `http.request.path matches "^/"`, servers, nil, nil)
	for _, host := range []string{"plain.example.com", "regex.example.com", "gw.example.com"} {
		m.addSolver(host, dir+"/", "solver.ns.svc:8089")
	}
	rendered := renderUpstreamsV2(m)
	doc := parseV2(t, rendered)

	for _, host := range []string{"plain.example.com", "regex.example.com", "gw.example.com"} {
		for path, want := range map[string]string{
			token: "solver.ns.svc:8089", dir: "solver.ns.svc:8089", dir + "/": "solver.ns.svc:8089",
			"/": "10.0.0.1:80", dir + "-not": "10.0.0.1:80",
		} {
			if _, got := v2Server(t, doc, host, path); got != want {
				t.Errorf("%s%s is served by %q, want %q\n%s", host, path, got, want, rendered)
			}
		}
	}
	// A host of plain paths stays one: nothing is evaluated for it.
	for key, route := range doc.Hosts["plain.example.com"].Paths {
		if route.MatchExpr != "" {
			t.Errorf("plain.example.com has an expression route %q", key)
		}
	}
	if _, ok := doc.Hosts["plain.example.com"].Paths[dir]; !ok {
		t.Errorf("plain.example.com has no path %q\n%s", dir, rendered)
	}
}

// A passthrough host is handed on as it comes: Synapse serves no path of
// it. A solver for one, or for a name a passthrough wildcard covers, is not
// written, the host stays what it was, and the Ingress is told.
func TestRoutes_ASolverOnAPassthroughHostIsSaid(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	raw := routedIngress("raw", ptr("public"), "raw.example.com")
	raw.Annotations = map[string]string{"synapse.gen0sec.com/ssl-passthrough": "true"}
	wild := routedIngress("wild", ptr("public"), "*.raw.test")
	wild.Annotations = raw.Annotations
	var solvers []client.Object
	for i, host := range []string{"raw.example.com", "a.raw.test"} {
		solver := routedIngress(fmt.Sprintf("cm-acme-http-solver-%d", i), ptr("public"), host)
		solver.Spec.Rules[0].HTTP.Paths[0].Path = "/.well-known/acme-challenge/tok"
		solvers = append(solvers, solver)
	}
	r := newRouteReconciler(t, append([]client.Object{edge, classFor("public", edge), raw, wild}, solvers...)...)
	events := record.NewFakeRecorder(16)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)
	rendered := upstreamsOf(t, r, edge)
	doc := parseV2(t, rendered)

	if want := []string{"*.raw.test", "raw.example.com"}; !slices.Equal(sortedKeys(doc.Hosts), want) {
		t.Errorf("the hosts are %q, want %q\n%s", sortedKeys(doc.Hosts), want, rendered)
	}
	for host, entry := range doc.Hosts {
		if !entry.TLS.Passthrough || len(entry.Paths) != 0 {
			t.Errorf("%s is no longer only passed through\n%s", host, rendered)
		}
	}
	told := 0
	for len(events.Events) > 0 {
		if event := <-events.Events; strings.HasPrefix(event, corev1.EventTypeWarning+" SolverUnreachable ") {
			told++
		}
	}
	if told != 2 {
		t.Errorf("%d solvers were told they cannot be reached, want 2", told)
	}
}

// The older modes write a `use-regex` path as it came: what their Synapse
// makes of it is not this change's to decide.
func TestRender_TheOlderModesRegularExpressionsAreAsTheyCame(t *testing.T) {
	out := filepath.Join(t.TempDir(), "u.yaml")
	ing := routedIngress("versioned", ptr("synapse"), "api.example.com")
	ing.Annotations = map[string]string{"synapse.gen0sec.com/use-regex": "true"}
	first := ing.Spec.Rules[0].HTTP.Paths[0]
	var paths []networkingv1.HTTPIngressPath
	for _, p := range []string{`/v\d+/items`, "/app/(?!admin).*"} {
		next := first
		next.Path = p
		paths = append(paths, next)
	}
	ing.Spec.Rules[0].HTTP.Paths = paths
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{pathRegexExpr(`/v\d+/items`), pathRegexExpr("/app/(?!admin).*")} {
		if !strings.Contains(string(rendered), fmt.Sprintf("%q", want)) {
			t.Errorf("the file has no route by %s\n%s", want, rendered)
		}
	}
}

// What an Ingress asks for that Synapse would refuse a v2 file over is left
// out for itself, and the rest of the Ingress is served.
func TestRoutes_WhatSynapseWouldRefuseTheFileOver(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	path := func(p string) networkingv1.HTTPIngressPath {
		return networkingv1.HTTPIngressPath{Path: p, PathType: ptr(networkingv1.PathTypeImplementationSpecific), Backend: networkingv1.IngressBackend{
			Service: &networkingv1.IngressServiceBackend{Name: "app", Port: networkingv1.ServiceBackendPort{Number: 80}}}}
	}
	versioned := routedIngress("versioned", ptr("public"), "api.example.com")
	versioned.Annotations = map[string]string{"synapse.gen0sec.com/use-regex": "true"}
	// The second is no regular expression Synapse has: a look-ahead.
	versioned.Spec.Rules[0].HTTP.Paths = []networkingv1.HTTPIngressPath{path(`/v\d+/items`), path("/app/(?!admin).*"), path("/ok")}
	aliased := routedIngress("aliased", ptr("public"), "www.example.com")
	aliased.Annotations = map[string]string{"synapse.gen0sec.com/server-alias": "*, www.*.example.com, *., alt.example.com, *.wild.example.com"}
	solver := routedIngress("cm-acme-http-solver", ptr("public"), "www.example.com")
	solver.Spec.Rules[0].HTTP.Paths[0].Path = "/.well-known/acme-challenge/tok"

	r := newRouteReconciler(t, edge, classFor("public", edge), versioned, aliased, solver)
	mustReconcileRoutes(t, r, edge)
	rendered := upstreamsOf(t, r, edge)
	doc := parseV2(t, rendered)

	var exprs []string
	for _, route := range doc.Hosts["api.example.com"].Paths {
		exprs = append(exprs, route.MatchExpr)
	}
	slices.Sort(exprs)
	// As Synapse's engine has them: a class written out, and nothing of the
	// path it would not compile.
	want := []string{`http.request.path matches "^/ok"`, `http.request.path matches "^/v[0-9]+/items"`}
	if !slices.Equal(exprs, want) {
		t.Errorf("api.example.com is routed by %q, want %q\n%s", exprs, want, rendered)
	}
	hosts := sortedKeys(doc.Hosts)
	if want := []string{"*.wild.example.com", "alt.example.com", "api.example.com", "www.example.com"}; !slices.Equal(hosts, want) {
		t.Errorf("the hosts are %q, want %q", hosts, want)
	}
	if route, ok := doc.Hosts["www.example.com"].Paths["/.well-known/acme-challenge/tok"]; !ok || route.Upstream != "app.default.svc.cluster.local:80" {
		t.Errorf("the solver's route on its host is %+v\n%s", route, rendered)
	}
	if len(doc.Internal) != 0 {
		t.Errorf("the file has internal paths: %+v", doc.Internal)
	}
}

// Headers a route sets are its `headers:`; ones it adds to or removes are
// transform rules, which is where Synapse has those.
func TestRenderUpstreamsV2_HeaderRules(t *testing.T) {
	m := newRenderModel()
	m.addRoute("h.example.com", "/", []backend{{addr: "10.0.0.1:80"}}, annSettings{}, []string{"X-Set: 1"}, []string{"X-Frame-Options: DENY"})
	rc := m.hosts["h.example.com"]["/"]
	rc.reqRemove = []string{"X-Debug"}
	rc.reqAdd = []string{"X-Trace: a", "X-Trace: b: c", "X-Other: z"}
	rc.respRemove = []string{"Server", "X-Powered-By"}
	rc.respAdd = []string{"Set-Cookie: flag=1; Path=/"}
	rendered := renderUpstreamsV2(m)
	route := parseV2(t, rendered).Hosts["h.example.com"].Paths["/"]

	if got := route.Headers.Request; len(got) != 1 || got[0] != "X-Set: 1" {
		t.Errorf("request headers set are %q", got)
	}
	if got := route.Headers.Response; len(got) != 1 || got[0] != "X-Frame-Options: DENY" {
		t.Errorf("response headers set are %q", got)
	}
	req, resp := route.Transforms.RequestHeaders, route.Transforms.ResponseHeaders
	if len(req) != 1 || len(resp) != 1 {
		t.Fatalf("transform rules are %+v and %+v, want one each\n%s", req, resp, rendered)
	}
	if got := req[0].Remove; len(got) != 1 || got[0] != "X-Debug" {
		t.Errorf("request headers removed are %q", got)
	}
	// Values of one name are added in the order they were given, and a
	// value may have a colon in it.
	if got := req[0].Add["X-Trace"]; len(got) != 2 || got[0] != "a" || got[1] != "b: c" {
		t.Errorf("X-Trace is added as %q", got)
	}
	if got := req[0].Add["X-Other"]; len(got) != 1 || got[0] != "z" {
		t.Errorf("X-Other is added as %q", got)
	}
	if got := resp[0].Remove; len(got) != 2 {
		t.Errorf("response headers removed are %q", got)
	}
	if got := resp[0].Add["Set-Cookie"]; len(got) != 1 || got[0] != "flag=1; Path=/" {
		t.Errorf("Set-Cookie is added as %q", got)
	}

	// A route that neither adds nor removes has no transforms at all.
	m = newRenderModel()
	m.addRoute("h.example.com", "/", []backend{{addr: "10.0.0.1:80"}}, annSettings{}, nil, nil)
	if rendered := renderUpstreamsV2(m); strings.Contains(rendered, "transforms") {
		t.Errorf("transforms for a route that has none:\n%s", rendered)
	}
}
