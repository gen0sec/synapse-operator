package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
