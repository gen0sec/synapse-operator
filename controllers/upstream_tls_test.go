package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestParseAnnotations_UpstreamTLS(t *testing.T) {
	const syn, ngx = annPrefix, nginxPrefix
	cases := []struct {
		name        string
		ann         map[string]string
		verify      *bool
		sni         string
		refusedKeys []string
	}{
		{"nothing", map[string]string{}, nil, "", nil},
		{"synapse verify false", map[string]string{syn + "upstream-tls-verify": "false"}, ptr(false), "", nil},
		{"synapse verify true", map[string]string{syn + "upstream-tls-verify": "true"}, ptr(true), "", nil},
		{"nginx verify off", map[string]string{ngx + "proxy-ssl-verify": "off"}, ptr(false), "", nil},
		{"nginx verify on", map[string]string{ngx + "proxy-ssl-verify": "on"}, ptr(true), "", nil},
		{"nginx verify false", map[string]string{ngx + "proxy-ssl-verify": "false"}, ptr(false), "", nil},
		{"synapse verify wins over nginx", map[string]string{syn + "upstream-tls-verify": "true", ngx + "proxy-ssl-verify": "off"}, ptr(true), "", nil},
		{"synapse sni", map[string]string{syn + "upstream-tls-sni": "api.internal.example.com"}, nil, "api.internal.example.com", nil},
		{"nginx name", map[string]string{ngx + "proxy-ssl-name": "api.internal.example.com"}, nil, "api.internal.example.com", nil},
		{"synapse sni wins over nginx", map[string]string{syn + "upstream-tls-sni": "a.example.com", ngx + "proxy-ssl-name": "b.example.com"}, nil, "a.example.com", nil},
		{"garbage verify", map[string]string{syn + "upstream-tls-verify": "maybe"}, nil, "", []string{syn + "upstream-tls-verify"}},
		{"garbage nginx verify", map[string]string{ngx + "proxy-ssl-verify": "default"}, nil, "", []string{ngx + "proxy-ssl-verify"}},
		{"an sni Synapse refuses", map[string]string{syn + "upstream-tls-sni": "127.1"}, nil, "", []string{syn + "upstream-tls-sni"}},
		{"a bad synapse sni is not replaced by the nginx one", map[string]string{syn + "upstream-tls-sni": "a/b", ngx + "proxy-ssl-name": "ok.example.com"}, nil, "", []string{syn + "upstream-tls-sni"}},
		{"one bad value leaves the other field", map[string]string{syn + "upstream-tls-verify": "false", syn + "upstream-tls-sni": "a:443"}, ptr(false), "", []string{syn + "upstream-tls-sni"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAnnotations(tc.ann).upstreamTLS
			if (got.verify == nil) != (tc.verify == nil) || (got.verify != nil && *got.verify != *tc.verify) {
				t.Errorf("verify is %v, want %v", got.verify, tc.verify)
			}
			if got.sni != tc.sni {
				t.Errorf("sni is %q, want %q", got.sni, tc.sni)
			}
			if len(got.refused) != len(tc.refusedKeys) {
				t.Fatalf("refused is %q, want one entry for each of %q", got.refused, tc.refusedKeys)
			}
			for i, key := range tc.refusedKeys {
				if !strings.Contains(got.refused[i], key) {
					t.Errorf("refused[%d] is %q, want it to name %q", i, got.refused[i], key)
				}
			}
		})
	}
}

// The cases are those of Synapse's docs (docs/CONFIGURATION.md, "Upstream
// TLS", the `sni` row) and of its own tests for sni_is_usable.
func TestUsableSNI(t *testing.T) {
	for _, sni := range []string{
		"10.0.0.1", "2001:db8::2", "[2001:db8::2]", "::1", "[::1]", "::ffff:10.0.0.1", "[::ffff:10.0.0.1]",
		"backend.test", "Backend.Test", "a_b.svc.test", "a-b.svc.test", "localhost", "svc.test.",
		"xn--bcher-kva.test", "api.internal.example.com", "xn--bcher-kva.example",
		"app.default.svc.cluster.local",
	} {
		if !usableSNI(sni) {
			t.Errorf("%q is usable as written", sni)
		}
	}
	for _, sni := range []string{
		"", " ", "[::1", "::1]", "[localhost", "localhost]", "[[::1]]",
		"a.test/x", "a/b", "a.test?x", "a?b", "a.test#x", "a#b", "u@host", "a@b", "a%41.test", "a%b",
		"a.test b", "a b", " a.test", "a.test:80", "a:443",
		"127.1", "0x7f.1", "2130706433", "bücher.test", "bücher.example", "*.example.com",
	} {
		if usableSNI(sni) {
			t.Errorf("%q cannot be used as written", sni)
		}
	}
}

func TestServiceFQDN(t *testing.T) {
	if got := serviceFQDN("shop", "app", ""); got != "app.shop.svc.cluster.local" {
		t.Errorf("default cluster domain: %q", got)
	}
	if got := serviceFQDN("shop", "app", "k8s.example"); got != "app.shop.svc.k8s.example" {
		t.Errorf("custom cluster domain: %q", got)
	}
}

type v2TLSOf struct {
	Verify *bool  `json:"verify"`
	SNI    string `json:"sni"`
	CA     string `json:"ca"`
}

// An HTTPS route says which name its certificate has to carry, whether or not
// it is probed. A route that is not HTTPS says nothing.
func TestRenderUpstreamsV2_TLSToBackendsSaysTheName(t *testing.T) {
	servers := []backend{{addr: "10.0.0.1:443"}}
	sni := upstreamTLSAnn{sni: "app.zzz.svc.cluster.local"}
	build := func(m *renderModel) {
		m.addRoute("h.example.com", "/", servers, annSettings{upstreamTLS: sni}, nil, nil)
		m.addRoute("h.example.com", "/tls", servers, annSettings{ssl: ptr(true), upstreamTLS: sni}, nil, nil)
		m.addRoute("h.example.com", "/http", servers, annSettings{ssl: ptr(false), upstreamTLS: sni}, nil, nil)
		m.addRoute("h.example.com", "/unprobed", servers, annSettings{ssl: ptr(true), healthcheck: ptr(false), upstreamTLS: sni}, nil, nil)
		m.addRegexRoute("h.example.com", "/v[0-9]+", servers, annSettings{ssl: ptr(true), upstreamTLS: sni}, nil, nil)
		m.addExprRoute("h.example.com", "gateway:00000", `http.request.method eq "POST"`, servers, nil, nil)
	}
	routes, _ := v2Routes(t, build)
	for key, route := range routes {
		switch key {
		case "/tls", "/unprobed", regexRouteKey("/v[0-9]+"):
			if route.UpstreamTLS == nil || route.UpstreamTLS.SNI != "app.zzz.svc.cluster.local" {
				t.Errorf("route %q: upstream_tls is %+v, want the Service name", key, route.UpstreamTLS)
				continue
			}
			if route.UpstreamTLS.Verify != nil || route.UpstreamTLS.CA != "" {
				t.Errorf("route %q: upstream_tls says %+v beyond the name", key, route.UpstreamTLS)
			}
		default:
			if route.UpstreamTLS != nil {
				t.Errorf("route %q is not HTTPS and says upstream_tls %+v", key, route.UpstreamTLS)
			}
		}
	}
	if routes["/unprobed"].SSLEnabled != nil {
		t.Error("the unprobed route says ssl_enabled beside health_check type none")
	}
}

func TestRenderUpstreamsV2_UpstreamTLSAnnotations(t *testing.T) {
	servers := []backend{{addr: "10.0.0.1:443"}}
	build := func(m *renderModel) {
		m.addRoute("h.example.com", "/off", servers, annSettings{ssl: ptr(true), upstreamTLS: upstreamTLSAnn{verify: ptr(false), sni: "b.example.com"}}, nil, nil)
		m.addRoute("h.example.com", "/on", servers, annSettings{ssl: ptr(true), upstreamTLS: upstreamTLSAnn{verify: ptr(true), sni: "b.example.com"}}, nil, nil)
		m.addRoute("h.example.com", "/unset", servers, annSettings{ssl: ptr(true), upstreamTLS: upstreamTLSAnn{sni: "b.example.com"}}, nil, nil)
	}
	routes, _ := v2Routes(t, build)
	for key, want := range map[string]*bool{"/off": ptr(false), "/on": ptr(true), "/unset": nil} {
		got := routes[key].UpstreamTLS
		if got == nil || got.SNI != "b.example.com" {
			t.Fatalf("route %q: upstream_tls is %+v", key, got)
		}
		if (got.Verify == nil) != (want == nil) || (got.Verify != nil && *got.Verify != *want) {
			t.Errorf("route %q: verify is %v, want %v", key, got.Verify, want)
		}
	}
}

// renderIngresses renders Ingresses in the central-mode shape the older modes
// share (a routes file written by IngressReconciler) and returns the file
// and the events.
func renderIngressesWith(t *testing.T, clusterDomain string, objs ...client.Object) (string, *record.FakeRecorder) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "u.yaml")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	events := record.NewFakeRecorder(64)
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: clusterDomain, Recorder: events}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered), events
}

func TestRender_HTTPSBackendsGetTheServiceNameAsSNI(t *testing.T) {
	https := annotated(ingressIn("zzz", "secure", "secure.example.com", "/"), "backend-protocol", "HTTPS")
	unprobed := annotated(ingressIn("zzz", "bare", "bare.example.com", "/"), "backend-protocol", "HTTPS", "healthcheck", "false")
	regex := annotated(ingressIn("zzz", "rx", "rx.example.com", "/v[0-9]+"), "backend-protocol", "HTTPS", "use-regex", "true")
	plain := ingressIn("zzz", "plain", "plain.example.com", "/")
	rendered, events := renderIngressesWith(t, "cluster.local", https, unprobed, regex, plain)
	doc := parseV2(t, rendered)

	for _, host := range []string{"secure.example.com", "bare.example.com", "rx.example.com"} {
		for key, route := range doc.Hosts[host].Paths {
			if route.UpstreamTLS == nil || route.UpstreamTLS.SNI != "app.zzz.svc.cluster.local" {
				t.Errorf("%s %s: upstream_tls is %+v\n%s", host, key, route.UpstreamTLS, rendered)
			}
		}
		if len(doc.Hosts[host].Paths) != 1 {
			t.Errorf("%s has %d routes", host, len(doc.Hosts[host].Paths))
		}
	}
	if doc.Hosts["bare.example.com"].Paths["/"].SSLEnabled != nil {
		t.Error("the unprobed route says ssl_enabled")
	}
	if got := doc.Hosts["plain.example.com"].Paths["/"].UpstreamTLS; got != nil {
		t.Errorf("a plain HTTP route says upstream_tls %+v", got)
	}
	if got := eventsOf(events, "InvalidUpstreamTLS"); len(got) != 0 {
		t.Errorf("events: %v", got)
	}
}

func TestRender_ClusterDomainIsInTheDefaultSNI(t *testing.T) {
	https := annotated(ingressIn("zzz", "secure", "secure.example.com", "/"), "backend-protocol", "HTTPS")
	rendered, _ := renderIngressesWith(t, "k8s.example", https)
	got := parseV2(t, rendered).Hosts["secure.example.com"].Paths["/"].UpstreamTLS
	if got == nil || got.SNI != "app.zzz.svc.k8s.example" {
		t.Errorf("upstream_tls is %+v\n%s", got, rendered)
	}
}

func TestRender_TheAnnotatedSNIWinsOverTheDefault(t *testing.T) {
	https := annotated(ingressIn("zzz", "secure", "secure.example.com", "/"),
		"backend-protocol", "HTTPS", "upstream-tls-sni", "backend.internal", "upstream-tls-verify", "false")
	rendered, events := renderIngressesWith(t, "cluster.local", https)
	got := parseV2(t, rendered).Hosts["secure.example.com"].Paths["/"].UpstreamTLS
	if got == nil || got.SNI != "backend.internal" || got.Verify == nil || *got.Verify {
		t.Errorf("upstream_tls is %+v\n%s", got, rendered)
	}
	if got := eventsOf(events, "InvalidUpstreamTLS"); len(got) != 0 {
		t.Errorf("events: %v", got)
	}
}

// A refused annotation costs its own object a value, never another route.
func TestRender_ABadSNIIsRefusedAndTheRouteKeepsTheDefault(t *testing.T) {
	bad := annotated(ingressIn("zzz", "bad", "bad.example.com", "/"),
		"backend-protocol", "HTTPS", "upstream-tls-sni", "127.1")
	sibling := annotated(ingressIn("zzz", "good", "good.example.com", "/"),
		"backend-protocol", "HTTPS", "upstream-tls-sni", "backend.internal", "upstream-tls-verify", "false")
	rendered, events := renderIngressesWith(t, "cluster.local", bad, sibling)
	doc := parseV2(t, rendered)

	if strings.Contains(rendered, "127.1") {
		t.Errorf("the file names the refused value:\n%s", rendered)
	}
	got := doc.Hosts["bad.example.com"].Paths["/"].UpstreamTLS
	if got == nil || got.SNI != "app.zzz.svc.cluster.local" {
		t.Errorf("the refused route's upstream_tls is %+v, want the default name", got)
	}
	other := doc.Hosts["good.example.com"].Paths["/"].UpstreamTLS
	if other == nil || other.SNI != "backend.internal" || other.Verify == nil || *other.Verify {
		t.Errorf("the sibling's upstream_tls is %+v, want it as annotated", other)
	}
	warnings := eventsOf(events, "InvalidUpstreamTLS")
	if len(warnings) != 1 || !strings.Contains(warnings[0], "Warning") || !strings.Contains(warnings[0], "127.1") {
		t.Errorf("events: %v, want one Warning InvalidUpstreamTLS naming the value", warnings)
	}
}

func TestRender_AGarbageVerifyIsRefusedAndKeepsTheDefault(t *testing.T) {
	ing := annotated(ingressIn("zzz", "v", "v.example.com", "/"), "backend-protocol", "HTTPS", "upstream-tls-verify", "maybe")
	rendered, events := renderIngressesWith(t, "cluster.local", ing)
	got := parseV2(t, rendered).Hosts["v.example.com"].Paths["/"].UpstreamTLS
	if got == nil || got.Verify != nil {
		t.Errorf("upstream_tls is %+v, want no verify", got)
	}
	if w := eventsOf(events, "InvalidUpstreamTLS"); len(w) != 1 {
		t.Errorf("events: %v", w)
	}
}

// olderModeGateway is the objects an older-mode HTTPRoute needs.
func olderModeGateway(rules []gwv1.HTTPRouteRule, annotations map[string]string) []client.Object {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "gw", "zzz"
	gw.Spec.GatewayClassName = "synapse"
	gw.Spec.Listeners = []gwv1.Listener{{Name: "http", Port: 80, Protocol: gwv1.HTTPProtocolType}}
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace, rt.Annotations = "route", "zzz", annotations
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "gw"}}
	rt.Spec.Hostnames = []gwv1.Hostname{"gw.example.com"}
	rt.Spec.Rules = rules
	return []client.Object{gc, gw, rt}
}

func renderOlderModeGateway(t *testing.T, objs []client.Object) (string, *record.FakeRecorder) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "u.yaml")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	events := record.NewFakeRecorder(64)
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", GatewayAPI: true, UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local", Recorder: events}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered), events
}

func TestRender_TheOlderModesHTTPRouteTakesTheServiceNameAsSNI(t *testing.T) {
	https := map[string]string{annPrefix + "backend-protocol": "HTTPS"}
	one := []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{backendRef("", "api")}}}
	two := []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{backendRef("", "api"), backendRef("other", "canary")}}}

	t.Run("one Service gives its name", func(t *testing.T) {
		rendered, events := renderOlderModeGateway(t, olderModeGateway(one, https))
		got := parseV2(t, rendered).Hosts["gw.example.com"].Paths["/"].UpstreamTLS
		if got == nil || got.SNI != "api.zzz.svc.cluster.local" {
			t.Errorf("upstream_tls is %+v\n%s", got, rendered)
		}
		if w := eventsOf(events, "UpstreamTLSSNIAmbiguous"); len(w) != 0 {
			t.Errorf("events: %v", w)
		}
	})
	t.Run("two Services give the first and a warning", func(t *testing.T) {
		rendered, events := renderOlderModeGateway(t, olderModeGateway(two, https))
		got := parseV2(t, rendered).Hosts["gw.example.com"].Paths["/"].UpstreamTLS
		if got == nil || got.SNI != "api.zzz.svc.cluster.local" {
			t.Errorf("upstream_tls is %+v\n%s", got, rendered)
		}
		if w := eventsOf(events, "UpstreamTLSSNIAmbiguous"); len(w) != 1 || !strings.Contains(w[0], "Warning") {
			t.Errorf("events: %v, want one Warning UpstreamTLSSNIAmbiguous", w)
		}
	})
	t.Run("two Services with an sni annotation are not ambiguous", func(t *testing.T) {
		ann := map[string]string{annPrefix + "backend-protocol": "HTTPS", annPrefix + "upstream-tls-sni": "backend.internal"}
		rendered, events := renderOlderModeGateway(t, olderModeGateway(two, ann))
		got := parseV2(t, rendered).Hosts["gw.example.com"].Paths["/"].UpstreamTLS
		if got == nil || got.SNI != "backend.internal" {
			t.Errorf("upstream_tls is %+v\n%s", got, rendered)
		}
		if w := eventsOf(events, "UpstreamTLSSNIAmbiguous"); len(w) != 0 {
			t.Errorf("events: %v", w)
		}
	})
	t.Run("a plain HTTP route says nothing", func(t *testing.T) {
		rendered, _ := renderOlderModeGateway(t, olderModeGateway(one, nil))
		if got := parseV2(t, rendered).Hosts["gw.example.com"].Paths["/"].UpstreamTLS; got != nil {
			t.Errorf("upstream_tls is %+v", got)
		}
	})
	t.Run("a bad sni on an HTTPRoute is refused", func(t *testing.T) {
		ann := map[string]string{annPrefix + "backend-protocol": "HTTPS", annPrefix + "upstream-tls-sni": "a:443"}
		rendered, events := renderOlderModeGateway(t, olderModeGateway(one, ann))
		got := parseV2(t, rendered).Hosts["gw.example.com"].Paths["/"].UpstreamTLS
		if got == nil || got.SNI != "api.zzz.svc.cluster.local" || strings.Contains(rendered, "a:443") {
			t.Errorf("upstream_tls is %+v\n%s", got, rendered)
		}
		if w := eventsOf(events, "InvalidUpstreamTLS"); len(w) != 1 {
			t.Errorf("events: %v", w)
		}
	})
}
