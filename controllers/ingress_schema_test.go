package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// renderSchema renders objs the way --ingress-mode does, with host ownership
// on or off, and returns the routes file and the events.
func renderSchema(t *testing.T, ownership bool, objs ...client.Object) (string, *record.FakeRecorder) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "u.yaml")
	events := record.NewFakeRecorder(64)
	r := &IngressReconciler{
		Client:           fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(),
		IngressClassName: "synapse", UpstreamsOutPath: out, CertsOutDir: t.TempDir(),
		ClusterDomain: "cluster.local", Recorder: events,
	}
	if ownership {
		r.HostOwnership = true
		r.HostClaims = types.NamespacedName{Namespace: "default", Name: "claims"}
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), events
}

// Whatever is in the objects, the routes file is a v2 file.
func TestRender_EveryModeWritesAV2File(t *testing.T) {
	plain := func() client.Object { return routedIngress("plain", ptr("synapse"), "plain.example.com") }
	pass := func() client.Object {
		return annotated(routedIngress("pass", ptr("synapse"), "pass.example.com"), "ssl-passthrough", "true")
	}
	cases := []struct {
		name string
		objs []client.Object
	}{
		{"no passthrough", []client.Object{plain()}},
		{"passthrough", []client.Object{plain(), pass()}},
		{"no objects", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rendered, _ := renderSchema(t, false, tc.objs...)
			parseV2(t, rendered)
			if strings.Contains(rendered, "\nupstreams:") {
				t.Errorf("a v1 section in:\n%s", rendered)
			}
		})
	}

	t.Run("GatewayAPI", func(t *testing.T) {
		got := renderOlderMode(t, olderModeObjects())
		file := got[:strings.Index(got, "# certificates\n")]
		parseV2(t, file)
		if strings.Contains(file, "\nupstreams:") {
			t.Errorf("a v1 section in:\n%s", file)
		}
	})

	t.Run("ConfigMap output", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(plain()).Build()
		r := &IngressReconciler{Client: c, IngressClassName: "synapse", ClusterDomain: "cluster.local",
			UpstreamsOutConfigMap: types.NamespacedName{Namespace: "ns", Name: "out"}}
		if _, _, _, err := r.render(context.Background()); err != nil {
			t.Fatal(err)
		}
		var cm corev1.ConfigMap
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "out"}, &cm); err != nil {
			t.Fatal(err)
		}
		parseV2(t, cm.Data[UpstreamsKey])
	})

	t.Run("RenderOnce", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "u.yaml")
		r := &IngressReconciler{
			Client:           fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(plain()).Build(),
			IngressClassName: "synapse", UpstreamsOutPath: out, CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local",
		}
		if err := r.RenderOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		parseV2(t, string(b))
	})
}

// The v2 file says what the v1 file said, since the annotations mean what
// they meant.
func TestRender_TheOlderModesSayWhatTheV1FileSaid(t *testing.T) {
	probed := routedIngress("probed", ptr("synapse"), "probed.example.com")
	unprobed := annotated(routedIngress("unprobed", ptr("synapse"), "unprobed.example.com"), "healthcheck", "false")
	rendered, _ := renderSchema(t, false, probed, unprobed)
	doc := parseV2(t, rendered)

	if doc.Timeouts.Read == nil || *doc.Timeouts.Read != 120 {
		t.Errorf("timeouts.read = %v, want 120\n%s", doc.Timeouts.Read, rendered)
	}
	if f := doc.Proxy.Fingerprints.Forward; f == nil || *f {
		t.Errorf("proxy.fingerprints.forward = %v, want false\n%s", f, rendered)
	}
	for host, want := range map[string]string{"probed.example.com": "", "unprobed.example.com": "none"} {
		route := doc.Hosts[host].Paths["/"]
		// A backend that is never probed says nothing of TLS: a v2 file
		// that does beside `type: none` is refused.
		if want == "" && (route.SSLEnabled == nil || *route.SSLEnabled) {
			t.Errorf("%s: ssl_enabled = %v, want false\n%s", host, route.SSLEnabled, rendered)
		}
		got := ""
		if route.HealthCheck != nil {
			got = route.HealthCheck.Type
		}
		if got != want {
			t.Errorf("%s: health_check type = %q, want %q\n%s", host, got, want, rendered)
		}
	}
}

// A bare `*` is no host Synapse's v2 file takes. It is left out whatever else
// is in the render.
func TestRender_ABareStarAliasIsRefusedInEveryMode(t *testing.T) {
	for _, ownership := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			t.Run(fmt.Sprintf("ownership=%v/passthrough=%v", ownership, passthrough), func(t *testing.T) {
				ing := annotated(routedIngress("aliased", ptr("synapse"), "www.example.com"), "server-alias", "*, ok.example.com")
				objs := []client.Object{ing}
				if passthrough {
					objs = append(objs, annotated(routedIngress("pass", ptr("synapse"), "tls.example.com"), "ssl-passthrough", "true"))
				}
				rendered, events := renderSchema(t, ownership, objs...)
				hosts := parseV2(t, rendered).Hosts
				if _, ok := hosts["*"]; ok {
					t.Errorf("the file has the host *\n%s", rendered)
				}
				if _, ok := hosts["ok.example.com"]; !ok {
					t.Errorf("ok.example.com is not served\n%s", rendered)
				}
				if got := eventsOf(events, "InvalidServerAlias"); len(got) != 1 {
					t.Errorf("events = %q, want one InvalidServerAlias", got)
				}
			})
		}
	}
}

// A use-regex path is written as Synapse's engine will read it, or not at all.
func TestRender_RegexPathsAreCanonicalisedInEveryMode(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprintf("passthrough=%v", passthrough), func(t *testing.T) {
			good := annotated(ingressIn("default", "good", "good.example.com", "/v[0-9]+/.*"), "use-regex", "true")
			bad := annotated(ingressIn("default", "bad", "bad.example.com", "/("), "use-regex", "true")
			objs := []client.Object{good, bad}
			if passthrough {
				objs = append(objs, annotated(routedIngress("pass", ptr("synapse"), "tls.example.com"), "ssl-passthrough", "true"))
			}
			rendered, events := renderSchema(t, false, objs...)
			doc := parseV2(t, rendered)
			if _, ok := doc.Hosts["good.example.com"].Paths["expr:(?-s:/v[0-9]+/.*)"]; !ok {
				t.Errorf("no canonical regex path on good.example.com\n%s", rendered)
			}
			for path := range doc.Hosts["bad.example.com"].Paths {
				t.Errorf("bad.example.com has the path %q\n%s", path, rendered)
			}
			if got := eventsOf(events, "UnsupportedMatch"); len(got) != 1 {
				t.Errorf("events = %q, want one UnsupportedMatch", got)
			}
		})
	}
}

// An HTTPRoute RegularExpression path is held to the same rule.
func TestRender_RegexPathsAreCanonicalisedInEveryMode_GatewayAPI(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "gw", "default"
	gw.Spec.GatewayClassName = "synapse"
	gw.Spec.Listeners = []gwv1.Listener{{Name: "http", Port: 80, Protocol: gwv1.HTTPProtocolType}}
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "route", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "gw"}}
	rt.Spec.Hostnames = []gwv1.Hostname{"gw.example.com"}
	backend := []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
		BackendObjectReference: gwv1.BackendObjectReference{Name: "gwapp", Port: ptr(gwv1.PortNumber(8080))}}}}
	re := gwv1.PathMatchRegularExpression
	for _, v := range []string{"/v[0-9]+/.*", "/("} {
		rt.Spec.Rules = append(rt.Spec.Rules, gwv1.HTTPRouteRule{
			Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: &re, Value: ptr(v)}}},
			BackendRefs: backend,
		})
	}
	events := record.NewFakeRecorder(64)
	out := filepath.Join(t.TempDir(), "u.yaml")
	r := &IngressReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(gc, gw, rt).
			WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build(),
		IngressClassName: "synapse", ClusterDomain: "cluster.local", GatewayAPI: true,
		UpstreamsOutPath: out, CertsOutDir: t.TempDir(), Recorder: events,
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	paths := parseV2(t, string(b)).Hosts["gw.example.com"].Paths
	if _, ok := paths["expr:(?-s:/v[0-9]+/.*)"]; !ok || len(paths) != 1 {
		t.Errorf("paths = %v, want only the canonical regex\n%s", paths, b)
	}
	if got := eventsOf(events, "UnsupportedMatch"); len(got) != 1 {
		t.Errorf("events = %q, want one UnsupportedMatch", got)
	}
}

// An alias a v2 file refuses is left out without a passthrough host too.
func TestRender_AliasesV2RefusesAreLeftOutInEveryMode(t *testing.T) {
	ing := annotated(routedIngress("aliased", ptr("synapse"), "www.example.com"), "server-alias", "www.*.example.com, ok.example.com")
	rendered, events := renderSchema(t, false, ing)
	hosts := parseV2(t, rendered).Hosts
	if _, ok := hosts["www.*.example.com"]; ok {
		t.Errorf("the file has the host www.*.example.com\n%s", rendered)
	}
	if _, ok := hosts["ok.example.com"]; !ok {
		t.Errorf("ok.example.com is not served\n%s", rendered)
	}
	if got := eventsOf(events, "InvalidServerAlias"); len(got) != 1 {
		t.Errorf("events = %q, want one InvalidServerAlias", got)
	}
}
