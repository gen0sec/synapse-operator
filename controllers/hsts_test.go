package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const hstsKey = annPrefix

func TestParseHSTS(t *testing.T) {
	const year = "31536000"
	cases := []struct {
		name    string
		ann     map[string]string
		want    *hstsPolicy
		refused []string
	}{
		{name: "nothing set"},
		{name: "off", ann: map[string]string{"hsts": "off"}, want: &hstsPolicy{off: true}},
		{name: "false is off", ann: map[string]string{"hsts": "False"}, want: &hstsPolicy{off: true}},
		{name: "off wins over the rest", ann: map[string]string{"hsts": "off", "hsts-max-age": "600"},
			want: &hstsPolicy{off: true}, refused: []string{"hsts-max-age"}},
		{name: "on is no setting", ann: map[string]string{"hsts": "on", "hsts-max-age": "600"},
			want: &hstsPolicy{maxAge: 600}, refused: []string{"synapse.gen0sec.com/hsts"}},
		{name: "max age", ann: map[string]string{"hsts-max-age": "600"}, want: &hstsPolicy{maxAge: 600}},
		{name: "max age zero clears", ann: map[string]string{"hsts-max-age": "0"}, want: &hstsPolicy{}},
		{name: "max age not a number", ann: map[string]string{"hsts-max-age": "1y"}, refused: []string{"hsts-max-age"}},
		{name: "max age negative", ann: map[string]string{"hsts-max-age": "-1"}, refused: []string{"hsts-max-age"}},
		{name: "max age too big", ann: map[string]string{"hsts-max-age": "99999999999999999999"}, refused: []string{"hsts-max-age"}},
		{name: "subdomains", ann: map[string]string{"hsts-max-age": "600", "hsts-include-subdomains": "true"},
			want: &hstsPolicy{maxAge: 600, includeSubdomains: true}},
		{name: "subdomains false", ann: map[string]string{"hsts-max-age": "600", "hsts-include-subdomains": "false"},
			want: &hstsPolicy{maxAge: 600}},
		{name: "subdomains junk", ann: map[string]string{"hsts-max-age": "600", "hsts-include-subdomains": "yes"},
			want: &hstsPolicy{maxAge: 600}, refused: []string{"hsts-include-subdomains"}},
		{name: "preload", ann: map[string]string{"hsts-max-age": year, "hsts-include-subdomains": "true", "hsts-preload": "true"},
			want: &hstsPolicy{maxAge: 31536000, includeSubdomains: true, preload: true}},
		{name: "preload with a short age is dropped", ann: map[string]string{"hsts-max-age": "31535999", "hsts-include-subdomains": "true", "hsts-preload": "true"},
			want: &hstsPolicy{maxAge: 31535999, includeSubdomains: true}, refused: []string{"hsts-preload"}},
		{name: "preload without subdomains is dropped", ann: map[string]string{"hsts-max-age": year, "hsts-preload": "true"},
			want: &hstsPolicy{maxAge: 31536000}, refused: []string{"hsts-preload"}},
		{name: "preload false needs nothing", ann: map[string]string{"hsts-max-age": "600", "hsts-preload": "false"},
			want: &hstsPolicy{maxAge: 600}},
		{name: "preload junk", ann: map[string]string{"hsts-max-age": year, "hsts-preload": "1"},
			want: &hstsPolicy{maxAge: 31536000}, refused: []string{"hsts-preload"}},
		{name: "flags without a max age name nothing to say", ann: map[string]string{"hsts-include-subdomains": "true", "hsts-preload": "true"},
			refused: []string{"hsts-include-subdomains", "hsts-preload"}},
		{name: "a false flag without a max age is no setting", ann: map[string]string{"hsts-include-subdomains": "false"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ann := map[string]string{}
			for k, v := range tc.ann {
				if !strings.Contains(k, "/") {
					k = annPrefix + k
				}
				ann[k] = v
			}
			got := parseHSTS(ann)
			switch {
			case got.policy == nil && tc.want != nil, got.policy != nil && tc.want == nil,
				got.policy != nil && *got.policy != *tc.want:
				t.Errorf("policy = %+v, want %+v", got.policy, tc.want)
			}
			if len(got.refused) != len(tc.refused) {
				t.Fatalf("refused = %q, want %d entries naming %q", got.refused, len(tc.refused), tc.refused)
			}
			for i, w := range tc.refused {
				if !strings.Contains(got.refused[i], w) {
					t.Errorf("refused[%d] = %q, want it to name %q", i, got.refused[i], w)
				}
			}
		})
	}
}

func TestParseHSTS_NginxKeysAreNotRead(t *testing.T) {
	if got := parseHSTS(map[string]string{nginxPrefix + "hsts-max-age": "600"}); got.policy != nil || len(got.refused) != 0 {
		t.Errorf("got %+v, want the nginx key ignored", got)
	}
}

func hstsIngress(ns, name, host string, kv ...string) client.Object {
	return annotated(ingressIn(ns, name, host, "/"), kv...)
}

func TestRender_HSTSPerHost(t *testing.T) {
	out, events := renderSchema(t, false,
		hstsIngress("default", "a", "a.example.com", "hsts-max-age", "600", "hsts-include-subdomains", "true"),
		hstsIngress("default", "b", "b.example.com", "hsts", "off"),
		hstsIngress("default", "c", "c.example.com"),
	)
	for _, want := range []string{
		"  \"a.example.com\":\n    tls:\n      terminate: {}\n    hsts:\n      max_age: 600\n      include_subdomains: true\n",
		"  \"b.example.com\":\n    tls:\n      terminate: {}\n    hsts: \"off\"\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Count(out, "hsts:") != 2 {
		t.Errorf("the host with no annotation has an hsts block:\n%s", out)
	}
	if got := eventsOf(events, "InvalidHSTS"); len(got) != 0 {
		t.Errorf("unexpected events %q", got)
	}
}

func TestRender_HSTSInvalidDropsOnlyThatSetting(t *testing.T) {
	out, events := renderSchema(t, false,
		hstsIngress("default", "a", "a.example.com", "hsts-max-age", "600", "hsts-include-subdomains", "true", "hsts-preload", "true"))
	if !strings.Contains(out, "      max_age: 600\n      include_subdomains: true\n") || strings.Contains(out, "preload") {
		t.Errorf("want preload dropped and the rest kept:\n%s", out)
	}
	got := eventsOf(events, "InvalidHSTS")
	if len(got) != 1 || !strings.Contains(got[0], "Warning") || !strings.Contains(got[0], "hsts-preload") {
		t.Errorf("events = %q, want one Warning InvalidHSTS naming hsts-preload", got)
	}
}

func TestRender_HSTSSkipsAPassthroughHost(t *testing.T) {
	out, _ := renderSchema(t, false,
		annotated(ingressIn("default", "p", "p.example.com", "/"), "ssl-passthrough", "true", "hsts-max-age", "600"))
	if strings.Contains(out, "hsts") {
		t.Errorf("Synapse refuses hsts on a passthrough host:\n%s", out)
	}
}

func TestRender_HSTSHostConflictFirstWriterWins(t *testing.T) {
	out, events := renderSchema(t, false,
		hstsIngress("aaa", "first", "shop.example.com", "hsts-max-age", "600"),
		// Each has a path of its own: a dropped route claims nothing.
		annotated(ingressIn("aaa", "second", "shop.example.com", "/b"), "hsts-max-age", "1200"),
		annotated(ingressIn("aaa", "same", "shop.example.com", "/c"), "hsts-max-age", "600"),
	)
	if strings.Count(out, "hsts:") != 1 || !strings.Contains(out, "max_age: 600\n") || strings.Contains(out, "1200") {
		t.Errorf("want the first writer's policy only:\n%s", out)
	}
	got := eventsOf(events, "HostSettingConflict")
	if len(got) != 1 || !strings.Contains(got[0], "Warning") || !strings.Contains(got[0], "shop.example.com") {
		t.Errorf("events = %q, want one Warning HostSettingConflict, for the loser with a different policy only", got)
	}
}

func TestRender_HSTSAliasesGetThePolicyToo(t *testing.T) {
	out, _ := renderSchema(t, false,
		annotated(ingressIn("default", "a", "a.example.com", "/"), "hsts-max-age", "600", "server-alias", "www.a.example.com"))
	if strings.Count(out, "max_age: 600") != 2 {
		t.Errorf("want the policy on the host and its alias:\n%s", out)
	}
}

func TestRender_HSTSProxyWide(t *testing.T) {
	r, out, events := hstsReconciler(t, &DefaultHSTS{MaxAgeSeconds: 31536000, IncludeSubdomains: true, Preload: true},
		hstsIngress("default", "a", "a.example.com"))
	_ = r
	want := "hsts:\n  max_age: 31536000\n  include_subdomains: true\n  preload: true\n"
	if !strings.Contains(out, "proxy:\n  fingerprints:\n    forward: false\n"+want) {
		t.Errorf("missing top-level block %q in\n%s", want, out)
	}
	if got := eventsOf(events, "InvalidHSTS"); len(got) != 0 {
		t.Errorf("unexpected events %q", got)
	}
}

func TestRender_HSTSProxyWidePreloadIsPreChecked(t *testing.T) {
	_, out, events := hstsReconciler(t, &DefaultHSTS{MaxAgeSeconds: 600, Preload: true})
	if !strings.Contains(out, "hsts:\n  max_age: 600\n") || strings.Contains(out, "preload") {
		t.Errorf("want preload dropped:\n%s", out)
	}
	if got := eventsOf(events, "InvalidHSTS"); len(got) != 1 {
		t.Errorf("events = %q, want one InvalidHSTS", got)
	}
}

func TestRender_HSTSProxyWideAndPerHost(t *testing.T) {
	_, out, _ := hstsReconciler(t, &DefaultHSTS{MaxAgeSeconds: 600},
		hstsIngress("default", "a", "a.example.com", "hsts", "off"))
	if !strings.Contains(out, "\nhsts:\n  max_age: 600\n") || !strings.Contains(out, "    hsts: \"off\"\n") {
		t.Errorf("want both levels:\n%s", out)
	}
}

func TestRender_NothingSetWritesNoHSTS(t *testing.T) {
	_, out, _ := hstsReconciler(t, nil, hstsIngress("default", "a", "a.example.com"))
	if strings.Contains(out, "hsts") {
		t.Errorf("hsts written with nothing set:\n%s", out)
	}
}

func hstsReconciler(t *testing.T, d *DefaultHSTS, objs ...client.Object) (*IngressReconciler, string, *record.FakeRecorder) {
	t.Helper()
	events := record.NewFakeRecorder(64)
	r := &IngressReconciler{
		Client:           fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(),
		IngressClassName: "synapse", UpstreamsOutPath: filepath.Join(t.TempDir(), "u.yaml"), CertsOutDir: t.TempDir(),
		ClusterDomain: "cluster.local", Recorder: events, DefaultHSTS: d,
		EventsOn: routedIngress("proxy", nil, ""),
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(r.UpstreamsOutPath)
	if err != nil {
		t.Fatal(err)
	}
	return r, string(b), events
}

func TestRenderGateways_HSTSAnnotations(t *testing.T) {
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "gw", "default"
	gw.Spec.GatewayClassName = "synapse"
	route := func(name string, kv map[string]string) *gwv1.HTTPRoute {
		rt := &gwv1.HTTPRoute{}
		rt.Name, rt.Namespace = name, "default"
		rt.Annotations = kv
		rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "gw"}}
		rt.Spec.Hostnames = []gwv1.Hostname{"gw.example.com"}
		rt.Spec.Rules = []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
			BackendObjectReference: gwv1.BackendObjectReference{Name: "app", Port: ptr(gwv1.PortNumber(80))}}}}}}
		return rt
	}
	a := route("a", map[string]string{hstsKey + "hsts-max-age": "600", hstsKey + "hsts-preload": "true"})
	b := route("b", map[string]string{hstsKey + "hsts-max-age": "1200"})
	// A route of its own on the host, or b would be dropped and claim nothing.
	pt, p := gwv1.PathMatchPathPrefix, "/b"
	b.Spec.Rules[0].Matches = []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: &pt, Value: &p}}}
	events := record.NewFakeRecorder(16)
	r := &IngressReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(gc, gw, a, b).
			WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build(),
		ClusterDomain: "cluster.local", GatewayAPI: true, Recorder: events,
	}
	m := newRenderModel()
	r.renderGateways(context.Background(), m)
	out := renderUpstreamsV2(m)
	if !strings.Contains(out, "    hsts:\n      max_age: 600\n") || strings.Contains(out, "preload") || strings.Contains(out, "1200") {
		t.Errorf("want a's policy without preload:\n%s", out)
	}
	var reasons []string
	for len(events.Events) > 0 {
		e := <-events.Events
		for _, reason := range []string{"InvalidHSTS", "HostSettingConflict"} {
			if strings.Contains(e, " "+reason+" ") {
				reasons = append(reasons, reason)
			}
		}
	}
	slices.Sort(reasons)
	if !slices.Equal(reasons, []string{"HostSettingConflict", "InvalidHSTS"}) {
		t.Errorf("events %q, want InvalidHSTS and HostSettingConflict", reasons)
	}
}

// hstsMixed renders Ingresses and HTTPRoutes together, as one Gateway of the
// class serves the routes.
func hstsMixed(t *testing.T, ownership bool, objs ...client.Object) (string, *record.FakeRecorder) {
	t.Helper()
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "gw", "default"
	gw.Spec.GatewayClassName = "synapse"
	events := record.NewFakeRecorder(64)
	out := filepath.Join(t.TempDir(), "u.yaml")
	r := &IngressReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(append([]client.Object{gc, gw}, objs...)...).
			WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build(),
		IngressClassName: "synapse", UpstreamsOutPath: out, CertsOutDir: t.TempDir(),
		ClusterDomain: "cluster.local", GatewayAPI: true, Recorder: events,
		DefaultHSTS: &DefaultHSTS{MaxAgeSeconds: 300},
		EventsOn:    routedIngress("proxy", nil, ""),
	}
	if ownership {
		r.HostOwnership = true
		r.HostClaims = types.NamespacedName{Namespace: "default", Name: "claims"}
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), events
}

func hstsRoute(ns, name, host, path string, kv ...string) *gwv1.HTTPRoute {
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = name, ns
	rt.Annotations = map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		rt.Annotations[hstsKey+kv[i]] = kv[i+1]
	}
	gwNS := gwv1.Namespace("default")
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "gw", Namespace: &gwNS}}
	rt.Spec.Hostnames = []gwv1.Hostname{gwv1.Hostname(host)}
	pt := gwv1.PathMatchPathPrefix
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches: []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: &pt, Value: &path}}},
		BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
			BackendObjectReference: gwv1.BackendObjectReference{Name: "app", Port: ptr(gwv1.PortNumber(80))}}}}}}
	return rt
}

// hostBlock is the part of the routes file that is host's.
func hostBlock(t *testing.T, out, host string) string {
	t.Helper()
	i := strings.Index(out, "\n  \""+host+"\":\n")
	if i < 0 {
		t.Fatalf("no host %s in\n%s", host, out)
	}
	rest := out[i+1:]
	if j := strings.Index(rest[1:], "\n  \""); j >= 0 {
		rest = rest[:j+1]
	}
	return rest
}

func TestRender_HSTSOnlyTheHostsOwnerSetsIt(t *testing.T) {
	for _, ownership := range []bool{false, true} {
		t.Run(fmt.Sprintf("ownership=%v", ownership), func(t *testing.T) {
			out, events := hstsMixed(t, ownership,
				ingressIn("a", "shop", "shop.example.com", "/"),
				hstsRoute("b", "intruder", "shop.example.com", "/other", "hsts", "off"))
			if strings.Contains(hostBlock(t, out, "shop.example.com"), "hsts") {
				t.Errorf("a host of namespace a took the hsts of namespace b:\n%s", out)
			}
			if !strings.Contains(out, "\nhsts:\n  max_age: 300\n") {
				t.Errorf("the proxy-wide hsts is gone:\n%s", out)
			}
			got := eventsOf(events, "HostSettingConflict")
			if len(got) != 1 || !strings.Contains(got[0], "Warning") || !strings.Contains(got[0], "namespace a,") {
				t.Errorf("events = %q, want one Warning HostSettingConflict naming namespace a", got)
			}
		})
	}
}

func TestRender_HSTSDroppedRouteClaimsNothing(t *testing.T) {
	// b's path is a's: the route is dropped, and with it the claim.
	out, events := hstsMixed(t, false,
		ingressIn("a", "shop", "shop.example.com", "/"),
		hstsRoute("b", "intruder", "shop.example.com", "/", "hsts", "off"))
	if strings.Contains(hostBlock(t, out, "shop.example.com"), "hsts") {
		t.Errorf("a dropped route set the hsts of the host:\n%s", out)
	}
	if got := eventsOf(events, "RouteConflict"); len(got) != 1 {
		t.Errorf("events = %q, want the route dropped", got)
	}
}

func TestRender_HSTSDroppedRouteDoesNotTakeTheHostEither(t *testing.T) {
	// The first object in order has no route on the host that is programmed,
	// so the owner is the first that has one.
	missing := ingressIn("a", "broken", "shop.example.com", "/")
	missing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "web"}
	out, _ := hstsMixed(t, false, missing,
		annotated(ingressIn("b", "works", "shop.example.com", "/"), "hsts-max-age", "600"))
	if !strings.Contains(hostBlock(t, out, "shop.example.com"), "max_age: 600") {
		t.Errorf("the working Ingress's policy is not rendered:\n%s", out)
	}
}

func TestRender_HSTSUnresolvedBackendClaimsNothing(t *testing.T) {
	broken := annotated(ingressIn("a", "broken", "shop.example.com", "/"), "hsts-max-age", "100")
	broken.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "web"}
	out, _ := hstsMixed(t, false, broken,
		annotated(ingressIn("a", "works", "shop.example.com", "/api"), "hsts-max-age", "600"))
	block := hostBlock(t, out, "shop.example.com")
	if !strings.Contains(block, "max_age: 600") || strings.Contains(block, "100") {
		t.Errorf("want the working Ingress's policy only:\n%s", out)
	}
}

func TestRender_HSTSSameNamespaceFirstWriterWinsAcrossKinds(t *testing.T) {
	out, events := hstsMixed(t, false,
		annotated(ingressIn("a", "shop", "shop.example.com", "/"), "hsts-max-age", "600"),
		hstsRoute("a", "later", "shop.example.com", "/other", "hsts-max-age", "1200"))
	block := hostBlock(t, out, "shop.example.com")
	if !strings.Contains(block, "max_age: 600") || strings.Contains(block, "1200") {
		t.Errorf("want the first writer's policy:\n%s", out)
	}
	if got := eventsOf(events, "HostSettingConflict"); len(got) != 1 {
		t.Errorf("events = %q, want one HostSettingConflict", got)
	}
}
