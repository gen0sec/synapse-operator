package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ingressIn is routedIngress in a namespace, with the given path.
func ingressIn(ns, name, host, path string) *networkingv1.Ingress {
	ing := routedIngress(name, ptr("synapse"), host)
	ing.Namespace = ns
	ing.Spec.Rules[0].HTTP.Paths[0].Path = path
	return ing
}

func annotated(ing *networkingv1.Ingress, kv ...string) *networkingv1.Ingress {
	ing.Annotations = map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		ing.Annotations["synapse.gen0sec.com/"+kv[i]] = kv[i+1]
	}
	return ing
}

// olderModeObjects is what the golden file is rendered from: namespaces
// that share a host, a wildcard, a regex path, server-alias, a solver, a
// host-bound certificate and a Gateway. It has no certificate without a
// host, whose file name is not settled.
func olderModeObjects() []client.Object {
	shop := ingressIn("zzz", "shop", "shop.example.com", "/")
	shop.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"shop.example.com"}, SecretName: "shop-tls"}}
	annotated(shop, "server-alias", "www.shop.example.com")
	api := annotated(ingressIn("zzz", "api", "api.example.com", "/v[0-9]+/.*"), "use-regex", "true")
	solver := ingressIn("zzz", "cm-acme-http-solver-1", "shop.example.com", "/.well-known/acme-challenge/tok")
	guest := ingressIn("aaa", "guest", "shop.example.com", "/guest")
	guest.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"shop.example.com"}, SecretName: "guest-tls"}}
	wild := ingressIn("aaa", "wild", "*.wild.example.com", "/")

	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "gw", "zzz"
	gw.Spec.GatewayClassName = "synapse"
	gw.Spec.Listeners = []gwv1.Listener{{Name: "http", Port: 80, Protocol: gwv1.HTTPProtocolType}}
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "route", "zzz"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "gw"}}
	rt.Spec.Hostnames = []gwv1.Hostname{"gw.example.com"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
		BackendObjectReference: gwv1.BackendObjectReference{Name: "gwapp", Port: ptr(gwv1.PortNumber(8080))}}}}}}

	return []client.Object{
		shop, api, solver, guest, wild, gc, gw, rt,
		tlsSecret("zzz", "shop-tls", "CRT-shop", "KEY-shop"),
		tlsSecret("aaa", "guest-tls", "CRT-guest", "KEY-guest"),
	}
}

// renderOlderMode renders objs the way --ingress-mode --gateway-api does and
// returns the routes file and a sorted listing of the certificates dir.
func renderOlderMode(t *testing.T, objs []client.Object) string {
	t.Helper()
	dir := t.TempDir()
	r := &IngressReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
			WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build(),
		IngressClassName: "synapse", ClusterDomain: "cluster.local", GatewayAPI: true,
		UpstreamsOutPath: filepath.Join(t.TempDir(), "upstreams.yaml"), CertsOutDir: dir,
	}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	routes, err := os.ReadFile(r.UpstreamsOutPath)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, fmt.Sprintf("%s: %s\n", e.Name(), body))
	}
	sort.Strings(files)
	return string(routes) + "# certificates\n" + strings.Join(files, "")
}

// The older modes' output is pinned: a change to it is a change of behaviour
// for everyone who runs them, and has to be made on purpose.
func TestOlderModes_OutputIsPinned(t *testing.T) {
	passthrough := annotated(ingressIn("zzz", "raw", "raw.example.com", "/"), "ssl-passthrough", "true")
	got := "# v2: no passthrough host\n" + renderOlderMode(t, olderModeObjects()) +
		"# v2: a passthrough host\n" + renderOlderMode(t, append(olderModeObjects(), passthrough))

	path := filepath.Join("testdata", "render", "older_mode.yaml")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update-golden to create it)", err)
	}
	if got != string(want) {
		t.Errorf("output differs from %s (run with -update-golden if the change is intended)\n--- got ---\n%s--- want ---\n%s", path, got, want)
	}
}
