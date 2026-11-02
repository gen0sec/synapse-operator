package controllers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// testCAPEM is a real, parseable CA certificate. Each name gives another one.
func testCAPEM(t *testing.T, name string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// caSecret is an Opaque Secret that holds a CA under ca.crt.
func caSecret(ns, name, ca string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"ca.crt": []byte(ca)},
	}
}

// caKeyOf is the name Synapse is told of a CA: its content decides it.
func caKeyOf(ca string) string {
	sum := sha256.Sum256([]byte(ca))
	return "ca-" + hex.EncodeToString(sum[:])[:16] + ".pem"
}

func httpsIngress(ns, name, host string, kv ...string) *networkingv1.Ingress {
	ing := routedIngress(name, ptr("public"), host)
	ing.Namespace = ns
	ing.Annotations = map[string]string{annPrefix + "backend-protocol": "HTTPS"}
	for i := 0; i < len(kv); i += 2 {
		ing.Annotations[kv[i]] = kv[i+1]
	}
	return ing
}

func caKeys(cm *corev1.ConfigMap) []string {
	var keys []string
	for k := range cm.Data {
		if strings.HasPrefix(k, "ca-") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func routesConfigMap(t *testing.T, r *SynapseRouteReconciler, proxy *synapsev1alpha1.SynapseProxy) *corev1.ConfigMap {
	t.Helper()
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: proxy.Namespace, Name: proxy.Name + "-upstreams"}
	if err := r.Get(context.Background(), key, &cm); err != nil {
		t.Fatal(err)
	}
	return &cm
}

func TestParseAnnotations_UpstreamTLSCA(t *testing.T) {
	const syn, ngx = annPrefix, nginxPrefix
	cases := []struct {
		name    string
		ann     map[string]string
		ns, sec string
		refused []string
	}{
		{"nothing", map[string]string{}, "", "", nil},
		{"synapse, own namespace", map[string]string{syn + "upstream-tls-ca": "backend-ca"}, "", "backend-ca", nil},
		{"synapse, namespaced", map[string]string{syn + "upstream-tls-ca": "pki/backend-ca"}, "pki", "backend-ca", nil},
		{"nginx", map[string]string{ngx + "proxy-ssl-secret": "pki/backend-ca"}, "pki", "backend-ca", nil},
		{"synapse wins over nginx", map[string]string{syn + "upstream-tls-ca": "a", ngx + "proxy-ssl-secret": "pki/b"}, "", "a", nil},
		{"a name with a slash too many", map[string]string{syn + "upstream-tls-ca": "a/b/c"}, "", "", []string{syn + "upstream-tls-ca"}},
		{"an empty name", map[string]string{ngx + "proxy-ssl-secret": "pki/"}, "", "", []string{ngx + "proxy-ssl-secret"}},
		{"an empty namespace", map[string]string{syn + "upstream-tls-ca": "/a"}, "", "", []string{syn + "upstream-tls-ca"}},
		{"an empty value", map[string]string{syn + "upstream-tls-ca": " "}, "", "", []string{syn + "upstream-tls-ca"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAnnotations(tc.ann).upstreamTLS
			if got.caNS != tc.ns || got.caName != tc.sec {
				t.Errorf("CA is %q/%q, want %q/%q", got.caNS, got.caName, tc.ns, tc.sec)
			}
			if len(got.refused) != len(tc.refused) {
				t.Fatalf("refused is %q, want one entry for each of %q", got.refused, tc.refused)
			}
			for i, key := range tc.refused {
				if !strings.Contains(got.refused[i], key) {
					t.Errorf("refused[%d] is %q, want it to name %q", i, got.refused[i], key)
				}
			}
		})
	}
}

func TestRoutes_UpstreamCAIsProjectedBesideTheRoutesFile(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	ca := testCAPEM(t, "backend")
	want := caKeyOf(ca)
	withCA := func(name, host, secret string) *networkingv1.Ingress {
		return httpsIngress("default", name, host, annPrefix+"upstream-tls-ca", secret)
	}
	one := withCA("one", "one.example.com", "ca-a")
	r := newRouteReconciler(t, edge, classFor("public", edge), one,
		withCA("two", "two.example.com", "ca-b"),
		caSecret("default", "ca-a", ca), caSecret("default", "ca-b", ca))
	mustReconcileRoutes(t, r, edge)

	cm := routesConfigMap(t, r, edge)
	if keys := caKeys(cm); len(keys) != 1 || keys[0] != want {
		t.Fatalf("CA keys are %v, want only %s", keys, want)
	}
	if cm.Data[want] != ca {
		t.Errorf("the CA file differs from the PEM of the Secret:\n%s", cm.Data[want])
	}
	doc := parseV2(t, cm.Data[UpstreamsKey])
	for _, host := range []string{"one.example.com", "two.example.com"} {
		got := doc.Hosts[host].Paths["/"].UpstreamTLS
		if got == nil || got.CA != "/etc/synapse/upstreams/"+want {
			t.Errorf("%s: upstream_tls is %+v", host, got)
		}
	}

	var ing networkingv1.Ingress
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(one), &ing); err != nil {
		t.Fatal(err)
	}
	delete(ing.Annotations, annPrefix+"upstream-tls-ca")
	if err := r.Update(context.Background(), &ing); err != nil {
		t.Fatal(err)
	}
	var two networkingv1.Ingress
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "two"}, &two); err != nil {
		t.Fatal(err)
	}
	delete(two.Annotations, annPrefix+"upstream-tls-ca")
	if err := r.Update(context.Background(), &two); err != nil {
		t.Fatal(err)
	}
	mustReconcileRoutes(t, r, edge)
	if keys := caKeys(routesConfigMap(t, r, edge)); len(keys) != 0 {
		t.Errorf("CA keys %v remain after the annotations went", keys)
	}
}

func TestRoutes_UpstreamCAForbidsOtherNamespaces(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	var read []string
	funcs := interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Secret); ok {
			read = append(read, key.String())
		}
		return c.Get(ctx, key, obj, opts...)
	}}
	events := record.NewFakeRecorder(64)
	r := &SynapseRouteReconciler{
		Client: fake.NewClientBuilder().WithScheme(proxyTestScheme(t)).WithInterceptorFuncs(funcs).
			WithObjects(edge, classFor("public", edge),
				httpsIngress("default", "shop", "shop.example.com", annPrefix+"upstream-tls-ca", "pki/ca"),
				httpsIngress("default", "nginx", "nginx.example.com", nginxPrefix+"proxy-ssl-secret", "pki/ca"),
				caSecret("pki", "ca", testCAPEM(t, "other"))).
			WithStatusSubresource(&networkingv1.Ingress{}).Build(),
		ClusterDomain: "cluster.local", Recorder: events,
	}
	mustReconcileRoutes(t, r, edge)

	for _, k := range read {
		if strings.HasPrefix(k, "pki/") {
			t.Errorf("the Secret %s of another namespace was read", k)
		}
	}
	cm := routesConfigMap(t, r, edge)
	if keys := caKeys(cm); len(keys) != 0 {
		t.Errorf("CA keys %v, want none", keys)
	}
	doc := parseV2(t, cm.Data[UpstreamsKey])
	for _, host := range []string{"shop.example.com", "nginx.example.com"} {
		if got := doc.Hosts[host].Paths["/"].UpstreamTLS; got != nil && got.CA != "" {
			t.Errorf("%s keeps the CA %q", host, got.CA)
		}
	}
	if got := eventsOf(events, "UpstreamCAForbidden"); len(got) != 2 {
		t.Errorf("UpstreamCAForbidden events: %v", got)
	}
}

func TestRoutes_UpstreamCAUnusableIsSaid(t *testing.T) {
	good := testCAPEM(t, "good")
	badCert := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")}))
	noCA := caSecret("default", "no-key", good)
	noCA.Data = map[string][]byte{"other.crt": []byte(good)}
	cases := []struct {
		name string
		objs []client.Object
	}{
		{"a Secret that is not there", nil},
		{"a Secret without ca.crt", []client.Object{noCA}},
		{"a ca.crt that is no PEM", []client.Object{caSecret("default", "bad", "this is not PEM")}},
		{"a PEM block that is no certificate", []client.Object{caSecret("default", "bad", badCert)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secret := "bad"
			if tc.name == "a Secret without ca.crt" {
				secret = "no-key"
			}
			edge := testProxy("synapse-os", "edge")
			events := record.NewFakeRecorder(64)
			objs := append([]client.Object{edge, classFor("public", edge),
				httpsIngress("default", "broken", "broken.example.com", annPrefix+"upstream-tls-ca", secret),
				httpsIngress("default", "fine", "fine.example.com", annPrefix+"upstream-tls-ca", "good"),
				caSecret("default", "good", good)}, tc.objs...)
			r := newRouteReconciler(t, objs...)
			r.Recorder = events
			mustReconcileRoutes(t, r, edge)

			cm := routesConfigMap(t, r, edge)
			doc := parseV2(t, cm.Data[UpstreamsKey])
			if got := doc.Hosts["broken.example.com"].Paths["/"]; got.Upstream == "" && len(got.Upstreams) == 0 {
				t.Errorf("the route of the broken CA is gone:\n%s", cm.Data[UpstreamsKey])
			}
			if got := doc.Hosts["broken.example.com"].Paths["/"].UpstreamTLS; got != nil && got.CA != "" {
				t.Errorf("the broken route names the CA %q", got.CA)
			}
			if got := doc.Hosts["fine.example.com"].Paths["/"].UpstreamTLS; got == nil || got.CA != "/etc/synapse/upstreams/"+caKeyOf(good) {
				t.Errorf("the other route is %+v", got)
			}
			// No file is named that is not in the ConfigMap.
			for _, route := range []string{"broken.example.com", "fine.example.com"} {
				if tls := doc.Hosts[route].Paths["/"].UpstreamTLS; tls != nil && tls.CA != "" {
					if _, ok := cm.Data[strings.TrimPrefix(tls.CA, "/etc/synapse/upstreams/")]; !ok {
						t.Errorf("%s names %s, which the ConfigMap does not hold", route, tls.CA)
					}
				}
			}
			if keys := caKeys(cm); len(keys) != 1 {
				t.Errorf("CA keys %v, want only the good one", keys)
			}
			if got := eventsOf(events, "UpstreamCAUnusable"); len(got) != 1 {
				t.Errorf("UpstreamCAUnusable events: %v", got)
			}
		})
	}
}

func TestRender_TheOlderModesWriteTheCABesideTheFile(t *testing.T) {
	ca := testCAPEM(t, "older")
	dir := t.TempDir()
	out := filepath.Join(dir, "upstreams.yaml")
	for name, body := range map[string]string{
		"ca-0000000000000000.pem": "stale", "ca-keep.crt": "not ours", "notes.txt": "mine",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ing := httpsIngress("zzz", "secure", "secure.example.com", annPrefix+"upstream-tls-ca", "backend-ca")
	ing.Spec.IngressClassName = ptr("synapse")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing, caSecret("zzz", "backend-ca", ca)).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}

	want := caKeyOf(ca)
	if got, err := os.ReadFile(filepath.Join(dir, want)); err != nil || string(got) != ca {
		t.Errorf("CA file %s: %q, %v", want, got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ca-0000000000000000.pem")); !os.IsNotExist(err) {
		t.Errorf("the stale CA file remains: %v", err)
	}
	for _, kept := range []string{"ca-keep.crt", "notes.txt"} {
		if _, err := os.Stat(filepath.Join(dir, kept)); err != nil {
			t.Errorf("%s was touched: %v", kept, err)
		}
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := parseV2(t, string(rendered)).Hosts["secure.example.com"].Paths["/"].UpstreamTLS; got == nil || got.CA != filepath.Join(dir, want) {
		t.Errorf("upstream_tls is %+v", got)
	}

	// The file is there before any routes file that names it.
	t.Run("before the routes file", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "upstreams.yaml")
		if err := os.MkdirAll(filepath.Join(out, "block"), 0o755); err != nil {
			t.Fatal(err)
		}
		r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
			CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local"}
		if _, _, _, err := r.render(context.Background()); err == nil {
			t.Fatal("the routes file could be written over a directory")
		}
		if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
			t.Errorf("the CA file is not there when the routes file fails: %v", err)
		}
	})
}

func TestRenderUpstreamsV2_ProxyLevelUpstreamTLS(t *testing.T) {
	ca := testCAPEM(t, "proxy")
	build := func(spec *synapsev1alpha1.UpstreamTLSSpec, objs ...client.Object) (*SynapseRouteReconciler, *synapsev1alpha1.SynapseProxy, *record.FakeRecorder) {
		edge := testProxy("synapse-os", "edge")
		edge.Spec.UpstreamTLS = spec
		events := record.NewFakeRecorder(64)
		r := newRouteReconciler(t, append([]client.Object{edge, classFor("public", edge)}, objs...)...)
		r.Recorder = events
		mustReconcileRoutes(t, r, edge)
		return r, edge, events
	}

	t.Run("a CA is written and named at the top", func(t *testing.T) {
		r, edge, _ := build(&synapsev1alpha1.UpstreamTLSSpec{CASecretRef: &synapsev1alpha1.CASecretReference{Name: "internal-ca", Key: "ca.crt"}},
			caSecret("synapse-os", "internal-ca", ca))
		cm := routesConfigMap(t, r, edge)
		doc := parseV2(t, cm.Data[UpstreamsKey])
		if doc.UpstreamTLS == nil || doc.UpstreamTLS.CA != "/etc/synapse/upstreams/"+caKeyOf(ca) {
			t.Errorf("upstream_tls is %+v\n%s", doc.UpstreamTLS, cm.Data[UpstreamsKey])
		}
		if cm.Data[caKeyOf(ca)] != ca {
			t.Error("the CA file is not in the ConfigMap")
		}
	})
	t.Run("verify false is written", func(t *testing.T) {
		r, edge, _ := build(&synapsev1alpha1.UpstreamTLSSpec{Verify: ptr(false)})
		doc := parseV2(t, upstreamsOf(t, r, edge))
		if doc.UpstreamTLS == nil || doc.UpstreamTLS.Verify == nil || *doc.UpstreamTLS.Verify {
			t.Errorf("upstream_tls is %+v", doc.UpstreamTLS)
		}
	})
	t.Run("nothing set writes nothing", func(t *testing.T) {
		r, edge, _ := build(nil)
		if doc := parseV2(t, upstreamsOf(t, r, edge)); doc.UpstreamTLS != nil {
			t.Errorf("upstream_tls is %+v", doc.UpstreamTLS)
		}
	})
	t.Run("an Ingress that says verify true still says it", func(t *testing.T) {
		ing := httpsIngress("default", "strict", "strict.example.com", annPrefix+"upstream-tls-verify", "true")
		r, edge, _ := build(&synapsev1alpha1.UpstreamTLSSpec{Verify: ptr(false)}, ing)
		doc := parseV2(t, upstreamsOf(t, r, edge))
		if got := doc.Hosts["strict.example.com"].Paths["/"].UpstreamTLS; got == nil || got.Verify == nil || !*got.Verify {
			t.Errorf("upstream_tls of the route is %+v", got)
		}
		if doc.UpstreamTLS == nil || doc.UpstreamTLS.Verify == nil || *doc.UpstreamTLS.Verify {
			t.Errorf("upstream_tls at the top is %+v", doc.UpstreamTLS)
		}
	})
	t.Run("a CA that cannot be used is said on the proxy", func(t *testing.T) {
		r, edge, events := build(&synapsev1alpha1.UpstreamTLSSpec{Verify: ptr(true), CASecretRef: &synapsev1alpha1.CASecretReference{Name: "missing", Key: "ca.crt"}})
		cm := routesConfigMap(t, r, edge)
		doc := parseV2(t, cm.Data[UpstreamsKey])
		if doc.UpstreamTLS == nil || doc.UpstreamTLS.CA != "" || doc.UpstreamTLS.Verify == nil || !*doc.UpstreamTLS.Verify {
			t.Errorf("upstream_tls is %+v", doc.UpstreamTLS)
		}
		if got := eventsOf(events, "UpstreamCAUnusable"); len(got) != 1 {
			t.Errorf("events: %v", got)
		}
	})
	t.Run("another key of the Secret", func(t *testing.T) {
		s := caSecret("synapse-os", "internal-ca", "")
		s.Data = map[string][]byte{"root.pem": []byte(ca)}
		r, edge, _ := build(&synapsev1alpha1.UpstreamTLSSpec{CASecretRef: &synapsev1alpha1.CASecretReference{Name: "internal-ca", Key: "root.pem"}}, s)
		if doc := parseV2(t, upstreamsOf(t, r, edge)); doc.UpstreamTLS == nil || doc.UpstreamTLS.CA == "" {
			t.Errorf("upstream_tls is %+v", doc.UpstreamTLS)
		}
	})
}

func TestRoutes_AChangedCASecretRerenders(t *testing.T) {
	tls := tlsSecret("default", "site", "crt", "key")
	withCA := caSecret("default", "ca", "pem")
	without := caSecret("default", "other", "")
	without.Data = map[string][]byte{"password": []byte("x")}
	for _, tc := range []struct {
		name string
		obj  client.Object
		want bool
	}{
		{"a TLS Secret", tls, true},
		{"an Opaque Secret with ca.crt", withCA, true},
		{"an Opaque Secret without it", without, false},
		{"not a Secret", &corev1.ConfigMap{}, false},
	} {
		if got := routeSecretWatched(tc.obj); got != tc.want {
			t.Errorf("%s: watched is %v, want %v", tc.name, got, tc.want)
		}
	}
}
