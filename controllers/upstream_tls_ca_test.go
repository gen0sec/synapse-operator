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
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

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

func TestSecretWatch_ChecksTheOldObjectToo(t *testing.T) {
	with := caSecret("default", "ca", "pem")
	without := caSecret("default", "ca", "")
	without.Data = map[string][]byte{"password": []byte("x")}
	p := secretWatch(nil)
	if !p.Update(event.UpdateEvent{ObjectOld: with, ObjectNew: without}) {
		t.Error("a Secret that lost ca.crt is not enqueued")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: without, ObjectNew: with}) {
		t.Error("a Secret that gained ca.crt is not enqueued")
	}
	if p.Update(event.UpdateEvent{ObjectOld: without, ObjectNew: without}) {
		t.Error("a Secret that never held a CA is enqueued")
	}
}

func TestSecretWatch_MatchesTheSecretNamedBySpecWhateverItsKeys(t *testing.T) {
	custom := caSecret("synapse-os", "pki", "")
	custom.Data = map[string][]byte{"root.pem": []byte("x")}
	other := caSecret("synapse-os", "unrelated", "")
	other.Data = map[string][]byte{"root.pem": []byte("x")}
	elsewhere := caSecret("tenant", "pki", "")
	elsewhere.Data = map[string][]byte{"root.pem": []byte("x")}
	p := secretWatch(func(ns, name string) bool { return ns == "synapse-os" && name == "pki" })
	for _, tc := range []struct {
		name string
		obj  client.Object
		want bool
	}{
		{"the referenced Secret", custom, true},
		{"another Secret of the namespace", other, false},
		{"the same name in another namespace", elsewhere, false},
	} {
		if got := p.Create(event.CreateEvent{Object: tc.obj}); got != tc.want {
			t.Errorf("%s: create is %v, want %v", tc.name, got, tc.want)
		}
		if got := p.Delete(event.DeleteEvent{Object: tc.obj}); got != tc.want {
			t.Errorf("%s: delete is %v, want %v", tc.name, got, tc.want)
		}
		if got := p.Update(event.UpdateEvent{ObjectOld: tc.obj, ObjectNew: tc.obj}); got != tc.want {
			t.Errorf("%s: update is %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSecretWatch_TheRendererNamesTheProxyCASecret(t *testing.T) {
	custom := caSecret("synapse-os", "pki", "")
	custom.Data = map[string][]byte{"root.pem": []byte("x")}
	r := &IngressReconciler{DefaultUpstreamTLS: &DefaultUpstreamTLS{
		CASecret: types.NamespacedName{Namespace: "synapse-os", Name: "pki"}, CAKey: "root.pem"}}
	if !r.secretPredicate().Create(event.CreateEvent{Object: custom}) {
		t.Error("the Secret of spec.upstreamTLS.caSecretRef is not watched")
	}
	if (&IngressReconciler{}).secretPredicate().Create(event.CreateEvent{Object: custom}) {
		t.Error("a Secret is watched with no reference to it")
	}
}

func TestHashConfigMapContent_CAFilesAreIgnoredLikeTheRoutes(t *testing.T) {
	ignored := map[string]struct{}{UpstreamsKey: {}}
	cm := func(data map[string]string) *corev1.ConfigMap { return &corev1.ConfigMap{Data: data} }
	base := map[string]string{"synapse.yaml": "a: 1", UpstreamsKey: "x", "ca-0123456789abcdef.pem": "one"}
	rotated := map[string]string{"synapse.yaml": "a: 1", UpstreamsKey: "y", "ca-fedcba9876543210.pem": "two", "ca-0011223344556677.pem": "three"}
	if a, b := hashConfigMapContent(cm(base), ignored, false), hashConfigMapContent(cm(rotated), ignored, false); a != b {
		t.Error("a CA file changes the hash while upstreams.yaml is ignored")
	}
	other := map[string]string{"synapse.yaml": "a: 1", "ca-bundle.pem": "one"}
	more := map[string]string{"synapse.yaml": "a: 1", "ca-bundle.pem": "two"}
	if a, b := hashConfigMapContent(cm(other), ignored, false), hashConfigMapContent(cm(more), ignored, false); a == b {
		t.Error("a key that is no CA file of ours is ignored")
	}
	if a, b := hashConfigMapContent(cm(base), nil, false), hashConfigMapContent(cm(rotated), nil, false); a == b {
		t.Error("CA files are ignored when upstreams.yaml is not")
	}
}

// paddedCA is a CA bundle of about size bytes: one real certificate, then
// comment lines, which PEM parsing skips.
func paddedCA(t *testing.T, name string, size int) string {
	t.Helper()
	ca := testCAPEM(t, name)
	return ca + strings.Repeat("#"+strings.Repeat("-", 62)+"\n", (size-len(ca))/64)
}

func TestRoutes_UpstreamCAIsBudgetedPerNamespace(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	var objs []client.Object
	objs = append(objs, edge, classFor("public", edge))
	var bundles []string
	for i, n := range []string{"a", "b", "c"} {
		ca := paddedCA(t, "big-"+n, 60*1024)
		bundles = append(bundles, ca)
		objs = append(objs,
			httpsIngress("busy", n, n+".example.com", annPrefix+"upstream-tls-ca", "ca-"+n),
			caSecret("busy", "ca-"+n, ca))
		_ = i
	}
	// The same bundle again costs the namespace nothing.
	objs = append(objs, httpsIngress("busy", "a2", "a2.example.com", annPrefix+"upstream-tls-ca", "ca-a"))
	// Another namespace has its own share.
	quiet := paddedCA(t, "quiet", 60*1024)
	objs = append(objs,
		httpsIngress("quiet", "q", "q.example.com", annPrefix+"upstream-tls-ca", "ca-q"),
		caSecret("quiet", "ca-q", quiet))

	events := record.NewFakeRecorder(64)
	r := newRouteReconciler(t, objs...)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)

	doc := parseV2(t, routesConfigMap(t, r, edge).Data[UpstreamsKey])
	caOf := func(host string) string {
		if tls := doc.Hosts[host].Paths["/"].UpstreamTLS; tls != nil {
			return tls.CA
		}
		return ""
	}
	for host, ca := range map[string]string{"a.example.com": bundles[0], "b.example.com": bundles[1], "a2.example.com": bundles[0], "q.example.com": quiet} {
		if want := "/etc/synapse/upstreams/" + caKeyOf(ca); caOf(host) != want {
			t.Errorf("%s has the CA %q, want %q", host, caOf(host), want)
		}
	}
	if got := caOf("c.example.com"); got != "" {
		t.Errorf("c.example.com has the CA %q over the namespace cap", got)
	}
	got := eventsOf(events, "UpstreamCAUnusable")
	if len(got) != 1 || !strings.Contains(got[0], "128 KiB") {
		t.Errorf("UpstreamCAUnusable events: %v, want one that names the 128 KiB cap", got)
	}
}

func TestRoutes_UpstreamCAUnusableSaysWhatTheRouteFallsBackTo(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	events := record.NewFakeRecorder(64)
	r := newRouteReconciler(t, edge, classFor("public", edge),
		httpsIngress("default", "broken", "broken.example.com", annPrefix+"upstream-tls-ca", "missing"))
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)
	got := eventsOf(events, "UpstreamCAUnusable")
	if len(got) != 1 {
		t.Fatalf("UpstreamCAUnusable events: %v", got)
	}
	if strings.Contains(got[0], "the route has no CA") || !strings.Contains(got[0], "upstream_tls.ca") || !strings.Contains(got[0], "system roots") {
		t.Errorf("the event is %q, want it to name the proxy-wide CA and the system roots", got[0])
	}
}

func TestCAPath(t *testing.T) {
	out := filepath.Join(t.TempDir(), "upstreams.yaml")
	cm := types.NamespacedName{Namespace: "ns", Name: "routes"}
	tests := []struct {
		name string
		r    *IngressReconciler
		want string
	}{
		{"configmap, no dir: the SynapseProxy mount", &IngressReconciler{UpstreamsOutConfigMap: cm}, "/etc/synapse/upstreams/ca-1.pem"},
		{"configmap, dir set", &IngressReconciler{UpstreamsOutConfigMap: cm, UpstreamsCADir: "/shared"}, "/shared/ca-1.pem"},
		{"configmap, dir with a trailing slash", &IngressReconciler{UpstreamsOutConfigMap: cm, UpstreamsCADir: "/shared/"}, "/shared/ca-1.pem"},
		{"file output beside the routes file", &IngressReconciler{UpstreamsOutPath: out}, filepath.Join(filepath.Dir(out), "ca-1.pem")},
		{"file output ignores the dir", &IngressReconciler{UpstreamsOutPath: out, UpstreamsCADir: "/shared"}, filepath.Join(filepath.Dir(out), "ca-1.pem")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.caPath("ca-1.pem"); got != tt.want {
				t.Fatalf("caPath = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRoutes_TheProxyWideCAIsLoadedBeforeTheRoutes(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	proxyCA := paddedCA(t, "proxy-wide", 16*1024)
	edge.Spec.UpstreamTLS = &synapsev1alpha1.UpstreamTLSSpec{
		CASecretRef: &synapsev1alpha1.CASecretReference{Name: "internal-ca", Key: "ca.crt"},
	}
	objs := []client.Object{edge, classFor("public", edge), caSecret("synapse-os", "internal-ca", proxyCA)}
	// Together the tenant CAs of the proxy's namespace fill its 128 KiB share
	// on their own, so the proxy-wide CA fits only if it is taken first.
	var bundles []string
	for _, n := range []string{"a", "b"} {
		ca := paddedCA(t, "tenant-"+n, 60*1024)
		bundles = append(bundles, ca)
		objs = append(objs,
			httpsIngress("synapse-os", n, n+".example.com", annPrefix+"upstream-tls-ca", "ca-"+n),
			caSecret("synapse-os", "ca-"+n, ca))
	}

	events := record.NewFakeRecorder(64)
	r := newRouteReconciler(t, objs...)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)

	cm := routesConfigMap(t, r, edge)
	doc := parseV2(t, cm.Data[UpstreamsKey])
	if want := "/etc/synapse/upstreams/" + caKeyOf(proxyCA); doc.UpstreamTLS == nil || doc.UpstreamTLS.CA != want {
		t.Errorf("the proxy-wide upstream_tls is %+v, want the CA %q", doc.UpstreamTLS, want)
	}
	if cm.Data[caKeyOf(proxyCA)] != proxyCA {
		t.Error("the proxy-wide CA file is not in the ConfigMap")
	}
	caOf := func(host string) string {
		if tls := doc.Hosts[host].Paths["/"].UpstreamTLS; tls != nil {
			return tls.CA
		}
		return ""
	}
	if want := "/etc/synapse/upstreams/" + caKeyOf(bundles[0]); caOf("a.example.com") != want {
		t.Errorf("a.example.com has the CA %q, want %q", caOf("a.example.com"), want)
	}
	if got := caOf("b.example.com"); got != "" {
		t.Errorf("b.example.com has the CA %q, want the last tenant CA refused", got)
	}
	got := eventsOf(events, "UpstreamCAUnusable")
	if len(got) != 1 || !strings.Contains(got[0], "128 KiB") || strings.Contains(got[0], "spec.upstreamTLS.caSecretRef") {
		t.Errorf("UpstreamCAUnusable events: %v, want one for the tenant CA that names the 128 KiB cap", got)
	}
}

// objEvents records the events with the namespace/name of their object, which
// FakeRecorder does not say for an object with no type information.
type objEvents struct{ got []string }

func (o *objEvents) Event(obj runtime.Object, etype, reason, msg string) {
	name := ""
	if co, ok := obj.(client.Object); ok {
		name = co.GetNamespace() + "/" + co.GetName()
	}
	o.got = append(o.got, name+" "+reason+" "+msg)
}

func (o *objEvents) Eventf(obj runtime.Object, etype, reason, msgFmt string, args ...any) {
	o.Event(obj, etype, reason, fmt.Sprintf(msgFmt, args...))
}

func (o *objEvents) AnnotatedEventf(obj runtime.Object, _ map[string]string, etype, reason, msgFmt string, args ...any) {
	o.Eventf(obj, etype, reason, msgFmt, args...)
}

func (o *objEvents) reason(reason string) []string {
	var out []string
	for _, e := range o.got {
		if strings.Contains(e, " "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

// padRoutes are ingresses of the namespace pad that carry no CA. Their long
// paths make the routes file big without many objects.
func padRoutes(n int) []client.Object {
	var objs []client.Object
	long := "/" + strings.Repeat("x", 900)
	for i := 0; i < n; i++ {
		ing := routedIngress(fmt.Sprintf("p%04d", i), ptr("public"), fmt.Sprintf("p%04d.example.com", i))
		ing.Namespace = "pad"
		ing.Spec.Rules[0].HTTP.Paths[0].Path = long
		objs = append(objs, ing)
	}
	return objs
}

func TestRoutes_TheConfigMapStaysUnderItsLimitByDroppingTheLastCA(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	objs := append([]client.Object{edge, classFor("public", edge)}, padRoutes(padCount)...)
	bundles := map[string]string{}
	for _, n := range []string{"a", "b", "c"} {
		ca := paddedCA(t, "big-"+n, 60*1024)
		bundles[n] = ca
		objs = append(objs,
			httpsIngress(n, "web", n+".example.com", annPrefix+"upstream-tls-ca", "ca-"+n),
			caSecret(n, "ca-"+n, ca))
	}
	events := &objEvents{}
	r := newRouteReconciler(t, objs...)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)

	cm := routesConfigMap(t, r, edge)
	size := 0
	for k, v := range cm.Data {
		size += len(k) + len(v)
	}
	if size > maxRoutesConfigMapBytes {
		t.Errorf("the ConfigMap holds %d bytes, over the %d budget", size, maxRoutesConfigMapBytes)
	}
	want := []string{caKeyOf(bundles["a"]), caKeyOf(bundles["b"])}
	sort.Strings(want)
	if got := caKeys(cm); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("CA keys are %v, want the first two bundles %v", got, want)
	}
	doc := parseV2(t, cm.Data[UpstreamsKey])
	for host, ca := range map[string]string{"a.example.com": bundles["a"], "b.example.com": bundles["b"]} {
		if tls := doc.Hosts[host].Paths["/"].UpstreamTLS; tls == nil || tls.CA != "/etc/synapse/upstreams/"+caKeyOf(ca) {
			t.Errorf("%s: upstream_tls is %+v, want its CA kept", host, tls)
		}
	}
	if tls := doc.Hosts["c.example.com"].Paths["/"].UpstreamTLS; tls == nil || tls.CA != "" {
		t.Errorf("c.example.com: upstream_tls is %+v, want it kept without a CA", tls)
	}
	got := events.reason("UpstreamCAUnusable")
	if len(got) != 1 || !strings.HasPrefix(got[0], "c/web ") || !strings.Contains(got[0], "the routes ConfigMap is full") {
		t.Errorf("UpstreamCAUnusable events: %v, want one on c/web that says the routes ConfigMap is full", got)
	}
}

func TestRoutes_AFullConfigMapTellsEveryNamespaceThatSharesTheDroppedCA(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	objs := append([]client.Object{edge, classFor("public", edge)}, padRoutes(padCount+40)...)
	first := paddedCA(t, "first", 60*1024)
	shared := paddedCA(t, "shared", 60*1024)
	objs = append(objs,
		httpsIngress("a", "web", "a.example.com", annPrefix+"upstream-tls-ca", "ca"), caSecret("a", "ca", first),
		httpsIngress("b", "web", "b.example.com", annPrefix+"upstream-tls-ca", "ca"), caSecret("b", "ca", shared),
		httpsIngress("c", "web", "c.example.com", annPrefix+"upstream-tls-ca", "ca"), caSecret("c", "ca", shared))
	events := &objEvents{}
	r := newRouteReconciler(t, objs...)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)

	if keys := caKeys(routesConfigMap(t, r, edge)); len(keys) != 1 || keys[0] != caKeyOf(first) {
		t.Errorf("CA keys are %v, want only the first bundle", keys)
	}
	got := strings.Join(events.reason("UpstreamCAUnusable"), "\n")
	for _, owner := range []string{"b/web", "c/web"} {
		if !strings.Contains(got, owner) || strings.Count(got, owner+" UpstreamCAUnusable") != 1 {
			t.Errorf("want one UpstreamCAUnusable on %s, got:\n%s", owner, got)
		}
	}
	if strings.Contains(got, "a/web") {
		t.Errorf("the kept CA's route was told it is unusable:\n%s", got)
	}
}

func TestRoutes_TheProxyWideCAIsDroppedWithoutRoutesToDropFirst(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	edge.Spec.UpstreamTLS = &synapsev1alpha1.UpstreamTLSSpec{
		CASecretRef: &synapsev1alpha1.CASecretReference{Name: "internal-ca", Key: "ca.crt"},
	}
	proxyCA := paddedCA(t, "proxy-wide", 60*1024)
	objs := append([]client.Object{edge, classFor("public", edge), caSecret("synapse-os", "internal-ca", proxyCA)}, padRoutes(padCount+100)...)
	events := &objEvents{}
	r := newRouteReconciler(t, objs...)
	r.Recorder = events
	mustReconcileRoutes(t, r, edge)

	cm := routesConfigMap(t, r, edge)
	if keys := caKeys(cm); len(keys) != 0 {
		t.Errorf("CA keys %v remain", keys)
	}
	if doc := parseV2(t, cm.Data[UpstreamsKey]); doc.UpstreamTLS != nil {
		t.Errorf("the proxy-wide upstream_tls is %+v, want none", doc.UpstreamTLS)
	}
	got := events.reason("UpstreamCAUnusable")
	if len(got) != 1 || !strings.HasPrefix(got[0], "synapse-os/edge ") || !strings.Contains(got[0], "the routes ConfigMap is full") {
		t.Errorf("UpstreamCAUnusable events: %v, want one on the proxy", got)
	}
}

func TestRoutes_RoutesAloneOverTheLimitFailTheRender(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	objs := append([]client.Object{edge, classFor("public", edge)}, padRoutes(1200)...)
	r := newRouteReconciler(t, objs...)
	err := reconcileRoutes(t, r, edge)
	if err == nil || !strings.Contains(err.Error(), "RoutesTooLarge") {
		t.Fatalf("reconcile error is %v, want RoutesTooLarge", err)
	}
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: edge.Namespace, Name: edge.Name + "-upstreams"}
	if err := r.Get(context.Background(), key, &cm); !apierrors.IsNotFound(err) {
		t.Errorf("the ConfigMap was written (get: %v)", err)
	}
}

const padCount = 840
