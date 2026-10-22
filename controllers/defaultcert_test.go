package controllers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var t0 = time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)

func clockAt(t *time.Time) func() time.Time { return func() time.Time { return *t } }

func parseDefaultCert(t *testing.T, crt, key []byte) *x509.Certificate {
	t.Helper()
	if _, err := tls.X509KeyPair(crt, key); err != nil {
		t.Fatalf("crt and key are not a pair: %v", err)
	}
	blk, _ := pem.Decode(crt)
	if blk == nil {
		t.Fatalf("crt is not PEM: %q", crt)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readDefault(t *testing.T, dir string) (crt, key []byte) {
	t.Helper()
	crt, err := os.ReadFile(filepath.Join(dir, defaultCertStem+".crt"))
	if err != nil {
		t.Fatalf("default certificate missing: %v", err)
	}
	key, err = os.ReadFile(filepath.Join(dir, defaultCertStem+".key"))
	if err != nil {
		t.Fatalf("default key missing: %v", err)
	}
	return crt, key
}

func TestDefaultCert_StemSurvivesTheNamingRules(t *testing.T) {
	if got := sanitizeStem(defaultCertStem); got != defaultCertStem {
		t.Fatalf("sanitizeStem changes the stem: %q", got)
	}
	for _, ext := range []string{".crt", ".key"} {
		if p := validation.IsConfigMapKey(defaultCertStem + ext); len(p) > 0 {
			t.Fatalf("%s is not a Secret key: %v", defaultCertStem+ext, p)
		}
	}
	// A tenant's host cannot take the stem either.
	if s, _ := certStem("_default", "ns", "sec"); s == defaultCertStem {
		t.Fatalf("a host took the default certificate's stem")
	}
}

func TestProjectCerts_GeneratesTheDefaultCertificateOnceAndReusesIt(t *testing.T) {
	dir := t.TempDir()
	now := t0
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	r := &IngressReconciler{Client: c, CertsOutDir: dir, Now: clockAt(&now)}

	ch, n, err := r.projectCerts(context.Background(), newRenderModel())
	if err != nil || !ch || n != 0 {
		t.Fatalf("first render: ch=%v n=%d err=%v (the default is not a tenant certificate)", ch, n, err)
	}
	crt, key := readDefault(t, dir)
	cert := parseDefaultCert(t, crt, key)
	if cert.Subject.CommonName != "synapse-default" {
		t.Errorf("CN=%q", cert.Subject.CommonName)
	}
	if len(cert.DNSNames)+len(cert.IPAddresses)+len(cert.EmailAddresses)+len(cert.URIs) != 0 {
		t.Errorf("the default certificate has SANs: %v", cert.DNSNames)
	}
	if pk, ok := cert.PublicKey.(*ecdsa.PublicKey); !ok || pk.Curve != elliptic.P256() {
		t.Errorf("public key is not ECDSA P-256: %T", cert.PublicKey)
	}
	if cert.NotBefore.After(now) {
		t.Errorf("NotBefore=%v is after now", cert.NotBefore)
	}
	if got := cert.NotAfter.Sub(now); got < 364*24*time.Hour || got > 366*24*time.Hour {
		t.Errorf("validity is %v, want one year", got)
	}
	if st, _ := os.Stat(filepath.Join(dir, defaultCertStem+".key")); st.Mode().Perm() != 0o600 {
		t.Errorf("key mode=%v", st.Mode().Perm())
	}

	now = now.Add(24 * time.Hour)
	ch, _, err = r.projectCerts(context.Background(), newRenderModel())
	if err != nil || ch {
		t.Fatalf("second render: ch=%v err=%v (an unchanged default must not be rewritten)", ch, err)
	}
	crt2, key2 := readDefault(t, dir)
	if string(crt2) != string(crt) || string(key2) != string(key) {
		t.Fatal("the default certificate was regenerated while valid")
	}
}

func TestProjectCerts_RegeneratesTheDefaultCertificateOnlyWhenItMust(t *testing.T) {
	good := func(t *testing.T, dir string, now time.Time) (crt, key []byte) {
		r := &IngressReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
			CertsOutDir: dir, Now: func() time.Time { return now }}
		if _, _, err := r.projectCerts(context.Background(), newRenderModel()); err != nil {
			t.Fatal(err)
		}
		return readDefault(t, dir)
	}
	cases := []struct {
		name   string
		later  time.Duration
		break_ func(dir string)
		regen  bool
	}{
		{"29 days to expiry", 365*24*time.Hour - 29*24*time.Hour, nil, true},
		{"31 days to expiry", 365*24*time.Hour - 31*24*time.Hour, nil, false},
		{"expired", 400 * 24 * time.Hour, nil, true},
		{"unparseable crt", time.Hour, func(d string) {
			_ = os.WriteFile(filepath.Join(d, defaultCertStem+".crt"), []byte("junk"), 0o644)
		}, true},
		{"missing key", time.Hour, func(d string) { _ = os.Remove(filepath.Join(d, defaultCertStem+".key")) }, true},
		{"missing crt", time.Hour, func(d string) { _ = os.Remove(filepath.Join(d, defaultCertStem+".crt")) }, true},
		{"key of another certificate", time.Hour, func(d string) {
			other := t.TempDir()
			_, k := good(t, other, t0)
			_ = os.WriteFile(filepath.Join(d, defaultCertStem+".key"), k, 0o600)
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			crt, _ := good(t, dir, t0)
			if tc.break_ != nil {
				tc.break_(dir)
			}
			crt2, key2 := good(t, dir, t0.Add(tc.later))
			parseDefaultCert(t, crt2, key2)
			if regen := string(crt2) != string(crt); regen != tc.regen {
				t.Fatalf("regenerated=%v, want %v", regen, tc.regen)
			}
		})
	}
}

func TestProjectCerts_PruneKeepsTheDefaultCertificate(t *testing.T) {
	dir := t.TempDir()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("a", "b", "CRT", "KEY")).Build()
	r := &IngressReconciler{Client: c, CertsOutDir: dir, Now: func() time.Time { return t0 }}
	m := newRenderModel()
	m.addCert("x.example.com", "x.example.com", "a", "b", nil)
	if _, n, err := r.projectCerts(context.Background(), m); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, _, err := r.projectCerts(context.Background(), newRenderModel()); err != nil {
		t.Fatal(err)
	}
	readDefault(t, dir)
	if _, err := os.Stat(filepath.Join(dir, "x.example.com.crt")); !os.IsNotExist(err) {
		t.Fatal("the tenant certificate must be pruned")
	}
}

func TestProjectCertsToSecret_ReplaceKeepsTheDefaultCertificate(t *testing.T) {
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("a", "b", "CRT", "KEY")).Build()
	now := t0
	r := &IngressReconciler{Client: c, CertsOutSecret: out, Now: clockAt(&now)}
	get := func() corev1.Secret {
		var s corev1.Secret
		if err := c.Get(context.Background(), out, &s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	m := newRenderModel()
	m.addCert("x.example.com", "x.example.com", "a", "b", nil)
	if _, n, err := r.projectCertsToSecret(context.Background(), m); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v (the default is not a tenant certificate)", n, err)
	}
	first := get()
	cert := parseDefaultCert(t, first.Data[defaultCertStem+".crt"], first.Data[defaultCertStem+".key"])
	if cert.Subject.CommonName != "synapse-default" {
		t.Fatalf("CN=%q", cert.Subject.CommonName)
	}
	if string(first.Data["x.example.com.crt"]) != "CRT" {
		t.Fatalf("tenant certificate missing: %v", first.Data)
	}

	// The tenant goes; the Data is replaced; the default stays, unchanged.
	now = now.Add(48 * time.Hour)
	ch, _, err := r.projectCertsToSecret(context.Background(), newRenderModel())
	if err != nil || !ch {
		t.Fatalf("ch=%v err=%v", ch, err)
	}
	second := get()
	if len(second.Data) != 2 ||
		string(second.Data[defaultCertStem+".crt"]) != string(first.Data[defaultCertStem+".crt"]) ||
		string(second.Data[defaultCertStem+".key"]) != string(first.Data[defaultCertStem+".key"]) {
		t.Fatalf("Data after the replace: %d keys, default changed or lost", len(second.Data))
	}
	if ch, _, _ := r.projectCertsToSecret(context.Background(), newRenderModel()); ch {
		t.Fatal("an unchanged render must report no change")
	}

	// Near expiry it is replaced.
	now = t0.Add(340 * 24 * time.Hour)
	if _, _, err := r.projectCertsToSecret(context.Background(), newRenderModel()); err != nil {
		t.Fatal(err)
	}
	third := get()
	if string(third.Data[defaultCertStem+".crt"]) == string(first.Data[defaultCertStem+".crt"]) {
		t.Fatal("a default certificate 25 days from expiry was kept")
	}
	parseDefaultCert(t, third.Data[defaultCertStem+".crt"], third.Data[defaultCertStem+".key"])
}

func TestProjectCertsToSecret_UnparseableDefaultIsReplaced(t *testing.T) {
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	old := &corev1.Secret{}
	old.Namespace, old.Name = out.Namespace, out.Name
	old.Labels = map[string]string{ingressCertsManagedLabel: "true"}
	old.Data = map[string][]byte{defaultCertStem + ".crt": []byte("junk"), defaultCertStem + ".key": []byte("junk")}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(old).Build()
	r := &IngressReconciler{Client: c, CertsOutSecret: out, Now: func() time.Time { return t0 }}
	if _, _, err := r.projectCertsToSecret(context.Background(), newRenderModel()); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	_ = c.Get(context.Background(), out, &got)
	parseDefaultCert(t, got.Data[defaultCertStem+".crt"], got.Data[defaultCertStem+".key"])
}

// With tenant certificates present, the stored stems are theirs plus the
// operator's own, and the default config names the operator's.
func TestDefaultCert_NoTenantCertificateIsTheDefault(t *testing.T) {
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("a", "b", "CRT", "KEY"), tlsSecret("c", "d", "CRT2", "KEY2")).Build()
	r := &IngressReconciler{Client: c, CertsOutSecret: out, Now: func() time.Time { return t0 }}
	m := newRenderModel()
	m.addCert("", "a_b", "a", "b", nil)
	m.addCert("", "c_d", "c", "d", nil)
	if _, n, err := r.projectCertsToSecret(context.Background(), m); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	var got corev1.Secret
	_ = c.Get(context.Background(), out, &got)
	keys := map[string]bool{}
	for k := range got.Data {
		keys[k] = true
	}
	for _, k := range []string{"a_b.crt", "a_b.key", "c_d.crt", "c_d.key", "_default.crt", "_default.key"} {
		if !keys[k] {
			t.Errorf("missing %s: %v", k, keys)
		}
	}
	if len(keys) != 6 {
		t.Errorf("unexpected keys: %v", keys)
	}

	cfg := string(renderOK(t, ProxyConfigInput{Proxy: proxyForConfig()}).YAML)
	if want := "default_certificate: " + defaultCertStem + "\n"; !contains(cfg, want) {
		t.Errorf("the default config does not name %q:\n%s", defaultCertStem, cfg)
	}
}

func TestRenderProxyConfig_SpecConfigOverridesTheDefaultCertificate(t *testing.T) {
	cfg := string(renderOK(t, ProxyConfigInput{Proxy: proxyForConfig(rawConfig(
		`{"proxy": {"default_certificate": "a_b"}}`))}).YAML)
	if !contains(cfg, "default_certificate: a_b\n") || contains(cfg, defaultCertStem) {
		t.Errorf("the override did not win:\n%s", cfg)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
