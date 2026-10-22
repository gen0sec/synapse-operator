package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestCertStem(t *testing.T) {
	if s, b := certStem("foo.example.com", "ns", "sec"); s != "foo.example.com" || !b {
		t.Fatalf("concrete host ⇒ host-bound: %q %v", s, b)
	}
	if s, b := certStem("*.example.com", "ns", "sec"); b || s != "ns_sec" {
		t.Fatalf("wildcard ⇒ secret-derived, not host-bound: %q %v", s, b)
	}
	if s, b := certStem("", "ns", "sec"); b || s != "ns_sec" {
		t.Fatalf("no host ⇒ secret-derived, not host-bound: %q %v", s, b)
	}
	if s, _ := certStem("a/b c*d", "n", "s"); s != "a_b_c_d" {
		t.Fatalf("sanitize odd chars: %q", s)
	}
	// "_" cannot appear in a namespace or Secret name, so the separator
	// keeps ns "a" + Secret "b-c" apart from ns "a-b" + Secret "c", which
	// "-" joined into the same stem.
	s1, _ := certStem("", "a", "b-c")
	s2, _ := certStem("", "a-b", "c")
	if s1 == s2 || s1 != "a_b-c" || s2 != "a-b_c" {
		t.Fatalf("hostless stems must not collide: %q vs %q", s1, s2)
	}
}

// A hostless cert written under the old "ns-secret" name is replaced by
// the "ns_secret" name on the first render after an upgrade.
func TestProjectCerts_UpgradeRenamesHostlessStem(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"a-b-c.crt", "a-b-c.key"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("a", "b-c", "CRT", "KEY")).Build()
	r := &IngressReconciler{Client: c, CertsOutDir: dir}
	stem, _ := certStem("", "a", "b-c")
	m := newRenderModel()
	m.addCert("", stem, "a", "b-c", nil)
	if _, n, err := r.projectCerts(context.Background(), m); err != nil || n != 1 {
		t.Fatalf("project: n=%d err=%v", n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a_b-c.crt")); string(b) != "CRT" {
		t.Fatalf("new crt=%q", b)
	}
	for _, n := range []string{"a-b-c.crt", "a-b-c.key"} {
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Fatalf("old %s must be pruned: %v", n, err)
		}
	}
}

func TestProjectCertsToSecret_UpgradeDropsOldHostlessKey(t *testing.T) {
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	old := &corev1.Secret{}
	old.Namespace, old.Name = out.Namespace, out.Name
	old.Labels = map[string]string{ingressCertsManagedLabel: "true"}
	old.Data = map[string][]byte{"a-b-c.crt": []byte("old"), "a-b-c.key": []byte("old")}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(old, tlsSecret("a", "b-c", "CRT", "KEY")).Build()
	r := &IngressReconciler{Client: c, CertsOutSecret: out}
	stem, _ := certStem("", "a", "b-c")
	m := newRenderModel()
	m.addCert("", stem, "a", "b-c", nil)
	if _, _, err := r.projectCertsToSecret(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), out, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data["a_b-c.crt"]) != "CRT" {
		t.Fatalf("new key missing: %v", got.Data)
	}
	if _, ok := got.Data["a-b-c.crt"]; ok {
		t.Fatalf("old key must be gone after one render: %v", got.Data)
	}
}

func TestAddCert_FirstWriterWins(t *testing.T) {
	m := newRenderModel()
	m.addCert("h.example.com", "h.example.com", "ns", "sec1", nil)
	m.addCert("h.example.com", "h.example.com", "ns", "sec2", nil) // dup host+stem
	m.addCert("", "ns-w", "ns", "wild", nil)                       // no host binding
	if cp := m.certProjections["h.example.com"]; cp.name != "sec1" {
		t.Fatalf("first writer per stem must win: %+v", cp)
	}
	if m.hostCert["h.example.com"] != "h.example.com" {
		t.Fatalf("host binding: %v", m.hostCert)
	}
	if _, ok := m.certProjections["ns-w"]; !ok {
		t.Fatal("no-host cert still projected")
	}
	if len(m.hostCert) != 1 {
		t.Fatalf("host=\"\" must not create a hostCert entry: %v", m.hostCert)
	}
	// invalid inputs are ignored.
	m.addCert("x", "", "ns", "s", nil)
	if _, ok := m.hostCert["x"]; ok {
		t.Fatal("empty stem must be ignored")
	}
}

func TestRenderUpstreams_PerHostCertificate(t *testing.T) {
	m := newRenderModel()
	m.addRoute("tls.example.com", "/", []backend{{addr: "b:80"}}, annSettings{}, nil, nil)
	m.hostCert["tls.example.com"] = "tls.example.com"
	out := renderUpstreams(m)
	if !strings.Contains(out, "\"tls.example.com\":\n    certificate: \"tls.example.com\"\n    paths:\n") {
		t.Fatalf("per-host certificate: must sit between host and paths:\n%s", out)
	}
	// No binding ⇒ no certificate line.
	m2 := newRenderModel()
	m2.addRoute("plain.example.com", "/", []backend{{addr: "b:80"}}, annSettings{}, nil, nil)
	if strings.Contains(renderUpstreams(m2), "certificate:") {
		t.Fatal("unbound host must not emit certificate:")
	}
}

func TestPruneCerts(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"keep.crt", "keep.key", "stale.crt", "stale.key", "junk.tmp", "other.txt"} {
		if err := os.WriteFile(filepath.Join(d, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	n := pruneCerts(d, map[string]struct{}{"keep": {}})
	if n != 3 { // stale.crt, stale.key, junk.tmp
		t.Fatalf("pruned=%d (want 3)", n)
	}
	for _, keep := range []string{"keep.crt", "keep.key", "other.txt"} {
		if _, err := os.Stat(filepath.Join(d, keep)); err != nil {
			t.Fatalf("%s must be kept: %v", keep, err)
		}
	}
	if _, err := os.Stat(filepath.Join(d, "stale.crt")); !os.IsNotExist(err) {
		t.Fatal("stale.crt must be pruned")
	}
}

func tlsSecret(ns, name, crt, key string) *corev1.Secret {
	s := &corev1.Secret{}
	s.Namespace, s.Name = ns, name
	s.Type = corev1.SecretTypeTLS
	s.Data = map[string][]byte{
		corev1.TLSCertKey:       []byte(crt),
		corev1.TLSPrivateKeyKey: []byte(key),
	}
	return s
}

func TestProjectCerts(t *testing.T) {
	dir := t.TempDir()
	sec := tlsSecret("default", "g0s-tls", "CRT-A", "KEY-A")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(sec).Build()
	r := &IngressReconciler{Client: c, CertsOutDir: dir,
		IsLeader: func() bool { return false }} // projection is NOT leader-gated

	m := newRenderModel()
	m.addCert("a.example.com", "a.example.com", "default", "g0s-tls", nil)
	ch, n, err := r.projectCerts(context.Background(), m)
	if err != nil || !ch || n != 1 {
		t.Fatalf("project: ch=%v n=%d err=%v", ch, n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a.example.com.crt")); string(b) != "CRT-A" {
		t.Fatalf("crt content=%q (non-leader must still project — per-pod)", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a.example.com.key")); string(b) != "KEY-A" {
		t.Fatalf("key content=%q", b)
	}
	// Idempotent: same model ⇒ no change.
	if ch, _, _ := r.projectCerts(context.Background(), m); ch {
		t.Fatal("unchanged projection must report no change")
	}
	// Rotation: Secret data changes ⇒ reprojected.
	sec.Data[corev1.TLSCertKey] = []byte("CRT-B")
	_ = c.Update(context.Background(), sec)
	if ch, _, _ := r.projectCerts(context.Background(), m); !ch {
		t.Fatal("rotated Secret must reproject")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a.example.com.crt")); string(b) != "CRT-B" {
		t.Fatalf("rotation not applied: %q", b)
	}
	// Removal: empty model ⇒ prune the cert files.
	ch, n, _ = r.projectCerts(context.Background(), newRenderModel())
	if !ch || n != 0 {
		t.Fatalf("prune: ch=%v n=%d", ch, n)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.example.com.crt")); !os.IsNotExist(err) {
		t.Fatal("unreferenced cert must be pruned")
	}

	// Missing Secret ⇒ skipped (no hard error, no file).
	m2 := newRenderModel()
	m2.addCert("z.example.com", "z.example.com", "default", "nope", nil)
	if ch, n, err := r.projectCerts(context.Background(), m2); err != nil || ch || n != 0 {
		t.Fatalf("missing Secret must be skipped softly: ch=%v n=%d err=%v", ch, n, err)
	}
	// Non-TLS Secret (no tls.crt) ⇒ skipped.
	bad := &corev1.Secret{}
	bad.Namespace, bad.Name = "default", "bad"
	bad.Data = map[string][]byte{"foo": []byte("bar")}
	c2 := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(bad).Build()
	r2 := &IngressReconciler{Client: c2, CertsOutDir: t.TempDir()}
	m3 := newRenderModel()
	m3.addCert("b.example.com", "b.example.com", "default", "bad", nil)
	if _, n, _ := r2.projectCerts(context.Background(), m3); n != 0 {
		t.Fatalf("non-TLS Secret must be skipped, n=%d", n)
	}

	// CertsOutDir empty ⇒ disabled no-op.
	rOff := &IngressReconciler{Client: c}
	if ch, n, err := rOff.projectCerts(context.Background(), m); ch || n != 0 || err != nil {
		t.Fatalf("CertsOutDir empty must be a no-op: ch=%v n=%d err=%v", ch, n, err)
	}
}

// Central-mode cert projection: certs land in a Secret the separate
// proxy pod mounts (mirrors projectCerts, which writes to a local dir).
func TestProjectCertsToSecret(t *testing.T) {
	src := tlsSecret("default", "g0s-tls", "CRT-A", "KEY-A")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(src).Build()
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	r := &IngressReconciler{Client: c, CertsOutSecret: out}

	m := newRenderModel()
	m.addCert("a.example.com", "a.example.com", "default", "g0s-tls", nil)
	ch, n, err := r.projectCertsToSecret(context.Background(), m)
	if err != nil || !ch || n != 1 {
		t.Fatalf("project: ch=%v n=%d err=%v", ch, n, err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), out, &got); err != nil {
		t.Fatalf("output secret not created: %v", err)
	}
	if string(got.Data["a.example.com.crt"]) != "CRT-A" || string(got.Data["a.example.com.key"]) != "KEY-A" {
		t.Fatalf("output secret data wrong: %v", got.Data)
	}
	if got.Labels[ingressCertsManagedLabel] != "true" {
		t.Fatalf("managed label missing: %v", got.Labels)
	}
	if got.Type != corev1.SecretTypeOpaque {
		t.Fatalf("type=%v", got.Type)
	}
	// Idempotent: same model ⇒ no change.
	if ch, _, _ := r.projectCertsToSecret(context.Background(), m); ch {
		t.Fatal("unchanged projection must report no change")
	}
	// Rotation: source Secret data changes ⇒ reprojected.
	src.Data[corev1.TLSCertKey] = []byte("CRT-B")
	_ = c.Update(context.Background(), src)
	if ch, _, _ := r.projectCertsToSecret(context.Background(), m); !ch {
		t.Fatal("rotated Secret must reproject")
	}
	_ = c.Get(context.Background(), out, &got)
	if string(got.Data["a.example.com.crt"]) != "CRT-B" {
		t.Fatalf("rotation not applied: %q", got.Data["a.example.com.crt"])
	}
	// Removal: empty model ⇒ stems pruned (Data emptied).
	ch, n, _ = r.projectCertsToSecret(context.Background(), newRenderModel())
	if !ch || n != 0 {
		t.Fatalf("prune: ch=%v n=%d", ch, n)
	}
	_ = c.Get(context.Background(), out, &got)
	if len(got.Data) != 2 || got.Data[defaultCertStem+".crt"] == nil {
		t.Fatalf("unreferenced certs must be pruned from secret, leaving the default: %v", keysOf(got.Data))
	}
	// Missing source Secret ⇒ skipped softly (no error, no change).
	m2 := newRenderModel()
	m2.addCert("z.example.com", "z.example.com", "default", "nope", nil)
	if ch, n, err := r.projectCertsToSecret(context.Background(), m2); err != nil || ch || n != 0 {
		t.Fatalf("missing Secret must be skipped softly: ch=%v n=%d err=%v", ch, n, err)
	}
	// CertsOutSecret unset ⇒ disabled no-op.
	rOff := &IngressReconciler{Client: c}
	if ch, n, err := rOff.projectCertsToSecret(context.Background(), m); ch || n != 0 || err != nil {
		t.Fatalf("CertsOutSecret empty must be a no-op: ch=%v n=%d err=%v", ch, n, err)
	}
}

// render() collects Ingress spec.tls (empty Hosts ⇒ the Ingress's
// rule hosts) and emits the per-host certificate: binding.
func TestRender_CollectsIngressTLS(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(t.TempDir(), "u.yaml")
	sec := tlsSecret("default", "g0s-tls", "C", "K")
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.TLS = []networkingv1.IngressTLS{{SecretName: "g0s-tls"}} // empty Hosts
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{Path: "/", Backend: networkingv1.IngressBackend{
				Service: &networkingv1.IngressServiceBackend{Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing, sec).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: dir, ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "app.example.com.crt")); string(b) != "C" {
		t.Fatalf("empty-Hosts spec.tls must bind the rule host: %q", b)
	}
	s, _ := os.ReadFile(out)
	if got := parseV2(t, string(s)).Hosts["app.example.com"].TLS.Terminate; got == nil || got.Cert != "app.example.com" {
		t.Fatalf("per-host certificate not emitted: terminate is %+v\n%s", got, s)
	}
}

// renderGateways collects listener certificateRefs (Terminate).
func TestRenderGateways_CollectsListenerCerts(t *testing.T) {
	dir := t.TempDir()
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "synapse-gw", "default"
	gw.Spec.GatewayClassName = gwv1.ObjectName("synapse")
	hn := gwv1.Hostname("svc.example.com")
	gw.Spec.Listeners = []gwv1.Listener{{
		Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443, Hostname: &hn,
		TLS: &gwv1.ListenerTLSConfig{
			CertificateRefs: []gwv1.SecretObjectReference{{Name: gwv1.ObjectName("svc-tls")}},
		},
	}}
	rt := &gwv1.HTTPRoute{}
	rt.Name, rt.Namespace = "app", "default"
	rt.Spec.ParentRefs = []gwv1.ParentReference{{Name: "synapse-gw", Namespace: ptr(gwv1.Namespace("default"))}}
	rt.Spec.Hostnames = []gwv1.Hostname{"svc.example.com"}
	rt.Spec.Rules = []gwv1.HTTPRouteRule{{
		Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Value: ptr("/")}}},
		BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "whoami", Port: ptr(gwv1.PortNumber(80))}}}},
	}}
	sec := tlsSecret("default", "svc-tls", "GC", "GK")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(gc, gw, rt, sec).
		WithStatusSubresource(&gwv1.GatewayClass{}, &gwv1.Gateway{}, &gwv1.HTTPRoute{}).Build()
	r := &IngressReconciler{Client: c, ClusterDomain: "cluster.local", GatewayAPI: true, CertsOutDir: dir}
	m := newRenderModel()
	r.renderGateways(context.Background(), m)
	if cp, ok := m.certProjections["svc.example.com"]; !ok || cp.name != "svc-tls" {
		t.Fatalf("listener certRef not collected: %+v", m.certProjections)
	}
	if _, _, err := r.projectCerts(context.Background(), m); err != nil {
		t.Fatalf("project: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "svc.example.com.crt")); string(b) != "GC" {
		t.Fatalf("listener cert not projected: %q", b)
	}
}

// Synapse names a certificate by its file stem, except that for the stems
// fullchain, cert and chain it takes the name of the directory above. A host
// of that name would be registered under the certs directory's name instead.
// "default" is the name the proxy's default_certificate picks by.
func TestCertStem_ReservedNamesAreNotHostStems(t *testing.T) {
	for _, h := range []string{"cert", "chain", "fullchain", "default"} {
		if s, b := certStem(h, "ns", "sec"); s != "ns_sec" || !b {
			t.Errorf("host %q: stem %q bound %v, want ns_sec bound", h, s, b)
		}
	}
	if s, b := certStem("cert.example.com", "ns", "sec"); s != "cert.example.com" || !b {
		t.Errorf("cert.example.com is not reserved: %q %v", s, b)
	}
}

func longHost(last byte) string {
	l := strings.Repeat("a", 63)
	return l + "." + l + "." + l + "." + strings.Repeat("a", 60) + string(last)
}

func TestCertStem_FitsAKeyAndAFileName(t *testing.T) {
	const max = 249 // 253 less ".crt"
	h1, h2 := longHost('b'), longHost('c')
	if len(h1) != 253 {
		t.Fatalf("test host is %d chars", len(h1))
	}
	s1, b1 := certStem(h1, "ns", "sec")
	s2, _ := certStem(h2, "ns", "sec")
	if len(s1) != max || !b1 {
		t.Errorf("253-char host: stem of %d chars, bound %v, want %d", len(s1), b1, max)
	}
	if s1 == s2 {
		t.Errorf("hosts that differ in their last char share the stem %q", s1)
	}
	if s, _ := certStem(h1, "ns", "sec"); s != s1 {
		t.Errorf("the stem is not stable: %q vs %q", s, s1)
	}
	exact := strings.Repeat("a", max)
	if s, _ := certStem(exact, "ns", "sec"); s != exact {
		t.Errorf("a stem of %d chars must stay as it is", max)
	}
	if s, _ := certStem(exact+"a", "ns", "sec"); len(s) != max || s == exact {
		t.Errorf("a stem of %d chars must be shortened: %d chars", max+1, len(s))
	}
	s3, b3 := certStem("", strings.Repeat("n", 63), strings.Repeat("s", 253))
	if len(s3) != max || b3 {
		t.Errorf("long ns and Secret: stem of %d chars, bound %v", len(s3), b3)
	}
}

func TestProjectCerts_LongHostDoesNotFailTheRender(t *testing.T) {
	dir := t.TempDir()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("default", "g0s-tls", "CRT", "KEY")).Build()
	r := &IngressReconciler{Client: c, CertsOutDir: dir}
	host := longHost('b')
	stem, _ := certStem(host, "default", "g0s-tls")
	m := newRenderModel()
	m.addCert(host, stem, "default", "g0s-tls", nil)
	if _, n, err := r.projectCerts(context.Background(), m); err != nil || n != 1 {
		t.Fatalf("project: n=%d err=%v", n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, stem+".crt")); string(b) != "CRT" {
		t.Fatalf("crt=%q", b)
	}
}

func TestProjectCertsToSecret_LongHostIsStored(t *testing.T) {
	out := types.NamespacedName{Namespace: "synapse-os", Name: "synapse-proxy-certs"}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(tlsSecret("default", "g0s-tls", "CRT", "KEY")).Build()
	r := &IngressReconciler{Client: c, CertsOutSecret: out}
	host := longHost('b')
	stem, _ := certStem(host, "default", "g0s-tls")
	m := newRenderModel()
	m.addCert(host, stem, "default", "g0s-tls", nil)
	if _, n, err := r.projectCertsToSecret(context.Background(), m); err != nil || n != 1 {
		t.Fatalf("project: n=%d err=%v", n, err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), out, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data[stem+".crt"]) != "CRT" || string(got.Data[stem+".key"]) != "KEY" {
		t.Fatalf("the long host's certificate is not stored: %d keys", len(got.Data))
	}
}

func TestRender_ReservedHostGetsItsOwnCertificate(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(t.TempDir(), "u.yaml")
	sec := tlsSecret("default", "g0s-tls", "C", "K")
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = "g0s", "default"
	ing.Spec.IngressClassName = ptr("synapse")
	ing.Spec.TLS = []networkingv1.IngressTLS{{SecretName: "g0s-tls", Hosts: []string{"cert", "default"}}}
	rule := func(host string) networkingv1.IngressRule {
		return networkingv1.IngressRule{
			Host: host,
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{Path: "/", Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: "whoami", Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}}
	}
	ing.Spec.Rules = []networkingv1.IngressRule{rule("cert"), rule("default")}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing, sec).Build()
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: dir, ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cert.crt")); err == nil {
		t.Error("cert.crt would be registered under the directory's name")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "default_g0s-tls.crt")); string(b) != "C" {
		t.Errorf("default_g0s-tls.crt=%q", b)
	}
	s, _ := os.ReadFile(out)
	hosts := parseV2(t, string(s)).Hosts
	for _, h := range []string{"cert", "default"} {
		if tm := hosts[h].TLS.Terminate; tm == nil || tm.Cert != "default_g0s-tls" {
			t.Errorf("the cert: of %q does not name the file:\n%s", h, s)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "default.crt")); err == nil {
		t.Error("default.crt would be the proxy's default certificate")
	}
}
