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
	if s, b := certStem("*.example.com", "ns", "sec"); b || s != "ns-sec" {
		t.Fatalf("wildcard ⇒ secret-derived, not host-bound: %q %v", s, b)
	}
	if s, b := certStem("", "ns", "sec"); b || s != "ns-sec" {
		t.Fatalf("no host ⇒ secret-derived, not host-bound: %q %v", s, b)
	}
	if s, _ := certStem("a/b c*d", "n", "s"); s != "a_b_c_d" {
		t.Fatalf("sanitize odd chars: %q", s)
	}
}

func TestAddCert_FirstWriterWins(t *testing.T) {
	m := newRenderModel()
	m.addCert("h.example.com", "h.example.com", "ns", "sec1")
	m.addCert("h.example.com", "h.example.com", "ns", "sec2") // dup host+stem
	m.addCert("", "ns-w", "ns", "wild")                       // no host binding
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
	m.addCert("x", "", "ns", "s")
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

func TestWriteFileIfChanged(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.key")
	if ch, err := writeFileIfChanged(p, []byte("A"), 0o600); err != nil || !ch {
		t.Fatalf("first write changed: %v %v", ch, err)
	}
	fi, _ := os.Stat(p)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", fi.Mode().Perm())
	}
	if ch, _ := writeFileIfChanged(p, []byte("A"), 0o600); ch {
		t.Fatal("unchanged must be no-op")
	}
	if ch, _ := writeFileIfChanged(p, []byte("B"), 0o600); !ch {
		t.Fatal("changed must rewrite")
	}
	if b, _ := os.ReadFile(p); string(b) != "B" {
		t.Fatalf("content=%q", b)
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
	m.addCert("a.example.com", "a.example.com", "default", "g0s-tls")
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
	m2.addCert("z.example.com", "z.example.com", "default", "nope")
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
	m3.addCert("b.example.com", "b.example.com", "default", "bad")
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
	m.addCert("a.example.com", "a.example.com", "default", "g0s-tls")
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
	if len(got.Data) != 0 {
		t.Fatalf("unreferenced certs must be pruned from secret: %v", got.Data)
	}
	// Missing source Secret ⇒ skipped softly (no error, no change).
	m2 := newRenderModel()
	m2.addCert("z.example.com", "z.example.com", "default", "nope")
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
	if s, _ := os.ReadFile(out); !strings.Contains(string(s), "certificate: \"app.example.com\"") {
		t.Fatalf("per-host certificate: not emitted:\n%s", s)
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
