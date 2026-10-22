package controllers

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// tlsIngress is routedIngress with a TLS entry for its host.
func tlsIngress(name, host, secret string) *networkingv1.Ingress {
	ing := routedIngress(name, ptr("synapse"), host)
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{host}, SecretName: secret}}
	return ing
}

// eventsOn records which object each event was recorded on, which a
// FakeRecorder cannot say for objects listed from the fake client.
type eventsOn struct{ got []string }

func (e *eventsOn) add(obj runtime.Object, etype, reason string) {
	if reason != "CertificateNotWritten" {
		return
	}
	o := obj.(client.Object)
	e.got = append(e.got, etype+" "+reason+" "+reflect.TypeOf(obj).Elem().Name()+" "+o.GetNamespace()+"/"+o.GetName())
}
func (e *eventsOn) Event(obj runtime.Object, etype, reason, _ string) { e.add(obj, etype, reason) }
func (e *eventsOn) Eventf(obj runtime.Object, etype, reason, _ string, _ ...interface{}) {
	e.add(obj, etype, reason)
}
func (e *eventsOn) AnnotatedEventf(obj runtime.Object, _ map[string]string, etype, reason, _ string, _ ...interface{}) {
	e.add(obj, etype, reason)
}

func nextEvent(t *testing.T, events *record.FakeRecorder) string {
	t.Helper()
	select {
	case e := <-events.Events:
		return e
	default:
		return ""
	}
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// A certificate that cannot be written must not take the routes, or the other
// certificates, down with it. A directory where the file belongs fails the
// write with EISDIR, as root or not.
func TestProjectCerts_AWriteErrorSkipsThatCertificate(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bad.example.com.key"), 0o755); err != nil {
		t.Fatal(err)
	}
	events := record.NewFakeRecorder(8)
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: dir, Recorder: events}
	before := counterValue(t, mCertErrors)

	out := renderToFile(t, r,
		tlsIngress("bad", "bad.example.com", "bad-tls"),
		tlsIngress("good", "good.example.com", "good-tls"),
		tlsSecret("default", "bad-tls", "CRT-bad", "KEY-bad"),
		tlsSecret("default", "good-tls", "CRT-good", "KEY-good"),
	)

	wantHosts(t, out, []string{"bad.example.com", "good.example.com"}, nil)
	if got, err := os.ReadFile(filepath.Join(dir, "good.example.com.crt")); err != nil || string(got) != "CRT-good" {
		t.Errorf("the other certificate is %q, %v", got, err)
	}
	if got := counterValue(t, mCertErrors); got != before+1 {
		t.Errorf("certificate error counter went from %v to %v", before, got)
	}
	if e := nextEvent(t, events); !strings.HasPrefix(e, corev1.EventTypeWarning+" CertificateNotWritten ") || !strings.Contains(e, "bad.example.com") {
		t.Errorf("event = %q, want a warning that names the certificate", e)
	}
	for _, f := range []string{"bad.example.com.key", "bad.example.com.crt"} {
		if exists(filepath.Join(dir, f)) {
			t.Errorf("%s is left in the certificates directory", f)
		}
	}
}

// The key is written before the certificate. When the certificate cannot be
// written, the key must not be left behind alone: a certificate is whole or
// absent.
func TestProjectCerts_KeyWriteErrorLeavesNoHalfPair(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bad.example.com.crt"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: dir}

	renderToFile(t, r,
		tlsIngress("bad", "bad.example.com", "bad-tls"),
		tlsSecret("default", "bad-tls", "CRT-bad", "KEY-bad"),
	)

	for _, f := range []string{"bad.example.com.key", "bad.example.com.crt"} {
		if exists(filepath.Join(dir, f)) {
			t.Errorf("%s is left behind, half of a pair", f)
		}
	}
}

// A certificates directory that cannot be made skips every certificate. The
// routes are still written.
func TestProjectCerts_CertsDirUnavailableStillWritesRoutes(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "certs")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	events := record.NewFakeRecorder(8)
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: notADir, Recorder: events}
	before := counterValue(t, mCertErrors)
	mCerts.Set(7)

	out := renderToFile(t, r,
		tlsIngress("a", "a.example.com", "a-tls"),
		tlsIngress("b", "b.example.com", "b-tls"),
		tlsSecret("default", "a-tls", "CRT-a", "KEY-a"),
		tlsSecret("default", "b-tls", "CRT-b", "KEY-b"),
	)

	wantHosts(t, out, []string{"a.example.com", "b.example.com"}, nil)
	if got := counterValue(t, mCertErrors); got != before+2 {
		t.Errorf("certificate error counter went from %v to %v, want +2", before, got)
	}
	if got := gaugeValue(t, mCerts); got != 0 {
		t.Errorf("certificate gauge = %v, want 0", got)
	}
	for range 2 {
		if e := nextEvent(t, events); !strings.HasPrefix(e, corev1.EventTypeWarning+" CertificateNotWritten ") {
			t.Errorf("event = %q, want CertificateNotWritten", e)
		}
	}
}

// While a certificate is not written the render is retried, or the failure is
// permanent without anybody noticing. Once it works the retry stops.
func TestReconcile_RequeuesWhileACertificateIsNotWritten(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bad.example.com.key"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &IngressReconciler{
		IngressClassName: "synapse",
		CertsOutDir:      dir,
		UpstreamsOutPath: filepath.Join(t.TempDir(), "upstreams.yaml"),
		ClusterDomain:    "cluster.local",
	}
	r.Client = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		[]client.Object{
			tlsIngress("bad", "bad.example.com", "bad-tls"),
			tlsSecret("default", "bad-tls", "CRT-bad", "KEY-bad"),
		}...).Build()

	res, err := r.Reconcile(context.Background(), ctrl.Request{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Errorf("RequeueAfter = %v, want a minute", res.RequeueAfter)
	}

	// The failed stem was pruned, so the next render writes it.
	res, err = r.Reconcile(context.Background(), ctrl.Request{})
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v after the certificate was written, want none", res.RequeueAfter)
	}
}

func tempFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var tmp []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			tmp = append(tmp, e.Name())
		}
	}
	return tmp
}

// A rotation that cannot be written leaves the pair that works. The files are
// staged first, so the failure comes before anything on disk changes.
func TestProjectCerts_AFailedRewriteKeepsTheLastGoodPair(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{"old.example.com.key": "KEY-old", "old.example.com.crt": "CRT-old"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The key is staged, then the certificate cannot be.
	if err := os.Mkdir(filepath.Join(dir, certTempName("old.example.com", ".crt")), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: dir}
	before := counterValue(t, mCertErrors)

	renderToFile(t, r,
		tlsIngress("old", "old.example.com", "old-tls"),
		tlsSecret("default", "old-tls", "CRT-new", "KEY-new"),
	)

	for name, want := range map[string]string{"old.example.com.key": "KEY-old", "old.example.com.crt": "CRT-old"} {
		if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(got) != want {
			t.Errorf("%s is %q, %v; want the old %q", name, got, err, want)
		}
	}
	if got := counterValue(t, mCertErrors); got != before+1 {
		t.Errorf("certificate error counter went from %v to %v", before, got)
	}
	if tmp := tempFiles(t, dir); len(tmp) != 0 {
		t.Errorf("temporary files left behind: %v", tmp)
	}
}

// A certificate that was never written and cannot be is absent, not half
// there, and leaves no temporary file.
func TestProjectCerts_ANewCertificateThatFailsIsAbsent(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, certTempName("new.example.com", ".crt")), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: dir}

	renderToFile(t, r,
		tlsIngress("new", "new.example.com", "new-tls"),
		tlsSecret("default", "new-tls", "CRT-new", "KEY-new"),
	)

	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("certificates directory holds %d entries, want none: %v", len(entries), entries)
	}
}

// The event lands on the object that references the certificate.
func TestProjectCerts_TheEventIsRecordedOnTheReferencingObject(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bad.example.com.key"), 0o755); err != nil {
		t.Fatal(err)
	}
	events := &eventsOn{}
	r := &IngressReconciler{IngressClassName: "synapse", CertsOutDir: dir, Recorder: events}
	renderToFile(t, r,
		tlsIngress("bad", "bad.example.com", "bad-tls"),
		tlsSecret("default", "bad-tls", "CRT-bad", "KEY-bad"),
	)
	if want := []string{"Warning CertificateNotWritten Ingress default/bad"}; !reflect.DeepEqual(events.got, want) {
		t.Errorf("events = %v, want %v", events.got, want)
	}
}

func TestProjectCerts_TheEventForAListenerCertificateIsRecordedOnTheGateway(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "gw.example.com.key"), 0o755); err != nil {
		t.Fatal(err)
	}
	gc := &gwv1.GatewayClass{}
	gc.Name = "synapse"
	gc.Spec.ControllerName = gwv1.GatewayController(ControllerName)
	gw := &gwv1.Gateway{}
	gw.Name, gw.Namespace = "edge", "default"
	gw.Spec.GatewayClassName = "synapse"
	gw.Spec.Listeners = []gwv1.Listener{{
		Name: "https", Port: 443, Protocol: gwv1.HTTPSProtocolType,
		Hostname: ptr(gwv1.Hostname("gw.example.com")),
		TLS: &gwv1.ListenerTLSConfig{
			Mode:            ptr(gwv1.TLSModeTerminate),
			CertificateRefs: []gwv1.SecretObjectReference{{Name: "gw-tls"}},
		},
	}}
	events := &eventsOn{}
	r := &IngressReconciler{IngressClassName: "synapse", GatewayAPI: true, CertsOutDir: dir, Recorder: events}
	renderToFile(t, r, gc, gw, tlsSecret("default", "gw-tls", "CRT-gw", "KEY-gw"))
	if want := []string{"Warning CertificateNotWritten Gateway default/edge"}; !reflect.DeepEqual(events.got, want) {
		t.Errorf("events = %v, want %v", events.got, want)
	}
}

// A certificate that cannot be moved into place after the key was must not
// leave the new key next to the old certificate.
func TestWriteCertPair_PutsTheOldKeyBackWhenTheCertificateCannotBeMoved(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x.key"), []byte("KEY-old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "x.crt"), 0o755); err != nil { // a rename onto it fails
		t.Fatal(err)
	}

	if _, err := writeCertPair(dir, "x", []byte("CRT-new"), []byte("KEY-new")); err == nil {
		t.Fatal("write succeeded, want an error")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "x.key")); err != nil || string(got) != "KEY-old" {
		t.Errorf("key is %q, %v; want the old one", got, err)
	}
	if tmp := tempFiles(t, dir); len(tmp) != 0 {
		t.Errorf("temporary files left behind: %v", tmp)
	}
}

// Synapse's cert watcher ignores renames. It rescans on the creation of a
// file whose name starts with ".." or ends in .crt, .key or .pem
// (synapse-utils tools.rs reload_relevant_path), so a staged file has to be
// the first kind, and must not be the second, which the loader would read.
func TestCertTempName_IsWhatSynapseWatcherReactsToAndLoaderIgnores(t *testing.T) {
	stems := []string{"shop.example.com", strings.Repeat("a", 16), strings.Repeat("a", 245), strings.Repeat("a", 246), strings.Repeat("a", maxCertStem)}
	for _, stem := range stems {
		for _, ext := range []string{".crt", ".key"} {
			checkCertTempName(t, stem, ext)
		}
	}
	if name := certTempName(strings.Repeat("a", 245), ".crt"); !strings.Contains(name, strings.Repeat("a", 245)) {
		t.Errorf("a 245-char stem fits under its own name, got %q", name)
	}
	if a, b := certTempName(strings.Repeat("a", 246), ".crt"), certTempName(strings.Repeat("a", 245)+"b", ".crt"); a == b {
		t.Errorf("two long stems staged under one name %q", a)
	}
}

func checkCertTempName(t *testing.T, stem, ext string) {
	t.Helper()
	name := certTempName(stem, ext)
	if len(name) > 255 {
		t.Errorf("staged name for a %d-char stem is %d bytes, over a file name's 255", len(stem), len(name))
	}
	if !strings.HasPrefix(name, "..") {
		t.Errorf("%q does not start with .., so creating it does not trigger a reload", name)
	}
	for _, loaded := range []string{".crt", ".key", ".pem"} {
		if strings.HasSuffix(name, loaded) {
			t.Errorf("%q ends in %s, the loader would read it", name, loaded)
		}
	}
}

// A certificates directory that cannot be made is retried, not forgotten.
func TestReconcile_RequeuesWhileTheCertsDirIsUnavailable(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "certs")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := &IngressReconciler{
		IngressClassName: "synapse",
		CertsOutDir:      notADir,
		UpstreamsOutPath: filepath.Join(t.TempDir(), "upstreams.yaml"),
		ClusterDomain:    "cluster.local",
	}
	r.Client = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		[]client.Object{
			tlsIngress("a", "a.example.com", "a-tls"),
			tlsSecret("default", "a-tls", "CRT-a", "KEY-a"),
		}...).Build()

	res, err := r.Reconcile(context.Background(), ctrl.Request{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Errorf("RequeueAfter = %v, want a minute", res.RequeueAfter)
	}
}
