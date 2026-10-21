package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// tlsIngress is routedIngress with a TLS entry for its host.
func tlsIngress(name, host, secret string) *networkingv1.Ingress {
	ing := routedIngress(name, ptr("synapse"), host)
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{host}, SecretName: secret}}
	return ing
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
