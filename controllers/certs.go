package controllers

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
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
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ingressCertsManagedLabel marks a Secret as written by the ingress
// controller's central-mode cert projection. Mirrors
// ingressUpstreamsManagedLabel so the operator's writers are
// distinguishable.
const ingressCertsManagedLabel = "synapse.gen0sec.com/ingress-certs"

// Multi-cert TLS: the operator projects each referenced Kubernetes TLS
// Secret (Ingress spec.tls / Gateway listener certificateRefs) into
// synapse's certificates directory as <stem>.crt / <stem>.key.
//
// synapse scans that dir for <stem>.crt+<stem>.key pairs and inotify-
// hot-reloads on change (acme disabled ⇒ no SIGHUP / restart needed;
// the cert watcher is independent of the upstreams reload). SNI then
// resolves via upstreams_cert_map (the per-host `certificate:` we also
// emit) then name_map exact (<host>.crt file-stem wins) then wildcard
// (cert SAN) then default. The dir is OPERATOR-OWNED: files no longer
// backed by a Secret are pruned, so cert rotation/removal converges
// with no pod restart. Projection is per-pod (never leader-gated, like
// the upstreams render).

// certStem picks the file-stem (== synapse cert name) for a host's
// Secret. A concrete host binds 1:1 (enables the strong name_map
// exact-match + upstreams_cert_map paths). Wildcard / no-host certs
// get a Secret-derived stem, "<ns>_<secret>": "_" cannot appear in a
// namespace or Secret name, so two stems never collide ("-" joined ns
// "a" + Secret "b-c" and ns "a-b" + Secret "c" into one). They rely
// on synapse's wildcard SAN match
// (host == "" ⇒ caller must not set a per-host upstreams binding).
func certStem(host, ns, secret string) (stem string, hostBound bool) {
	if host != "" && !strings.HasPrefix(host, "*.") {
		hostBound = true
		stem = sanitizeStem(host)
		if reservedCertStems[stem] {
			stem = sanitizeStem(ns + "_" + secret)
		}
	} else {
		stem = sanitizeStem(ns + "_" + secret)
	}
	return fitStem(stem), hostBound
}

// reservedCertStems are the file stems Synapse does not take at face value.
// For fullchain, cert and chain, cert_logical_name takes the parent
// directory's name instead. "default" was the operator's default_certificate
// before defaultCertStem, and a spec.config or --ingress-mode config may still
// name it; a host of that name would then become the certificate for every
// name no other certificate matches. defaultCertStem is the operator's own.
var reservedCertStems = map[string]bool{"fullchain": true, "cert": true, "chain": true, "default": true, defaultCertStem: true}

// defaultCertStem is the stem of the certificate the operator generates
// itself. No host can have it: a DNS name does not start with "_", and
// "<namespace>_<secret>" starts with a namespace.
const defaultCertStem = "_default"

const (
	defaultCertCN       = "synapse-default"
	defaultCertValidity = 365 * 24 * time.Hour
	// defaultCertRenewBefore is how close to its expiry the default
	// certificate is replaced.
	defaultCertRenewBefore = 30 * 24 * time.Hour
	// defaultCertReserve is room kept in the certificates Secret for the
	// default certificate and its key (about 1 KiB of PEM).
	defaultCertReserve = 2000
)

func (r *IngressReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// defaultCertificate returns the operator's own certificate: the one given if
// it is a usable pair with more than defaultCertRenewBefore left, otherwise a
// new self-signed one. Synapse serves it for every name no other certificate
// matches (proxy.default_certificate), so that name never falls to a tenant's
// certificate, which is what Synapse does when the named one is missing.
func defaultCertificate(crt, key []byte, now time.Time) (newCrt, newKey []byte, err error) {
	if usableDefaultCert(crt, key, now) {
		return crt, key, nil
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: defaultCertCN},
		// An hour back, so a node whose clock is a little behind accepts it.
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(defaultCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

func usableDefaultCert(crt, key []byte, now time.Time) bool {
	pair, err := tls.X509KeyPair(crt, key)
	if err != nil || len(pair.Certificate) == 0 {
		return false
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return false
	}
	return leaf.NotAfter.Sub(now) >= defaultCertRenewBefore
}

// maxCertStem is the longest stem whose <stem>.crt is a valid Secret key and
// file name.
const maxCertStem = validation.DNS1123SubdomainMaxLength - len(".crt")

// fitStem shortens a stem longer than maxCertStem to its head and a hash of
// the whole. A collision of the 64-bit hash is not handled.
func fitStem(stem string) string {
	if len(stem) <= maxCertStem {
		return stem
	}
	sum := sha256.Sum256([]byte(stem))
	return stem[:maxCertStem-17] + "_" + hex.EncodeToString(sum[:])[:16]
}

func sanitizeStem(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}

// projectCerts materializes m.certProjections into CertsOutDir and
// prunes operator-managed cert files no longer referenced. Returns
// (changed, projected, err). CertsOutDir == "" disables multi-cert
// (legacy single static-mount behavior). A missing / non-TLS Secret
// is skipped (logged + metric) without failing the whole render, and so
// is a certificate that cannot be written: a stem that has a pair on disk
// keeps it, one that has not is left out.
func (r *IngressReconciler) projectCerts(ctx context.Context, m *renderModel) (bool, int, error) {
	changed, n, _, err := r.writeCerts(ctx, m)
	return changed, n, err
}

// writeCerts is projectCerts that also returns how many certificates it
// could not write, for the caller to retry on.
func (r *IngressReconciler) writeCerts(ctx context.Context, m *renderModel) (changed bool, projected, failed int, err error) {
	if r.CertsOutDir == "" {
		return false, 0, 0, nil
	}
	logger := ctrl.LoggerFrom(ctx).WithName("certs")

	// Deterministic order so logs/behavior are reproducible.
	stems := make([]string, 0, len(m.certProjections))
	for s := range m.certProjections {
		stems = append(stems, s)
	}
	sort.Strings(stems)

	if err := os.MkdirAll(r.CertsOutDir, 0o755); err != nil {
		for _, stem := range stems {
			r.certNotWritten(logger, m.certProjections[stem], fmt.Errorf("certs dir %s: %w", r.CertsOutDir, err), false)
		}
		mCerts.Set(0)
		return false, 0, len(stems), nil
	}

	want := map[string]struct{}{} // stems successfully projected
	dch, dfailed := r.projectDefaultCert(logger)
	changed = dch
	if dfailed {
		failed++
	}
	if !dfailed || certPairExists(r.CertsOutDir, defaultCertStem) {
		want[defaultCertStem] = struct{}{} // the last good pair keeps serving
	}
	for _, stem := range stems {
		cp := m.certProjections[stem]
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: cp.ns, Name: cp.name}, &sec); err != nil {
			logger.Info("referenced TLS Secret unavailable; skipping cert (host will fall back to default)",
				"secret", cp.ns+"/"+cp.name, "stem", stem, "err", err.Error())
			mCertErrors.Inc()
			continue
		}
		crt := sec.Data[corev1.TLSCertKey]       // "tls.crt"
		key := sec.Data[corev1.TLSPrivateKeyKey] // "tls.key"
		if len(crt) == 0 || len(key) == 0 {
			logger.Info("referenced Secret is not a usable TLS Secret (missing tls.crt/tls.key); skipping",
				"secret", cp.ns+"/"+cp.name, "type", string(sec.Type), "stem", stem)
			mCertErrors.Inc()
			continue
		}
		pch, err := writeCertPair(r.CertsOutDir, stem, crt, key)
		if err != nil {
			kept := certPairExists(r.CertsOutDir, stem)
			r.certNotWritten(logger, cp, err, kept)
			failed++
			if kept {
				want[stem] = struct{}{} // the last good pair keeps serving
			}
			continue
		}
		if pch {
			changed = true
			logger.Info("projected TLS cert", "stem", stem, "secret", cp.ns+"/"+cp.name)
		}
		want[stem] = struct{}{}
	}

	if pruned := pruneCerts(r.CertsOutDir, want); pruned > 0 {
		changed = true
		logger.Info("pruned stale operator-managed cert files", "count", pruned)
	}
	// The default certificate is the operator's, not a projected Secret.
	n := len(want)
	if _, ok := want[defaultCertStem]; ok {
		n--
	}
	mCerts.Set(float64(n))
	return changed, n, failed, nil
}

func certPairExists(dir, stem string) bool {
	for _, ext := range []string{".key", ".crt"} {
		if fi, err := os.Stat(filepath.Join(dir, stem+ext)); err != nil || !fi.Mode().IsRegular() {
			return false
		}
	}
	return true
}

// writeCertPair makes <stem>.key and <stem>.crt hold key and crt. Both are
// staged first and renamed into place after, so a failure to write leaves the
// pair that was there. A file whose content is already right is not touched.
// A rename that fails after the key moved puts the old key back, or removes
// the new one when there was none.
func writeCertPair(dir, stem string, crt, key []byte) (bool, error) {
	type file struct {
		ext     string
		content []byte
		mode    os.FileMode
	}
	// Key BEFORE crt: synapse's dir scan keys on <stem>.crt and only pairs
	// when <stem>.key exists, so the crt rename is what makes the pair loadable.
	var todo []file
	for _, f := range []file{{".key", key, 0o600}, {".crt", crt, 0o644}} {
		if cur, err := os.ReadFile(filepath.Join(dir, stem+f.ext)); err == nil && string(cur) == string(f.content) {
			continue
		}
		todo = append(todo, f)
	}
	if len(todo) == 0 {
		return false, nil
	}
	for _, f := range todo {
		defer os.Remove(filepath.Join(dir, certTempName(stem, f.ext)))
	}
	for _, f := range todo {
		if err := os.WriteFile(filepath.Join(dir, certTempName(stem, f.ext)), f.content, f.mode); err != nil {
			return false, fmt.Errorf("stage %s%s: %w", stem, f.ext, err)
		}
	}
	oldKey, oldKeyErr := os.ReadFile(filepath.Join(dir, stem+".key"))
	for i, f := range todo {
		if err := os.Rename(filepath.Join(dir, certTempName(stem, f.ext)), filepath.Join(dir, stem+f.ext)); err != nil {
			if i > 0 { // the key was moved, the certificate was not
				keyPath := filepath.Join(dir, stem+".key")
				if oldKeyErr != nil {
					os.Remove(keyPath)
				} else if os.WriteFile(filepath.Join(dir, certTempName(stem, ".key")), oldKey, 0o600) == nil {
					os.Rename(filepath.Join(dir, certTempName(stem, ".key")), keyPath)
				}
			}
			return false, fmt.Errorf("write %s%s: %w", stem, f.ext, err)
		}
	}
	return true, nil
}

// certTempName is where a certificate file is staged before it is renamed
// into place. The ".." prefix is what synapse's cert watcher treats as a
// Kubernetes atomic-update marker (synapse-utils tools.rs
// reload_relevant_path): creating the file triggers a rescan even
// though the rename that follows is invisible to it. The name does not end
// in .crt or .key, so the loader never reads it, and prune removes it. A stem
// too long to stage under its own name is staged under a hash of it.
func certTempName(stem, ext string) string {
	name := ".." + stem + ext + ".tmp"
	if len(name) <= 255 { // a file name limit; the longest stem is 249
		return name
	}
	sum := sha256.Sum256([]byte(stem))
	return ".." + hex.EncodeToString(sum[:8]) + ext + ".tmp"
}

// certNotWritten records a certificate that could not be written to
// CertsOutDir. kept says whether a pair from an earlier render is still on
// disk and keeps serving; if not, the host falls back to the default.
func (r *IngressReconciler) certNotWritten(logger logr.Logger, cp certProjection, err error, kept bool) {
	outcome := "host will fall back to default"
	if kept {
		outcome = "the previous pair stays in place"
	}
	logger.Error(err, "TLS cert not written; skipping ("+outcome+")",
		"secret", cp.ns+"/"+cp.name, "stem", cp.stem)
	mCertErrors.Inc()
	r.emit(cp.from, corev1.EventTypeWarning, "CertificateNotWritten",
		"certificate %s from Secret %s/%s could not be written: %v", cp.stem, cp.ns, cp.name, err)
}

// projectDefaultCert keeps <defaultCertStem>.crt/.key in CertsOutDir, a pair
// that is generated again only when defaultCertificate says so. Like a
// tenant's certificate it is not allowed to fail the render: on an error the
// pair on disk stays, and failed tells the caller to retry.
func (r *IngressReconciler) projectDefaultCert(logger logr.Logger) (changed, failed bool) {
	curCrt, _ := os.ReadFile(filepath.Join(r.CertsOutDir, defaultCertStem+".crt"))
	curKey, _ := os.ReadFile(filepath.Join(r.CertsOutDir, defaultCertStem+".key"))
	crt, key, err := defaultCertificate(curCrt, curKey, r.now())
	if err == nil {
		changed, err = writeCertPair(r.CertsOutDir, defaultCertStem, crt, key)
	}
	if err != nil {
		outcome := "hosts without a certificate get none"
		if certPairExists(r.CertsOutDir, defaultCertStem) {
			outcome = "the previous pair stays in place"
		}
		logger.Error(err, "default TLS cert not written ("+outcome+")", "stem", defaultCertStem)
		mCertErrors.Inc()
		return false, true
	}
	return changed, false
}

// maxCertsSecretBytes is how much certificate material goes into the output
// Secret. The API server limits a Secret to 1 MiB; this leaves room for the
// rest of the object.
const maxCertsSecretBytes = 1000 * 1000

// projectCertsToSecret is the central-mode analogue of projectCerts: it
// materializes m.certProjections into the CertsOutSecret Secret as
// <stem>.crt/<stem>.key data keys, which the separate synapse-proxy pod
// mounts as its certificates dir. The Secret's Data is replaced wholesale
// every render, so stems no longer referenced are pruned automatically (no
// separate prune pass like the dir mode). A missing / non-TLS source
// Secret is skipped (logged + metric) without failing the render.
// CertsOutSecret unset ⇒ no-op. Returns (changed, projected, err).
func (r *IngressReconciler) projectCertsToSecret(ctx context.Context, m *renderModel) (bool, int, error) {
	if r.CertsOutSecret.Name == "" {
		return false, 0, nil
	}
	logger := ctrl.LoggerFrom(ctx).WithName("certs")

	// Deterministic order so logs are reproducible.
	stems := make([]string, 0, len(m.certProjections))
	for s := range m.certProjections {
		stems = append(stems, s)
	}
	sort.Strings(stems)

	data := map[string][]byte{}
	size := defaultCertReserve
	for _, stem := range stems {
		cp := m.certProjections[stem]
		// A Secret that the API server refuses takes every certificate with
		// it, and with them the whole render. So one that cannot be stored is
		// left out, and its host falls back to the default certificate.
		if problems := validation.IsConfigMapKey(stem + ".crt"); len(problems) > 0 {
			logger.Info("certificate cannot be stored under its name; skipping (host will fall back to default)",
				"secret", cp.ns+"/"+cp.name, "stem", stem, "why", strings.Join(problems, "; "))
			mCertErrors.Inc()
			continue
		}
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: cp.ns, Name: cp.name}, &sec); err != nil {
			logger.Info("referenced TLS Secret unavailable; skipping cert (host will fall back to default)",
				"secret", cp.ns+"/"+cp.name, "stem", stem, "err", err.Error())
			mCertErrors.Inc()
			continue
		}
		crt := sec.Data[corev1.TLSCertKey]       // "tls.crt"
		key := sec.Data[corev1.TLSPrivateKeyKey] // "tls.key"
		if len(crt) == 0 || len(key) == 0 {
			logger.Info("referenced Secret is not a usable TLS Secret (missing tls.crt/tls.key); skipping",
				"secret", cp.ns+"/"+cp.name, "type", string(sec.Type), "stem", stem)
			mCertErrors.Inc()
			continue
		}
		// Stems are taken in name order, so which certificates are left out
		// when the Secret is full does not change from one render to the next.
		entry := len(crt) + len(key) + 2*len(stem) + len(".crt") + len(".key")
		if size+entry > maxCertsSecretBytes {
			logger.Info("certificates Secret is full; skipping cert (host will fall back to default)",
				"secret", cp.ns+"/"+cp.name, "stem", stem, "limitBytes", maxCertsSecretBytes)
			mCertErrors.Inc()
			continue
		}
		size += entry
		data[stem+".crt"] = crt
		data[stem+".key"] = key
	}

	out := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      r.CertsOutSecret.Name,
		Namespace: r.CertsOutSecret.Namespace,
	}}
	var changed bool
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, out, func() error {
		if err := r.claimOutput(out); err != nil {
			return err
		}
		if out.Labels == nil {
			out.Labels = map[string]string{}
		}
		out.Labels[ingressCertsManagedLabel] = "true"
		out.Type = corev1.SecretTypeOpaque
		// The Data is replaced wholesale, so the default certificate is
		// carried over from the current one here.
		crt, key, err := defaultCertificate(out.Data[defaultCertStem+".crt"], out.Data[defaultCertStem+".key"], r.now())
		if err != nil {
			return fmt.Errorf("default certificate: %w", err)
		}
		full := make(map[string][]byte, len(data)+2)
		for k, v := range data {
			full[k] = v
		}
		full[defaultCertStem+".crt"] = crt
		full[defaultCertStem+".key"] = key
		if !bytesMapEqual(out.Data, full) {
			out.Data = full
			changed = true
		}
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	if op == controllerutil.OperationResultCreated {
		changed = true
	}
	n := len(data) / 2
	if changed {
		logger.Info("projected TLS certs to secret",
			"secret", r.CertsOutSecret.Namespace+"/"+r.CertsOutSecret.Name, "certs", n)
	}
	if r.OwnerRef != nil {
		mProxyCerts.WithLabelValues(r.CertsOutSecret.Namespace, r.OwnerRef.Name).Set(float64(n))
	} else {
		mCerts.Set(float64(n))
	}
	return changed, n, nil
}

// bytesMapEqual reports whether two map[string][]byte have identical
// keys and values.
func bytesMapEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || !bytes.Equal(va, vb) {
			return false
		}
	}
	return true
}

// pruneCerts removes <stem>.crt/<stem>.key (and stray .tmp) for stems
// not in want — the cert dir is operator-owned, so a deleted Ingress/
// Secret converges to no cert (SNI then falls back to default).
func pruneCerts(dir string, want map[string]struct{}) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	removed := 0
	for _, e := range entries {
		n := e.Name()
		var stem string
		switch {
		case strings.HasSuffix(n, ".crt"):
			stem = strings.TrimSuffix(n, ".crt")
		case strings.HasSuffix(n, ".key"):
			stem = strings.TrimSuffix(n, ".key")
		case strings.HasSuffix(n, ".tmp"):
			if err := os.Remove(filepath.Join(dir, n)); err == nil {
				removed++
			}
			continue
		default:
			continue
		}
		if _, ok := want[stem]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, n)); err == nil {
			removed++
		}
	}
	return removed
}
