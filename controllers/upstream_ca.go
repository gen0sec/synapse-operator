package controllers

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// defaultCAKey is the key of a Secret that holds a CA bundle.
	defaultCAKey = "ca.crt"
	// maxCABytes caps one CA bundle, and maxCAFilesBytes all of them in a
	// render: they share a ConfigMap with the routes, which holds 1 MiB.
	maxCABytes      = 64 * 1024
	maxCAFilesBytes = 512 * 1024
)

// caFileRE is the name of a file this controller writes for a CA. Anything
// else in the directory or the ConfigMap is not its to remove.
var caFileRE = regexp.MustCompile(`^ca-[0-9a-f]{16}\.pem$`)

// DefaultUpstreamTLS is the proxy-wide `upstream_tls`, from the spec of a
// SynapseProxy.
type DefaultUpstreamTLS struct {
	Verify *bool
	// CASecret and CAKey say where the CA is; the Secret is in the
	// namespace of the proxy.
	CASecret types.NamespacedName
	CAKey    string
}

// caFileName names a CA by its content, so two Secrets with the same CA share
// a file and a changed CA is a changed path.
func caFileName(pemBytes []byte) string {
	sum := sha256.Sum256(pemBytes)
	return "ca-" + hex.EncodeToString(sum[:])[:16] + ".pem"
}

// checkCAPEM says why pemBytes cannot be a CA bundle: it holds no
// certificate, or one that does not parse.
func checkCAPEM(pemBytes []byte) error {
	if len(pemBytes) > maxCABytes {
		return fmt.Errorf("it is larger than %d bytes", maxCABytes)
	}
	certs := 0
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("a certificate in it does not parse: %v", err)
		}
		certs++
	}
	if certs == 0 {
		return errors.New("it holds no PEM certificate")
	}
	return nil
}

// caDir is where the pods read the CA files: beside the routes file.
func (r *IngressReconciler) caPath(file string) string {
	if r.usesConfigMapOutput() {
		return path.Join(path.Dir(proxyUpstreamsFile), file)
	}
	return filepath.Join(filepath.Dir(r.UpstreamsOutPath), file)
}

// addCAFile records a CA file for the render and returns false when the
// files would no longer fit the ConfigMap.
func (m *renderModel) addCAFile(file string, pemBytes []byte) bool {
	if _, ok := m.caFiles[file]; ok {
		return true
	}
	if m.caBytes+len(pemBytes) > maxCAFilesBytes {
		return false
	}
	if m.caFiles == nil {
		m.caFiles = map[string]string{}
	}
	m.caFiles[file] = string(pemBytes)
	m.caBytes += len(pemBytes)
	return true
}

// loadCA reads the CA of the Secret ns/name under key and records its file.
// It returns the path the pods read it at, or why it cannot be used. The
// error is only ever about the Secret, never about the cluster being down.
func (r *IngressReconciler) loadCA(ctx context.Context, m *renderModel, ns, name, key string) (string, string, error) {
	var sec corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &sec); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Sprintf("the Secret %s/%s does not exist", ns, name), nil
		}
		return "", "", err
	}
	data, ok := sec.Data[key]
	if !ok {
		return "", fmt.Sprintf("the Secret %s/%s has no key %q", ns, name, key), nil
	}
	if err := checkCAPEM(data); err != nil {
		return "", fmt.Sprintf("%s/%s key %q cannot be used: %v", ns, name, key, err), nil
	}
	file := caFileName(data)
	if !m.addCAFile(file, data) {
		return "", fmt.Sprintf("%s/%s key %q does not fit with the other CA files in the routes ConfigMap", ns, name, key), nil
	}
	return r.caPath(file), "", nil
}

// resolveUpstreamCA turns the CA that obj names into a file of the render.
// The Secret is read from the namespace of obj only. A route whose CA cannot
// be had keeps none, and so is checked against the system roots: it fails
// closed and the rest are served.
func (r *IngressReconciler) resolveUpstreamCA(ctx context.Context, m *renderModel, obj client.Object, a *annSettings) {
	u := &a.upstreamTLS
	if u.caName == "" || a.ssl == nil || !*a.ssl {
		return
	}
	if u.caNS != "" && u.caNS != obj.GetNamespace() {
		r.emit(obj, corev1.EventTypeWarning, "UpstreamCAForbidden",
			"the CA Secret %s/%s is in another namespace; a CA is read from the namespace of the object only, and the route has none",
			u.caNS, u.caName)
		return
	}
	file, why, err := r.loadCA(ctx, m, obj.GetNamespace(), u.caName, defaultCAKey)
	if err != nil {
		// The cluster could not be read: the route is left as it is, and the
		// next render tries again.
		m.caErr = errors.Join(m.caErr, err)
		return
	}
	if why != "" {
		r.emit(obj, corev1.EventTypeWarning, "UpstreamCAUnusable", "%s; the route has no CA", why)
		return
	}
	u.caFile = file
}

// resolveDefaultUpstreamTLS gives the model the proxy-wide upstream_tls.
func (r *IngressReconciler) resolveDefaultUpstreamTLS(ctx context.Context, m *renderModel) {
	d := r.DefaultUpstreamTLS
	if d == nil {
		return
	}
	cfg := &upstreamTLSCfg{verify: d.Verify}
	if d.CASecret.Name != "" {
		key := d.CAKey
		if key == "" {
			key = defaultCAKey
		}
		file, why, err := r.loadCA(ctx, m, d.CASecret.Namespace, d.CASecret.Name, key)
		switch {
		case err != nil:
			m.caErr = errors.Join(m.caErr, err)
		case why != "":
			r.emit(r.EventsOn, corev1.EventTypeWarning, "UpstreamCAUnusable", "spec.upstreamTLS.caSecretRef: %s; routes with no CA of their own are checked against the system roots", why)
		default:
			cfg.ca = file
		}
	}
	if cfg.verify != nil || cfg.ca != "" {
		m.defaultTLS = cfg
	}
}

// pruneCAFiles removes the CA files in dir that the render does not name.
func pruneCAFiles(dir string, keep map[string]string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !caFileRE.MatchString(e.Name()) {
			continue
		}
		if _, ok := keep[e.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// writeCAFiles writes the CA files of the render beside the routes file.
func writeCAFiles(dir string, files map[string]string) error {
	for name, body := range files {
		if _, err := writeIfChanged(filepath.Join(dir, name), body); err != nil {
			return err
		}
	}
	return nil
}

// routeSecretWatched says whether a change to o can change what is rendered:
// a certificate Secret, or one that holds a CA.
func routeSecretWatched(o client.Object) bool {
	s, ok := o.(*corev1.Secret)
	if !ok {
		return false
	}
	if s.Type == corev1.SecretTypeTLS {
		return true
	}
	_, has := s.Data[defaultCAKey]
	return has
}
