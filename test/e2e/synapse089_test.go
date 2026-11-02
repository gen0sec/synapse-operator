//go:build e2e

package e2e

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// What Synapse 0.8.9 needs of the operator: a signing key for sticky
// sessions, and the name and the CA it checks the certificate of a TLS
// back-end against.
const (
	annPrefixE2E = "synapse.gen0sec.com/"

	stickyKeySecret = "edge-sticky-key"
	stickyKeyFile   = "/etc/synapse/sticky/sticky.key"
	proxyCASecret   = "edge-upstream-ca"

	stickyHost       = "sticky.e2e.test"
	secureHost       = "secure.e2e.test"
	selfSignedHost   = "selfsigned.e2e.test"
	altNameHost      = "altname.e2e.test"
	badSNIHost       = "badsni.e2e.test"
	unprobedHost     = "unprobed.e2e.test"
	routeCAHost      = "routeca.e2e.test"
	noRouteCAHost    = "norouteca.e2e.test"
	otherNSCAHost    = "othernsca.e2e.test"
	badPEMHost       = "badpem.e2e.test"
	rotateRouteHost  = "rotateroute.e2e.test"
	rotateProxyHost  = "rotateproxy.e2e.test"
	altSNI           = "alt.backend.test"
	backendDomain    = ".e2e-apps.svc.cluster.local"
	stickyCookieName = "backend_id"
)

// sentinels counts the hosts notServed has added.
var sentinels int

// authority is a certificate authority of the test's own.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newAuthority(t *testing.T, name string) authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a server certificate for names.
func (a authority) issue(t *testing.T, names ...string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: names[0]},
		DNSNames:     names,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

// tlsBackend is a backend that serves TLS on 8443 with the given pair, and a
// Service for it on port 443.
func (c *cluster) tlsBackend(name string, certPEM, keyPEM []byte) []client.Object {
	labels := map[string]string{"app": name}
	return []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name + "-tls"},
			Type:       corev1.SecretTypeTLS,
			Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:            "backend",
							Image:           c.backendImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "NAME", Value: name}, {Name: "PORT", Value: "8443"}, {Name: "TLS_DIR", Value: "/tls"},
							},
							Ports:        []corev1.ContainerPort{{ContainerPort: 8443}},
							VolumeMounts: []corev1.VolumeMount{{Name: "tls", MountPath: "/tls", ReadOnly: true}},
							ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromInt32(8443), Scheme: corev1.URISchemeHTTPS},
							}},
						}},
						Volumes: []corev1.Volume{{Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: name + "-tls"},
						}}},
					},
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name},
			Spec: corev1.ServiceSpec{
				Selector: labels,
				Ports:    []corev1.ServicePort{{Port: 443, TargetPort: intstr.FromInt32(8443)}},
			},
		},
	}
}

// httpsIngress routes host to a tlsBackend, over HTTPS, with annotations.
func httpsIngress(name, host, backend string, annotations map[string]string) *networkingv1.Ingress {
	ing := ingress(name, className, host, backend)
	ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number = 443
	ing.Annotations = map[string]string{annPrefixE2E + "backend-protocol": "HTTPS"}
	for k, v := range annotations {
		ing.Annotations[annPrefixE2E+k] = v
	}
	return ing
}

func caSecret(namespace, name string, data []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       map[string][]byte{"ca.crt": data},
	}
}

// setAnnotations changes the annotations of an Ingress of the apps namespace.
func (c *cluster) setAnnotations(t *testing.T, name string, set map[string]string) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ing networkingv1.Ingress
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: appsNamespace, Name: name}, &ing); err != nil {
			return err
		}
		for k, v := range set {
			ing.Annotations[annPrefixE2E+k] = v
		}
		return c.k8s.Update(c.ctx, &ing)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (c *cluster) setSecret(t *testing.T, namespace, name string, data []byte) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var s corev1.Secret
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, &s); err != nil {
			return err
		}
		s.Data = map[string][]byte{"ca.crt": data}
		return c.k8s.Update(c.ctx, &s)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// routesFile is the file the proxy's pods read, from the ConfigMap, with the
// names of the CA files beside it.
func (c *cluster) routesFile() (file string, cas []string, err error) {
	var cm corev1.ConfigMap
	if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: routesConfigMap}, &cm); err != nil {
		return "", nil, err
	}
	for k := range cm.Data {
		if strings.HasPrefix(k, "ca-") && strings.HasSuffix(k, ".pem") {
			cas = append(cas, k)
		}
	}
	return cm.Data[routesKey], cas, nil
}

// warned reports "" when an Event of the reason, a Warning, is on the
// Ingress or on the proxy.
func (c *cluster) warned(namespace, object, reason string) string {
	var events corev1.EventList
	if err := c.k8s.List(c.ctx, &events, client.InNamespace(namespace)); err != nil {
		return err.Error()
	}
	for _, e := range events.Items {
		if e.Reason == reason && e.InvolvedObject.Name == object && e.Type == corev1.EventTypeWarning {
			return ""
		}
	}
	return fmt.Sprintf("%d events, none %s on %s", len(events.Items), reason, object)
}

// notServed fails the test when host answers 200 from backend. It first
// waits until the routes file names the host and then until a host added
// after it is served, so that the file with the host in it is the one that
// Synapse runs: a host that answers nothing is then a host Synapse refuses
// to serve, not one it has not heard of.
func (c *cluster) notServed(t *testing.T, host, backend string) {
	t.Helper()
	eventually(t, soon, host+" is in the routes file", func() string {
		file, _, err := c.routesFile()
		switch {
		case err != nil:
			return err.Error()
		case !strings.Contains(file, `"`+host+`":`):
			return "not yet"
		}
		return ""
	})
	sentinels++
	sentinel := fmt.Sprintf("sentinel%d.e2e.test", sentinels)
	c.create(t, ingress(fmt.Sprintf("sentinel%d", sentinels), className, sentinel, "plain"))
	eventually(t, afterASync, "a host added after "+host+" is served", func() string { return c.routed("http", sentinel, "plain") })
	for range 5 {
		a, err := c.get("http", host)
		if err == nil && a.status == http.StatusOK && a.backend == backend {
			t.Fatalf("%s is served by %s: the back-end's certificate should not be trusted", host, backend)
		}
		time.Sleep(time.Second)
	}
}

func (c *cluster) named(namespace, name string) (*synapsev1alpha1.SynapseProxy, error) {
	var p synapsev1alpha1.SynapseProxy
	return &p, c.k8s.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, &p)
}

// notReadyBecause reports "" when the proxy says it is not ready for reason.
func (c *cluster) notReadyBecause(name, reason string) string {
	p, err := c.named(proxyNamespace, name)
	if err != nil {
		return err.Error()
	}
	cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
	switch {
	case cond == nil:
		return "no Ready condition"
	case cond.Status != metav1.ConditionFalse || cond.Reason != reason:
		return fmt.Sprintf("Ready is %s (%s), want False (%s)", cond.Status, cond.Reason, reason)
	}
	return ""
}

func (c *cluster) readyNamed(name string) string {
	p, err := c.named(proxyNamespace, name)
	if err != nil {
		return err.Error()
	}
	cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
	switch {
	case cond == nil:
		return "no Ready condition"
	case cond.ObservedGeneration != p.Generation:
		return "status of an earlier spec"
	case cond.Status != metav1.ConditionTrue:
		return fmt.Sprintf("not ready: %s: %s", cond.Reason, cond.Message)
	}
	return ""
}

// extraProxy creates a second proxy, with a class of its own, in the proxy
// namespace.
func (c *cluster) extraProxy(t *testing.T, name string) {
	t.Helper()
	group, scope, ns := synapsev1alpha1.GroupVersion.Group, networkingv1.IngressClassParametersReferenceScopeNamespace, proxyNamespace
	class := &networkingv1.IngressClass{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-" + name},
		Spec: networkingv1.IngressClassSpec{
			Controller: controllerName,
			Parameters: &networkingv1.IngressClassParametersReference{APIGroup: &group, Kind: "SynapseProxy", Name: name, Scope: &scope, Namespace: &ns},
		},
	}
	t.Cleanup(func() { _ = c.k8s.Delete(c.ctx, class) })
	c.create(t, class, &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: name},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:     c.synapseImage,
			Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
		},
	})
}

func (c *cluster) secret(namespace, name string) (*corev1.Secret, error) {
	var s corev1.Secret
	return &s, c.k8s.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, &s)
}

// stickyCookieOf is the sticky cookie an answer sets, as a Cookie header.
func stickyCookieOf(a answer) string {
	for _, ck := range (&http.Response{Header: a.header}).Cookies() {
		if ck.Name == stickyCookieName {
			return ck.Name + "=" + ck.Value
		}
	}
	return ""
}

var signedCookie = regexp.MustCompile(`^backend_id=[0-9a-f]+\.[0-9a-f]{32}$`)

// stickyCookie reports "" when an answer for the sticky host sets a signed
// sticky cookie, and a request that sends the cookie back is served. The
// operator routes to a Service's address, so the cookie pins a client to
// the Service, not to a pod behind it.
func (c *cluster) stickyCookie() string {
	a, err := c.get("http", stickyHost)
	switch {
	case err != nil:
		return err.Error()
	case a.status != http.StatusOK:
		return fmt.Sprintf("answered %d", a.status)
	}
	cookie := stickyCookieOf(a)
	if cookie == "" {
		return "no sticky cookie in " + fmt.Sprint(a.header)
	}
	if !signedCookie.MatchString(cookie) {
		return fmt.Sprintf("the cookie %q is not a signed one", cookie)
	}
	b, err := c.requestWith(http.MethodGet, "http", stickyHost, "/", http.Header{"Cookie": {cookie}})
	switch {
	case err != nil:
		return err.Error()
	case b.status != http.StatusOK || b.backend != "sticky":
		return fmt.Sprintf("with the cookie: answered %d from %q", b.status, b.backend)
	}
	return ""
}

// TestSynapse089 is what Synapse 0.8.9 asks of the operator, on one proxy:
// the key sticky sessions are signed with, and TLS to back-ends that is
// checked. The back-ends of the TLS part have certificates made here, so what
// is checked is Synapse's own verdict, in a request.
func TestSynapse089(t *testing.T) {
	c := newCluster(t)
	step := func(name string, run func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, run) {
			t.FailNow()
		}
	}
	svcName := func(n string) string { return n + backendDomain }

	ca1 := newAuthority(t, "e2e CA one")
	ca2 := newAuthority(t, "e2e CA two")
	ca3 := newAuthority(t, "e2e CA three")
	ca5 := newAuthority(t, "e2e CA five")

	var stickyKey []byte

	step("a proxy with a sticky Ingress is ready, and serves it", func(t *testing.T) {
		eventually(t, soon, "the operator is available", c.operatorAvailable)
		c.clear(t)
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("plain")...)
		c.create(t, c.backend("sticky")...)

		stickyIngress := ingress("sticky", className, stickyHost, "sticky")
		stickyIngress.Annotations = map[string]string{annPrefixE2E + "sticky-sessions": "true"}
		group, scope, ns := synapsev1alpha1.GroupVersion.Group, networkingv1.IngressClassParametersReferenceScopeNamespace, proxyNamespace
		c.create(t,
			&networkingv1.IngressClass{
				ObjectMeta: metav1.ObjectMeta{Name: className},
				Spec: networkingv1.IngressClassSpec{
					Controller: controllerName,
					Parameters: &networkingv1.IngressClassParametersReference{APIGroup: &group, Kind: "SynapseProxy", Name: proxyKey.Name, Scope: &scope, Namespace: &ns},
				},
			},
			stickyIngress,
			// The proxy-wide CA exists before the proxy does.
			caSecret(proxyNamespace, proxyCASecret, ca1.pem),
		)
		c.create(t, &synapsev1alpha1.SynapseProxy{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyKey.Namespace, Name: proxyKey.Name},
			Spec: synapsev1alpha1.SynapseProxySpec{
				Image:       c.synapseImage,
				Service:     &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
				Listeners:   []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
				UpstreamTLS: &synapsev1alpha1.UpstreamTLSSpec{CASecretRef: &synapsev1alpha1.CASecretReference{Name: proxyCASecret}},
			},
		})
		eventually(t, afterASync, "the proxy becomes ready", c.ready)
		// Synapse refuses the whole routes file when sticky sessions have no
		// key, so a served host is a loaded file.
		eventually(t, afterASync, "the sticky host is served", func() string { return c.routed("http", stickyHost, "sticky") })
	})

	step("the sticky key is a generated Secret, mounted for Synapse and named in the routes file", func(t *testing.T) {
		s, err := c.secret(proxyNamespace, stickyKeySecret)
		if err != nil {
			t.Fatal(err)
		}
		stickyKey = s.Data["sticky.key"]
		if len(stickyKey) < 32 {
			t.Errorf("the key is %d bytes, Synapse needs 32", len(stickyKey))
		}
		if bytes.Equal(stickyKey, make([]byte, len(stickyKey))) {
			t.Error("the key is all zeroes")
		}
		if ref := metav1.GetControllerOf(s); ref == nil || ref.Kind != "SynapseProxy" || ref.Name != proxyKey.Name {
			t.Errorf("the Secret is controlled by %+v, want the proxy", ref)
		}
		d := c.deployment(t)
		var volume *corev1.Volume
		for i, v := range d.Spec.Template.Spec.Volumes {
			if v.Secret != nil && v.Secret.SecretName == stickyKeySecret {
				volume = &d.Spec.Template.Spec.Volumes[i]
			}
		}
		if volume == nil {
			t.Fatalf("no volume of the pods holds %s", stickyKeySecret)
		}
		if mode := volume.Secret.DefaultMode; mode == nil || *mode != 0o400 {
			got := "unset"
			if mode != nil {
				got = fmt.Sprintf("%04o", *mode)
			}
			t.Errorf("the key is mounted with mode %s, want 0400", got)
		}
		mounted := false
		for _, m := range d.Spec.Template.Spec.Containers[0].VolumeMounts {
			if m.Name == volume.Name && m.MountPath == "/etc/synapse/sticky" && m.ReadOnly {
				mounted = true
			}
		}
		if !mounted {
			t.Errorf("the key is not mounted read-only at /etc/synapse/sticky: %+v", d.Spec.Template.Spec.Containers[0].VolumeMounts)
		}
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(file, "secret_file: \""+stickyKeyFile+"\"") || !strings.Contains(file, "sticky_sessions:") {
			t.Errorf("the routes file does not name the key:\n%s", file)
		}
		// The key stays in its Secret.
		var configMaps corev1.ConfigMapList
		var secrets corev1.SecretList
		if err := c.k8s.List(c.ctx, &configMaps, client.InNamespace(proxyNamespace)); err != nil {
			t.Fatal(err)
		}
		if err := c.k8s.List(c.ctx, &secrets, client.InNamespace(proxyNamespace)); err != nil {
			t.Fatal(err)
		}
		b64 := []byte(base64.StdEncoding.EncodeToString(stickyKey))
		for _, cm := range configMaps.Items {
			for k, v := range cm.Data {
				if strings.Contains(v, string(stickyKey)) || strings.Contains(v, string(b64)) {
					t.Errorf("the key is in ConfigMap %s/%s", cm.Name, k)
				}
			}
		}
		for _, sec := range secrets.Items {
			if sec.Name == stickyKeySecret {
				continue
			}
			for k, v := range sec.Data {
				if bytes.Contains(v, stickyKey) {
					t.Errorf("the key is in Secret %s/%s", sec.Name, k)
				}
			}
		}
	})

	step("a sticky route sets a sticky cookie, and takes it back", func(t *testing.T) {
		eventually(t, soon, "the sticky cookie works", c.stickyCookie)
	})

	step("a Secret of the user's with a key of 32 bytes is adopted, and one that is too short is reported", func(t *testing.T) {
		key := bytes.Repeat([]byte("k"), 40)
		c.create(t,
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "adopt-sticky-key"},
				Data:       map[string][]byte{"sticky.key": key},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "short-sticky-key"},
				Data:       map[string][]byte{"sticky.key": []byte("too short")},
			},
		)
		owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "key-owner"}}
		c.create(t, owner)
		c.create(t, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: proxyNamespace, Name: "taken-sticky-key",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID,
					Controller: ptrTo(true), BlockOwnerDeletion: ptrTo(true),
				}},
			},
			Data: map[string][]byte{"sticky.key": key},
		})
		for _, name := range []string{"adopt", "short", "taken"} {
			c.extraProxy(t, name)
		}
		eventually(t, afterASync, "the proxy with a key of its own is ready", func() string { return c.readyNamed("adopt") })
		s, err := c.secret(proxyNamespace, "adopt-sticky-key")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(s.Data["sticky.key"], key) {
			t.Error("the key of the user's Secret was changed")
		}
		if ref := metav1.GetControllerOf(s); ref != nil {
			t.Errorf("the Secret is controlled by %+v, want it left to its owner, nobody", ref)
		}
		eventually(t, soon, "a key too short is reported", func() string { return c.notReadyBecause("short", "StickyKeyUnusable") })
		s, err = c.secret(proxyNamespace, "short-sticky-key")
		if err != nil {
			t.Fatal(err)
		}
		if string(s.Data["sticky.key"]) != "too short" || metav1.GetControllerOf(s) != nil {
			t.Errorf("the short Secret was changed or taken: %q, %+v", s.Data["sticky.key"], metav1.GetControllerOf(s))
		}
		eventually(t, soon, "a Secret of another owner is a name collision", func() string { return c.notReadyBecause("taken", "NameCollision") })
		// Fixed by the user, the proxy goes on.
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			s, err := c.secret(proxyNamespace, "short-sticky-key")
			if err != nil {
				return err
			}
			s.Data = map[string][]byte{"sticky.key": key}
			return c.k8s.Update(c.ctx, s)
		})
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, afterASync, "the proxy is ready once the key is fixed", func() string { return c.readyNamed("short") })
		for _, name := range []string{"adopt", "short", "taken"} {
			p, err := c.named(proxyNamespace, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.k8s.Delete(c.ctx, p); err != nil {
				t.Fatal(err)
			}
		}
		// What is adopted is not the proxy's to delete with it.
		eventually(t, soon, "the proxy with the adopted key is gone", func() string {
			if _, err := c.named(proxyNamespace, "adopt"); err == nil {
				return "still there"
			}
			return ""
		})
		if _, err := c.secret(proxyNamespace, "adopt-sticky-key"); err != nil {
			t.Errorf("the adopted Secret went with the proxy: %v", err)
		}
	})

	step("a deleted key Secret is made again with a new key, and a restart brings the pods to it", func(t *testing.T) {
		old := c.deployment(t)
		oldPods := c.pods(t)
		s, err := c.secret(proxyNamespace, stickyKeySecret)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.k8s.Delete(c.ctx, s); err != nil {
			t.Fatal(err)
		}
		var fresh []byte
		eventually(t, soon, "the Secret is made again", func() string {
			s, err := c.secret(proxyNamespace, stickyKeySecret)
			switch {
			case err != nil:
				return err.Error()
			case len(s.Data["sticky.key"]) < 32:
				return "the new key is too short"
			}
			fresh = s.Data["sticky.key"]
			return ""
		})
		if bytes.Equal(fresh, stickyKey) {
			t.Error("the Secret was made again with the same key")
		}
		stickyKey = fresh
		// kubectl rollout restart.
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			d, err := c.getDeployment()
			if err != nil {
				return err
			}
			if d.Spec.Template.Annotations == nil {
				d.Spec.Template.Annotations = map[string]string{}
			}
			d.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
			return c.k8s.Update(c.ctx, d)
		})
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, afterASync, "the pods are replaced", func() string {
			if reason := c.ready(); reason != "" {
				return reason
			}
			all, err := c.listPods(true)
			if err != nil {
				return err.Error()
			}
			for _, uid := range all {
				for _, o := range oldPods {
					if uid == o {
						return "an old pod is still there"
					}
				}
			}
			return ""
		})
		_ = old
		eventually(t, afterASync, "the sticky host is served", func() string { return c.routed("http", stickyHost, "sticky") })
		eventually(t, soon, "the sticky cookie works with the new key", c.stickyCookie)
	})

	step("a key of the proxy's own that was cut short is written again", func(t *testing.T) {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			s, err := c.secret(proxyNamespace, stickyKeySecret)
			if err != nil {
				return err
			}
			s.Data = map[string][]byte{"sticky.key": []byte("cut short")}
			return c.k8s.Update(c.ctx, s)
		})
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, afterASync, "the key is written again", func() string {
			s, err := c.secret(proxyNamespace, stickyKeySecret)
			switch {
			case err != nil:
				return err.Error()
			case len(s.Data["sticky.key"]) < 32:
				return "the key is still too short"
			}
			return ""
		})
	})

	step("back-ends with TLS are set up, each a different case", func(t *testing.T) {
		secureCert, secureKey := ca1.issue(t, svcName("secure"))
		c.create(t, c.tlsBackend("secure", secureCert, secureKey)...)
		// A certificate made by nobody the proxy trusts: self-signed.
		selfCert, selfKey, _ := certificate(t, svcName("selfsigned"))
		c.create(t, c.tlsBackend("selfsigned", selfCert, selfKey)...)
		// Trusted, but for a name other than its Service's.
		altCert, altKey := ca1.issue(t, altSNI)
		c.create(t, c.tlsBackend("altname", altCert, altKey)...)
		// A private CA that only the Ingresses that name it trust.
		privCert, privKey := ca2.issue(t, svcName("privca"))
		c.create(t, c.tlsBackend("privca", privCert, privKey)...)
		// The CA of these two is rotated in.
		rotCert, rotKey := ca3.issue(t, svcName("rotateroute"))
		c.create(t, c.tlsBackend("rotateroute", rotCert, rotKey)...)
		rotProxyCert, rotProxyKey := ca5.issue(t, svcName("rotateproxy"))
		c.create(t, c.tlsBackend("rotateproxy", rotProxyCert, rotProxyKey)...)

		c.create(t,
			caSecret(appsNamespace, "app-ca", ca2.pem),
			caSecret(appsNamespace, "bad-ca", []byte("this is not a certificate")),
			// Holds the wrong CA until it is rotated.
			caSecret(appsNamespace, "rotating-ca", ca2.pem),
		)
		c.create(t,
			httpsIngress("secure", secureHost, "secure", nil),
			httpsIngress("selfsigned", selfSignedHost, "selfsigned", nil),
			httpsIngress("altname", altNameHost, "altname", nil),
			httpsIngress("badsni", badSNIHost, "secure", map[string]string{"upstream-tls-sni": "127.1"}),
			httpsIngress("unprobed", unprobedHost, "secure", map[string]string{"healthcheck": "false"}),
			httpsIngress("routeca", routeCAHost, "privca", map[string]string{"upstream-tls-ca": "app-ca"}),
			httpsIngress("norouteca", noRouteCAHost, "privca", nil),
			httpsIngress("othernsca", otherNSCAHost, "privca", map[string]string{"upstream-tls-ca": proxyNamespace + "/" + proxyCASecret}),
			httpsIngress("badpem", badPEMHost, "privca", map[string]string{"upstream-tls-ca": "bad-ca"}),
			httpsIngress("rotateroute", rotateRouteHost, "rotateroute", map[string]string{"upstream-tls-ca": "rotating-ca"}),
			httpsIngress("rotateproxy", rotateProxyHost, "rotateproxy", nil),
		)
	})

	step("a back-end with a certificate for its Service name is served, with no annotation", func(t *testing.T) {
		eventually(t, afterASync, "the secure host is served", func() string { return c.routed("http", secureHost, "secure") })
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(file, `sni: "`+svcName("secure")+`"`) {
			t.Errorf("the route has no default sni of the Service name:\n%s", file)
		}
	})

	step("the proxy-wide CA is a file beside the routes, and the routes name it", func(t *testing.T) {
		file, cas, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if len(cas) == 0 {
			t.Fatalf("no CA file in the routes ConfigMap:\n%s", file)
		}
		if !regexp.MustCompile(`(?m)^upstream_tls:\n(  .*\n)*  ca: \S*ca-[0-9a-f]{16}\.pem`).MatchString(file) {
			t.Errorf("the routes file has no proxy-wide upstream_tls.ca:\n%s", file)
		}
	})

	step("a route with healthcheck false is served over TLS, with upstream_tls beside it", func(t *testing.T) {
		eventually(t, soon, "the unprobed host is served", func() string { return c.routed("http", unprobedHost, "secure") })
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(file, "type: none") {
			t.Errorf("no route is unprobed:\n%s", file)
		}
	})

	step("a bad sni annotation is refused, its route keeps the default, and the others are served", func(t *testing.T) {
		eventually(t, soon, "InvalidUpstreamTLS is raised on the Ingress", func() string {
			return c.warned(appsNamespace, "badsni", "InvalidUpstreamTLS")
		})
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(file, "127.1") {
			t.Errorf("the refused sni is in the routes file:\n%s", file)
		}
		eventually(t, soon, "the host with the bad annotation is served with the default name", func() string { return c.routed("http", badSNIHost, "secure") })
		eventually(t, soon, "the sticky host is served", func() string { return c.routed("http", stickyHost, "sticky") })
		eventually(t, soon, "the secure host is served", func() string { return c.routed("http", secureHost, "secure") })
	})

	step("a self-signed back-end, and one for another name, are not served", func(t *testing.T) {
		c.notServed(t, selfSignedHost, "selfsigned")
		c.notServed(t, altNameHost, "altname")
	})

	step("upstream-tls-verify false serves a self-signed back-end, and upstream-tls-sni a back-end for another name", func(t *testing.T) {
		c.setAnnotations(t, "selfsigned", map[string]string{"upstream-tls-verify": "false"})
		c.setAnnotations(t, "altname", map[string]string{"upstream-tls-sni": altSNI})
		eventually(t, afterASync, "the self-signed host is served", func() string { return c.routed("http", selfSignedHost, "selfsigned") })
		eventually(t, afterASync, "the host with an sni of its own is served", func() string { return c.routed("http", altNameHost, "altname") })
	})

	step("upstream-tls-ca in the Ingress's namespace is trusted, and only there", func(t *testing.T) {
		eventually(t, afterASync, "the host with a CA of its own is served", func() string { return c.routed("http", routeCAHost, "privca") })
		c.notServed(t, noRouteCAHost, "privca")
		eventually(t, soon, "UpstreamCAForbidden is raised for a CA of another namespace", func() string {
			return c.warned(appsNamespace, "othernsca", "UpstreamCAForbidden")
		})
		c.notServed(t, otherNSCAHost, "privca")
		eventually(t, soon, "UpstreamCAUnusable is raised for a CA that is not PEM", func() string {
			return c.warned(appsNamespace, "badpem", "UpstreamCAUnusable")
		})
		c.notServed(t, badPEMHost, "privca")
		// The back-end of the proxy-wide CA is served still.
		eventually(t, soon, "the secure host is served", func() string { return c.routed("http", secureHost, "secure") })
	})

	step("spec.upstreamTLS.verify false serves a back-end of an unknown CA, and an annotation overrides it", func(t *testing.T) {
		// verify alone, with no CA, still makes an upstream_tls block.
		c.edit(t, func(s *synapsev1alpha1.SynapseProxySpec) {
			s.UpstreamTLS = &synapsev1alpha1.UpstreamTLSSpec{Verify: ptrTo(false)}
		})
		eventually(t, afterASync, "the proxy is ready", c.ready)
		eventually(t, afterASync, "a back-end of a CA nobody gave is served", func() string { return c.routed("http", noRouteCAHost, "privca") })
		c.setAnnotations(t, "norouteca", map[string]string{"upstream-tls-verify": "true"})
		eventually(t, afterASync, "the annotation turns the check on again", func() string {
			a, err := c.get("http", noRouteCAHost)
			if err != nil || (a.status == http.StatusOK && a.backend == "privca") {
				return "still served"
			}
			return ""
		})
		c.edit(t, func(s *synapsev1alpha1.SynapseProxySpec) {
			s.UpstreamTLS = &synapsev1alpha1.UpstreamTLSSpec{CASecretRef: &synapsev1alpha1.CASecretReference{Name: proxyCASecret}}
		})
		eventually(t, afterASync, "the proxy is ready", c.ready)
	})

	step("a rotated CA is trusted once it is in the Secret", func(t *testing.T) {
		c.notServed(t, rotateRouteHost, "rotateroute")
		c.notServed(t, rotateProxyHost, "rotateproxy")
		c.setSecret(t, appsNamespace, "rotating-ca", ca3.pem)
		c.setSecret(t, proxyNamespace, proxyCASecret, append(append([]byte{}, ca1.pem...), ca5.pem...))
		eventually(t, afterASync, "the host of the rotated route CA is served", func() string { return c.routed("http", rotateRouteHost, "rotateroute") })
		eventually(t, afterASync, "the host of the rotated proxy-wide CA is served", func() string { return c.routed("http", rotateProxyHost, "rotateproxy") })
		eventually(t, soon, "the first back-end is served still", func() string { return c.routed("http", secureHost, "secure") })
	})

	step("deleting the proxy removes the key Secret", func(t *testing.T) {
		if err := c.k8s.Delete(c.ctx, c.proxy(t)); err != nil {
			t.Fatal(err)
		}
		eventually(t, afterASync, "the key Secret is gone", func() string {
			if _, err := c.secret(proxyNamespace, stickyKeySecret); err == nil {
				return "still there"
			}
			return ""
		})
	})
}
