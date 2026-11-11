//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

const (
	hstsProxyAge = 15768000

	hstsDefaultHost  = "hstsdefault.e2e.test"
	hstsOverrideHost = "hstsoverride.e2e.test"
	hstsOffHost      = "hstsoff.e2e.test"
	hstsBadAgeHost   = "hstsbadage.e2e.test"
	hstsConflictHost = "hstsconflict.e2e.test"
)

// hstsIngress routes host over HTTPS to the plain back-end, with hsts
// annotations, and returns the Secret that holds its certificate.
func hstsIngress(t *testing.T, name, host string, annotations map[string]string) (*networkingv1.Ingress, *corev1.Secret) {
	t.Helper()
	certPEM, keyPEM, _ := certificate(t, host)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name + "-tls"},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	ing := ingress(name, className, host, "plain")
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{host}, SecretName: secret.Name}}
	if len(annotations) > 0 {
		ing.Annotations = map[string]string{}
	}
	for k, v := range annotations {
		ing.Annotations[annPrefixE2E+k] = v
	}
	return ing, secret
}

// hstsHeader is the Strict-Transport-Security header of an HTTPS response
// for host, and whether the response is a 200 from the plain back-end.
func (c *cluster) hstsHeader(host string) (header string, reason string) {
	if reason := c.routed("https", host, "plain"); reason != "" {
		return "", reason
	}
	a, err := c.get("https", host)
	if err != nil {
		return "", err.Error()
	}
	return a.header.Get("Strict-Transport-Security"), ""
}

// hstsIs reports "" when the HTTPS response for host carries a header with
// max-age and includeSubDomains as given, or, with maxAge < 0, no header.
func (c *cluster) hstsIs(host string, maxAge int, subdomains bool) string {
	header, reason := c.hstsHeader(host)
	if reason != "" {
		return reason
	}
	if maxAge < 0 {
		if header != "" {
			return fmt.Sprintf("%s sends Strict-Transport-Security %q, want none", host, header)
		}
		return ""
	}
	if !regexp.MustCompile(fmt.Sprintf(`(?i)\bmax-age=%d\b`, maxAge)).MatchString(header) {
		return fmt.Sprintf("%s sends Strict-Transport-Security %q, want max-age=%d", host, header, maxAge)
	}
	if got := strings.Contains(strings.ToLower(header), "includesubdomains"); got != subdomains {
		return fmt.Sprintf("%s sends Strict-Transport-Security %q, includeSubDomains want %v", host, header, subdomains)
	}
	if strings.Contains(strings.ToLower(header), "preload") {
		return fmt.Sprintf("%s sends Strict-Transport-Security %q with preload, want none", host, header)
	}
	return ""
}

func TestHSTS(t *testing.T) {
	c := newCluster(t)
	step := func(name string, run func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, run) {
			t.FailNow()
		}
	}

	step("a proxy with spec.hsts serves a host with the proxy-wide header", func(t *testing.T) {
		eventually(t, soon, "the operator is available", c.operatorAvailable)
		c.clear(t)
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("plain")...)

		group, scope, ns := synapsev1alpha1.GroupVersion.Group, networkingv1.IngressClassParametersReferenceScopeNamespace, proxyNamespace
		c.create(t, &networkingv1.IngressClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec: networkingv1.IngressClassSpec{
				Controller: controllerName,
				Parameters: &networkingv1.IngressClassParametersReference{APIGroup: &group, Kind: "SynapseProxy", Name: proxyKey.Name, Scope: &scope, Namespace: &ns},
			},
		})
		hosts := []struct {
			name, host string
			ann        map[string]string
		}{
			{"hdefault", hstsDefaultHost, nil},
			{"hoverride", hstsOverrideHost, map[string]string{"hsts-max-age": "600", "hsts-include-subdomains": "true"}},
			{"hoff", hstsOffHost, map[string]string{"hsts": "off"}},
			{"hbadage", hstsBadAgeHost, map[string]string{"hsts-max-age": "abc"}},
			{"hconf-a", hstsConflictHost, map[string]string{"hsts-max-age": "111"}},
			{"hconf-b", hstsConflictHost, map[string]string{"hsts-max-age": "222"}},
		}
		for _, h := range hosts {
			ing, secret := hstsIngress(t, h.name, h.host, h.ann)
			if h.name == "hconf-b" {
				// The certificate of the first Ingress is the host's.
				ing.Spec.TLS = nil
				// Its own path, so that its route is programmed and it claims.
				ing.Spec.Rules[0].HTTP.Paths[0].Path = "/b"
			}
			if h.name != "hconf-b" {
				c.create(t, secret)
			}
			c.create(t, ing)
		}
		c.create(t, &synapsev1alpha1.SynapseProxy{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyKey.Namespace, Name: proxyKey.Name},
			Spec: synapsev1alpha1.SynapseProxySpec{
				Image:     c.synapseImage,
				Service:   &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
				Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}, {Name: "https", Port: 443, Protocol: "TLS"}},
				HSTS:      &synapsev1alpha1.HSTSSpec{MaxAgeSeconds: hstsProxyAge, IncludeSubdomains: true},
			},
		})
		eventually(t, afterASync, "the proxy becomes ready", c.ready)
		eventually(t, afterASync, "the host has the proxy-wide header", func() string { return c.hstsIs(hstsDefaultHost, hstsProxyAge, true) })
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`(?m)^hsts:\n  max_age: 15768000\n  include_subdomains: true\n`).MatchString(file) {
			t.Errorf("the routes file has no top-level hsts:\n%s", file)
		}
	})

	step("annotations on one Ingress replace the proxy-wide header for its host only", func(t *testing.T) {
		eventually(t, afterASync, "the host has its own header", func() string { return c.hstsIs(hstsOverrideHost, 600, true) })
		// Control: the other host still has the proxy-wide value.
		if reason := c.hstsIs(hstsDefaultHost, hstsProxyAge, true); reason != "" {
			t.Error(reason)
		}
		file, _, err := c.routesFile()
		if err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`(?m)^    hsts:\n      max_age: 600\n      include_subdomains: true\n`).MatchString(file) {
			t.Errorf("the host has no hsts of its own in the routes file:\n%s", file)
		}
	})

	step("hsts: off removes the header for the host", func(t *testing.T) {
		eventually(t, afterASync, "the host has no header", func() string { return c.hstsIs(hstsOffHost, -1, false) })
		// Control: a host without the annotation still has one.
		if reason := c.hstsIs(hstsDefaultHost, hstsProxyAge, true); reason != "" {
			t.Error(reason)
		}
	})

	step("an invalid hsts-max-age is warned, the file loads and the host keeps the proxy-wide header", func(t *testing.T) {
		eventually(t, soon, "InvalidHSTS is warned on the Ingress", func() string { return c.warned(appsNamespace, "hbadage", "InvalidHSTS") })
		eventually(t, afterASync, "the host falls back to the proxy-wide header", func() string { return c.hstsIs(hstsBadAgeHost, hstsProxyAge, true) })
		// The other hosts of the same file are served: it was not refused.
		if reason := c.hstsIs(hstsOverrideHost, 600, true); reason != "" {
			t.Error(reason)
		}
		cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("E2E_KUBECONFIG"))
		if err != nil {
			t.Fatal(err)
		}
		cs, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var pods corev1.PodList
		if err := c.k8s.List(c.ctx, &pods, client.InNamespace(proxyNamespace), client.MatchingLabels{proxyLabel: proxyKey.Name}); err != nil || len(pods.Items) == 0 {
			t.Fatalf("no pod of the proxy (%v)", err)
		}
		for _, p := range pods.Items {
			raw, err := cs.CoreV1().Pods(proxyNamespace).GetLogs(p.Name, &corev1.PodLogOptions{}).DoRaw(c.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) == 0 {
				t.Errorf("the log of %s is empty, so its absence of an error says nothing", p.Name)
			}
			if strings.Contains(string(raw), "Failed to parse") {
				t.Errorf("Synapse failed to parse the routes file:\n%s", raw)
			}
		}
	})

	step("two Ingresses that disagree on a host: the first wins, the second is warned", func(t *testing.T) {
		eventually(t, soon, "HostSettingConflict is warned on the second Ingress", func() string { return c.warned(appsNamespace, "hconf-b", "HostSettingConflict") })
		if reason := c.warned(appsNamespace, "hconf-a", "HostSettingConflict"); reason == "" {
			t.Error("the first Ingress is warned of a conflict")
		}
		eventually(t, afterASync, "the host has the first Ingress's header", func() string { return c.hstsIs(hstsConflictHost, 111, false) })
	})

	step("the API server refuses preload without a year and includeSubdomains", func(t *testing.T) {
		p := c.proxy(t)
		p.Spec.HSTS = &synapsev1alpha1.HSTSSpec{MaxAgeSeconds: 100, Preload: true}
		err := c.k8s.Update(c.ctx, p)
		if err == nil {
			t.Fatal("the API server took spec.hsts with preload and maxAgeSeconds 100")
		}
		if !strings.Contains(err.Error(), "preload needs maxAgeSeconds") {
			t.Errorf("refused for another reason: %v", err)
		}
		if got := c.proxy(t).Spec.HSTS; got == nil || got.MaxAgeSeconds != hstsProxyAge || got.Preload {
			t.Errorf("the stored spec.hsts is %+v", got)
		}
	})
}
