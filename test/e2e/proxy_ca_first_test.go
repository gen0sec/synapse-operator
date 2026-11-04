//go:build e2e

package e2e

import (
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// paddedCA is a CA bundle of about size bytes: the certificate and comment
// lines, which a PEM reader skips.
func paddedCA(a authority, size int) []byte {
	pad := "#" + strings.Repeat("-", 62) + "\n"
	return append(append([]byte{}, a.pem...), strings.Repeat(pad, (size-len(a.pem))/len(pad))...)
}

// The CAs of a namespace share 128 KiB in the routes. The proxy-wide CA is
// taken first, so tenants that fill the proxy's own namespace cannot get it
// refused.
func TestProxyWideCAIsKeptWhenTenantsFillTheNamespaceShare(t *testing.T) {
	c := newCluster(t)
	proxyAuthority := newAuthority(t, "e2e proxy-wide CA")
	proxyCA := paddedCA(proxyAuthority, 16*1024)

	eventually(t, soon, "the operator is available", c.operatorAvailable)
	c.clear(t)
	c.create(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
	)

	// A back-end only the proxy-wide CA vouches for.
	secureCert, secureKey := proxyAuthority.issue(t, "secure"+backendDomain)
	c.create(t, c.tlsBackend("secure", secureCert, secureKey)...)

	// Two tenants in the proxy's namespace, whose CAs of 60 KiB each do not
	// fit beside the proxy-wide one: the second is refused.
	tenantCert, tenantKey, _ := certificate(t, "tenant"+backendDomain)
	for _, obj := range c.tlsBackend("tenant", tenantCert, tenantKey) {
		obj.SetNamespace(proxyNamespace)
		c.create(t, obj)
	}
	tenants := []client.Object{}
	for _, n := range []string{"a", "b"} {
		ca := paddedCA(newAuthority(t, "e2e tenant "+n), 60*1024)
		ing := httpsIngress("tenant-"+n, "tenant-"+n+".e2e.test", "tenant", map[string]string{"upstream-tls-ca": "ca-" + n})
		ing.Namespace = proxyNamespace
		tenants = append(tenants, caSecret(proxyNamespace, "ca-"+n, ca), ing)
	}

	group, scope, ns := synapsev1alpha1.GroupVersion.Group, networkingv1.IngressClassParametersReferenceScopeNamespace, proxyNamespace
	c.create(t,
		&networkingv1.IngressClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec: networkingv1.IngressClassSpec{
				Controller: controllerName,
				Parameters: &networkingv1.IngressClassParametersReference{APIGroup: &group, Kind: "SynapseProxy", Name: proxyKey.Name, Scope: &scope, Namespace: &ns},
			},
		},
		caSecret(proxyNamespace, proxyCASecret, proxyCA),
		httpsIngress("secure", secureHost, "secure", nil),
	)
	c.create(t, tenants...)
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

	eventually(t, afterASync, "the last tenant CA is refused as unusable", func() string {
		return c.warned(proxyNamespace, "tenant-b", "UpstreamCAUnusable")
	})
	eventually(t, afterASync, "the route that relies on the proxy-wide CA is served", func() string {
		return c.routed("http", secureHost, "secure")
	})

	file, cas, err := c.routesFile()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^upstream_tls:\n(  .*\n)*  ca: \S*ca-[0-9a-f]{16}\.pem`).MatchString(file) {
		t.Errorf("the routes file has no proxy-wide upstream_tls.ca:\n%s", file)
	}
	// The proxy-wide CA and the first tenant's; the second tenant's is out.
	if len(cas) != 2 {
		t.Errorf("the routes ConfigMap has %d CA files %v, want 2: the proxy-wide CA and the first tenant's", len(cas), cas)
	}
}
