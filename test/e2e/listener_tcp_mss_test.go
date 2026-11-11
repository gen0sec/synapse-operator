//go:build e2e

package e2e

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// spec.listeners[].tcpMSS reaches the proxy's config on its listener only,
// Synapse starts and serves with the key, and the API refuses a value below
// the minimum. The clamp itself needs a BPF firewall mode, which this
// cluster does not give, so it is not checked here.
func TestListenerTCPMSSIsRendered(t *testing.T) {
	c := newCluster(t)
	eventually(t, soon, "the operator is available", c.operatorAvailable)
	c.clear(t)
	c.create(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
	)
	c.create(t, c.backend("shop")...)

	group, scope, ns := synapsev1alpha1.GroupVersion.Group, networkingv1.IngressClassParametersReferenceScopeNamespace, proxyNamespace
	c.create(t,
		&networkingv1.IngressClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec: networkingv1.IngressClassSpec{
				Controller: controllerName,
				Parameters: &networkingv1.IngressClassParametersReference{
					APIGroup: &group, Kind: "SynapseProxy", Name: proxyKey.Name, Scope: &scope, Namespace: &ns,
				},
			},
		},
		ingress("shop", className, shopHost, "shop"),
		&synapsev1alpha1.SynapseProxy{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyKey.Namespace, Name: proxyKey.Name},
			Spec: synapsev1alpha1.SynapseProxySpec{
				Image:   c.synapseImage,
				Service: &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
				Listeners: []synapsev1alpha1.Listener{
					{Name: "http", Port: 80, Protocol: "HTTP", TCPMSS: ptrTo(int32(1360))},
					{Name: "https", Port: 443, Protocol: "TLS"},
				},
			},
		},
	)

	eventually(t, afterASync, "the proxy becomes ready", c.ready)
	eventually(t, soon, "the proxy serves a route", func() string { return c.routed("http", shopHost, "shop") })

	d := c.deployment(t)
	secret, err := c.secret(proxyNamespace, configSecret(d))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Proxy struct {
			Listeners []map[string]any `json:"listeners"`
		} `json:"proxy"`
	}
	if err := yaml.Unmarshal(secret.Data["config.yaml"], &cfg); err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, l := range cfg.Proxy.Listeners {
		name, _ := l["name"].(string)
		if mss, ok := l["tcp_mss"]; ok {
			got[name] = mss
		}
	}
	if len(cfg.Proxy.Listeners) != 2 {
		t.Fatalf("the config has %d listeners, want 2: %v", len(cfg.Proxy.Listeners), cfg.Proxy.Listeners)
	}
	if fmt.Sprint(got) != "map[http:1360]" {
		t.Errorf("tcp_mss by listener is %v, want only http: 1360", got)
	}

	for _, mss := range []int32{87, 32768} {
		err := c.k8s.Create(c.ctx, &synapsev1alpha1.SynapseProxy{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "bad-mss"},
			Spec: synapsev1alpha1.SynapseProxySpec{
				Image:     c.synapseImage,
				Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP", TCPMSS: ptrTo(mss)}},
			},
		}, client.DryRunAll)
		if !apierrors.IsInvalid(err) {
			t.Errorf("tcpMSS %d: the API answered %v, want it refused as invalid", mss, err)
		}
	}
}
