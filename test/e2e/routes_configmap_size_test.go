//go:build e2e

package e2e

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The routes file and the CA files of a SynapseProxy share one ConfigMap,
// which the API server refuses above 1 MiB. The operator keeps them under
// 1 MiB minus 16 KiB: a render that is over drops the CA file added last, and
// is done again.
const (
	routesBudget = 1024*1024 - 16*1024

	// Three CAs of 40 KiB fill 120 of the 128 KiB a namespace may bring, and
	// are all added before any pad route. 885 pad routes of about 1050 bytes
	// make the routes file about 930 KB: with all three CAs it is over the
	// budget, and with two it is under.
	sizeCAKiB   = 40
	sizePads    = 885
	sizeMorePad = 200
)

var sizeCAHosts = []string{"sizea.e2e.test", "sizeb.e2e.test", "sizec.e2e.test"}

// sizeRoutesDoc is the part of the routes file that says which route has a CA.
type sizeRoutesDoc struct {
	Hosts map[string]struct {
		Paths map[string]struct {
			UpstreamTLS *struct {
				CA string `json:"ca"`
			} `json:"upstream_tls"`
		} `json:"paths"`
	} `json:"hosts"`
}

// padIngress is a route that carries no CA. Its long path makes the routes
// file big without many objects.
func padIngress(i int) *networkingv1.Ingress {
	ing := ingress(fmt.Sprintf("pad-%04d", i), className, fmt.Sprintf("pad%04d.e2e.test", i), "plain")
	ing.Spec.Rules[0].HTTP.Paths[0].Path = "/" + strings.Repeat("x", 900)
	return ing
}

func cmSize(data map[string]string) int {
	n := 0
	for k, v := range data {
		n += len(k) + len(v)
	}
	return n
}

// eventsOn lists the Warning Events of reason on the object.
func (c *cluster) eventsOn(namespace, object, reason string) ([]corev1.Event, error) {
	var list corev1.EventList
	if err := c.k8s.List(c.ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var out []corev1.Event
	for _, e := range list.Items {
		if e.Reason == reason && e.InvolvedObject.Name == object && e.Type == corev1.EventTypeWarning {
			out = append(out, e)
		}
	}
	return out, nil
}

func (c *cluster) routesConfigMapObject() (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: routesConfigMap}, &cm)
	return &cm, err
}

func TestRoutesConfigMapStaysUnderItsLimit(t *testing.T) {
	c := newCluster(t)
	cfg, err := clientcmd.BuildConfigFromFlags("", env(t, "E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Hundreds of objects: the default client waits between requests.
	cfg.QPS, cfg.Burst = 200, 400
	fast, err := client.New(cfg, client.Options{Scheme: c.k8s.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	step := func(name string, run func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, run) {
			t.FailNow()
		}
	}

	var authorities []authority
	var bundles [][]byte
	for _, n := range []string{"a", "b", "c"} {
		a := newAuthority(t, "e2e size CA "+n)
		authorities = append(authorities, a)
		bundles = append(bundles, paddedCA(a, sizeCAKiB*1024))
	}

	// noFailure fails the test when Synapse said it could not read its routes.
	noFailure := func(t *testing.T) {
		t.Helper()
		var pods corev1.PodList
		if err := c.k8s.List(c.ctx, &pods, client.InNamespace(proxyNamespace), client.MatchingLabels{proxyLabel: proxyKey.Name}); err != nil || len(pods.Items) == 0 {
			t.Fatalf("no pod of the proxy (%v)", err)
		}
		for _, p := range pods.Items {
			raw, err := cs.CoreV1().Pods(proxyNamespace).GetLogs(p.Name, &corev1.PodLogOptions{Container: "synapse"}).DoRaw(c.ctx)
			if err != nil {
				t.Fatalf("log of %s: %v", p.Name, err)
			}
			if strings.Contains(string(raw), "Failed to parse") {
				t.Errorf("Synapse failed to parse its routes:\n%s", raw)
			}
		}
	}

	var firstData map[string]string
	var firstVersion string

	step("the CA file added last is dropped when the routes ConfigMap would be over its budget", func(t *testing.T) {
		eventually(t, soon, "the operator is available", c.operatorAvailable)
		c.clear(t)
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("plain")...)
		// "ca-a" < "ca-b" < "ca-c" < "pad-...": the routes render in this
		// order, so the CA of ca-c is the one added last.
		for i, n := range []string{"a", "b", "c"} {
			certPEM, keyPEM := authorities[i].issue(t, "tls"+n+backendDomain)
			c.create(t, c.tlsBackend("tls"+n, certPEM, keyPEM)...)
			c.create(t,
				caSecret(appsNamespace, "ca-"+n, bundles[i]),
				httpsIngress("ca-"+n, sizeCAHosts[i], "tls"+n, map[string]string{"upstream-tls-ca": "ca-" + n}))
		}
		for i := 0; i < sizePads; i++ {
			if err := fast.Create(c.ctx, padIngress(i)); err != nil {
				t.Fatalf("create pad %d: %v", i, err)
			}
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
			&synapsev1alpha1.SynapseProxy{
				ObjectMeta: metav1.ObjectMeta{Namespace: proxyKey.Namespace, Name: proxyKey.Name},
				Spec: synapsev1alpha1.SynapseProxySpec{
					Image:     c.synapseImage,
					Service:   &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
				},
			},
		)

		var cm *corev1.ConfigMap
		eventually(t, afterASync, "the routes ConfigMap is written, with the last CA's route in it", func() string {
			var err error
			if cm, err = c.routesConfigMapObject(); err != nil {
				return err.Error()
			}
			if !strings.Contains(cm.Data[routesKey], sizeCAHosts[2]) {
				return "the routes do not name " + sizeCAHosts[2]
			}
			return ""
		})
		// The pads arrive while the operator renders: let the ConfigMap settle
		// before it is taken as the reference for step 2.
		for stable, deadline := 0, time.Now().Add(3*time.Minute); stable < 4 && time.Now().Before(deadline); {
			time.Sleep(3 * time.Second)
			next, err := c.routesConfigMapObject()
			if err != nil {
				t.Fatal(err)
			}
			if next.ResourceVersion == cm.ResourceVersion {
				stable++
			} else {
				stable = 0
			}
			cm = next
		}
		firstData, firstVersion = cm.Data, cm.ResourceVersion

		if got := cmSize(cm.Data); got > routesBudget {
			t.Errorf("the routes ConfigMap holds %d bytes, over the budget of %d", got, routesBudget)
		}
		// The two CAs that fit are kept, the last one is not there.
		var files int
		for k, v := range cm.Data {
			if k == routesKey {
				continue
			}
			files++
			switch v {
			case string(bundles[0]), string(bundles[1]):
			case string(bundles[2]):
				t.Errorf("the dropped CA is still in the ConfigMap as %s", k)
			default:
				t.Errorf("unexpected key %s in the ConfigMap", k)
			}
		}
		if files != 2 {
			t.Errorf("the ConfigMap has %d CA files, want the first two", files)
		}
		var doc sizeRoutesDoc
		if err := yaml.Unmarshal([]byte(cm.Data[routesKey]), &doc); err != nil {
			t.Fatalf("the routes are not YAML: %v", err)
		}
		for i, h := range sizeCAHosts {
			p, ok := doc.Hosts[h].Paths["/"]
			switch {
			case !ok || p.UpstreamTLS == nil:
				t.Errorf("%s has no upstream_tls: it must stay a route over HTTPS", h)
			case i < 2 && !strings.HasPrefix(p.UpstreamTLS.CA, "/etc/synapse/upstreams/ca-"):
				t.Errorf("%s: ca is %q, want a CA file kept", h, p.UpstreamTLS.CA)
			case i == 2 && p.UpstreamTLS.CA != "":
				t.Errorf("%s: ca is %q, want none", h, p.UpstreamTLS.CA)
			}
		}
		if len(doc.Hosts) < sizePads {
			t.Errorf("the routes hold %d hosts, want the %d pads and the CA hosts", len(doc.Hosts), sizePads)
		}

		eventually(t, soon, "the owner of the dropped CA is warned", func() string {
			evs, err := c.eventsOn(appsNamespace, "ca-c", "UpstreamCAUnusable")
			if err != nil {
				return err.Error()
			}
			for _, e := range evs {
				if strings.Contains(e.Message, "the routes ConfigMap is full") {
					t.Logf("event on ca-c, seen %d time(s): %s", e.Count, e.Message)
					return ""
				}
			}
			return fmt.Sprintf("%d UpstreamCAUnusable events on ca-c, none says the routes ConfigMap is full", len(evs))
		})
		for _, n := range []string{"ca-a", "ca-b"} {
			evs, err := c.eventsOn(appsNamespace, n, "UpstreamCAUnusable")
			if err != nil {
				t.Fatal(err)
			}
			if len(evs) != 0 {
				t.Errorf("%s keeps its CA, and has an UpstreamCAUnusable event: %s", n, evs[0].Message)
			}
		}

		eventually(t, afterASync, "the proxy becomes ready", c.ready)
		eventually(t, afterASync, "a route that kept its CA is served", func() string { return c.routed("http", sizeCAHosts[0], "tlsa") })
		eventually(t, soon, "the other kept route is served", func() string { return c.routed("http", sizeCAHosts[1], "tlsb") })
		noFailure(t)
	})

	step("routes alone over the budget fail the render and leave the ConfigMap as it was", func(t *testing.T) {
		for i := sizePads; i < sizePads+sizeMorePad; i++ {
			if err := fast.Create(c.ctx, padIngress(i)); err != nil {
				t.Fatalf("create pad %d: %v", i, err)
			}
		}
		eventually(t, afterASync, "the proxy says the routes are too large", func() string {
			evs, err := c.eventsOn(proxyNamespace, proxyKey.Name, "RoutesNotRendered")
			if err != nil {
				return err.Error()
			}
			for _, e := range evs {
				if strings.Contains(e.Message, "RoutesTooLarge") {
					t.Logf("event on the proxy, seen %d time(s): %s", e.Count, e.Message)
					return ""
				}
			}
			return fmt.Sprintf("%d RoutesNotRendered events, none says RoutesTooLarge", len(evs))
		})
		time.Sleep(5 * time.Second)
		cm, err := c.routesConfigMapObject()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(cm.Data, firstData) || cm.ResourceVersion != firstVersion {
			t.Errorf("the ConfigMap changed (version %s to %s, %d to %d bytes), want it as it was", firstVersion, cm.ResourceVersion, cmSize(firstData), cmSize(cm.Data))
		}
		if cmSize(cm.Data) > routesBudget {
			t.Errorf("the routes ConfigMap holds %d bytes, over the budget of %d", cmSize(cm.Data), routesBudget)
		}
		if reason := c.routed("http", sizeCAHosts[0], "tlsa"); reason != "" {
			t.Errorf("a route of the last good render is no longer served: %s", reason)
		}
		noFailure(t)
	})
}
