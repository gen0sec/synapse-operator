//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

const (
	operatorNamespace = "synapse-os"
	// The proxy and what it serves are in different namespaces, as they
	// usually are: routes and certificates have to cross.
	proxyNamespace = "e2e-edge"
	appsNamespace  = "e2e-apps"
	className      = "e2e-edge"

	shopHost    = "shop.e2e.test"
	apiHost     = "api.e2e.test"
	otherHost   = "other.e2e.test"
	gatewayHost = "gw.e2e.test"

	// What a user, or a tool of theirs, sees of a proxy. Written out here
	// and not taken from the operator's code, on purpose: to rename one is a
	// breaking change, and this test is what should notice.
	controllerName       = "gen0sec.com/synapse"
	conditionReady       = "Ready"
	conditionConfigValid = "ConfigValid"
	reasonInvalidConfig  = "InvalidConfig"
	reasonConfigInvalid  = "ConfigInvalid"
	proxyLabel           = "synapse.gen0sec.com/proxy"
	routesConfigMap      = "edge-upstreams"
	routesKey            = "upstreams.yaml"
	certificatesSecret   = "edge-certs"

	// How long a change is given to show. A proxy's routes and certificates
	// reach its pods through mounted volumes, which the kubelet refreshes
	// about once a minute; more often while a pod is new, which is why these
	// steps can be over in a second.
	soon       = 2 * time.Minute
	afterASync = 5 * time.Minute
)

var proxyKey = types.NamespacedName{Namespace: proxyNamespace, Name: "edge"}

// cluster is the cluster under test and the two addresses its load balancer
// is reached at from here.
type cluster struct {
	ctx context.Context
	k8s client.Client
	// gatewayAPI says the cluster has the Gateway API's kinds.
	gatewayAPI bool

	httpAddr, httpsAddr        string
	synapseImage, backendImage string
}

func env(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s is not set; test/e2e/run.sh sets up a cluster and everything this test needs", name)
	}
	return v
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	// From the named file only: never whatever cluster kubectl points at.
	cfg, err := clientcmd.BuildConfigFromFlags("", env(t, "E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	// A request that gets no answer fails, and is made again, where it
	// would otherwise hold the test up until the whole run is cancelled.
	cfg.Timeout = 30 * time.Second
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, synapsev1alpha1.AddToScheme, gwv1.Install} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	// Whether the cluster has the Gateway API is asked of the cluster, not
	// of the operator: an older one refuses its CRDs.
	gatewayAPI := false
	if dc, err := discovery.NewDiscoveryClientForConfig(cfg); err == nil {
		if list, err := dc.ServerResourcesForGroupVersion(gwv1.GroupName + "/v1"); err == nil {
			gatewayAPI = slices.ContainsFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Kind == "HTTPRoute" })
		}
	}
	return &cluster{
		gatewayAPI: gatewayAPI,
		ctx:        context.Background(), k8s: k8s,
		httpAddr: env(t, "E2E_HTTP_ADDR"), httpsAddr: env(t, "E2E_HTTPS_ADDR"),
		synapseImage: env(t, "E2E_SYNAPSE_IMAGE"), backendImage: env(t, "E2E_BACKEND_IMAGE"),
	}
}

// eventually polls check until it returns "" or the time is up, and fails
// the test with the last reason it gave. It returns how long it took.
func eventually(t *testing.T, within time.Duration, what string, check func() string) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	for {
		reason := check()
		if reason == "" {
			return time.Since(start).Round(time.Second)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: after %s, %s", what, within, reason)
		}
		time.Sleep(time.Second)
	}
}

func (c *cluster) create(t *testing.T, objs ...client.Object) {
	t.Helper()
	for _, obj := range objs {
		if err := c.k8s.Create(c.ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}
}

// clear removes what an earlier run on the same cluster left.
func (c *cluster) clear(t *testing.T) {
	t.Helper()
	leftovers := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		&networkingv1.IngressClass{ObjectMeta: metav1.ObjectMeta{Name: className}},
	}
	if c.gatewayAPI {
		leftovers = append(leftovers, &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: className}})
	}
	for _, obj := range leftovers {
		if err := c.k8s.Delete(c.ctx, obj); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	}
	eventually(t, soon, "what an earlier run left is removed", func() string {
		for _, obj := range leftovers {
			if err := c.k8s.Get(c.ctx, client.ObjectKeyFromObject(obj), obj); !apierrors.IsNotFound(err) {
				return fmt.Sprintf("%T %s is still there (%v)", obj, obj.GetName(), err)
			}
		}
		return ""
	})
}

// backend is a Deployment of the test's HTTP server and the Service in front
// of it. Every response names the backend it came from.
func (c *cluster) backend(name string) []client.Object {
	labels := map[string]string{"app": name}
	return []client.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:            "backend",
						Image:           c.backendImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env:             []corev1.EnvVar{{Name: "NAME", Value: name}},
						Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromInt32(8080)},
						}},
					}}},
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name},
			Spec: corev1.ServiceSpec{
				Selector: labels,
				Ports:    []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}},
			},
		},
	}
}

// ingress routes host to the backend of the given name, for the given class.
func ingress(name, class, host, backend string) *networkingv1.Ingress {
	prefix := networkingv1.PathTypePrefix
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: name},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &class,
			Rules: []networkingv1.IngressRule{{
				Host: host,
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path: "/", PathType: &prefix,
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
							Name: backend, Port: networkingv1.ServiceBackendPort{Number: 80},
						}},
					}},
				}},
			}},
		},
	}
}

// certificate makes a self-signed certificate for host. It returns the PEM
// pair a TLS Secret holds and the certificate as a server presents it.
func certificate(t *testing.T, host string) (certPEM, keyPEM, der []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), der
}

// answer is what came back for one request.
type answer struct {
	status  int
	backend string
	// header holds the response's headers.
	header http.Header
	// served is the certificate the server presented, over TLS.
	served []byte
}

// get sends one request for host through the load balancer, on a connection
// of its own.
func (c *cluster) get(scheme, host string) (answer, error) {
	return c.request(http.MethodGet, scheme, host, "/")
}

// request sends one request, as get does, with a method and a path.
func (c *cluster) request(method, scheme, host, path string) (answer, error) {
	addr := c.httpAddr
	if scheme == "https" {
		addr = c.httpsAddr
	}
	requests := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, addr)
			},
			// Not verified here: the test compares the certificate itself.
			TLSClientConfig: &tls.Config{ServerName: host, InsecureSkipVerify: true},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequest(method, scheme+"://"+host+path, nil)
	if err != nil {
		return answer{}, err
	}
	resp, err := requests.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	a := answer{status: resp.StatusCode, backend: resp.Header.Get("X-Backend"), header: resp.Header}
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		a.served = resp.TLS.PeerCertificates[0].Raw
	}
	return a, nil
}

// routed reports "" when a request for host reaches backend.
func (c *cluster) routed(scheme, host, backend string) string {
	a, err := c.get(scheme, host)
	switch {
	case err != nil:
		return err.Error()
	case a.status != http.StatusOK || a.backend != backend:
		return fmt.Sprintf("%s://%s answered %d from backend %q, want 200 from %q", scheme, host, a.status, a.backend, backend)
	}
	return ""
}

// getProxy, getDeployment and listPods leave an error to the caller. Inside
// eventually, a request that failed is a reason to look again, not the end
// of the test; proxy, deployment and pods are for everywhere else.
func (c *cluster) getProxy() (*synapsev1alpha1.SynapseProxy, error) {
	var p synapsev1alpha1.SynapseProxy
	return &p, c.k8s.Get(c.ctx, proxyKey, &p)
}

func (c *cluster) proxy(t *testing.T) *synapsev1alpha1.SynapseProxy {
	t.Helper()
	p, err := c.getProxy()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// edit changes the proxy's spec, as a user would. The operator writes the
// proxy's status at any moment, so the update may have to be made again.
func (c *cluster) edit(t *testing.T, change func(*synapsev1alpha1.SynapseProxySpec)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		p, err := c.getProxy()
		if err != nil {
			return err
		}
		change(&p.Spec)
		return c.k8s.Update(c.ctx, p)
	})
	if err != nil {
		t.Fatalf("update the proxy: %v", err)
	}
}

// ready reports "" when the proxy says its spec, as it is now, is what runs.
func (c *cluster) ready() string {
	p, err := c.getProxy()
	if err != nil {
		return err.Error()
	}
	cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
	switch {
	case cond == nil:
		return "the proxy has no Ready condition"
	case cond.ObservedGeneration != p.Generation:
		return "the proxy's status is of an earlier spec"
	case cond.Status != metav1.ConditionTrue:
		return fmt.Sprintf("the proxy is not ready: %s: %s", cond.Reason, cond.Message)
	}
	return ""
}

func (c *cluster) getDeployment() (*appsv1.Deployment, error) {
	var d appsv1.Deployment
	return &d, c.k8s.Get(c.ctx, proxyKey, &d)
}

func (c *cluster) deployment(t *testing.T) *appsv1.Deployment {
	t.Helper()
	d, err := c.getDeployment()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// configSecret is the Secret the Deployment's pods get their config from.
func configSecret(d *appsv1.Deployment) string {
	for _, v := range d.Spec.Template.Spec.Volumes {
		if v.Name == "config" && v.Secret != nil {
			return v.Secret.SecretName
		}
	}
	return ""
}

// listPods returns the UIDs of the proxy's pods: those that are staying, or,
// with leaving, also those that have been told to stop and have not yet.
func (c *cluster) listPods(leaving bool) ([]types.UID, error) {
	var list corev1.PodList
	err := c.k8s.List(c.ctx, &list, client.InNamespace(proxyNamespace), client.MatchingLabels{proxyLabel: proxyKey.Name})
	if err != nil {
		return nil, err
	}
	var uids []types.UID
	for _, p := range list.Items {
		if leaving || p.DeletionTimestamp.IsZero() {
			uids = append(uids, p.UID)
		}
	}
	slices.Sort(uids)
	return uids, nil
}

// pods returns the UIDs of the proxy's pods that are not on their way out.
func (c *cluster) pods(t *testing.T) []types.UID {
	t.Helper()
	uids, err := c.listPods(false)
	if err != nil {
		t.Fatal(err)
	}
	return uids
}

func (c *cluster) operatorAvailable() string {
	var d appsv1.Deployment
	if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: operatorNamespace, Name: "synapse-operator"}, &d); err != nil {
		return err.Error()
	}
	if d.Status.ObservedGeneration != d.Generation || d.Status.UpdatedReplicas < 1 || d.Status.AvailableReplicas != d.Status.Replicas {
		return fmt.Sprintf("the operator's Deployment is at %+v", d.Status)
	}
	return ""
}

// One proxy, from before it exists until after it is gone. The steps build on
// each other, so the first that fails ends the test.
func TestSynapseProxy(t *testing.T) {
	c := newCluster(t)
	step := func(name string, run func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, run) {
			t.FailNow()
		}
	}

	step("the operator is running", func(t *testing.T) {
		eventually(t, soon, "the operator is available", c.operatorAvailable)
		c.clear(t)
	})

	step("a proxy serves an Ingress that was there before it", func(t *testing.T) {
		c.create(t,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appsNamespace}},
		)
		c.create(t, c.backend("shop")...)
		c.create(t, c.backend("api")...)

		// The class hands its Ingresses to a proxy that does not exist yet.
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
		)

		c.create(t, &synapsev1alpha1.SynapseProxy{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyKey.Namespace, Name: proxyKey.Name},
			Spec: synapsev1alpha1.SynapseProxySpec{
				Image:   c.synapseImage,
				Service: &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
				Listeners: []synapsev1alpha1.Listener{
					{Name: "http", Port: 80, Protocol: "HTTP"},
					{Name: "https", Port: 443, Protocol: "TLS"},
				},
			},
		})
		// Given long: on a cluster that has just started this waits for more
		// than the proxy. The cluster pulls its load balancer's image the
		// first time a Service asks for one, and that can take a minute.
		ready := eventually(t, afterASync, "the proxy becomes ready", c.ready)

		p := c.proxy(t)
		if p.Status.ReadyReplicas != 1 || p.Status.ConfigHash == "" || len(p.Status.Addresses) == 0 {
			t.Errorf("a ready proxy reports %d ready replicas, configuration %q, addresses %v",
				p.Status.ReadyReplicas, p.Status.ConfigHash, p.Status.Addresses)
		}
		served := eventually(t, afterASync, "the Ingress is served", func() string { return c.routed("http", shopHost, "shop") })
		t.Logf("ready after %s, served %s after that", ready, served)

		// What it routes by is a file in Synapse's v2 schema, which says
		// what the v1 file it used to be said: the host has no certificate
		// and is served, and the backend is not sent the client's
		// fingerprints, which a v2 file has sent on unless it says no.
		var routes corev1.ConfigMap
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: routesConfigMap}, &routes); err != nil {
			t.Fatal(err)
		}
		if file := routes.Data[routesKey]; !strings.Contains(file, "\nversion: 2\n") {
			t.Errorf("the proxy's routes are not a v2 file:\n%s", file)
		}
		a, err := c.get("http", shopHost)
		if err != nil {
			t.Fatal(err)
		}
		seen := a.header.Get("X-Seen-Headers")
		if !strings.Contains(seen, "host") && !strings.Contains(seen, "accept") && !strings.Contains(seen, "user-agent") {
			t.Errorf("the backend names no request header it was sent: %q", seen)
		}
		for _, name := range strings.Split(seen, ",") {
			if strings.Contains(name, "ja4") || strings.Contains(name, "g0s") {
				t.Errorf("the backend was sent the client's fingerprint in %q; all it was sent: %s", name, seen)
			}
		}
	})

	step("a host nobody routes is not served", func(t *testing.T) {
		a, err := c.get("http", "nobody.e2e.test")
		if err != nil {
			t.Fatal(err)
		}
		if a.status != http.StatusNotFound || a.backend != "" {
			t.Errorf("answered %d from backend %q, want 404 from none", a.status, a.backend)
		}
	})

	step("the Ingress is told where the proxy is", func(t *testing.T) {
		eventually(t, soon, "the proxy's address is on the Ingress", func() string {
			var svc corev1.Service
			if err := c.k8s.Get(c.ctx, proxyKey, &svc); err != nil {
				return err.Error()
			}
			var ing networkingv1.Ingress
			if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: appsNamespace, Name: "shop"}, &ing); err != nil {
				return err.Error()
			}
			var want, got []string
			for _, in := range svc.Status.LoadBalancer.Ingress {
				want = append(want, in.IP+in.Hostname)
			}
			for _, in := range ing.Status.LoadBalancer.Ingress {
				got = append(got, in.IP+in.Hostname)
			}
			if len(want) == 0 || !slices.Equal(got, want) {
				return fmt.Sprintf("the Service is at %v and the Ingress says %v", want, got)
			}
			return ""
		})
	})

	certPEM, keyPEM, first := certificate(t, apiHost)
	apiTLS := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: "api-tls"},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}

	step("an Ingress added later is served, with its certificate, and one of another class is not", func(t *testing.T) {
		c.create(t, ingress("other", "someone-elses", otherHost, "shop"))
		api := ingress("api", className, apiHost, "api")
		api.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{apiHost}, SecretName: apiTLS.Name}}
		c.create(t, apiTLS, api)

		took := eventually(t, afterASync, "the new Ingress is served over TLS", func() string {
			if reason := c.routed("https", apiHost, "api"); reason != "" {
				return reason
			}
			if a, _ := c.get("https", apiHost); !bytes.Equal(a.served, first) {
				return "the certificate served is not the one in the Ingress's Secret"
			}
			return ""
		})
		t.Logf("served %s after it was created", took)

		// Rendered after the other class's Ingress was created, so its
		// absence is not a matter of time.
		var routes corev1.ConfigMap
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: routesConfigMap}, &routes); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(routes.Data[routesKey], otherHost) {
			t.Errorf("the proxy's routes include an Ingress of another class:\n%s", routes.Data[routesKey])
		}
		if a, err := c.get("http", otherHost); err != nil || a.status != http.StatusNotFound {
			t.Errorf("a host of another class answered %d (%v), want 404", a.status, err)
		}
		if reason := c.routed("http", shopHost, "shop"); reason != "" {
			t.Errorf("the first Ingress is no longer served: %s", reason)
		}
	})

	step("a renewed certificate is served", func(t *testing.T) {
		certPEM, keyPEM, renewed := certificate(t, apiHost)
		if err := c.k8s.Get(c.ctx, client.ObjectKeyFromObject(apiTLS), apiTLS); err != nil {
			t.Fatal(err)
		}
		apiTLS.Data = map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM}
		if err := c.k8s.Update(c.ctx, apiTLS); err != nil {
			t.Fatal(err)
		}
		took := eventually(t, afterASync, "the renewed certificate is served", func() string {
			a, err := c.get("https", apiHost)
			switch {
			case err != nil:
				return err.Error()
			case bytes.Equal(a.served, first):
				return "the old certificate is still served"
			case !bytes.Equal(a.served, renewed):
				return "a certificate is served that is neither the old nor the renewed one"
			case a.status != http.StatusOK || a.backend != "api":
				return fmt.Sprintf("answered %d from backend %q", a.status, a.backend)
			}
			return ""
		})
		t.Logf("served %s after it was renewed", took)
	})

	step("a Gateway's routes are served as the API says they match", func(t *testing.T) {
		if !c.gatewayAPI {
			t.Skip("this cluster does not have the Gateway API")
		}
		certPEM, keyPEM, cert := certificate(t, gatewayHost)
		group, ns := gwv1.Group(synapsev1alpha1.GroupVersion.Group), gwv1.Namespace(proxyNamespace)
		all := &gwv1.AllowedRoutes{Namespaces: &gwv1.RouteNamespaces{From: ptrTo(gwv1.NamespacesFromAll)}}
		host := gwv1.Hostname(gatewayHost)
		gw := &gwv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "web"},
			Spec: gwv1.GatewaySpec{
				GatewayClassName: className,
				Listeners: []gwv1.Listener{
					{Name: "http", Port: 80, Protocol: gwv1.HTTPProtocolType, AllowedRoutes: all},
					{Name: "https", Port: 443, Protocol: gwv1.HTTPSProtocolType, Hostname: &host, AllowedRoutes: all,
						TLS: &gwv1.ListenerTLSConfig{CertificateRefs: []gwv1.SecretObjectReference{{Name: "gw-tls"}}}},
				},
			},
		}
		match := func(kind gwv1.PathMatchType, path, method string) []gwv1.HTTPRouteMatch {
			m := gwv1.HTTPRouteMatch{Path: &gwv1.HTTPPathMatch{Type: &kind, Value: &path}}
			if method != "" {
				m.Method = ptrTo(gwv1.HTTPMethod(method))
			}
			return []gwv1.HTTPRouteMatch{m}
		}
		to := func(service string) []gwv1.HTTPBackendRef {
			var ref gwv1.HTTPBackendRef
			ref.Name, ref.Port = gwv1.ObjectName(service), ptrTo(gwv1.PortNumber(80))
			return []gwv1.HTTPBackendRef{ref}
		}
		parent := []gwv1.ParentReference{{Namespace: &ns, Name: "web"}}
		route := &gwv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: "gw"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: parent},
				Hostnames:       []gwv1.Hostname{host},
				Rules: []gwv1.HTTPRouteRule{
					{Matches: match(gwv1.PathMatchPathPrefix, "/", ""), BackendRefs: to("shop")},
					{Matches: match(gwv1.PathMatchExact, "/exact", ""), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchPathPrefix, "/api", "POST"), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchRegularExpression, "/v[0-9]+/items$", ""), BackendRefs: to("api")},
					// What means something to a regular expression: a dot
					// in a prefix, a class written with a backslash, and
					// an alternative, which an anchor in front does not hold.
					{Matches: match(gwv1.PathMatchPathPrefix, "/api/v1.0", ""), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchRegularExpression, `/n\d+/things`, ""), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchRegularExpression, "/foo|/bar", ""), BackendRefs: to("api")},
					// A rule the proxy cannot carry out. Its requests are
					// not the first rule's to serve.
					{Matches: match(gwv1.PathMatchPathPrefix, "/held", ""), BackendRefs: to("api"),
						Filters: []gwv1.HTTPRouteFilter{{Type: gwv1.HTTPRouteFilterURLRewrite, URLRewrite: &gwv1.HTTPURLRewriteFilter{
							Path: &gwv1.HTTPPathModifier{Type: gwv1.PrefixMatchHTTPPathModifier, ReplacePrefixMatch: ptrTo("/")},
						}}}},
				},
			},
		}
		// Headers, on a host of its own that has nothing but path prefixes.
		setRequest := gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: &gwv1.HTTPHeaderFilter{Set: []gwv1.HTTPHeader{{Name: "X-From-Gateway", Value: "yes"}}}}
		setResponse := gwv1.HTTPRouteFilter{Type: gwv1.HTTPRouteFilterResponseHeaderModifier,
			ResponseHeaderModifier: &gwv1.HTTPHeaderFilter{Set: []gwv1.HTTPHeader{{Name: "X-Resp", Value: "1"}}}}
		headers := &gwv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: "gw-headers"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: parent},
				Hostnames:       []gwv1.Hostname{"headers." + host},
				Rules: []gwv1.HTTPRouteRule{
					{Matches: match(gwv1.PathMatchPathPrefix, "/", ""), BackendRefs: to("shop"), Filters: []gwv1.HTTPRouteFilter{setRequest}},
					{Matches: match(gwv1.PathMatchPathPrefix, "/bare", ""), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchPathPrefix, "/resp", ""), BackendRefs: to("api"), Filters: []gwv1.HTTPRouteFilter{setResponse}},
				},
			},
		}
		// Every name under a wildcard, where Synapse 0.8.7 tries plain
		// paths and nothing else.
		wildcard := &gwv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: "gw-wildcard"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: parent},
				Hostnames:       []gwv1.Hostname{"*.wild." + host},
				Rules: []gwv1.HTTPRouteRule{
					{Matches: match(gwv1.PathMatchPathPrefix, "/", ""), BackendRefs: to("shop")},
					{Matches: match(gwv1.PathMatchPathPrefix, "/api", ""), BackendRefs: to("api")},
					{Matches: match(gwv1.PathMatchExact, "/only", ""), BackendRefs: to("api")},
				},
			},
		}
		// Something the proxy cannot do: it has to say so, not serve it as
		// if the condition were not there.
		onHeader := match(gwv1.PathMatchPathPrefix, "/canary", "")
		onHeader[0].Headers = []gwv1.HTTPHeaderMatch{{Name: "X-Canary", Value: "1"}}
		unsupported := &gwv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: appsNamespace, Name: "gw-on-header"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: parent},
				Hostnames:       []gwv1.Hostname{"canary." + host},
				Rules:           []gwv1.HTTPRouteRule{{Matches: onHeader, BackendRefs: to("api")}},
			},
		}
		c.create(t,
			&gwv1.GatewayClass{
				ObjectMeta: metav1.ObjectMeta{Name: className},
				Spec: gwv1.GatewayClassSpec{
					ControllerName: controllerName,
					ParametersRef:  &gwv1.ParametersReference{Group: group, Kind: "SynapseProxy", Name: proxyKey.Name, Namespace: &ns},
				},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: "gw-tls"},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
			},
			gw, route, unsupported, headers, wildcard,
		)

		accepted := func(rt *gwv1.HTTPRoute) (*metav1.Condition, string) {
			if err := c.k8s.Get(c.ctx, client.ObjectKeyFromObject(rt), rt); err != nil {
				return nil, err.Error()
			}
			if len(rt.Status.Parents) != 1 {
				return nil, fmt.Sprintf("route %s has %d parent statuses", rt.Name, len(rt.Status.Parents))
			}
			return meta.FindStatusCondition(rt.Status.Parents[0].Conditions, "Accepted"), ""
		}
		eventually(t, soon, "each object is told what became of it", func() string {
			if err := c.k8s.Get(c.ctx, client.ObjectKeyFromObject(gw), gw); err != nil {
				return err.Error()
			}
			switch cond, reason := accepted(route); {
			case reason != "":
				return reason
			case cond == nil || cond.Status != metav1.ConditionTrue:
				return fmt.Sprintf("the route is %+v", cond)
			}
			switch cond, reason := accepted(unsupported); {
			case reason != "":
				return reason
			case cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "UnsupportedValue":
				return fmt.Sprintf("the route that matches on a header is %+v, want False/UnsupportedValue", cond)
			}
			// What is left out of a route that is served is said too.
			for rt, part := range map[*gwv1.HTTPRoute]string{route: "URLRewrite", wildcard: "wildcard host", headers: ""} {
				switch cond, reason := accepted(rt); {
				case reason != "":
					return reason
				case cond == nil || cond.Status != metav1.ConditionTrue:
					return fmt.Sprintf("route %s is %+v", rt.Name, cond)
				}
				partly := meta.FindStatusCondition(rt.Status.Parents[0].Conditions, "PartiallyInvalid")
				switch {
				case part == "" && partly != nil:
					return fmt.Sprintf("route %s can be served whole, yet: %s", rt.Name, partly.Message)
				case part != "" && (partly == nil || !strings.Contains(partly.Message, part)):
					return fmt.Sprintf("route %s does not say what of it is left out: %+v", rt.Name, partly)
				}
			}
			switch {
			case !meta.IsStatusConditionTrue(gw.Status.Conditions, "Programmed"):
				return fmt.Sprintf("the Gateway is %+v", gw.Status.Conditions)
			case len(gw.Status.Addresses) == 0:
				return "the Gateway has no address"
			// Both routes attach to the listener for every host, the one
			// that cannot be programmed too: attaching is about who may,
			// not about what the route then asks for. Only the first is
			// for the host the other listener serves.
			case len(gw.Status.Listeners) != 2 || gw.Status.Listeners[0].AttachedRoutes != 4 || gw.Status.Listeners[1].AttachedRoutes != 1:
				return fmt.Sprintf("the listeners are %+v", gw.Status.Listeners)
			}
			return ""
		})

		// Which backend a request reaches is decided by Synapse, from
		// expressions the operator wrote: each of these is one where a
		// looser reading of the route would answer differently.
		requests := []struct{ method, path, backend string }{
			{"GET", "/", "shop"},
			{"GET", "/exact", "api"},
			{"GET", "/exact/more", "shop"},
			{"GET", "/exactly", "shop"},
			{"POST", "/api/x", "api"},
			{"GET", "/api/x", "shop"},
			{"POST", "/apiary", "shop"},
			{"GET", "/v2/items", "api"},
			{"GET", "/v2/items/9", "shop"},
			{"GET", "/api/v1.0/users", "api"},
			{"GET", "/api/v1X0/users", "shop"},
			{"GET", "/n7/things", "api"},
			{"GET", "/nx/things", "shop"},
			{"GET", "/bar", "api"},
			{"GET", "/x/bar", "shop"},
		}
		took := eventually(t, afterASync, "requests reach the backend the route says", func() string {
			for _, r := range requests {
				a, err := c.request(r.method, "http", gatewayHost, r.path)
				switch {
				case err != nil:
					return err.Error()
				case a.status != http.StatusOK || a.backend != r.backend:
					return fmt.Sprintf("%s %s answered %d from %q, want 200 from %q", r.method, r.path, a.status, a.backend, r.backend)
				}
			}
			a, err := c.request("GET", "https", gatewayHost, "/exact")
			switch {
			case err != nil:
				return err.Error()
			case !bytes.Equal(a.served, cert):
				return "the certificate served is not the Gateway listener's"
			case a.backend != "api":
				return fmt.Sprintf("over TLS, /exact answered from %q", a.backend)
			}
			return ""
		})
		t.Logf("served %s after they were created", took)
		if a, err := c.request("GET", "http", "canary."+gatewayHost, "/canary"); err != nil || a.status != http.StatusNotFound {
			t.Errorf("a route that could not be programmed answers %d (%v), want 404", a.status, err)
		}
		// Under the first rule's prefix, and not the first rule's.
		for _, path := range []string{"/held", "/held/x"} {
			if a, err := c.request("GET", "http", gatewayHost, path); err != nil || a.status != http.StatusNotFound {
				t.Errorf("GET %s, of a rule that cannot be carried out, answers %d from %q (%v), want 404", path, a.status, a.backend, err)
			}
		}

		// Headers are set by Synapse, by rules of its own about which
		// route's it takes for a request.
		type headerCase struct {
			path, backend string
			// seen is what the backend was sent in X-From-Gateway, and
			// resp what the client got in X-Resp.
			seen, resp string
		}
		eventually(t, afterASync, "each rule's headers are set, on its requests and on no others", func() string {
			for _, hc := range []headerCase{
				{"/", "shop", "yes", ""},
				{"/deep/down", "shop", "yes", ""},
				// Not the ones of the rule above it.
				{"/bare/x", "api", "", ""},
				{"/resp", "api", "", "1"},
			} {
				a, err := c.request("GET", "http", "headers."+gatewayHost, hc.path)
				switch {
				case err != nil:
					return err.Error()
				case a.status != http.StatusOK || a.backend != hc.backend:
					return fmt.Sprintf("%s answered %d from %q, want 200 from %q", hc.path, a.status, a.backend, hc.backend)
				case a.header.Get("X-Seen-From-Gateway") != hc.seen:
					return fmt.Sprintf("%s: the backend was sent X-From-Gateway %q, want %q", hc.path, a.header.Get("X-Seen-From-Gateway"), hc.seen)
				case a.header.Get("X-Resp") != hc.resp:
					return fmt.Sprintf("%s: the client got X-Resp %q, want %q", hc.path, a.header.Get("X-Resp"), hc.resp)
				// What is set for the backend is not sent back.
				case a.header.Get("X-From-Gateway") != "":
					return fmt.Sprintf("%s: the client got the request header back: X-From-Gateway %q", hc.path, a.header.Get("X-From-Gateway"))
				}
			}
			return ""
		})

		eventually(t, afterASync, "a wildcard host serves its path prefixes", func() string {
			// The exact path is left out, and its requests are the
			// first rule's as if it were not there.
			for path, backend := range map[string]string{"/": "shop", "/api/x": "api", "/only": "shop"} {
				for _, name := range []string{"a.wild." + gatewayHost, "deep.er.wild." + gatewayHost} {
					a, err := c.request("GET", "http", name, path)
					switch {
					case err != nil:
						return err.Error()
					case a.status != http.StatusOK || a.backend != backend:
						return fmt.Sprintf("%s%s answered %d from %q, want 200 from %q", name, path, a.status, a.backend, backend)
					}
				}
			}
			return ""
		})
	})

	step("a configuration change replaces the pods without a failed request", func(t *testing.T) {
		before := c.deployment(t)
		oldConfig, oldPods := configSecret(before), c.pods(t)

		// Requests through the load balancer, each on a new connection, from
		// a few clients at once, until the old pods have stopped. A pod that
		// stops while it is still being sent connections fails one of them.
		var mu sync.Mutex
		var sent int
		var failed []string
		stop := make(chan struct{})
		var clients sync.WaitGroup
		for range 4 {
			clients.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					reason := c.routed("http", shopHost, "shop")
					mu.Lock()
					sent++
					if reason != "" {
						failed = append(failed, time.Now().Format("15:04:05.000 ")+reason)
					}
					mu.Unlock()
					time.Sleep(10 * time.Millisecond)
				}
			})
		}

		// Every field that ends up in Synapse's configuration, and a
		// setting passed through as it is: Synapse has to start with what
		// the operator renders from them.
		c.edit(t, func(s *synapsev1alpha1.SynapseProxySpec) {
			s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"}
			s.TLS = &synapsev1alpha1.TLSSpec{Grade: "high"}
			s.TrustedProxies = []string{"10.0.0.0/8", "fd00::/8"}
			s.Config = &runtime.RawExtension{Raw: []byte(`{"proxy":{"h2c":false}}`)}
		})
		eventually(t, afterASync, "the pods are replaced and the old configuration removed", func() string {
			if reason := c.ready(); reason != "" {
				return reason
			}
			d, err := c.getDeployment()
			if err != nil {
				return err.Error()
			}
			if now := configSecret(d); now == oldConfig {
				return "the pods still mount " + now
			}
			// Stopped, not told to stop. An old pod goes on serving for a
			// while after it is told, and is stopped only then: the requests
			// have to keep coming until that has happened.
			all, err := c.listPods(true)
			if err != nil {
				return err.Error()
			}
			for _, uid := range all {
				if slices.Contains(oldPods, uid) {
					return "a pod with the old configuration has not stopped yet"
				}
			}
			err = c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: oldConfig}, &corev1.Secret{})
			if !apierrors.IsNotFound(err) {
				return fmt.Sprintf("the old configuration %s is still there (%v)", oldConfig, err)
			}
			return ""
		})
		close(stop)
		clients.Wait()

		t.Logf("%d requests during the rollout", sent)
		if sent < 100 {
			t.Errorf("only %d requests were sent during the rollout; that shows nothing", sent)
		}
		if len(failed) > 0 {
			t.Errorf("%d of %d requests failed during the rollout:\n  %s", len(failed), sent, strings.Join(failed, "\n  "))
		}
	})

	step("a configuration that cannot be rendered is reported, and changes nothing that runs", func(t *testing.T) {
		before := c.deployment(t)
		pods := c.pods(t)

		// mode is the operator's to set.
		valid := c.proxy(t).Spec.Config
		c.edit(t, func(s *synapsev1alpha1.SynapseProxySpec) {
			s.Config = &runtime.RawExtension{Raw: []byte(`{"mode":"agent"}`)}
		})
		eventually(t, soon, "the proxy reports the configuration", func() string {
			p, err := c.getProxy()
			if err != nil {
				return err.Error()
			}
			rendered := meta.FindStatusCondition(p.Status.Conditions, conditionConfigValid)
			ready := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
			switch {
			case rendered == nil || ready == nil || rendered.ObservedGeneration != p.Generation:
				return "the proxy's status is of an earlier spec"
			case rendered.Status != metav1.ConditionFalse || rendered.Reason != reasonInvalidConfig || !strings.Contains(rendered.Message, "mode"):
				return fmt.Sprintf("ConfigValid is %s/%s: %s", rendered.Status, rendered.Reason, rendered.Message)
			case ready.Status != metav1.ConditionFalse || ready.Reason != reasonConfigInvalid:
				return fmt.Sprintf("Ready is %s/%s: %s", ready.Status, ready.Reason, ready.Message)
			}
			return ""
		})
		if after := c.deployment(t); after.Generation != before.Generation {
			t.Errorf("the Deployment was changed: generation %d, was %d", after.Generation, before.Generation)
		}
		if now := c.pods(t); !slices.Equal(now, pods) {
			t.Errorf("the pods were replaced: %v, were %v", now, pods)
		}
		if reason := c.routed("http", shopHost, "shop"); reason != "" {
			t.Errorf("the proxy stopped serving: %s", reason)
		}

		c.edit(t, func(s *synapsev1alpha1.SynapseProxySpec) { s.Config = valid })
		eventually(t, soon, "the proxy is ready again once the configuration is put right", c.ready)
		if after := c.deployment(t); after.Generation != before.Generation {
			t.Errorf("putting the configuration right changed the Deployment: generation %d, was %d", after.Generation, before.Generation)
		}
	})

	step("the operator is replaced without disturbing the proxy, and carries on", func(t *testing.T) {
		pods := c.pods(t)

		var operators corev1.PodList
		if err := c.k8s.List(c.ctx, &operators, client.InNamespace(operatorNamespace)); err != nil {
			t.Fatal(err)
		}
		for i := range operators.Items {
			if err := c.k8s.Delete(c.ctx, &operators.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
		eventually(t, soon, "a new operator pod is available", func() string {
			var now corev1.PodList
			if err := c.k8s.List(c.ctx, &now, client.InNamespace(operatorNamespace)); err != nil {
				return err.Error()
			}
			for _, p := range now.Items {
				for _, old := range operators.Items {
					if p.UID == old.UID {
						return "the old operator pod is still there"
					}
				}
			}
			return c.operatorAvailable()
		})

		// Through the scale subresource, as `kubectl scale` and an
		// autoscaler do. That it is acted on shows the new operator works.
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			p, err := c.getProxy()
			if err != nil {
				return err
			}
			scale := &autoscalingv1.Scale{}
			if err := c.k8s.SubResource("scale").Get(c.ctx, p, scale); err != nil {
				return err
			}
			scale.Spec.Replicas = 2
			return c.k8s.SubResource("scale").Update(c.ctx, p, client.WithSubResourceBody(scale))
		})
		if err != nil {
			t.Fatalf("scale the proxy: %v", err)
		}
		eventually(t, afterASync, "the proxy runs two pods", func() string {
			if reason := c.ready(); reason != "" {
				return reason
			}
			p, err := c.getProxy()
			if err != nil {
				return err.Error()
			}
			if p.Status.ReadyReplicas != 2 {
				return fmt.Sprintf("%d pods are ready", p.Status.ReadyReplicas)
			}
			return ""
		})
		now := c.pods(t)
		for _, uid := range pods {
			if !slices.Contains(now, uid) {
				t.Errorf("a pod that was running before the operator was replaced is gone: %v, were %v", now, pods)
			}
		}
		if reason := c.routed("http", shopHost, "shop"); reason != "" {
			t.Errorf("the proxy stopped serving: %s", reason)
		}
	})

	step("deleting the proxy removes everything made for it", func(t *testing.T) {
		if err := c.k8s.Delete(c.ctx, c.proxy(t)); err != nil {
			t.Fatal(err)
		}
		eventually(t, afterASync, "nothing of the proxy is left", func() string {
			owned := client.MatchingLabels{proxyLabel: proxyKey.Name}
			for _, list := range []client.ObjectList{
				&appsv1.DeploymentList{}, &appsv1.ReplicaSetList{}, &corev1.PodList{},
				&corev1.ServiceList{}, &corev1.ServiceAccountList{}, &corev1.SecretList{}, &corev1.ConfigMapList{},
			} {
				if err := c.k8s.List(c.ctx, list, client.InNamespace(proxyNamespace), owned); err != nil {
					return err.Error()
				}
				if n := meta.LenList(list); n > 0 {
					return fmt.Sprintf("%d of %T are left", n, list)
				}
			}
			// The routes and the certificates carry no label of the proxy's.
			for name, obj := range map[string]client.Object{routesConfigMap: &corev1.ConfigMap{}, certificatesSecret: &corev1.Secret{}} {
				err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: name}, obj)
				if !apierrors.IsNotFound(err) {
					return fmt.Sprintf("%T %s is left (%v)", obj, name, err)
				}
			}
			return ""
		})
	})
}

func ptrTo[T any](v T) *T { return &v }
