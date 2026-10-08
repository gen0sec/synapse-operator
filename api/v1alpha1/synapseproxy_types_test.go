package v1alpha1_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"synapse-operator/api/v1alpha1"
	"synapse-operator/internal/testenv"
)

var (
	restCfg *rest.Config
	k8s     client.Client
	nameSeq atomic.Int64
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	cfg, stop, err := testenv.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = stop() }()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	restCfg, k8s = cfg, c
	return m.Run()
}

// validProxy is the smallest SynapseProxy the API accepts. Every rejection
// case below starts from it and breaks exactly one thing.
func validProxy() *v1alpha1.SynapseProxy {
	return &v1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("proxy-%d", nameSeq.Add(1)),
			Namespace: "default",
		},
		Spec: v1alpha1.SynapseProxySpec{
			Image: "example.test/synapse:1.0.0",
			Listeners: []v1alpha1.Listener{
				{Name: "http", Port: 80, Protocol: "HTTP"},
			},
		},
	}
}

func mustCreate(t *testing.T, p *v1alpha1.SynapseProxy) {
	t.Helper()
	if err := k8s.Create(context.Background(), p); err != nil {
		t.Fatalf("create %s: %v", p.Name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), p) })
}

func TestSynapseProxy_MinimalIsAcceptedAndDefaulted(t *testing.T) {
	p := validProxy()
	mustCreate(t, p)

	if p.Spec.Replicas == nil || *p.Spec.Replicas != 1 {
		t.Errorf("replicas = %v, want it defaulted to 1", p.Spec.Replicas)
	}
}

func TestSynapseProxy_FullSpecIsAccepted(t *testing.T) {
	replicas := int32(3)
	p := validProxy()
	p.Name = strings.Repeat("a", 48) // the longest name allowed
	p.Spec.Replicas = &replicas
	p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
	p.Spec.NodeSelector = map[string]string{"kubernetes.io/os": "linux"}
	p.Spec.Tolerations = []corev1.Toleration{{Key: "edge", Operator: corev1.TolerationOpExists}}
	p.Spec.Service = &v1alpha1.ProxyServiceSpec{
		Type:                  corev1.ServiceTypeLoadBalancer,
		Annotations:           map[string]string{"example.test/scheme": "internet-facing"},
		ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
	}
	p.Spec.Listeners = []v1alpha1.Listener{
		{Name: "http", Port: 80, Protocol: "HTTP"},
		{Name: "https", Port: 443, Protocol: "TLS"},
	}
	p.Spec.TLS = &v1alpha1.TLSSpec{Grade: "high"}
	p.Spec.TrustedProxies = []string{"10.0.0.0/8", "fd00::/8"}
	p.Spec.Logging = &v1alpha1.LoggingSpec{Level: "debug"}
	p.Spec.Platform = &v1alpha1.PlatformSpec{
		APIKeySecretRef: &v1alpha1.SecretKeyReference{Name: "synapse-credentials", Key: "API_KEY"},
	}
	mustCreate(t, p)
}

func TestSynapseProxy_Rejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*v1alpha1.SynapseProxy)
		want   string // substring of the API server's message
	}{
		{"an empty image",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Image = "" },
			"spec.image"},
		{"negative replicas",
			func(p *v1alpha1.SynapseProxy) { n := int32(-1); p.Spec.Replicas = &n },
			"spec.replicas in body should be greater than or equal to 0"},
		{"no listeners",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners = []v1alpha1.Listener{} },
			"should have at least 1 items"},
		{"more than 16 listeners",
			func(p *v1alpha1.SynapseProxy) {
				for i := range 16 {
					p.Spec.Listeners = append(p.Spec.Listeners,
						v1alpha1.Listener{Name: fmt.Sprintf("l%d", i), Port: int32(8000 + i), Protocol: "HTTP"})
				}
			},
			"must have at most 16 items"},
		{"two listeners with one name",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Listeners = append(p.Spec.Listeners, v1alpha1.Listener{Name: "http", Port: 8080, Protocol: "HTTP"})
			},
			"Duplicate value"},
		{"two listeners on one port",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Listeners = append(p.Spec.Listeners, v1alpha1.Listener{Name: "again", Port: 80, Protocol: "HTTP"})
			},
			"listener ports must be unique"},
		{"no HTTP listener",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Listeners = []v1alpha1.Listener{{Name: "https", Port: 443, Protocol: "TLS"}}
			},
			"at least one HTTP listener"},
		{"an unknown listener protocol",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Protocol = "UDP" },
			"spec.listeners[0].protocol"},
		{"a lower-case listener protocol",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Protocol = "http" },
			"spec.listeners[0].protocol"},
		{"listener port 0",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Port = 0 },
			"spec.listeners[0].port"},
		{"a listener port above 65535",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Port = 65536 },
			"spec.listeners[0].port"},
		{"an upper-case listener name",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Name = "HTTP" },
			"spec.listeners[0].name"},
		{"a listener name longer than a port name",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Listeners[0].Name = "sixteen-chars-xx" },
			"spec.listeners[0].name"},
		{"the unsafe TLS grade",
			func(p *v1alpha1.SynapseProxy) { p.Spec.TLS = &v1alpha1.TLSSpec{Grade: "unsafe"} },
			"spec.tls.grade"},
		{"an unknown log level",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Logging = &v1alpha1.LoggingSpec{Level: "verbose"} },
			"spec.logging.level"},
		{"a name longer than 48 characters",
			func(p *v1alpha1.SynapseProxy) { p.Name = strings.Repeat("a", 49) },
			"at most 48 characters"},
		{"a name that is not a DNS label",
			func(p *v1alpha1.SynapseProxy) { p.Name = "9-starts-with-a-digit" },
			"DNS-1035"},
		{"a Service type that cannot front a proxy",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Service = &v1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeExternalName}
			},
			"spec.service.type"},
		{"externalTrafficPolicy on a ClusterIP Service",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Service = &v1alpha1.ProxyServiceSpec{ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal}
			},
			"externalTrafficPolicy needs"},
		{"an unknown externalTrafficPolicy",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Service = &v1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer, ExternalTrafficPolicy: "Sometimes"}
			},
			"spec.service.externalTrafficPolicy"},
		{"a repeated trusted proxy",
			func(p *v1alpha1.SynapseProxy) { p.Spec.TrustedProxies = []string{"10.0.0.0/8", "10.0.0.0/8"} },
			"Duplicate value"},
		{"an empty trusted proxy",
			func(p *v1alpha1.SynapseProxy) { p.Spec.TrustedProxies = []string{""} },
			"spec.trustedProxies[0]"},
		{"more than 64 trusted proxies",
			func(p *v1alpha1.SynapseProxy) {
				for i := range 65 {
					p.Spec.TrustedProxies = append(p.Spec.TrustedProxies, fmt.Sprintf("10.0.%d.0/24", i))
				}
			},
			"must have at most 64 items"},
		{"an API key reference without a Secret name",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Platform = &v1alpha1.PlatformSpec{APIKeySecretRef: &v1alpha1.SecretKeyReference{Key: "API_KEY"}}
			},
			"spec.platform.apiKeySecretRef.name"},
		{"an API key reference without a key",
			func(p *v1alpha1.SynapseProxy) {
				p.Spec.Platform = &v1alpha1.PlatformSpec{APIKeySecretRef: &v1alpha1.SecretKeyReference{Name: "synapse-credentials"}}
			},
			"spec.platform.apiKeySecretRef.key"},
		{"a config that is not an object",
			func(p *v1alpha1.SynapseProxy) { p.Spec.Config = &runtime.RawExtension{Raw: []byte(`"mode: proxy"`)} },
			"spec.config"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validProxy()
			tc.mutate(p)
			err := k8s.Create(context.Background(), p)
			if err == nil {
				_ = k8s.Delete(context.Background(), p)
				t.Fatal("accepted, want it rejected")
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("error is not Invalid: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("message does not mention %q:\n%v", tc.want, err)
			}
		})
	}
}

// Every rule above is a rule about the spec. An object with no spec at all
// would slip past all of them, and the typed client cannot show it: it always
// writes one.
func TestSynapseProxy_RejectsAnObjectWithoutASpec(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       "SynapseProxy",
		"metadata":   map[string]any{"name": validProxy().Name, "namespace": "default"},
	}}
	err := k8s.Create(context.Background(), obj)
	if err == nil {
		_ = k8s.Delete(context.Background(), obj)
		t.Fatal("accepted, want it rejected")
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec") {
		t.Errorf("got %v, want an Invalid error naming spec", err)
	}
}

// The free-form config carries keys the typed spec does not model, so the API
// server must store it untouched rather than prune what it has no schema for.
func TestSynapseProxy_ConfigKeepsUnknownKeys(t *testing.T) {
	const raw = `{"proxy":{"h2c":true,"upstream":{"healthcheck":{"interval":5}}},"telemetry":{"enabled":false}}`
	p := validProxy()
	p.Spec.Config = &runtime.RawExtension{Raw: []byte(raw)}
	mustCreate(t, p)

	var got v1alpha1.SynapseProxy
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(p), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Config == nil || string(got.Spec.Config.Raw) != raw {
		t.Errorf("config came back as %s, want %s", got.Spec.Config, raw)
	}
}

// Status is the controller's to write. A client that updates the object must
// not be able to change it, and a status write must not change the spec.
func TestSynapseProxy_StatusIsASubresource(t *testing.T) {
	ctx := context.Background()
	p := validProxy()
	mustCreate(t, p)

	p.Spec.Image = "example.test/synapse:2.0.0"
	p.Status.ConfigHash = "forged"
	if err := k8s.Update(ctx, p); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.SynapseProxy
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(p), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ConfigHash != "" {
		t.Errorf("an update of the object wrote status.configHash = %q", got.Status.ConfigHash)
	}

	got.Spec.Image = "example.test/synapse:3.0.0"
	got.Status.ConfigHash = "abc123"
	got.Status.Conditions = []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "Applied",
		LastTransitionTime: metav1.Now(),
	}}
	if err := k8s.Status().Update(ctx, &got); err != nil {
		t.Fatalf("status update: %v", err)
	}
	var after v1alpha1.SynapseProxy
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(p), &after); err != nil {
		t.Fatal(err)
	}
	if after.Status.ConfigHash != "abc123" || len(after.Status.Conditions) != 1 {
		t.Errorf("status not stored: %+v", after.Status)
	}
	if after.Spec.Image != "example.test/synapse:2.0.0" {
		t.Errorf("a status update changed spec.image to %q", after.Spec.Image)
	}

	// One condition per type: a second "Ready" is a writer bug, not a state.
	after.Status.Conditions = append(after.Status.Conditions, after.Status.Conditions[0])
	if err := k8s.Status().Update(ctx, &after); !apierrors.IsInvalid(err) {
		t.Errorf("two conditions of one type: got %v, want Invalid", err)
	}
}

// A controller reports what it knows when it knows it. No status field may be
// required, or the first write that leaves one out is refused: a proxy with
// no pods yet has nothing to say about replicas.
func TestSynapseProxy_StatusFieldsAreOptional(t *testing.T) {
	ctx := context.Background()
	p := validProxy()
	mustCreate(t, p)

	patch := client.MergeFrom(p.DeepCopy())
	p.Status.ConfigHash = "abc123"
	if err := k8s.Status().Patch(ctx, p, patch); err != nil {
		t.Fatalf("a status patch that sets one field: %v", err)
	}

	scale := &autoscalingv1.Scale{}
	if err := k8s.SubResource("scale").Get(ctx, p, scale); err != nil {
		t.Fatalf("get scale: %v", err)
	}
	if scale.Status.Replicas != 0 {
		t.Errorf("scale reports %d replicas for a proxy that has reported none", scale.Status.Replicas)
	}
}

// `kubectl scale` and the HorizontalPodAutoscaler go through this subresource.
func TestSynapseProxy_ScaleSubresource(t *testing.T) {
	ctx := context.Background()
	p := validProxy()
	mustCreate(t, p)

	p.Status.Replicas = 1
	p.Status.Selector = "synapse.gen0sec.com/proxy=" + p.Name
	if err := k8s.Status().Update(ctx, p); err != nil {
		t.Fatalf("status update: %v", err)
	}

	scale := &autoscalingv1.Scale{}
	if err := k8s.SubResource("scale").Get(ctx, p, scale); err != nil {
		t.Fatalf("get scale: %v", err)
	}
	if scale.Spec.Replicas != 1 || scale.Status.Replicas != 1 || scale.Status.Selector != p.Status.Selector {
		t.Errorf("scale = spec %d, status %d, selector %q", scale.Spec.Replicas, scale.Status.Replicas, scale.Status.Selector)
	}

	scale.Spec.Replicas = 4
	if err := k8s.SubResource("scale").Update(ctx, p, client.WithSubResourceBody(scale)); err != nil {
		t.Fatalf("update scale: %v", err)
	}
	var got v1alpha1.SynapseProxy
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(p), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 4 {
		t.Errorf("replicas = %v after scaling to 4", got.Spec.Replicas)
	}
}

// A wrong JSONPath in a printer column is not an error anywhere: the column
// is just empty. So read the table `kubectl get` would print.
func TestSynapseProxy_PrinterColumns(t *testing.T) {
	ctx := context.Background()
	p := validProxy()
	// A namespace of its own, so the table has exactly one row; named after
	// the proxy, so the test can run more than once against one API server.
	p.Namespace = "columns-" + p.Name
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: p.Namespace}}); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, p)
	p.Status = v1alpha1.SynapseProxyStatus{
		ConfigHash:    "abc123",
		Replicas:      1,
		ReadyReplicas: 1,
		Addresses:     []string{"203.0.113.10", "203.0.113.11"},
		Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Applied",
			LastTransitionTime: metav1.Now(),
		}},
	}
	if err := k8s.Status().Update(ctx, p); err != nil {
		t.Fatalf("status update: %v", err)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := dc.RESTClient().Get().
		AbsPath("/apis", v1alpha1.GroupVersion.Group, v1alpha1.GroupVersion.Version, "namespaces", p.Namespace, "synapseproxies").
		SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io").
		Do(ctx).Raw()
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	var table metav1.Table
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("%d rows, want 1", len(table.Rows))
	}

	got := map[string]string{}
	wide := map[string]bool{}
	for i, col := range table.ColumnDefinitions {
		got[col.Name] = fmt.Sprint(table.Rows[0].Cells[i])
		wide[col.Name] = col.Priority != 0
	}
	want := map[string]string{
		"Name":      p.Name,
		"Ready":     "True",
		"Reason":    "Applied",
		"Desired":   "1",
		"Available": "1",
		"Address":   "203.0.113.10",
		"Image":     "example.test/synapse:1.0.0",
		"Config":    "abc123",
	}
	for name, cell := range want {
		if got[name] != cell {
			t.Errorf("column %s = %q, want %q", name, got[name], cell)
		}
	}
	if _, ok := got["Age"]; !ok {
		t.Error("no Age column")
	}
	for name, isWide := range wide {
		if want := name == "Image" || name == "Config"; isWide != want {
			t.Errorf("column %s: shown only with -o wide = %v, want %v", name, isWide, want)
		}
	}
}

func TestSynapseProxy_IsDiscoverableByShortNameAndCategory(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		t.Fatal(err)
	}
	list, err := dc.ServerResourcesForGroupVersion(v1alpha1.GroupVersion.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range list.APIResources {
		if r.Name != "synapseproxies" {
			continue
		}
		if r.Kind != "SynapseProxy" || !r.Namespaced {
			t.Errorf("kind %q namespaced %v", r.Kind, r.Namespaced)
		}
		if !slices.Contains(r.ShortNames, "synproxy") {
			t.Errorf("short names %v, want synproxy", r.ShortNames)
		}
		if !slices.Contains(r.Categories, "synapse") {
			t.Errorf("categories %v, want synapse", r.Categories)
		}
		return
	}
	t.Fatalf("synapseproxies not served; got %+v", list.APIResources)
}
