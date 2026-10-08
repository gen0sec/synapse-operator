package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// routedIngress is an Ingress with one host routed to the Service "app".
// class nil leaves spec.ingressClassName unset.
func routedIngress(name string, class *string, host string) *networkingv1.Ingress {
	ing := &networkingv1.Ingress{}
	ing.Name, ing.Namespace = name, "default"
	ing.Spec.IngressClassName = class
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: host,
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{Path: "/", Backend: networkingv1.IngressBackend{
				Service: &networkingv1.IngressServiceBackend{Name: "app", Port: networkingv1.ServiceBackendPort{Number: 80}}}}},
		}},
	}}
	return ing
}

func ourIngressClass(name string, isDefault bool) *networkingv1.IngressClass {
	ic := &networkingv1.IngressClass{}
	ic.Name = name
	ic.Spec.Controller = ControllerName
	if isDefault {
		ic.Annotations = map[string]string{"ingressclass.kubernetes.io/is-default-class": "true"}
	}
	return ic
}

func only(class string) func(string) bool {
	return func(c string) bool { return c == class }
}

// renderToFile renders with r into a scratch file and returns its content.
func renderToFile(t *testing.T, r *IngressReconciler, objs ...client.Object) string {
	t.Helper()
	r.Client = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	r.UpstreamsOutPath = filepath.Join(t.TempDir(), "upstreams.yaml")
	r.ClusterDomain = "cluster.local"
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out, err := os.ReadFile(r.UpstreamsOutPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func wantHosts(t *testing.T, rendered string, present []string, absent []string) {
	t.Helper()
	for _, h := range present {
		if !strings.Contains(rendered, `"`+h+`"`) {
			t.Errorf("host %s not rendered:\n%s", h, rendered)
		}
	}
	for _, h := range absent {
		if strings.Contains(rendered, `"`+h+`"`) {
			t.Errorf("host %s rendered, want it left out:\n%s", h, rendered)
		}
	}
}

func TestRender_IngressClassMatch(t *testing.T) {
	legacyAnnotated := routedIngress("legacy", nil, "legacy.example.com")
	legacyAnnotated.Annotations = map[string]string{"kubernetes.io/ingress.class": "edge"}
	objs := []client.Object{
		routedIngress("edge", ptr("edge"), "edge.example.com"),
		legacyAnnotated,
		routedIngress("internal", ptr("internal"), "internal.example.com"),
		routedIngress("named", ptr("synapse"), "named.example.com"),
		routedIngress("empty", ptr(""), "empty.example.com"),
		routedIngress("classless", nil, "classless.example.com"),
	}

	t.Run("decides instead of the class name", func(t *testing.T) {
		out := renderToFile(t, &IngressReconciler{IngressClassName: "synapse", IngressClassMatch: only("edge")}, objs...)
		wantHosts(t, out,
			[]string{"edge.example.com", "legacy.example.com"},
			[]string{"internal.example.com", "named.example.com", "empty.example.com", "classless.example.com"})
	})

	// A reconciler bound to no class must render nothing. In particular it
	// must not fall back to comparing against its (empty) class name, which
	// an Ingress with an empty class would equal.
	t.Run("matching nothing renders nothing", func(t *testing.T) {
		none := func(string) bool { return false }
		out := renderToFile(t, &IngressReconciler{IngressClassMatch: none}, objs...)
		wantHosts(t, out, nil, []string{
			"edge.example.com", "legacy.example.com", "internal.example.com",
			"named.example.com", "empty.example.com", "classless.example.com"})
	})

	t.Run("without it the class name decides, as before", func(t *testing.T) {
		out := renderToFile(t, &IngressReconciler{IngressClassName: "synapse"}, objs...)
		wantHosts(t, out,
			[]string{"named.example.com"},
			[]string{"edge.example.com", "legacy.example.com", "internal.example.com", "empty.example.com", "classless.example.com"})
	})
}

// An Ingress that names no class belongs to the default IngressClass. That
// only makes it ours when the default class is one this reconciler serves.
func TestRender_IngressClassMatch_DefaultClass(t *testing.T) {
	classless := routedIngress("classless", nil, "classless.example.com")

	out := renderToFile(t, &IngressReconciler{IngressClassMatch: only("edge")}, classless, ourIngressClass("edge", true))
	wantHosts(t, out, []string{"classless.example.com"}, nil)

	out = renderToFile(t, &IngressReconciler{IngressClassMatch: only("edge")}, classless, ourIngressClass("other", true))
	wantHosts(t, out, nil, []string{"classless.example.com"})

	out = renderToFile(t, &IngressReconciler{IngressClassMatch: only("edge")}, classless, ourIngressClass("edge", false))
	wantHosts(t, out, nil, []string{"classless.example.com"})
}

func testOwner(uid string) *metav1.OwnerReference {
	yes := true
	return &metav1.OwnerReference{
		APIVersion: "synapse.gen0sec.com/v1alpha1", Kind: "SynapseProxy", Name: "edge",
		UID: types.UID(uid), Controller: &yes, BlockOwnerDeletion: &yes,
	}
}

func controllerUID(obj metav1.Object) types.UID {
	if ref := metav1.GetControllerOf(obj); ref != nil {
		return ref.UID
	}
	return ""
}

var (
	ownedUpstreams = types.NamespacedName{Namespace: "synapse-os", Name: "edge-upstreams"}
	ownedCerts     = types.NamespacedName{Namespace: "synapse-os", Name: "edge-certs"}
)

// ownedReconciler renders one TLS Ingress into the two owned outputs.
func ownedReconciler(t *testing.T, owner *metav1.OwnerReference, extra ...client.Object) *IngressReconciler {
	t.Helper()
	ing := routedIngress("app", ptr("synapse"), "app.example.com")
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"app.example.com"}, SecretName: "app-tls"}}
	objs := append([]client.Object{ing, tlsSecret("default", "app-tls", "CRT", "KEY")}, extra...)
	return &IngressReconciler{
		Client:                fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(),
		IngressClassName:      "synapse",
		ClusterDomain:         "cluster.local",
		UpstreamsOutConfigMap: ownedUpstreams,
		CertsOutSecret:        ownedCerts,
		OwnerRef:              owner,
	}
}

func TestRender_OwnerRef_ControlsItsOutputs(t *testing.T) {
	ctx := context.Background()
	r := ownedReconciler(t, testOwner("uid-1"))
	if changed, _, _, err := r.render(ctx); err != nil || !changed {
		t.Fatalf("first render: changed=%v err=%v", changed, err)
	}

	var cm corev1.ConfigMap
	if err := r.Get(ctx, ownedUpstreams, &cm); err != nil {
		t.Fatal(err)
	}
	if controllerUID(&cm) != "uid-1" {
		t.Errorf("upstreams ConfigMap owners = %v, want it controlled by the owner", cm.OwnerReferences)
	}
	if !strings.Contains(cm.Data[UpstreamsKey], "app.example.com") {
		t.Errorf("upstreams not rendered:\n%s", cm.Data[UpstreamsKey])
	}

	var sec corev1.Secret
	if err := r.Get(ctx, ownedCerts, &sec); err != nil {
		t.Fatal(err)
	}
	if controllerUID(&sec) != "uid-1" {
		t.Errorf("certs Secret owners = %v, want it controlled by the owner", sec.OwnerReferences)
	}
	if string(sec.Data["app.example.com.crt"]) != "CRT" {
		t.Errorf("certificate not projected: %v", sec.Data)
	}

	// Its own outputs are not foreign on the next render.
	if changed, _, _, err := r.render(ctx); err != nil || changed {
		t.Fatalf("second render: changed=%v err=%v, want a no-op", changed, err)
	}
}

// With an owner, an object already sitting under an output's name belongs to
// someone else: a user, another tool, or a proxy that has since been replaced.
// Writing into it would hand it the routes, or the private keys.
func TestRender_OwnerRef_RefusesOutputsItDoesNotControl(t *testing.T) {
	ctx := context.Background()

	unowned := func(obj client.Object) client.Object { return obj }
	foreign := func(obj client.Object) client.Object {
		obj.SetOwnerReferences([]metav1.OwnerReference{*testOwner("uid-other")})
		return obj
	}
	existingCM := func() client.Object {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: ownedUpstreams.Namespace, Name: ownedUpstreams.Name},
			Data:       map[string]string{"theirs": "untouched"},
		}
	}
	existingSecret := func() client.Object {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ownedCerts.Namespace, Name: ownedCerts.Name},
			Data:       map[string][]byte{"theirs": []byte("untouched")},
		}
	}

	for name, mark := range map[string]func(client.Object) client.Object{"unowned": unowned, "controlled by another owner": foreign} {
		t.Run("upstreams ConfigMap "+name, func(t *testing.T) {
			r := ownedReconciler(t, testOwner("uid-1"), mark(existingCM()))
			if _, _, _, err := r.render(ctx); err == nil {
				t.Fatal("render succeeded, want an error")
			}
			var cm corev1.ConfigMap
			if err := r.Get(ctx, ownedUpstreams, &cm); err != nil {
				t.Fatal(err)
			}
			if _, written := cm.Data[UpstreamsKey]; written || cm.Data["theirs"] != "untouched" || cm.Labels[ingressUpstreamsManagedLabel] != "" {
				t.Errorf("the ConfigMap was written to: data %v, labels %v", cm.Data, cm.Labels)
			}
			if controllerUID(&cm) == "uid-1" {
				t.Error("the ConfigMap was adopted")
			}
		})

		t.Run("certs Secret "+name, func(t *testing.T) {
			r := ownedReconciler(t, testOwner("uid-1"), mark(existingSecret()))
			if _, _, _, err := r.render(ctx); err == nil {
				t.Fatal("render succeeded, want an error")
			}
			var sec corev1.Secret
			if err := r.Get(ctx, ownedCerts, &sec); err != nil {
				t.Fatal(err)
			}
			if len(sec.Data) != 1 || string(sec.Data["theirs"]) != "untouched" || sec.Labels[ingressCertsManagedLabel] != "" {
				t.Errorf("the Secret was written to: keys %d, labels %v", len(sec.Data), sec.Labels)
			}
			if controllerUID(&sec) == "uid-1" {
				t.Error("the Secret was adopted")
			}
		})
	}

	// Control: without an owner the reconciler writes into whatever is
	// there, which is what the existing central layout relies on.
	t.Run("without an owner an existing ConfigMap is written", func(t *testing.T) {
		r := ownedReconciler(t, nil, existingCM(), existingSecret())
		if _, _, _, err := r.render(ctx); err != nil {
			t.Fatalf("render: %v", err)
		}
		var cm corev1.ConfigMap
		if err := r.Get(ctx, ownedUpstreams, &cm); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(cm.Data[UpstreamsKey], "app.example.com") || cm.Data["theirs"] != "untouched" {
			t.Errorf("data = %v", cm.Data)
		}
		if len(cm.OwnerReferences) != 0 {
			t.Errorf("owners = %v, want none", cm.OwnerReferences)
		}
	})
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

// The process-wide gauges describe the single render of the legacy modes.
// Several owned renders in one process would overwrite each other there, so
// each reports under its owner instead.
func TestRender_OwnerRef_ReportsUnderItsOwnGauges(t *testing.T) {
	ctx := context.Background()
	const sentinel = 4242
	for _, g := range []prometheus.Gauge{mHosts, mRoutes, mCerts, mLastRenderTS, mReady} {
		g.Set(sentinel)
	}

	r := ownedReconciler(t, testOwner("uid-1"))
	if _, _, _, err := r.render(ctx); err != nil {
		t.Fatalf("render: %v", err)
	}

	for name, g := range map[string]prometheus.Gauge{"hosts": mHosts, "routes": mRoutes, "certs": mCerts, "last render": mLastRenderTS, "ready": mReady} {
		if got := gaugeValue(t, g); got != sentinel {
			t.Errorf("process-wide %s gauge = %v, want it untouched", name, got)
		}
	}
	for name, vec := range map[string]*prometheus.GaugeVec{"hosts": mProxyHosts, "routes": mProxyRoutes, "certs": mProxyCerts} {
		if got := gaugeValue(t, vec.WithLabelValues("synapse-os", "edge")); got != 1 {
			t.Errorf("per-owner %s gauge = %v, want 1", name, got)
		}
	}

	// Control: an unowned render still reports on the process-wide gauges.
	legacy := ownedReconciler(t, nil)
	if _, _, _, err := legacy.render(ctx); err != nil {
		t.Fatalf("render: %v", err)
	}
	for name, g := range map[string]prometheus.Gauge{"hosts": mHosts, "routes": mRoutes, "certs": mCerts, "ready": mReady} {
		if got := gaugeValue(t, g); got != 1 {
			t.Errorf("process-wide %s gauge = %v after an unowned render, want 1", name, got)
		}
	}
	if got := gaugeValue(t, mLastRenderTS); got == sentinel {
		t.Error("process-wide last-render timestamp not updated by an unowned render")
	}
}
