package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A host name may be 253 characters long, and so may a Secret key, but the
// key is the host name plus ".crt". The API server refuses that Secret, and
// one Ingress with such a host used to stop every later render for the
// proxy: no new route, no renewed certificate, for anybody.
//
// This runs against the real API server, because the fake client stores
// whatever it is given.
func TestRoutes_OneCertificateThatCannotBeStoredDoesNotStopTheRest(t *testing.T) {
	ctx := context.Background()
	k8s, err := client.New(apiServer(t), client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("routes-certs-%d", watchTestRuns.Add(1))
	create := func(obj client.Object) {
		t.Helper()
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}
	create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})

	edge := testProxy(ns, "edge")
	edge.UID = ""
	create(edge)
	create(classFor(ns, edge))

	label := strings.Repeat("a", 63)
	longHost := label + "." + label + "." + label + "." + strings.Repeat("b", 57) + ".io"
	if len(longHost) != 252 {
		t.Fatalf("host is %d characters", len(longHost))
	}
	ingress := func(name, host string) *networkingv1.Ingress {
		ing := routedIngress(name, ptr(ns), host)
		ing.Namespace = ns
		ing.Spec.Rules[0].HTTP.Paths[0].PathType = ptr(networkingv1.PathTypePrefix)
		ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{host}, SecretName: name + "-tls"}}
		tls := tlsSecret(ns, name+"-tls", "CRT-"+name, "KEY-"+name)
		tls.Type = corev1.SecretTypeTLS
		create(tls)
		return ing
	}
	create(ingress("long", longHost))
	create(ingress("shop", "shop.example.com"))

	r := &SynapseRouteReconciler{Client: k8s, ClusterDomain: "cluster.local"}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(edge)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var routes corev1.ConfigMap
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "edge-upstreams"}, &routes); err != nil {
		t.Fatalf("no routes were rendered: %v", err)
	}
	// Both hosts are routed; only the certificate of one is missing.
	wantHosts(t, routes.Data[UpstreamsKey], []string{"shop.example.com", longHost}, nil)

	var certs corev1.Secret
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "edge-certs"}, &certs); err != nil {
		t.Fatal(err)
	}
	if string(certs.Data["shop.example.com.crt"]) != "CRT-shop" || string(certs.Data["shop.example.com.key"]) != "KEY-shop" {
		t.Errorf("the other certificate was not projected; keys %v", keysOf(certs.Data))
	}
	if len(certs.Data) != 2 {
		t.Errorf("certs Secret holds %d keys, want the two of the certificate that fits", len(certs.Data))
	}
}

// A Secret holds at most 1 MiB. Past that, the certificates that fit are
// stored and the rest are left out; the write must not fail and take every
// certificate with it.
func TestProjectCertsToSecret_StaysUnderTheSizeOfASecret(t *testing.T) {
	const each = 30 * 1024
	var objs []client.Object
	m := newRenderModel()
	for i := range 40 { // 40 x 60 KiB is more than twice the limit
		name := fmt.Sprintf("tls-%02d", i)
		host := fmt.Sprintf("host-%02d.example.com", i)
		objs = append(objs, tlsSecret("default", name, strings.Repeat("c", each), strings.Repeat("k", each)))
		m.addCert(host, host, "default", name)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	out := types.NamespacedName{Namespace: "synapse-os", Name: "edge-certs"}
	r := &IngressReconciler{Client: c, CertsOutSecret: out}

	_, n, err := r.projectCertsToSecret(context.Background(), m)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), out, &got); err != nil {
		t.Fatal(err)
	}
	size := 0
	for key, value := range got.Data {
		size += len(key) + len(value)
	}
	if size > 1024*1024 {
		t.Errorf("the Secret holds %d bytes, more than a Secret may", size)
	}
	if n == 40 || len(got.Data) != 2*n {
		t.Errorf("%d certificates stored in %d keys, want fewer than all of them", n, len(got.Data))
	}
	// As many as fit: there is no room left for one more.
	if entry := 2*each + 2*len("host-00.example.com") + len(".crt") + len(".key"); maxCertsSecretBytes-size >= entry {
		t.Errorf("only %d certificates stored, with %d bytes left and %d needed for the next", n, maxCertsSecretBytes-size, entry)
	}
	// Which ones is not left to chance: the first by name.
	if _, ok := got.Data["host-00.example.com.crt"]; !ok {
		t.Errorf("the first certificate by name is not among those stored: %v", keysOf(got.Data))
	}
}

// A render that fails is retried, but somebody has to be told: the routes
// the proxy serves are otherwise stale with nothing saying so.
func TestRoutes_ReportsAFailedRenderOnTheProxy(t *testing.T) {
	edge := testProxy("synapse-os", "edge")
	theirs := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "synapse-os", Name: "edge-upstreams"}}
	r := newRouteReconciler(t, edge, theirs)
	events := record.NewFakeRecorder(8)
	r.Recorder = events
	before := counterValue(t, mProxyRenderErrors.WithLabelValues("synapse-os", "edge"))

	if err := reconcileRoutes(t, r, edge); err == nil {
		t.Fatal("reconcile succeeded, want an error")
	}

	select {
	case event := <-events.Events:
		if !strings.HasPrefix(event, corev1.EventTypeWarning+" RoutesNotRendered ") || !strings.Contains(event, "edge-upstreams") {
			t.Errorf("event = %q, want a warning that names what is wrong", event)
		}
	default:
		t.Error("no event on the proxy")
	}
	if got := counterValue(t, mProxyRenderErrors.WithLabelValues("synapse-os", "edge")); got != before+1 {
		t.Errorf("render error counter went from %v to %v", before, got)
	}
}
