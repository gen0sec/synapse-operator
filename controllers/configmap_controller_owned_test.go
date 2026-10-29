package controllers

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	ownedTestNS   = "synapse-os"
	ownedTestHash = "synapse.gen0sec.com/config-hash"
)

var ownedTestLabels = map[string]string{"app.kubernetes.io/name": "synapse"}

func controllerRef(apiVersion, kind string) metav1.OwnerReference {
	yes := true
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: "edge", UID: "uid-edge", Controller: &yes}
}

// resourceOwned marks obj as controlled by one of the operator's own resources.
func resourceOwned[T client.Object](obj T) T {
	obj.SetOwnerReferences([]metav1.OwnerReference{controllerRef("synapse.gen0sec.com/v1alpha1", "SynapseProxy")})
	return obj
}

func labelledMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: ownedTestNS, Labels: ownedTestLabels}
}

func newOwnedTestReconciler(t *testing.T, objs ...client.Object) *ConfigMapReconciler {
	t.Helper()
	return &ConfigMapReconciler{
		Client:               fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(),
		LabelSelector:        labels.SelectorFromSet(ownedTestLabels),
		ConfigHashAnnotation: ownedTestHash,
	}
}

func reconcileOwnedTest(t *testing.T, r *ConfigMapReconciler) {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ownedTestNS, Name: "cfg"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

// stampedHash reads the config-hash annotation off a workload's pod template.
func stampedHash(t *testing.T, r *ConfigMapReconciler, obj client.Object) string {
	t.Helper()
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("get %T %s: %v", obj, obj.GetName(), err)
	}
	switch w := obj.(type) {
	case *appsv1.Deployment:
		return w.Spec.Template.Annotations[ownedTestHash]
	case *appsv1.DaemonSet:
		return w.Spec.Template.Annotations[ownedTestHash]
	case *appsv1.StatefulSet:
		return w.Spec.Template.Annotations[ownedTestHash]
	}
	t.Fatalf("unsupported workload %T", obj)
	return ""
}

func TestOwnedByManagedResource(t *testing.T) {
	nonController := controllerRef("synapse.gen0sec.com/v1alpha1", "SynapseProxy")
	nonController.Controller = nil

	cases := []struct {
		name   string
		owners []metav1.OwnerReference
		want   bool
	}{
		{"no owner", nil, false},
		{"controlled by an operator resource", []metav1.OwnerReference{controllerRef("synapse.gen0sec.com/v1alpha1", "SynapseProxy")}, true},
		{"any version of the group counts", []metav1.OwnerReference{controllerRef("synapse.gen0sec.com/v1", "SynapseAgent")}, true},
		{"owned but not controlled", []metav1.OwnerReference{nonController}, false},
		{"controlled by another group", []metav1.OwnerReference{controllerRef("apps/v1", "ReplicaSet")}, false},
		{"controlled by a core kind", []metav1.OwnerReference{controllerRef("v1", "ConfigMap")}, false},
		{"group is not a prefix match", []metav1.OwnerReference{controllerRef("synapse.gen0sec.com.example/v1", "Other")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{OwnerReferences: tc.owners}}
			if got := ownedByManagedResource(cm); got != tc.want {
				t.Errorf("ownedByManagedResource = %v, want %v", got, tc.want)
			}
		})
	}
}

// A workload controlled by one of the operator's own resources already has a
// writer for its pod template. Stamping it here as well would make the two
// controllers undo each other.
func TestConfigHash_LeavesResourceOwnedWorkloadsAlone(t *testing.T) {
	cfg := &corev1.ConfigMap{ObjectMeta: labelledMeta("cfg"), Data: map[string]string{"config.yaml": "mode: proxy"}}

	plain := []client.Object{
		&appsv1.Deployment{ObjectMeta: labelledMeta("plain")},
		&appsv1.DaemonSet{ObjectMeta: labelledMeta("plain")},
		&appsv1.StatefulSet{ObjectMeta: labelledMeta("plain")},
	}
	owned := []client.Object{
		resourceOwned(&appsv1.Deployment{ObjectMeta: labelledMeta("owned")}),
		resourceOwned(&appsv1.DaemonSet{ObjectMeta: labelledMeta("owned")}),
		resourceOwned(&appsv1.StatefulSet{ObjectMeta: labelledMeta("owned")}),
	}

	r := newOwnedTestReconciler(t, append(append([]client.Object{cfg}, plain...), owned...)...)
	reconcileOwnedTest(t, r)

	for _, w := range plain {
		if stampedHash(t, r, w) == "" {
			t.Errorf("%T %s: not stamped; the legacy rollout path is broken", w, w.GetName())
		}
	}
	for _, w := range owned {
		if got := stampedHash(t, r, w); got != "" {
			t.Errorf("%T %s: stamped with %q, want it left alone", w, w.GetName(), got)
		}
	}
}

// Config an operator resource owns belongs to that resource's workload only.
// It must not move the namespace-wide hash, or a certificate renewal for one
// proxy would roll every other workload in the namespace.
func TestConfigHash_IgnoresResourceOwnedSources(t *testing.T) {
	cfg := &corev1.ConfigMap{ObjectMeta: labelledMeta("cfg"), Data: map[string]string{"config.yaml": "mode: proxy"}}
	ownedCM := resourceOwned(&corev1.ConfigMap{ObjectMeta: labelledMeta("edge-upstreams"), Data: map[string]string{"routes.yaml": "v1"}})
	ownedSecret := resourceOwned(&corev1.Secret{ObjectMeta: labelledMeta("edge-certs"), Data: map[string][]byte{"tls.crt": []byte("v1")}})
	deploy := &appsv1.Deployment{ObjectMeta: labelledMeta("plain")}

	r := newOwnedTestReconciler(t, cfg, ownedCM, ownedSecret, deploy)
	reconcileOwnedTest(t, r)
	before := stampedHash(t, r, deploy)
	if before == "" {
		t.Fatal("deployment not stamped from the plain ConfigMap")
	}

	ownedCM.Data["routes.yaml"] = "v2"
	ownedSecret.Data["tls.crt"] = []byte("v2")
	for _, obj := range []client.Object{ownedCM, ownedSecret} {
		if err := r.Update(context.Background(), obj); err != nil {
			t.Fatalf("update %s: %v", obj.GetName(), err)
		}
	}
	reconcileOwnedTest(t, r)
	if got := stampedHash(t, r, deploy); got != before {
		t.Errorf("hash moved from %s to %s on a change to resource-owned config", before, got)
	}

	// Control: the hash is live, so the assertion above is not vacuous.
	cfg.Data["config.yaml"] = "mode: agent"
	if err := r.Update(context.Background(), cfg); err != nil {
		t.Fatalf("update cfg: %v", err)
	}
	reconcileOwnedTest(t, r)
	if got := stampedHash(t, r, deploy); got == before {
		t.Error("hash did not move on a change to the plain ConfigMap")
	}
}

func TestConfigHash_ResourceOwnedSourcesAloneStampNothing(t *testing.T) {
	ownedSecret := resourceOwned(&corev1.Secret{ObjectMeta: labelledMeta("edge-certs"), Data: map[string][]byte{"tls.crt": []byte("v1")}})
	deploy := &appsv1.Deployment{ObjectMeta: labelledMeta("plain")}

	r := newOwnedTestReconciler(t, ownedSecret, deploy)
	reconcileOwnedTest(t, r)
	if got := stampedHash(t, r, deploy); got != "" {
		t.Errorf("deployment stamped with %q although the namespace has no config of its own", got)
	}
}

func TestConfigHash_WatchesOnlyUnownedLabelledSources(t *testing.T) {
	r := newOwnedTestReconciler(t)

	cases := []struct {
		name string
		obj  client.Object
		want bool
	}{
		{"labelled configmap", &corev1.ConfigMap{ObjectMeta: labelledMeta("cfg")}, true},
		{"labelled secret", &corev1.Secret{ObjectMeta: labelledMeta("creds")}, true},
		{"unlabelled configmap", &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: ownedTestNS}}, false},
		{"labelled configmap owned by a resource", resourceOwned(&corev1.ConfigMap{ObjectMeta: labelledMeta("edge-upstreams")}), false},
		{"labelled secret owned by a resource", resourceOwned(&corev1.Secret{ObjectMeta: labelledMeta("edge-certs")}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.isConfigSource(tc.obj); got != tc.want {
				t.Errorf("isConfigSource = %v, want %v", got, tc.want)
			}
		})
	}
}

// The hash is a function of which objects are sources as much as of what
// they hold. An update has to get through when the object was a source
// before it or is one after it: one that stops being a source, by losing the
// label or by gaining an owner, moves the hash just like a content change.
func TestConfigHash_SourceEvents(t *testing.T) {
	p := newOwnedTestReconciler(t).sourcePredicate()

	source := func() *corev1.ConfigMap { return &corev1.ConfigMap{ObjectMeta: labelledMeta("cfg")} }
	owned := func() *corev1.ConfigMap { return resourceOwned(source()) }
	unlabelled := func() *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: ownedTestNS}}
	}

	updates := []struct {
		name     string
		old, new client.Object
		want     bool
	}{
		{"a source changes", source(), source(), true},
		{"a source gains an owner", source(), owned(), true},
		{"a source loses its label", source(), unlabelled(), true},
		{"an owned object is released", owned(), source(), true},
		{"an object gains the label", unlabelled(), source(), true},
		{"an owned object changes", owned(), owned(), false},
		{"an unlabelled object changes", unlabelled(), unlabelled(), false},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("Update = %v, want %v", got, tc.want)
			}
		})
	}

	others := []struct {
		name string
		obj  client.Object
		want bool
	}{
		{"a source", source(), true},
		{"an owned object", owned(), false},
		{"an unlabelled object", unlabelled(), false},
	}
	for _, tc := range others {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Create(event.CreateEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Create = %v, want %v", got, tc.want)
			}
			if got := p.Delete(event.DeleteEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Delete = %v, want %v", got, tc.want)
			}
			if got := p.Generic(event.GenericEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Generic = %v, want %v", got, tc.want)
			}
		})
	}
}

// What the reconcile let through above then has to do: drop the object from
// the hash at once, not at whatever unrelated event comes next.
func TestConfigHash_RecomputesWhenASourceGainsAnOwner(t *testing.T) {
	ctx := context.Background()
	a := &corev1.ConfigMap{ObjectMeta: labelledMeta("cfg"), Data: map[string]string{"config.yaml": "mode: proxy"}}
	b := &corev1.ConfigMap{ObjectMeta: labelledMeta("extra"), Data: map[string]string{"extra.yaml": "x: 1"}}
	deploy := &appsv1.Deployment{ObjectMeta: labelledMeta("plain")}

	r := newOwnedTestReconciler(t, a, b, deploy)
	reconcileOwnedTest(t, r)
	both := stampedHash(t, r, deploy)

	alone := newOwnedTestReconciler(t, a.DeepCopy(), &appsv1.Deployment{ObjectMeta: labelledMeta("plain")})
	reconcileOwnedTest(t, alone)
	onlyA := stampedHash(t, alone, &appsv1.Deployment{ObjectMeta: labelledMeta("plain")})
	if both == onlyA {
		t.Fatal("the second ConfigMap does not contribute to the hash; the test proves nothing")
	}

	if err := r.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	resourceOwned(b)
	if err := r.Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(b)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := stampedHash(t, r, deploy); got != onlyA {
		t.Errorf("hash = %s, want %s: the hash of the remaining source alone", got, onlyA)
	}
}
