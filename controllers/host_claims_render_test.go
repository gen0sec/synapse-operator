package controllers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// ledgerRig is a proxy "edge" whose class is "public", rendered again and
// again on one clock.
type ledgerRig struct {
	t      *testing.T
	proxy  *synapsev1alpha1.SynapseProxy
	r      *SynapseRouteReconciler
	events *record.FakeRecorder
	now    time.Time
}

func newLedgerRig(t *testing.T, objs ...client.Object) *ledgerRig {
	t.Helper()
	return newLedgerRigWith(t, interceptor.Funcs{}, objs...)
}

func newLedgerRigWith(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *ledgerRig {
	t.Helper()
	proxy := testProxy("synapse-os", "edge")
	g := &ledgerRig{t: t, proxy: proxy, now: t0.Add(2 * time.Hour), events: record.NewFakeRecorder(64)}
	g.r = &SynapseRouteReconciler{
		Client: fake.NewClientBuilder().WithScheme(proxyTestScheme(t)).
			WithObjects(append([]client.Object{proxy, classFor("public", proxy)}, objs...)...).
			WithStatusSubresource(&networkingv1.Ingress{}).WithInterceptorFuncs(funcs).Build(),
		ClusterDomain: "cluster.local",
		Recorder:      g.events,
		Clock:         func() time.Time { return g.now },
	}
	return g
}

func (g *ledgerRig) reconcile() (ctrl.Result, error) {
	g.t.Helper()
	return g.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(g.proxy)})
}

func (g *ledgerRig) mustReconcile() ctrl.Result {
	g.t.Helper()
	res, err := g.reconcile()
	if err != nil {
		g.t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (g *ledgerRig) ledger() (*corev1.ConfigMap, bool) {
	g.t.Helper()
	var cm corev1.ConfigMap
	err := g.r.Get(context.Background(), proxyHostClaimsName(g.proxy), &cm)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		g.t.Fatal(err)
	}
	return &cm, true
}

func isLedger(obj client.Object) bool { return strings.HasSuffix(obj.GetName(), "-host-claims") }

func (g *ledgerRig) create(obj client.Object) {
	g.t.Helper()
	if err := g.r.Create(context.Background(), obj); err != nil {
		g.t.Fatal(err)
	}
}

func (g *ledgerRig) delete(obj client.Object) {
	g.t.Helper()
	if err := g.r.Delete(context.Background(), obj); err != nil {
		g.t.Fatal(err)
	}
}

func (g *ledgerRig) routes() string { return upstreamsOf(g.t, g.r, g.proxy) }

func TestOwnership_AnOldIngressEditedToNameTheHostDoesNotTakeIt(t *testing.T) {
	g := newLedgerRig(t, ownerIngress("zzz", "shop", ownedHost, "/", t0))
	g.mustReconcile()
	// Created a day before the owner, for another host, and edited since: its
	// creationTimestamp does not say when it named this one.
	g.create(ownerIngress("old", "edited", ownedHost, "/steal", t0.Add(-24*time.Hour)))
	g.mustReconcile()
	if out := g.routes(); strings.Contains(out, "/steal") || !strings.Contains(out, ownedHost) {
		t.Errorf("an old Ingress edited to name the host took it from its owner:\n%s", out)
	}
}

func TestOwnership_TheFirstRenderSeedsTheLedgerFromCreationTimestamps(t *testing.T) {
	g := newLedgerRig(t,
		ownerIngress("zzz", "shop", ownedHost, "/", t0),
		ownerIngress("aaa", "later", "b.example.com", "/", t0.Add(time.Hour)))
	g.mustReconcile()
	cm, ok := g.ledger()
	if !ok {
		t.Fatal("no ledger written")
	}
	body := cm.Data["claims"]
	for _, want := range []string{
		"zzz " + ownedHost + " " + t0.Format(time.RFC3339Nano),
		"aaa b.example.com " + t0.Add(time.Hour).Format(time.RFC3339Nano),
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("ledger lacks %q:\n%s", want, body)
		}
	}
}

func TestOwnership_TheLedgerIsControlledByTheProxy(t *testing.T) {
	ing := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	t.Run("it is made with the proxy as its controller", func(t *testing.T) {
		g := newLedgerRig(t, ing.DeepCopy())
		g.proxy.UID = "proxy-uid"
		g.mustReconcile()
		cm, _ := g.ledger()
		if ref := metav1.GetControllerOf(cm); ref == nil || ref.Kind != "SynapseProxy" || ref.Name != "edge" {
			t.Errorf("controller = %+v", ref)
		}
		if cm.Labels["synapse.gen0sec.com/host-claims"] != "true" {
			t.Errorf("labels = %v", cm.Labels)
		}
	})
	t.Run("one that is not controlled by it fails the render", func(t *testing.T) {
		foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "synapse-os", Name: "edge-host-claims"},
			Data: map[string]string{"claims": "zzz " + ownedHost + " " + t0.Format(time.RFC3339Nano) + "\n"}}
		g := newLedgerRig(t, ing.DeepCopy(), foreign)
		if _, err := g.reconcile(); err == nil || !strings.Contains(err.Error(), "not controlled") {
			t.Fatalf("err = %v, want one saying the ConfigMap is not controlled by the proxy", err)
		}
	})
}

func TestOwnership_HandOverOnDeletionIsImmediate(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	g := newLedgerRig(t, owner, ownerIngress("aaa", "next", ownedHost, "/next", t0.Add(time.Hour)))
	g.mustReconcile()
	if strings.Contains(g.routes(), "/next") {
		t.Fatalf("the newcomer was served beside the owner:\n%s", g.routes())
	}
	g.delete(owner)
	g.mustReconcile()
	if !strings.Contains(g.routes(), "/next") {
		t.Errorf("the host did not pass on in the render after the owner was deleted:\n%s", g.routes())
	}
}

func TestOwnership_ALedgerEntryOutlivesTheObjectForTheGrace(t *testing.T) {
	setup := func() (*ledgerRig, *networkingv1.Ingress) {
		owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
		g := newLedgerRig(t, owner, ownerIngress("aaa", "next", ownedHost, "/next", t0.Add(time.Hour)))
		g.mustReconcile()
		g.now = g.now.Add(10 * time.Minute)
		g.delete(owner)
		g.mustReconcile()
		if !strings.Contains(g.routes(), "/next") {
			t.Fatal("the host did not pass on")
		}
		return g, ownerIngress("zzz", "shop", ownedHost, "/", t0)
	}
	t.Run("recreated within the grace", func(t *testing.T) {
		g, again := setup()
		g.now = g.now.Add(20 * time.Minute)
		g.mustReconcile() // a render that finds it absent, within the grace
		g.now = g.now.Add(10 * time.Minute)
		g.create(again)
		g.mustReconcile()
		if out := g.routes(); strings.Contains(out, "/next") || !strings.Contains(out, "app.zzz.") {
			t.Errorf("the owner did not get its host back:\n%s", out)
		}
	})
	t.Run("recreated after the grace", func(t *testing.T) {
		g, again := setup()
		g.now = g.now.Add(90 * time.Minute)
		g.mustReconcile() // the render that finds the line past its grace
		g.now = g.now.Add(30 * time.Minute)
		g.create(again)
		g.mustReconcile()
		if out := g.routes(); !strings.Contains(out, "/next") || strings.Contains(out, "app.zzz.") {
			t.Errorf("a claim that was gone for longer than the grace kept its age:\n%s", out)
		}
	})
}

func TestOwnership_ALedgerThatCannotBeReadFailsTheRender(t *testing.T) {
	var deny atomic.Bool
	g := newLedgerRigWith(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok && strings.HasSuffix(key.Name, "-host-claims") && deny.Load() {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, key.Name, errors.New("denied"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, ownerIngress("zzz", "shop", ownedHost, "/", t0))
	g.mustReconcile()
	before := g.routes()
	g.create(ownerIngress("aaa", "more", "more.example.com", "/", t0))
	deny.Store(true)
	if _, err := g.reconcile(); err == nil || !apierrors.IsForbidden(errors.Unwrap(err)) && !strings.Contains(err.Error(), "denied") {
		t.Fatalf("err = %v, want the read error", err)
	}
	if after := g.routes(); after != before {
		t.Errorf("a render that could not read the ledger changed the routes:\n%s", after)
	}
}

func TestOwnership_ALedgerThatCannotBeWrittenStillRendersAndRequeues(t *testing.T) {
	for _, verb := range []string{"create", "update"} {
		t.Run(verb, func(t *testing.T) {
			var deny atomic.Bool
			fail := func() error {
				if deny.Load() {
					return errors.New("quota exceeded")
				}
				return nil
			}
			g := newLedgerRigWith(t, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if isLedger(obj) {
						if err := fail(); err != nil {
							return err
						}
					}
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if isLedger(obj) {
						if err := fail(); err != nil {
							return err
						}
					}
					return c.Update(ctx, obj, opts...)
				},
			}, ownerIngress("zzz", "shop", ownedHost, "/", t0))
			if verb == "update" {
				g.mustReconcile()
				g.create(ownerIngress("aaa", "more", "more.example.com", "/more", t0))
			}
			deny.Store(true)
			errs := counterValue(t, mHostClaimLedgerErrors.WithLabelValues("write"))
			res, err := g.reconcile()
			if err != nil {
				t.Fatalf("a ledger that cannot be written failed the render: %v", err)
			}
			if res.RequeueAfter != time.Minute {
				t.Errorf("RequeueAfter = %v, want a minute", res.RequeueAfter)
			}
			if verb == "update" && !strings.Contains(g.routes(), "/more") {
				t.Errorf("the routes were not written:\n%s", g.routes())
			}
			if verb == "create" && !strings.Contains(g.routes(), ownedHost) {
				t.Errorf("the routes were not written:\n%s", g.routes())
			}
			if got := counterValue(t, mHostClaimLedgerErrors.WithLabelValues("write")); got != errs+1 {
				t.Errorf("write errors went from %v to %v", errs, got)
			}
			if ev := eventsOf(g.events, "HostClaimLedgerNotWritten"); len(ev) != 1 || !strings.Contains(ev[0], "edge") {
				t.Errorf("events = %v", ev)
			}
		})
	}
}

func TestOwnership_ALedgerWriteConflictIsRetried(t *testing.T) {
	var conflicts, writes atomic.Int32
	var failOnce atomic.Bool
	g := newLedgerRigWith(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if isLedger(obj) {
				if failOnce.CompareAndSwap(true, false) {
					conflicts.Add(1)
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("changed"))
				}
				writes.Add(1)
			}
			return c.Update(ctx, obj, opts...)
		},
	}, ownerIngress("zzz", "shop", ownedHost, "/", t0))
	g.mustReconcile()
	g.create(ownerIngress("aaa", "more", "more.example.com", "/", t0))
	failOnce.Store(true)
	if res := g.mustReconcile(); res.RequeueAfter != 0 {
		t.Errorf("a conflict that was retried requeued: %v", res.RequeueAfter)
	}
	if conflicts.Load() != 1 || writes.Load() != 1 {
		t.Fatalf("conflicts=%d writes=%d, want 1 and 1", conflicts.Load(), writes.Load())
	}
	if cm, _ := g.ledger(); !strings.Contains(cm.Data["claims"], "aaa more.example.com ") {
		t.Errorf("the claim is not in the ledger:\n%s", cm.Data["claims"])
	}
	cm, _ := g.ledger()
	routes := g.routes()
	g.mustReconcile()
	if after, _ := g.ledger(); after.ResourceVersion != cm.ResourceVersion || g.routes() != routes {
		t.Errorf("a second render wrote again: ledger %s -> %s", cm.ResourceVersion, after.ResourceVersion)
	}
}

func TestSynapseRouteReconciler_NamesTheLedger(t *testing.T) {
	proxy := testProxy("synapse-os", "edge")
	if got := proxyHostClaimsName(proxy); got.Namespace != "synapse-os" || got.Name != "edge-host-claims" {
		t.Errorf("ledger = %v", got)
	}
}

// ledgerReconciler is an IngressReconciler that names a ledger, rendering to a
// file, on a clock the test sets.
func ledgerReconciler(t *testing.T, now *time.Time, funcs interceptor.Funcs, objs ...client.Object) *IngressReconciler {
	t.Helper()
	return &IngressReconciler{
		Client:           fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).WithInterceptorFuncs(funcs).Build(),
		IngressClassName: "synapse",
		UpstreamsOutPath: filepath.Join(t.TempDir(), "upstreams.yaml"),
		ClusterDomain:    "cluster.local",
		HostOwnership:    true,
		HostClaims:       types.NamespacedName{Namespace: "synapse-os", Name: "edge-host-claims"},
		RecordClaims:     true,
		Clock:            func() time.Time { return *now },
	}
}

func renderedFile(t *testing.T, r *IngressReconciler) string {
	t.Helper()
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out, err := os.ReadFile(r.UpstreamsOutPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func ledgerBody(t *testing.T, r *IngressReconciler) string {
	t.Helper()
	var cm corev1.ConfigMap
	if err := r.Get(context.Background(), r.HostClaims, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return ""
		}
		t.Fatal(err)
	}
	return cm.Data[hostClaimsKey]
}

func TestOwnership_OffNeitherReadsNorWritesTheLedger(t *testing.T) {
	now := t0.Add(2 * time.Hour)
	var touched atomic.Int32
	r := ledgerReconciler(t, &now, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok && strings.HasSuffix(key.Name, "-host-claims") {
				touched.Add(1)
				return apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, key.Name, errors.New("denied"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if isLedger(obj) {
				touched.Add(1)
			}
			return c.Create(ctx, obj, opts...)
		},
	}, ingressIn("zzz", "shop", ownedHost, "/"))
	r.HostOwnership = false
	if out := renderedFile(t, r); !strings.Contains(out, ownedHost) {
		t.Errorf("not rendered:\n%s", out)
	}
	if touched.Load() != 0 {
		t.Errorf("the ledger was touched %d times with ownership off", touched.Load())
	}
}

func TestOwnership_TheLedgerIsLeftAloneWhileOwnershipIsOff(t *testing.T) {
	now := t0.Add(2 * time.Hour)
	owner := ingressIn("zzz", "shop", ownedHost, "/")
	owner.CreationTimestamp = metav1.NewTime(t0)
	r := ledgerReconciler(t, &now, interceptor.Funcs{}, owner)
	renderedFile(t, r)
	before := ledgerBody(t, r)
	if !strings.Contains(before, "zzz "+ownedHost+" "+t0.Format(time.RFC3339Nano)) {
		t.Fatalf("ledger = %q", before)
	}

	// Ownership off: an older Ingress names the host, and nothing is recorded.
	r.HostOwnership = false
	edited := ingressIn("old", "edited", ownedHost, "/steal")
	edited.CreationTimestamp = metav1.NewTime(t0.Add(-24 * time.Hour))
	if err := r.Create(context.Background(), edited); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	renderedFile(t, r)
	if got := ledgerBody(t, r); got != before {
		t.Errorf("the ledger changed while ownership was off:\n%s", got)
	}

	// Back on: the owner keeps its age, and the claim made meanwhile is new.
	r.HostOwnership = true
	now = now.Add(time.Hour)
	if out := renderedFile(t, r); strings.Contains(out, "/steal") {
		t.Errorf("a claim made while ownership was off took the host:\n%s", out)
	}
	body := ledgerBody(t, r)
	if !strings.Contains(body, "zzz "+ownedHost+" "+t0.Format(time.RFC3339Nano)+"\n") ||
		!strings.Contains(body, "old "+ownedHost+" "+now.Format(time.RFC3339Nano)+"\n") {
		t.Errorf("ledger = %q", body)
	}
}

// countingReader counts the reads made through it.
type countingReader struct {
	client.Reader
	gets atomic.Int32
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets.Add(1)
	return c.Reader.Get(ctx, key, obj, opts...)
}

func TestOwnership_ARenderWithNothingToRecordCostsNoLiveRead(t *testing.T) {
	g := newLedgerRig(t)
	reads := &countingReader{Reader: g.r.Client}
	g.r.Reader = reads
	g.mustReconcile()
	g.mustReconcile()
	if n := reads.gets.Load(); n != 0 {
		t.Errorf("a proxy with no claims and no ledger read the ledger live %d times", n)
	}
	if _, ok := g.ledger(); ok {
		t.Error("a ledger was written for a proxy with no claims")
	}
}

func TestOwnership_ARenderWithNoInputChangedWritesNoLedger(t *testing.T) {
	var writes atomic.Int32
	g := newLedgerRigWith(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if isLedger(obj) {
				writes.Add(1)
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if isLedger(obj) {
				writes.Add(1)
			}
			return c.Update(ctx, obj, opts...)
		},
	}, ownerIngress("zzz", "shop", ownedHost, "/", t0))
	g.mustReconcile()
	first := writes.Load()
	g.now = g.now.Add(10 * time.Minute)
	g.mustReconcile()
	g.mustReconcile()
	if first != 1 || writes.Load() != first {
		t.Errorf("ledger written %d times by the first render and %d in all; want 1 and 1", first, writes.Load())
	}
}

func TestOwnership_AnAliasCannotForgeALedgerLine(t *testing.T) {
	g := newLedgerRig(t,
		ownerIngress("aaa", "shop", ownedHost, "/a", t0),
		ownerIngress("zzz", "later", ownedHost, "/z", t0.Add(time.Hour)))
	g.mustReconcile()
	forged := annotated(ownerIngress("evil", "x", "x.example.com", "/", t0.Add(3*time.Hour)),
		"server-alias", "y.example.com\naaa "+ownedHost)
	g.create(forged)
	for range 3 {
		g.now = g.now.Add(10 * time.Minute)
		g.mustReconcile()
	}
	if out := g.routes(); !strings.Contains(out, "/a") || strings.Contains(out, "/z") {
		t.Errorf("a forged ledger line moved the host from its owner:\n%s", out)
	}
	cm, _ := g.ledger()
	if strings.Contains(cm.Data["claims"], "y.example.com") {
		t.Errorf("the ledger holds the forged host:\n%s", cm.Data["claims"])
	}
	if got := eventsOf(g.events, "InvalidServerAlias"); len(got) == 0 {
		t.Error("the invalid alias was not said")
	}
}

func TestOwnership_AClaimThatReturnsAfterTheGraceWithNoRenderBetweenIsNew(t *testing.T) {
	zzz := ownerIngress("zzz", "shop", ownedHost, "/z", t0)
	g := newLedgerRig(t, zzz.DeepCopy(), ownerIngress("aaa", "later", ownedHost, "/a", t0.Add(time.Hour)))
	g.mustReconcile()
	g.delete(zzz)
	g.now = g.now.Add(10 * time.Minute)
	g.mustReconcile()
	g.now = g.now.Add(3 * time.Hour)
	g.create(zzz.DeepCopy())
	g.mustReconcile()
	if out := g.routes(); !strings.Contains(out, "/a") || strings.Contains(out, "/z") {
		t.Errorf("a claim gone for three hours took its old age back:\n%s", out)
	}
}

func TestOwnership_ALedgerWithLinesAndNoClaimsMarksThemAbsent(t *testing.T) {
	ing := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	g := newLedgerRig(t, ing.DeepCopy())
	g.mustReconcile()
	g.delete(ing)
	g.now = g.now.Add(10 * time.Minute)
	g.mustReconcile()
	cm, _ := g.ledger()
	if want := "zzz " + ownedHost; !strings.Contains(cm.Data["claims"], want) ||
		!strings.Contains(cm.Data["claims"], g.now.UTC().Format(time.RFC3339Nano)) {
		t.Errorf("the line was not marked absent now:\n%s", cm.Data["claims"])
	}
}

func floodIngress(hosts int) *networkingv1.Ingress {
	flood := ownerIngress("flood", "big", "flood.example.com", "/", t0)
	tls := networkingv1.IngressTLS{}
	for i := 0; i < hosts; i++ {
		tls.Hosts = append(tls.Hosts, fmt.Sprintf("flood-host-%06d.example.com", i))
	}
	flood.Spec.TLS = []networkingv1.IngressTLS{tls}
	return flood
}

func TestOwnership_AFloodedLedgerDoesNotTieAnOlderNewcomer(t *testing.T) {
	g := newLedgerRig(t, floodIngress(20000))
	g.mustReconcile()
	g.now = g.now.Add(time.Minute)
	g.create(ownerIngress("bbb", "shop", "new.example.com", "/b", g.now))
	g.mustReconcile()
	g.now = g.now.Add(time.Hour)
	g.create(ownerIngress("aaa", "later", "new.example.com", "/a", g.now))
	g.mustReconcile()
	if out := g.routes(); !strings.Contains(out, "/b") || strings.Contains(out, "/a") {
		t.Errorf("the host went to the namespace that sorts first, not to the one that named it first")
	}
}

// proxyEvents records the object each event is about.
type proxyEvents struct {
	reasons []string
	msgs    []string
	objs    []runtime.Object
}

func (p *proxyEvents) Event(o runtime.Object, _, reason, msg string) {
	p.objs, p.reasons, p.msgs = append(p.objs, o), append(p.reasons, reason), append(p.msgs, msg)
}
func (p *proxyEvents) Eventf(o runtime.Object, t, reason, f string, a ...any) {
	p.Event(o, t, reason, fmt.Sprintf(f, a...))
}
func (p *proxyEvents) AnnotatedEventf(o runtime.Object, _ map[string]string, t, reason, f string, a ...any) {
	p.Eventf(o, t, reason, f, a...)
}

func TestOwnership_AnOverQuotaNamespaceIsCountedAndWarnedOnTheProxy(t *testing.T) {
	g := newLedgerRig(t, floodIngress(maxHostClaimsPerNamespace+100))
	ev := &proxyEvents{}
	g.r.Recorder = ev
	before := counterValue(t, mHostClaimLedgerErrors.WithLabelValues("quota"))
	g.mustReconcile()
	if got := counterValue(t, mHostClaimLedgerErrors.WithLabelValues("quota")); got <= before {
		t.Errorf("quota counter did not move: %v -> %v", before, got)
	}
	n := 0
	for i, reason := range ev.reasons {
		if reason != "HostClaimQuotaExceeded" {
			continue
		}
		n++
		if _, ok := ev.objs[i].(*synapsev1alpha1.SynapseProxy); !ok {
			t.Errorf("event is on a %T, not the proxy", ev.objs[i])
		}
		if !strings.Contains(ev.msgs[i], "flood") || !strings.Contains(ev.msgs[i], "101") {
			t.Errorf("message = %q", ev.msgs[i])
		}
	}
	if n != 1 {
		t.Errorf("%d quota warnings, want 1 (reasons %v)", n, ev.reasons)
	}
}
