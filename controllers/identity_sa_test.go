package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	maxminddb "github.com/oschwald/maxminddb-golang/v2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func saSubject(ns, name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: ns, Name: name}
}

func crb(name, role string, subjects ...rbacv1.Subject) rbacv1.ClusterRoleBinding {
	return rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: role},
		Subjects:   subjects,
	}
}

func rb(ns, name, kind, role string, subjects ...rbacv1.Subject) rbacv1.RoleBinding {
	return rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		RoleRef:    rbacv1.RoleRef{Kind: kind, Name: role},
		Subjects:   subjects,
	}
}

func role(ns, name string, rules ...rbacv1.PolicyRule) rbacv1.Role {
	return rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Rules: rules}
}

var (
	wildcardVerbs     = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"*"}}
	wildcardResources = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"*"}, Verbs: []string{"get"}}
	readOnlyRule      = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list"}}
)

func TestResolveRecordsServiceAccount(t *testing.T) {
	mk := func(name, ip, sa string) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shop", Labels: map[string]string{"app": name}},
			Spec:       corev1.PodSpec{ServiceAccountName: sa},
			Status:     servingStatus(ip),
		}
	}
	entries := indexEntries(resolveIdentityEntries([]corev1.Pod{
		mk("a", "10.0.0.1", "builder"),
		mk("b", "10.0.0.2", ""),
	}))
	if got := entries["10.0.0.1"].serviceAccount; got != "builder" {
		t.Errorf("service account = %q, want builder", got)
	}
	if got := entries["10.0.0.2"].serviceAccount; got != "default" {
		t.Errorf("empty service account = %q, want default", got)
	}
}

func TestSAPrivilegeDerivation(t *testing.T) {
	cases := []struct {
		name string
		crbs []rbacv1.ClusterRoleBinding
		rbs  []rbacv1.RoleBinding
		rls  []rbacv1.Role
		crs  []rbacv1.ClusterRole
		ns   string
		sa   string
		want bool
	}{
		{name: "none", ns: "shop", sa: "app", want: false},
		{
			name: "cluster-admin clusterrolebinding",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", saSubject("shop", "app"))},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "other subject is not privileged",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", saSubject("shop", "other"))},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "same name in another namespace is not privileged",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", saSubject("ops", "app"))},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "clusterrolebinding to wildcard-verb clusterrole",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "wild", saSubject("shop", "app"))},
			crs:  []rbacv1.ClusterRole{{ObjectMeta: metav1.ObjectMeta{Name: "wild"}, Rules: []rbacv1.PolicyRule{readOnlyRule, wildcardVerbs}}},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "rolebinding to wildcard role",
			rbs:  []rbacv1.RoleBinding{rb("shop", "x", "Role", "wild", saSubject("shop", "app"))},
			rls:  []rbacv1.Role{role("shop", "wild", wildcardResources)},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "rolebinding to namespaced read-only role",
			rbs:  []rbacv1.RoleBinding{rb("shop", "x", "Role", "ro", saSubject("shop", "app"))},
			rls:  []rbacv1.Role{role("shop", "ro", readOnlyRule)},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "rolebinding resolves the role in its own namespace",
			rbs:  []rbacv1.RoleBinding{rb("shop", "x", "Role", "wild", saSubject("shop", "app"))},
			rls:  []rbacv1.Role{role("ops", "wild", wildcardVerbs)},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "rolebinding in another namespace does not count",
			rbs:  []rbacv1.RoleBinding{rb("ops", "x", "Role", "wild", saSubject("shop", "app"))},
			rls:  []rbacv1.Role{role("ops", "wild", wildcardVerbs)},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "rolebinding to cluster-admin in its namespace",
			rbs:  []rbacv1.RoleBinding{rb("shop", "x", "ClusterRole", "cluster-admin", saSubject("shop", "app"))},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "all service accounts group",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", rbacv1.Subject{Kind: rbacv1.GroupKind, Name: "system:serviceaccounts"})},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "namespace service accounts group",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", rbacv1.Subject{Kind: rbacv1.GroupKind, Name: "system:serviceaccounts:shop"})},
			ns:   "shop", sa: "app", want: true,
		},
		{
			name: "other namespace group does not count",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "cluster-admin", rbacv1.Subject{Kind: rbacv1.GroupKind, Name: "system:serviceaccounts:ops"})},
			ns:   "shop", sa: "app", want: false,
		},
		{
			name: "dangling roleRef is not privileged",
			crbs: []rbacv1.ClusterRoleBinding{crb("x", "gone", saSubject("shop", "app"))},
			ns:   "shop", sa: "app", want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newSAPrivilege(c.crbs, c.rbs, c.rls, c.crs)
			if got := p.privileged(c.ns, c.sa); got != c.want {
				t.Fatalf("privileged(%s/%s) = %v, want %v", c.ns, c.sa, got, c.want)
			}
		})
	}
}

func TestMMDBCarriesServiceAccount(t *testing.T) {
	mmdb, err := buildIdentityMMDB([]identityEntry{
		{ip: "10.0.0.1", workload: "w", namespace: "shop", app: "w", serviceAccount: "builder", saPrivileged: true},
		{ip: "10.0.0.2", workload: "v", namespace: "shop", app: "v", serviceAccount: "default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := maxminddb.OpenBytes(mmdb)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	type rec struct {
		ServiceAccount string `maxminddb:"service_account"`
		SAPrivileged   bool   `maxminddb:"sa_privileged"`
	}
	var a, b rec
	if err := reader.Lookup(netip.MustParseAddr("10.0.0.1")).Decode(&a); err != nil {
		t.Fatal(err)
	}
	if err := reader.Lookup(netip.MustParseAddr("10.0.0.2")).Decode(&b); err != nil {
		t.Fatal(err)
	}
	if a != (rec{"builder", true}) || b != (rec{"default", false}) {
		t.Fatalf("mmdb records: %+v %+v", a, b)
	}
}

func TestIdentityVersionTracksServiceAccount(t *testing.T) {
	base := []identityEntry{{ip: "1.1.1.1", workload: "w", namespace: "n", app: "a", serviceAccount: "s"}}
	sa := []identityEntry{{ip: "1.1.1.1", workload: "w", namespace: "n", app: "a", serviceAccount: "t"}}
	priv := []identityEntry{{ip: "1.1.1.1", workload: "w", namespace: "n", app: "a", serviceAccount: "s", saPrivileged: true}}
	if identityVersion(base) == identityVersion(sa) || identityVersion(base) == identityVersion(priv) {
		t.Fatal("identityVersion ignores the service account fields")
	}
}

type deltaSink struct {
	mu     sync.Mutex
	deltas []IdentityDelta
}

func (s *deltaSink) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/identity/delta" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var d IdentityDelta
		if err := json.Unmarshal(body, &d); err != nil {
			t.Errorf("delta body: %v", err)
		}
		s.mu.Lock()
		s.deltas = append(s.deltas, d)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
}

// A binding change re-derives the flag: the delta carries both keys, and
// removing the binding emits a delta that flips it back.
func TestFlushDeltaServiceAccountPrivilege(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", Labels: map[string]string{"app": "web"}},
		Spec:       corev1.PodSpec{ServiceAccountName: "builder"},
		Status:     servingStatus("10.0.0.1"),
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "builder-admin"},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{saSubject("shop", "builder")},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

	sink := &deltaSink{}
	srv := httptest.NewServer(sink.handler(t))
	defer srv.Close()
	r := &IdentityProducerReconciler{
		Client: cl, Log: logr.Discard(), UploadURL: srv.URL, HTTPClient: srv.Client(),
		index: map[string]identityEntry{},
	}
	ctx := context.Background()

	r.flushDelta(ctx)
	if err := cl.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	r.flushDelta(ctx)
	if err := cl.Delete(ctx, binding); err != nil {
		t.Fatal(err)
	}
	r.flushDelta(ctx)

	if len(sink.deltas) != 3 {
		t.Fatalf("got %d deltas, want 3 (initial, bound, unbound): %+v", len(sink.deltas), sink.deltas)
	}
	type view struct {
		sa   string
		priv bool
	}
	want := []view{{"builder", false}, {"builder", true}, {"builder", false}}
	for i, d := range sink.deltas {
		if len(d.Upserts) != 1 {
			t.Fatalf("delta %d: upserts %+v", i, d.Upserts)
		}
		u := d.Upserts[0]
		if got := (view{u.ServiceAccount, u.SAPrivileged}); got != want[i] {
			t.Errorf("delta %d: %+v, want %+v", i, got, want[i])
		}
	}
	raw, _ := json.Marshal(sink.deltas[1].Upserts[0])
	var keys map[string]any
	_ = json.Unmarshal(raw, &keys)
	for _, k := range []string{"service_account", "sa_privileged"} {
		if _, ok := keys[k]; !ok {
			t.Errorf("delta upsert lacks key %q: %s", k, raw)
		}
	}
	// A false flag must still be on the wire so an agent clears a stale true.
	raw, _ = json.Marshal(sink.deltas[2].Upserts[0])
	_ = json.Unmarshal(raw, &keys)
	if _, ok := keys["sa_privileged"]; !ok {
		t.Errorf("false sa_privileged omitted from the wire: %s", raw)
	}
}

func TestProducersRoleReadsRBAC(t *testing.T) {
	var role rbacv1.ClusterRole
	if err := json.Unmarshal(readManifest(t, "../config/rbac-producers.yaml")["ClusterRole"], &role); err != nil {
		t.Fatal(err)
	}
	readOnly := []string{"get", "list", "watch"}
	want := map[string][]string{
		"":                          {"serviceaccounts"},
		"rbac.authorization.k8s.io": {"rolebindings", "clusterrolebindings", "roles", "clusterroles"},
	}
	for group, resources := range want {
		for _, res := range resources {
			var verbs []string
			for _, rule := range role.Rules {
				if slices.Contains(rule.APIGroups, group) && slices.Contains(rule.Resources, res) {
					verbs = append(verbs, rule.Verbs...)
				}
			}
			slices.Sort(verbs)
			if !slices.Equal(slices.Compact(verbs), readOnly) {
				t.Errorf("%s/%s verbs = %v, want exactly %v", group, res, verbs, readOnly)
			}
		}
	}
}

// When the RBAC objects cannot be read, the last published flag is kept: a
// transient error or a missing grant must not clear a detection.
func TestRBACListFailureKeepsPublishedFlag(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", Labels: map[string]string{"app": "web"}},
		Spec:       corev1.PodSpec{ServiceAccountName: "builder"},
		Status:     servingStatus("10.0.0.1"),
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "builder-admin"},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{saSubject("shop", "builder")},
	}
	var failRBAC bool
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, binding).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, o ...client.ListOption) error {
			if _, ok := l.(*rbacv1.RoleList); ok && failRBAC {
				return context.DeadlineExceeded
			}
			return c.List(ctx, l, o...)
		}}).Build()

	sink := &deltaSink{}
	srv := httptest.NewServer(sink.handler(t))
	defer srv.Close()
	r := &IdentityProducerReconciler{
		Client: cl, Log: logr.Discard(), UploadURL: srv.URL, HTTPClient: srv.Client(),
		index: map[string]identityEntry{},
	}
	ctx := context.Background()

	r.flushDelta(ctx)
	if len(sink.deltas) != 1 || !sink.deltas[0].Upserts[0].SAPrivileged {
		t.Fatalf("setup: want one delta with a true flag, got %+v", sink.deltas)
	}

	failRBAC = true
	r.flushDelta(ctx)
	if len(sink.deltas) != 1 {
		t.Fatalf("flag flipped on an RBAC read failure: %+v", sink.deltas[1:])
	}
	r.buildAndUpload(ctx)
	if !r.index["10.0.0.1"].saPrivileged {
		t.Fatal("baseline reseeded the flag to false on an RBAC read failure")
	}

	// With no previous value there is nothing to keep: false.
	r2 := &IdentityProducerReconciler{Client: cl, Log: logr.Discard(), index: map[string]identityEntry{}}
	entries := []identityEntry{{ip: "10.0.0.1", namespace: "shop", serviceAccount: "builder"}}
	if r2.stampServiceAccountPrivilege(ctx, entries) || entries[0].saPrivileged {
		t.Fatal("a failed read with no previous value must report failure and leave false")
	}
}
