package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// apiRequest is one request seen by the fake download-api.
type apiRequest struct {
	Method string
	Path   string
	Query  string
	Auth   string
	Body   []byte
}

// downloadAPI records every request and answers with a programmable status.
type downloadAPI struct {
	srv *httptest.Server

	mu     sync.Mutex
	status int
	reqs   []apiRequest
}

func newDownloadAPI(t *testing.T) *downloadAPI {
	t.Helper()
	d := &downloadAPI{status: http.StatusOK}
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		d.mu.Lock()
		d.reqs = append(d.reqs, apiRequest{
			Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
			Auth: r.Header.Get("Authorization"), Body: body,
		})
		st := d.status
		d.mu.Unlock()
		w.WriteHeader(st)
	}))
	t.Cleanup(d.srv.Close)
	return d
}

func (d *downloadAPI) setStatus(s int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status = s
}

func (d *downloadAPI) requests() []apiRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]apiRequest(nil), d.reqs...)
}

func (d *downloadAPI) count() int { return len(d.requests()) }

func (d *downloadAPI) last(t *testing.T) apiRequest {
	t.Helper()
	rs := d.requests()
	if len(rs) == 0 {
		t.Fatal("download-api saw no request")
	}
	return rs[len(rs)-1]
}

func producerKube(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
}

func servingPod(name, ip string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "shop", Labels: labels,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: name + "-7d9f8c6b5", Controller: boolPtr(true)},
			},
		},
		Status: servingStatus(ip),
	}
}

func newIdentityProducer(kube client.Client, api *downloadAPI, clusterID string) *IdentityProducerReconciler {
	return &IdentityProducerReconciler{
		Client:     kube,
		Log:        logr.Discard(),
		UploadURL:  api.srv.URL + "/v1/",
		APIKey:     "k-test",
		HTTPClient: api.srv.Client(),
		ClusterID:  clusterID,
		epoch:      42,
		index:      map[string]identityEntry{},
	}
}

func newEdgeProducer(kube client.Client, api *downloadAPI, clusterID string) *EdgeProducerReconciler {
	return &EdgeProducerReconciler{
		Client:     kube,
		Log:        logr.Discard(),
		UploadURL:  api.srv.URL + "/v1/",
		APIKey:     "k-test",
		HTTPClient: api.srv.Client(),
		ClusterID:  clusterID,
		epoch:      42,
		index:      map[string]struct{}{},
	}
}

func decodeIdentityDelta(t *testing.T, r apiRequest) IdentityDelta {
	t.Helper()
	var d IdentityDelta
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("delta body is not JSON: %v\n%s", err, r.Body)
	}
	return d
}

func decodeEdgeDelta(t *testing.T, r apiRequest) EdgeDelta {
	t.Helper()
	var d EdgeDelta
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("delta body is not JSON: %v\n%s", err, r.Body)
	}
	return d
}

func TestIdentityProducer_UploadsBaseline(t *testing.T) {
	api := newDownloadAPI(t)
	kube := producerKube(t, servingPod("web", "10.1.0.5", map[string]string{"app": "web"}))
	r := newIdentityProducer(kube, api, "")

	r.buildAndUpload(context.Background())

	if api.count() != 1 {
		t.Fatalf("want 1 request, got %d", api.count())
	}
	got := api.last(t)
	if got.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.Method)
	}
	if got.Path != "/v1/identity/upload" {
		t.Errorf("path = %q, want /v1/identity/upload", got.Path)
	}
	if !strings.HasPrefix(got.Query, "version=") || len(got.Query) <= len("version=") {
		t.Errorf("query = %q, want a non-empty version", got.Query)
	}
	if got.Auth != "Bearer k-test" {
		t.Errorf("auth = %q, want Bearer k-test", got.Auth)
	}
	if len(got.Body) == 0 {
		t.Error("baseline body is empty")
	}
	if r.lastVersion == "" || !strings.Contains(got.Query, r.lastVersion) {
		t.Errorf("lastVersion %q not recorded or not in query %q", r.lastVersion, got.Query)
	}
}

func TestIdentityProducer_ClusterScope(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, cluster string
	}{{"scoped", "uid-123"}, {"empty", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			api := newDownloadAPI(t)
			kube := producerKube(t, servingPod("web", "10.1.0.5", map[string]string{"app": "web"}))
			r := newIdentityProducer(kube, api, tc.cluster)

			r.buildAndUpload(ctx)
			base := api.last(t)

			// Add a pod so a delta is emitted.
			if err := kube.Create(ctx, servingPod("db", "10.1.0.6", map[string]string{"app": "db"})); err != nil {
				t.Fatal(err)
			}
			r.flushDelta(ctx)
			delta := api.last(t)
			if delta.Path != "/v1/identity/delta" {
				t.Fatalf("last request path = %q, want the delta endpoint", delta.Path)
			}
			d := decodeIdentityDelta(t, delta)

			if tc.cluster != "" {
				if !strings.HasSuffix(base.Query, "&cluster=uid-123") {
					t.Errorf("baseline query = %q, want &cluster=uid-123", base.Query)
				}
				if d.Cluster != "uid-123" {
					t.Errorf("delta cluster = %q, want uid-123", d.Cluster)
				}
				return
			}
			if strings.Contains(base.Query, "cluster") {
				t.Errorf("baseline query = %q, must not carry cluster when id is empty", base.Query)
			}
			if strings.Contains(string(delta.Body), "cluster") {
				t.Errorf("delta body = %s, must omit cluster when id is empty", delta.Body)
			}
		})
	}
}

func TestIdentityProducer_SkipsUnchangedBaseline(t *testing.T) {
	ctx := context.Background()
	api := newDownloadAPI(t)
	kube := producerKube(t, servingPod("web", "10.1.0.5", map[string]string{"app": "web"}))
	r := newIdentityProducer(kube, api, "")

	r.buildAndUpload(ctx)
	r.buildAndUpload(ctx)
	if api.count() != 1 {
		t.Fatalf("unchanged identity must not re-upload: got %d requests", api.count())
	}

	if err := kube.Create(ctx, servingPod("db", "10.1.0.6", map[string]string{"app": "db"})); err != nil {
		t.Fatal(err)
	}
	r.buildAndUpload(ctx)
	if api.count() != 2 {
		t.Fatalf("changed identity must upload again: got %d requests", api.count())
	}
}

func TestIdentityProducer_DeltaSeqAdvancesOnlyOnSuccess(t *testing.T) {
	ctx := context.Background()
	api := newDownloadAPI(t)
	kube := producerKube(t, servingPod("web", "10.1.0.5", map[string]string{"app": "web"}))
	r := newIdentityProducer(kube, api, "")
	r.buildAndUpload(ctx)
	baseline := api.count()

	if err := kube.Create(ctx, servingPod("db", "10.1.0.6", map[string]string{"app": "db"})); err != nil {
		t.Fatal(err)
	}

	api.setStatus(http.StatusInternalServerError)
	r.flushDelta(ctx)
	if r.seq != 0 {
		t.Fatalf("seq advanced to %d after a failed POST", r.seq)
	}
	if _, ok := r.index["10.1.0.6"]; ok {
		t.Fatal("index advanced after a failed POST")
	}
	failed := decodeIdentityDelta(t, api.last(t))
	if failed.Seq != 1 {
		t.Fatalf("failed delta seq = %d, want 1", failed.Seq)
	}

	api.setStatus(http.StatusOK)
	r.flushDelta(ctx)
	if api.count() != baseline+2 {
		t.Fatalf("want the same diff re-sent, got %d requests", api.count()-baseline)
	}
	retried := decodeIdentityDelta(t, api.last(t))
	if retried.Seq != 1 || len(retried.Upserts) != 1 || retried.Upserts[0].IP != "10.1.0.6" {
		t.Errorf("retried delta = %+v, want seq 1 with the db upsert", retried)
	}
	if retried.Epoch != 42 {
		t.Errorf("epoch = %d, want 42", retried.Epoch)
	}
	if r.seq != 1 {
		t.Fatalf("seq = %d after success, want 1", r.seq)
	}

	// Nothing changed: no POST. Then a removal gets the next seq.
	r.flushDelta(ctx)
	if api.count() != baseline+2 {
		t.Fatalf("unchanged state must not POST a delta")
	}
	if err := kube.Delete(ctx, servingPod("db", "10.1.0.6", nil)); err != nil {
		t.Fatal(err)
	}
	r.flushDelta(ctx)
	rm := decodeIdentityDelta(t, api.last(t))
	if rm.Seq != 2 || len(rm.Removes) != 1 || rm.Removes[0] != "10.1.0.6" {
		t.Errorf("removal delta = %+v, want seq 2 removing 10.1.0.6", rm)
	}
}

// The code has no in-call retry: a failed baseline upload leaves lastVersion
// unset, so the next baseline pass (the periodic tick) uploads again.
func TestIdentityProducer_UploadFailureRetries(t *testing.T) {
	ctx := context.Background()
	api := newDownloadAPI(t)
	kube := producerKube(t, servingPod("web", "10.1.0.5", map[string]string{"app": "web"}))
	r := newIdentityProducer(kube, api, "")

	api.setStatus(http.StatusServiceUnavailable)
	r.buildAndUpload(ctx)
	if api.count() != 1 {
		t.Fatalf("want exactly 1 attempt per pass, got %d", api.count())
	}
	if r.lastVersion != "" {
		t.Fatalf("lastVersion = %q after a failed upload, want empty", r.lastVersion)
	}

	api.setStatus(http.StatusOK)
	r.buildAndUpload(ctx)
	if api.count() != 2 {
		t.Fatalf("next pass must retry the upload: got %d requests", api.count())
	}
	if r.lastVersion == "" {
		t.Fatal("lastVersion not recorded after a successful retry")
	}
	r.buildAndUpload(ctx)
	if api.count() != 2 {
		t.Fatalf("after success the unchanged baseline is skipped: got %d requests", api.count())
	}
}

func edgeFixture() []client.Object {
	port := intstr.FromInt(8080)
	api := pod("api-aaa", "shop", map[string]string{"app": "api"}, "api-7d9f8c6b5")
	fe := pod("frontend-bbb", "shop", map[string]string{"app": "frontend"}, "frontend-5f4b9c8d2")
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "api-allow", Namespace: "shop"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "frontend"}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Port: &port}},
			}},
		},
	}
	return []client.Object{&api, &fe, np}
}

func TestEdgeProducer_UploadsBaseline(t *testing.T) {
	api := newDownloadAPI(t)
	r := newEdgeProducer(producerKube(t, edgeFixture()...), api, "")

	r.buildAndUpload(context.Background())

	if api.count() != 1 {
		t.Fatalf("want 1 request, got %d", api.count())
	}
	got := api.last(t)
	if got.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.Method)
	}
	if got.Path != "/v1/policy-edges/upload" {
		t.Errorf("path = %q, want /v1/policy-edges/upload", got.Path)
	}
	if !strings.HasPrefix(got.Query, "version=edges-") {
		t.Errorf("query = %q, want version=edges-...", got.Query)
	}
	if got.Auth != "Bearer k-test" {
		t.Errorf("auth = %q, want Bearer k-test", got.Auth)
	}
	for _, want := range []string{"E shop/frontend > shop/api : 8080", "G shop/api"} {
		if !strings.Contains(string(got.Body), want) {
			t.Errorf("body missing %q:\n%s", want, got.Body)
		}
	}

	if r.lastVersion == "" {
		t.Error("lastVersion not recorded after a successful upload")
	}
	r.buildAndUpload(context.Background())
	if api.count() != 1 {
		t.Errorf("unchanged edge-set must not re-upload: got %d requests", api.count())
	}
}

func TestEdgeProducer_ClusterScope(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, cluster string
	}{{"scoped", "uid-123"}, {"empty", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			api := newDownloadAPI(t)
			kube := producerKube(t, edgeFixture()...)
			r := newEdgeProducer(kube, api, tc.cluster)

			r.buildAndUpload(ctx)
			base := api.last(t)

			if err := kube.Delete(ctx, &networkingv1.NetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "api-allow", Namespace: "shop"},
			}); err != nil {
				t.Fatal(err)
			}
			r.flushDelta(ctx)
			delta := api.last(t)
			if delta.Path != "/v1/policy-edges/delta" {
				t.Fatalf("last request path = %q, want the delta endpoint", delta.Path)
			}
			d := decodeEdgeDelta(t, delta)

			if tc.cluster != "" {
				if !strings.HasSuffix(base.Query, "&cluster=uid-123") {
					t.Errorf("baseline query = %q, want &cluster=uid-123", base.Query)
				}
				if d.Cluster != "uid-123" {
					t.Errorf("delta cluster = %q, want uid-123", d.Cluster)
				}
				return
			}
			if strings.Contains(base.Query, "cluster") {
				t.Errorf("baseline query = %q, must not carry cluster when id is empty", base.Query)
			}
			if strings.Contains(string(delta.Body), "cluster") {
				t.Errorf("delta body = %s, must omit cluster when id is empty", delta.Body)
			}
		})
	}
}

func TestEdgeProducer_DeltaLines(t *testing.T) {
	ctx := context.Background()
	api := newDownloadAPI(t)
	kube := producerKube(t, edgeFixture()...)
	r := newEdgeProducer(kube, api, "")
	r.buildAndUpload(ctx)

	// Unchanged: no delta.
	r.flushDelta(ctx)
	if api.count() != 1 {
		t.Fatalf("unchanged state must not POST a delta, got %d requests", api.count())
	}

	// Remove the policy: its lines are removed.
	if err := kube.Delete(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "api-allow", Namespace: "shop"},
	}); err != nil {
		t.Fatal(err)
	}
	r.flushDelta(ctx)
	got := api.last(t)
	if got.Method != http.MethodPost || got.Auth != "Bearer k-test" {
		t.Errorf("delta method/auth = %s / %q", got.Method, got.Auth)
	}
	d := decodeEdgeDelta(t, got)
	if d.Epoch != 42 || d.Seq != 1 {
		t.Errorf("epoch/seq = %d/%d, want 42/1", d.Epoch, d.Seq)
	}
	if len(d.Upserts) != 0 {
		t.Errorf("upserts = %v, want none", d.Upserts)
	}
	wantRm := map[string]bool{"E shop/frontend > shop/api : 8080": true, "G shop/api": true}
	for _, l := range d.Removes {
		delete(wantRm, l)
	}
	if len(wantRm) != 0 {
		t.Errorf("removes = %v, missing %v", d.Removes, wantRm)
	}

	// A failed POST keeps seq and the diff base; re-adding the policy upserts.
	if err := kube.Create(ctx, edgeFixture()[2]); err != nil {
		t.Fatal(err)
	}
	api.setStatus(http.StatusInternalServerError)
	r.flushDelta(ctx)
	if r.seq != 1 {
		t.Fatalf("seq = %d after failed POST, want 1", r.seq)
	}
	api.setStatus(http.StatusOK)
	r.flushDelta(ctx)
	d = decodeEdgeDelta(t, api.last(t))
	if d.Seq != 2 || len(d.Removes) != 0 {
		t.Errorf("re-add delta = %+v, want seq 2 and no removes", d)
	}
	found := false
	for _, l := range d.Upserts {
		if l == "E shop/frontend > shop/api : 8080" {
			found = true
		}
	}
	if !found {
		t.Errorf("upserts = %v, want the frontend->api edge", d.Upserts)
	}
}

// With no API key the download-api would refuse every request, so the
// producers make none and say why once.
func TestProducers_IdleWithoutAPIKey(t *testing.T) {
	for _, key := range []string{"", " ", " \t\n"} {
		for _, kind := range []string{"identity", "edge"} {
			t.Run(fmt.Sprintf("%s/%q", kind, key), func(t *testing.T) {
				api := newDownloadAPI(t)
				var lines int
				var mu sync.Mutex
				log := funcr.New(func(_, _ string) { mu.Lock(); lines++; mu.Unlock() }, funcr.Options{})
				kube := producerKube(t, servingPod("web-1", "10.0.0.1", map[string]string{"app": "web"}))

				var start func(context.Context) error
				if kind == "identity" {
					p := newIdentityProducer(kube, api, "")
					p.APIKey, p.Log = key, log
					start = p.Start
				} else {
					p := newEdgeProducer(kube, api, "")
					p.APIKey, p.Log = key, log
					start = p.Start
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- start(ctx) }()
				time.Sleep(500 * time.Millisecond)
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}

				if n := api.count(); n != 0 {
					t.Errorf("%d requests without an API key, want 0 (first: %s %s)", n, api.requests()[0].Method, api.requests()[0].Path)
				}
				mu.Lock()
				defer mu.Unlock()
				if lines != 1 {
					t.Errorf("%d log lines, want exactly 1", lines)
				}
			})
		}
	}
}
