package controllers

import (
	"net/netip"
	"testing"

	maxminddb "github.com/oschwald/maxminddb-golang/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func boolPtr(b bool) *bool { return &b }

func TestTrimReplicaSetHash(t *testing.T) {
	cases := map[string]string{
		"frontend-7d9f8c6b5": "frontend",        // deployment pod-template hash
		"api-server-5f4b9c":  "api-server",      // hyphenated name + hash
		"frontend":           "frontend",        // no hash
		"app-v2":             "app-v2",          // short suffix, not a hash
		"web-abcdefghijk":    "web-abcdefghijk", // suffix too long
	}
	for in, want := range cases {
		if got := trimReplicaSetHash(in); got != want {
			t.Errorf("trimReplicaSetHash(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWorkloadIdentity(t *testing.T) {
	// Deployment pod: owner is a ReplicaSet -> trim hash.
	dep := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend-7d9f8c6b5-abcde",
			Namespace: "shop",
			Labels:    map[string]string{"app.kubernetes.io/name": "frontend"},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "frontend-7d9f8c6b5", Controller: boolPtr(true)},
			},
		},
	}
	if w, app := workloadIdentity(dep); w != "frontend" || app != "frontend" {
		t.Errorf("deployment: got workload=%q app=%q, want frontend/frontend", w, app)
	}

	// StatefulSet pod: keyed per-pod (ordinal) so replicas are distinct nodes and
	// their replication/inter-broker traffic is a real edge, not a self-loop.
	sts := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-0",
			Namespace: "db",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "StatefulSet", Name: "postgres", Controller: boolPtr(true)},
			},
		},
	}
	if w, _ := workloadIdentity(sts); w != "postgres-0" {
		t.Errorf("statefulset: got workload=%q, want postgres-0", w)
	}

	// StrimziPodSet (kafka brokers): ordinal pods under a non-StatefulSet
	// controller — still keyed per-pod via the <controller>-<ordinal> shape.
	kafka := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "core-kafka-2",
			Namespace: "kafka",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "StrimziPodSet", Name: "core-kafka", Controller: boolPtr(true)},
			},
		},
	}
	if w, _ := workloadIdentity(kafka); w != "core-kafka-2" {
		t.Errorf("strimzipodset: got workload=%q, want core-kafka-2", w)
	}

	// Bare pod (no controller): falls back to app label.
	bare := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "debug",
			Namespace: "ops",
			Labels:    map[string]string{"app": "debugger"},
		},
	}
	if w, app := workloadIdentity(bare); w != "debugger" || app != "debugger" {
		t.Errorf("bare: got workload=%q app=%q, want debugger/debugger", w, app)
	}
}

func TestResolveSkipsHostNetwork(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-agent", Namespace: "kube-system"},
			Spec:       corev1.PodSpec{HostNetwork: true},
			Status:     corev1.PodStatus{PodIP: "10.0.0.5"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-1", Namespace: "shop",
				Labels:          map[string]string{"app": "web"},
				OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123", Controller: boolPtr(true)}},
			},
			Status: corev1.PodStatus{PodIP: "192.168.1.10"},
		},
	}
	entries := resolveIdentityEntries(pods)
	if len(entries) != 1 || entries[0].ip != "192.168.1.10" || entries[0].workload != "web" {
		t.Fatalf("resolveIdentityEntries skipped host-network wrong: %+v", entries)
	}
}

// TestBuildIdentityMMDBRoundTrip proves the produced MMDB decodes back to the
// same identity the agent's Rust reader expects (workload/namespace/app keys).
func TestBuildIdentityMMDBRoundTrip(t *testing.T) {
	entries := []identityEntry{
		{ip: "192.168.1.10", workload: "frontend", namespace: "shop", app: "web"},
		{ip: "10.2.3.4", workload: "kube-apiserver", namespace: "kube-system", app: "apiserver"},
	}
	mmdb, err := buildIdentityMMDB(entries)
	if err != nil {
		t.Fatalf("buildIdentityMMDB: %v", err)
	}
	reader, err := maxminddb.OpenBytes(mmdb)
	if err != nil {
		t.Fatalf("OpenBytes: %v", err)
	}
	defer reader.Close()

	var rec struct {
		Workload  string `maxminddb:"workload"`
		Namespace string `maxminddb:"namespace"`
		App       string `maxminddb:"app"`
	}
	res := reader.Lookup(netip.MustParseAddr("192.168.1.10"))
	if err := res.Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Workload != "frontend" || rec.Namespace != "shop" || rec.App != "web" {
		t.Fatalf("round-trip mismatch: %+v", rec)
	}

	// An unmapped IP must not resolve.
	if reader.Lookup(netip.MustParseAddr("8.8.8.8")).Found() {
		t.Fatal("external IP unexpectedly found in identity MMDB")
	}
}

// version is deterministic + change-sensitive.
func TestIdentityVersionStable(t *testing.T) {
	a := []identityEntry{{ip: "1.1.1.1", workload: "w", namespace: "n", app: "a"}}
	b := []identityEntry{{ip: "1.1.1.1", workload: "w", namespace: "n", app: "a"}}
	if identityVersion(a) != identityVersion(b) {
		t.Fatal("identityVersion not stable for identical input")
	}
	c := []identityEntry{{ip: "1.1.1.1", workload: "w2", namespace: "n", app: "a"}}
	if identityVersion(a) == identityVersion(c) {
		t.Fatal("identityVersion did not change when identity changed")
	}
}
