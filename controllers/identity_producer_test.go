package controllers

import (
	"net/netip"
	"testing"
	"time"

	maxminddb "github.com/oschwald/maxminddb-golang/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func boolPtr(b bool) *bool { return &b }

// servingStatus builds a Running+Ready pod status with the given IP so
// resolveIdentityEntries' podServing gate accepts it.
func servingStatus(ip string) corev1.PodStatus {
	return corev1.PodStatus{
		PodIP: ip,
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
	}
}

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
			Status: servingStatus("192.168.1.10"),
		},
	}
	entries := resolveIdentityEntries(pods)
	if len(entries) != 1 || entries[0].ip != "192.168.1.10" || entries[0].workload != "web" {
		t.Fatalf("resolveIdentityEntries skipped host-network wrong: %+v", entries)
	}
}

// TestResolveRecycleGuard is the regression for the IP-recycle race: a
// terminating pod that still reports an IP must not shadow the live pod that
// recycled it, and among serving pods the most-recently-started wins.
func TestResolveRecycleGuard(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	recent := metav1.NewTime(time.Now())

	// Dying CronJob pod: Running+Ready but has a DeletionTimestamp, still on .150.
	dyingStatus := servingStatus("192.168.7.150")
	dyingStatus.StartTime = &old
	dying := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scorer-old", Namespace: "scorer",
			DeletionTimestamp: &old,
			Labels:            map[string]string{"app": "scorer"},
			OwnerReferences:   []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "scorer-abc123", Controller: boolPtr(true)}},
		},
		Status: dyingStatus,
	}
	// Live pod that recycled .150, started later.
	liveStatus := servingStatus("192.168.7.150")
	liveStatus.StartTime = &recent
	live := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-db-xyz", Namespace: "demo-database",
			Labels:          map[string]string{"app": "demo-db"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "demo-db-77b4b4f7dd", Controller: boolPtr(true)}},
		},
		Status: liveStatus,
	}

	// Order-independent: the terminating pod is skipped, the live pod owns .150.
	for _, order := range [][]corev1.Pod{{dying, live}, {live, dying}} {
		entries := resolveIdentityEntries(order)
		if len(entries) != 1 || entries[0].namespace != "demo-database" || entries[0].workload != "demo-db" {
			t.Fatalf("recycle guard: .150 should resolve to live demo-db, got %+v", entries)
		}
	}

	// A pending (not-yet-Ready) pod does not claim an IP.
	pending := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "boot", Namespace: "x", Labels: map[string]string{"app": "boot"}},
		Status:     corev1.PodStatus{PodIP: "192.168.9.9", Phase: corev1.PodPending},
	}
	if entries := resolveIdentityEntries([]corev1.Pod{pending}); len(entries) != 0 {
		t.Fatalf("pending pod should not claim an IP, got %+v", entries)
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
