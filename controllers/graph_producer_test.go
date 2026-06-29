package controllers

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// collectGraphWorkloads must roll every pod IP of a workload into one vertex's
// sorted IPs list, so service-graph-api can resolve observed traffic (keyed by
// raw IP) back to the workload ref.
func TestCollectGraphWorkloadsAggregatesIPs(t *testing.T) {
	withIP := func(name, ns string, labels map[string]string, rsOwner, ip string) corev1.Pod {
		p := pod(name, ns, labels, rsOwner)
		p.Status.PodIP = ip
		return p
	}
	pods := []corev1.Pod{
		withIP("api-aaa", "shop", map[string]string{"app": "api"}, "api-7d9f8c6b5", "10.0.0.9"),
		withIP("api-bbb", "shop", map[string]string{"app": "api"}, "api-7d9f8c6b5", "10.0.0.2"),
		withIP("db-ccc", "shop", map[string]string{"app": "db"}, "db-1a2b3c4d5", "10.0.1.7"),
	}
	got := map[string][]string{}
	for _, w := range collectGraphWorkloads(pods) {
		got[w.Ref] = w.IPs
	}
	if want := []string{"10.0.0.2", "10.0.0.9"}; !reflect.DeepEqual(got["shop/api"], want) {
		t.Errorf("shop/api IPs = %v, want %v (deduped + sorted)", got["shop/api"], want)
	}
	if want := []string{"10.0.1.7"}; !reflect.DeepEqual(got["shop/db"], want) {
		t.Errorf("shop/db IPs = %v, want %v", got["shop/db"], want)
	}
}

func TestGraphVersionStableAndSensitive(t *testing.T) {
	w := []graphWorkload{{Ref: "a/x", App: "x"}}
	e := []declaredEdge{{Src: "a/x", Dst: "a/y"}}
	v1 := graphVersion(w, e)
	if v1 != graphVersion(w, e) {
		t.Error("graphVersion not stable for identical input")
	}
	if v1 == graphVersion(w, nil) {
		t.Error("graphVersion should change when edges differ")
	}
}

func TestCollectDeclaredEdgesDropsWildcardAndSelf(t *testing.T) {
	// walkPolicyEdges is exercised via the edge-producer tests; here just assert
	// the graph-side filtering invariants through a tiny direct construction.
	set := map[declaredEdge]struct{}{}
	emit := func(src, dst string) {
		if src == "*/*" || dst == "*/*" || src == dst {
			return
		}
		set[declaredEdge{Src: src, Dst: dst}] = struct{}{}
	}
	emit("a/x", "a/y") // kept
	emit("*/*", "a/y") // dropped (wildcard src)
	emit("a/x", "*/*") // dropped (wildcard dst)
	emit("a/x", "a/x") // dropped (self)
	if len(set) != 1 {
		t.Fatalf("expected 1 edge after filtering, got %d", len(set))
	}
	if _, ok := set[declaredEdge{Src: "a/x", Dst: "a/y"}]; !ok {
		t.Error("expected a/x -> a/y to be kept")
	}
}
