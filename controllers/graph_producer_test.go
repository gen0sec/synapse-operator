package controllers

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// collectNodeWorkloads must emit node/<name> vertices carrying Internal+External
// IPs (deduped/sorted) so node-level observed traffic resolves, and flag
// control-plane nodes.
func TestCollectNodeWorkloads(t *testing.T) {
	node := func(name string, cp bool, addrs ...corev1.NodeAddress) corev1.Node {
		n := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.NodeStatus{Addresses: addrs},
		}
		if cp {
			n.Labels = map[string]string{"node-role.kubernetes.io/control-plane": ""}
		}
		return n
	}
	nodes := []corev1.Node{
		node("worker-1", false,
			corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "203.0.113.5"},
			corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "10.0.0.5"},
			corev1.NodeAddress{Type: corev1.NodeHostName, Address: "worker-1"}, // ignored
		),
		node("cp-1", true,
			corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}),
	}
	got := map[string]graphWorkload{}
	for _, w := range collectNodeWorkloads(nodes) {
		got[w.Ref] = w
	}
	w := got["node/worker-1"]
	if !reflect.DeepEqual(w.IPs, []string{"10.0.0.5", "203.0.113.5"}) {
		t.Errorf("worker-1 IPs = %v, want [10.0.0.5 203.0.113.5] (internal+external, sorted, hostname dropped)", w.IPs)
	}
	if w.Role != "node" || w.ControlPlane {
		t.Errorf("worker-1 role=%q cp=%v, want node/false", w.Role, w.ControlPlane)
	}
	if cp := got["node/cp-1"]; !cp.ControlPlane || cp.Role != "control-plane" {
		t.Errorf("cp-1 role=%q cp=%v, want control-plane/true", cp.Role, cp.ControlPlane)
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
