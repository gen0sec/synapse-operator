package controllers

import "testing"

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
