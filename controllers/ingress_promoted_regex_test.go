package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A brace that is no repetition is literal in Go and an error in Synapse's
// engine, which is the reason the path is written back from the parse tree.
const bracePath = "/v{x"
const braceCanonical = `/v\{x`

func renderRegexOlderMode(t *testing.T, objs ...client.Object) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "u.yaml")
	r := &IngressReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(),
		IngressClassName: "synapse", UpstreamsOutPath: out, CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local"}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered)
}

// A v1 reconciler whose file a passthrough host turned into v2 writes the
// regex path as Synapse's engine reads it.
func TestRender_APromotedFileCanonicalisesRegexPaths(t *testing.T) {
	regex := annotated(ingressIn("zzz", "api", "api.example.com", bracePath), "use-regex", "true")
	raw := annotated(ingressIn("zzz", "raw", "raw.example.com", "/"), "ssl-passthrough", "true")
	doc := parseV2(t, renderRegexOlderMode(t, regex, raw))
	var exprs []string
	for _, rt := range doc.Hosts["api.example.com"].Paths {
		exprs = append(exprs, rt.MatchExpr)
	}
	if len(exprs) != 1 || !strings.Contains(exprs[0], braceCanonical) {
		t.Errorf("match_expr is %q, want one holding %q", exprs, braceCanonical)
	}
}

// Without a passthrough host the file is v1 and the path goes in as it came.
func TestRender_AV1FileKeepsRegexPathsAsTheyCame(t *testing.T) {
	regex := annotated(ingressIn("zzz", "api", "api.example.com", bracePath), "use-regex", "true")
	rendered := renderRegexOlderMode(t, regex)
	if strings.Contains(rendered, "version: 2") || !strings.Contains(rendered, bracePath) || strings.Contains(rendered, braceCanonical) {
		t.Errorf("v1 file changed:\n%s", rendered)
	}
}
