package controllers

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// resolverWithServices builds a fake-client-backed reconciler with the
// given Services pre-loaded, so substitution tests don't need to spin
// up an envtest API server.
func resolverWithServices(t *testing.T, svcs ...corev1.Service) *UpstreamsResolverReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	objs := make([]runtime.Object, 0, len(svcs))
	for i := range svcs {
		objs = append(objs, &svcs[i])
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return &UpstreamsResolverReconciler{Client: c, Scheme: scheme}
}

func svc(ns, name, clusterIP string) corev1.Service {
	return corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP},
	}
}

func TestResolveYAML_ClusterFQDN_Substituted(t *testing.T) {
	r := resolverWithServices(t,
		svc("services", "auth-api", "10.0.0.1"),
		svc("services", "data-api", "10.0.0.2"),
	)

	in := `provider: file
upstreams:
  api.gen0sec.com:
    paths:
      /v1/authcheck:
        servers:
          - auth-api.services.svc.cluster.local:80
      /v1/events:
        servers:
          - data-api.services.svc.cluster.local:80
`
	out, sum, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "10.0.0.1:80") || !strings.Contains(out, "10.0.0.2:80") {
		t.Errorf("expected ClusterIP substitution; got:\n%s", out)
	}
	if strings.Contains(out, "auth-api.services.svc.cluster.local") ||
		strings.Contains(out, "data-api.services.svc.cluster.local") {
		t.Errorf("source FQDN still present in output:\n%s", out)
	}
	if sum.total != 2 || sum.resolved != 2 || sum.passthrough != 0 {
		t.Errorf("counts: %+v", sum)
	}
}

func TestResolveYAML_MissingService_PassedThrough(t *testing.T) {
	r := resolverWithServices(t) // no services in the fake client
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /v1/authcheck:
        servers:
          - auth-api.services.svc.cluster.local:80
`
	out, sum, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "auth-api.services.svc.cluster.local:80") {
		t.Errorf("missing-Service backend should pass through:\n%s", out)
	}
	if sum.resolved != 0 || sum.passthrough != 1 {
		t.Errorf("counts: %+v", sum)
	}
}

func TestResolveYAML_IPLiteral_LeftAlone(t *testing.T) {
	r := resolverWithServices(t)
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /v1/authcheck:
        servers:
          - 10.0.0.5:80
`
	out, sum, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "10.0.0.5:80") {
		t.Errorf("IP literal must be preserved:\n%s", out)
	}
	if sum.passthrough != 1 {
		t.Errorf("counts: %+v", sum)
	}
}

func TestResolveYAML_HeadlessService_PassedThrough(t *testing.T) {
	r := resolverWithServices(t,
		svc("services", "headless-api", "None"),
	)
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /v1/authcheck:
        servers:
          - headless-api.services.svc.cluster.local:80
`
	out, _, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "headless-api.services.svc.cluster.local:80") {
		t.Errorf("headless service must pass through:\n%s", out)
	}
}

func TestResolveYAML_NonClusterDomain_PassedThrough(t *testing.T) {
	r := resolverWithServices(t)
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /external:
        servers:
          - other.example.com:443
`
	out, _, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "other.example.com:443") {
		t.Errorf("non-cluster host must pass through:\n%s", out)
	}
}

func TestResolveYAML_PreservesOtherPathFields(t *testing.T) {
	// The renderer must not drop ssl_enabled / match_expr / weights etc.
	r := resolverWithServices(t, svc("services", "auth-api", "10.0.0.1"))
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /v1/authcheck:
        ssl_enabled: false
        match_expr: 'http.request.path matches "^/v1/.*$"'
        servers:
          - auth-api.services.svc.cluster.local:80
`
	out, _, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	for _, expect := range []string{
		"ssl_enabled: false",
		`match_expr: http.request.path matches "^/v1/.*$"`,
		"10.0.0.1:80",
	} {
		if !strings.Contains(out, expect) {
			t.Errorf("output missing %q:\n%s", expect, out)
		}
	}
}

func TestResolveYAML_ShortFormUsesDefaultNamespace(t *testing.T) {
	r := resolverWithServices(t, svc("ingress-synapse", "telemetry-api", "10.0.0.9"))
	in := `upstreams:
  api.gen0sec.com:
    paths:
      /v1/telemetry:
        servers:
          - telemetry-api:80
`
	out, sum, err := r.resolveYAML(context.Background(), "ingress-synapse", in)
	if err != nil {
		t.Fatalf("resolveYAML: %v", err)
	}
	if !strings.Contains(out, "10.0.0.9:80") {
		t.Errorf("short-form Service in source ns should resolve:\n%s", out)
	}
	if sum.resolved != 1 {
		t.Errorf("counts: %+v", sum)
	}
}

func TestParseClusterFQDN(t *testing.T) {
	r := &UpstreamsResolverReconciler{}
	ns, name, ok := r.parseClusterFQDN("auth-api.services.svc.cluster.local")
	if !ok || ns != "services" || name != "auth-api" {
		t.Errorf("got ns=%q name=%q ok=%v", ns, name, ok)
	}
	if _, _, ok := r.parseClusterFQDN("example.com"); ok {
		t.Errorf("non-cluster hostname must not match")
	}
	if _, _, ok := r.parseClusterFQDN("svc.cluster.local"); ok {
		t.Errorf("malformed cluster FQDN must not match")
	}
}
