package controllers

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func pod(name, ns string, labels map[string]string, rsOwner string) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Status:     corev1.PodStatus{PodIP: "10.0.0.1"},
	}
	if rsOwner != "" {
		p.OwnerReferences = []metav1.OwnerReference{
			{Kind: "ReplicaSet", Name: rsOwner, Controller: boolPtr(true)},
		}
	}
	return p
}

func TestBuildEdgeDocIngress(t *testing.T) {
	pods := []corev1.Pod{
		pod("api-aaa", "shop", map[string]string{"app": "api"}, "api-7d9f8c6b5"),
		pod("frontend-bbb", "shop", map[string]string{"app": "frontend"}, "frontend-5f4b9c8d2"),
		pod("attacker-ccc", "shop", map[string]string{"app": "attacker"}, "attacker-1a2b3c4d5"),
	}
	// NP on app=api: allow ingress from app=frontend on 8080.
	port := intstr.FromInt(8080)
	nps := []networkingv1.NetworkPolicy{{
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
	}}

	doc, version, count := buildEdgeDoc(nps, pods, nil)
	if count != 1 {
		t.Fatalf("expected 1 edge, got %d\n%s", count, doc)
	}
	if !strings.Contains(doc, "E shop/frontend > shop/api : 8080") {
		t.Errorf("missing frontend->api edge:\n%s", doc)
	}
	if !strings.Contains(doc, "G shop/api") {
		t.Errorf("api should be governed:\n%s", doc)
	}
	// attacker is NOT a declared source -> no attacker edge.
	if strings.Contains(doc, "attacker") {
		t.Errorf("attacker should not appear:\n%s", doc)
	}
	if !strings.HasPrefix(version, "edges-") {
		t.Errorf("bad version: %s", version)
	}

	// Determinism: same input -> same version.
	_, version2, _ := buildEdgeDoc(nps, pods, nil)
	if version != version2 {
		t.Errorf("version not deterministic: %s vs %s", version, version2)
	}
}

func TestBuildEdgeDocNamedPort(t *testing.T) {
	// api exposes a named containerPort `http` -> 8080. The NetworkPolicy allows
	// frontend on the NAMED port `http`. The edge must resolve to :8080, not
	// widen to :* (which would let an attacker on any port slip past).
	apiPod := pod("api-aaa", "shop", map[string]string{"app": "api"}, "api-7d9f8c6b5")
	apiPod.Spec.Containers = []corev1.Container{{
		Name:  "api",
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
	}}
	pods := []corev1.Pod{
		apiPod,
		pod("frontend-bbb", "shop", map[string]string{"app": "frontend"}, "frontend-5f4b9c8d2"),
	}
	named := intstr.FromString("http")
	nps := []networkingv1.NetworkPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "api-allow", Namespace: "shop"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "frontend"}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Port: &named}},
			}},
		},
	}}
	doc, _, _ := buildEdgeDoc(nps, pods, nil)
	if !strings.Contains(doc, "E shop/frontend > shop/api : 8080") {
		t.Errorf("named port `http` should resolve to :8080:\n%s", doc)
	}
	if strings.Contains(doc, ": *") {
		t.Errorf("named port should NOT widen to any-port:\n%s", doc)
	}
}

func TestBuildEdgeDocEgress(t *testing.T) {
	// frontend has an egress policy: may egress ONLY to api on 8080.
	pods := []corev1.Pod{
		pod("frontend-aaa", "shop", map[string]string{"app": "frontend"}, "frontend-7d9f8c6b5"),
		pod("api-bbb", "shop", map[string]string{"app": "api"}, "api-5f4b9c8d2"),
	}
	port := intstr.FromInt(8080)
	nps := []networkingv1.NetworkPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "frontend-egress", Namespace: "shop"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "frontend"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{{
				To: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Port: &port}},
			}},
		},
	}}
	doc, _, _ := buildEdgeDoc(nps, pods, nil)
	if !strings.Contains(doc, "E shop/frontend > shop/api : 8080") {
		t.Errorf("egress should emit frontend->api edge:\n%s", doc)
	}
	if !strings.Contains(doc, "S shop/frontend") {
		t.Errorf("frontend should be an egress-governed source (S record):\n%s", doc)
	}
	// Egress-only policy with explicit policyTypes:[Egress] must NOT mark
	// frontend as an ingress-governed destination.
	if strings.Contains(doc, "G shop/frontend") {
		t.Errorf("egress-only policy should not govern ingress:\n%s", doc)
	}
}

func TestBuildEdgeDocEgressNamedPort(t *testing.T) {
	// frontend egress -> api on the NAMED port `http`; api exposes http->8080.
	// The egress edge must resolve to :8080 (not widen to :*).
	apiPod := pod("api-bbb", "shop", map[string]string{"app": "api"}, "api-5f4b9c8d2")
	apiPod.Spec.Containers = []corev1.Container{{
		Name:  "api",
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
	}}
	pods := []corev1.Pod{
		pod("frontend-aaa", "shop", map[string]string{"app": "frontend"}, "frontend-7d9f8c6b5"),
		apiPod,
	}
	named := intstr.FromString("http")
	nps := []networkingv1.NetworkPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "frontend-egress", Namespace: "shop"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "frontend"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{{
				To: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Port: &named}},
			}},
		},
	}}
	doc, _, _ := buildEdgeDoc(nps, pods, nil)
	if !strings.Contains(doc, "E shop/frontend > shop/api : 8080") {
		t.Errorf("egress named port `http` should resolve to :8080:\n%s", doc)
	}
	if strings.Contains(doc, ": *") {
		t.Errorf("egress named port should NOT widen to any-port:\n%s", doc)
	}
}

func TestBuildEdgeDocEmptyFromIsAllowAll(t *testing.T) {
	pods := []corev1.Pod{
		pod("ingress-aaa", "shop", map[string]string{"app": "ingress"}, "ingress-7d9f8c6b5"),
	}
	port := intstr.FromInt(443)
	nps := []networkingv1.NetworkPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "ingress-open", Namespace: "shop"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "ingress"}},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From:  nil, // allow from all
				Ports: []networkingv1.NetworkPolicyPort{{Port: &port}},
			}},
		},
	}}
	doc, _, _ := buildEdgeDoc(nps, pods, nil)
	if !strings.Contains(doc, "E */* > shop/ingress : 443") {
		t.Errorf("empty-from should emit allow-from-all edge:\n%s", doc)
	}
}
