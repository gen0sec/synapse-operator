package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInternetExposedRefs(t *testing.T) {
	pods := []corev1.Pod{
		pod("frontend-abc", "shop", map[string]string{"app": "frontend"}, ""),
		pod("api-xyz", "api", map[string]string{"app": "api"}, ""),
		pod("mysql-0", "data", map[string]string{"app": "mysql"}, ""),
	}
	services := []corev1.Service{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "frontend-lb"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Selector: map[string]string{"app": "frontend"}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "api", Name: "api-svc"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"app": "api"}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "mysql"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"app": "mysql"}}},
	}
	ingresses := []networkingv1.Ingress{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "api", Name: "api-ing"},
			Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{Backend: networkingv1.IngressBackend{
						Service: &networkingv1.IngressServiceBackend{Name: "api-svc"}}}}}}}}}},
	}

	got := internetExposedRefs(pods, services, ingresses)
	if !got["shop/frontend"] {
		t.Error("shop/frontend should be internet-exposed (LoadBalancer)")
	}
	if !got["api/api"] {
		t.Error("api/api should be internet-exposed (Ingress-backed ClusterIP)")
	}
	if got["data/mysql"] {
		t.Error("data/mysql should NOT be internet-exposed (ClusterIP only)")
	}
}

func TestIsControlPlane(t *testing.T) {
	cp := []string{"kube-apiserver", "etcd", "ccm", "kube-controller-manager", "cloud-controller-manager"}
	for _, w := range cp {
		if !isControlPlane(w) {
			t.Errorf("%q should be control-plane", w)
		}
	}
	notCP := []string{"cert-manager", "coredns", "kube-proxy", "ccm-proportional-autoscaler", "frontend"}
	for _, w := range notCP {
		if isControlPlane(w) {
			t.Errorf("%q should NOT be control-plane", w)
		}
	}
}
