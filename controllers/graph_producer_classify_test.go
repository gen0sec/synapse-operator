package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClassifyWorkloads(t *testing.T) {
	workloads := []graphWorkload{
		{Ref: "shop/frontend", Namespace: "shop", Workload: "frontend", App: "frontend"},
		{Ref: "data/mysql", Namespace: "data", Workload: "mysql", App: "mysql"},
		{Ref: "api/api", Namespace: "api", Workload: "api", App: "api"},
		{Ref: "kube-system/kube-apiserver", Namespace: "kube-system", Workload: "kube-apiserver", App: "kube-apiserver"},
		{Ref: "kube-system/cert-manager", Namespace: "kube-system", Workload: "cert-manager", App: "cert-manager"},
	}
	pods := []corev1.Pod{
		pod("frontend-abc", "shop", map[string]string{"app": "frontend"}, ""),
		pod("mysql-0", "data", map[string]string{"app": "mysql"}, ""),
		pod("api-xyz", "api", map[string]string{"app": "api"}, ""),
	}
	services := []corev1.Service{
		// LoadBalancer in front of shop/frontend.
		{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "frontend-lb"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Selector: map[string]string{"app": "frontend"}}},
		// ClusterIP for api, exposed only via the Ingress below.
		{ObjectMeta: metav1.ObjectMeta{Namespace: "api", Name: "api-svc"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"app": "api"}}},
		// ClusterIP for mysql with no LB/Ingress -> NOT exposed.
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

	classifyWorkloads(workloads, pods, services, ingresses)

	got := map[string]graphWorkload{}
	for _, w := range workloads {
		got[w.Ref] = w
	}

	check := func(ref, role string, ie, cp bool) {
		w := got[ref]
		if w.Role != role || w.InternetExposed != ie || w.ControlPlane != cp {
			t.Errorf("%s: role=%q ie=%v cp=%v, want role=%q ie=%v cp=%v",
				ref, w.Role, w.InternetExposed, w.ControlPlane, role, ie, cp)
		}
	}
	check("shop/frontend", "internet-exposed", true, false)           // LoadBalancer
	check("api/api", "internet-exposed", true, false)                 // Ingress-backed ClusterIP
	check("data/mysql", "internal", false, false)                     // ClusterIP only
	check("kube-system/kube-apiserver", "control-plane", false, true) // true control plane
	check("kube-system/cert-manager", "internal", false, false)       // kube-system add-on, NOT control-plane
}
