package controllers

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"strings"
)

// internetExposedRefs returns the set of workload refs ("namespace/workload")
// whose pods are selected by an internet-exposing Service (LoadBalancer /
// NodePort) or by a Service referenced from an Ingress backend. This is pure
// Kubernetes static analysis — the operator is the k8s-context provider, so it
// folds this into the identity artifact the agent consumes.
func internetExposedRefs(pods []corev1.Pod, services []corev1.Service, ingresses []networkingv1.Ingress) map[string]bool {
	// Services referenced by an Ingress backend expose their (usually ClusterIP)
	// Service to the internet.
	ingressBacked := map[string]bool{}
	for i := range ingresses {
		ing := &ingresses[i]
		if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
			ingressBacked[ing.Namespace+"/"+ing.Spec.DefaultBackend.Service.Name] = true
		}
		for _, rule := range ing.Spec.Rules {
			if rule.HTTP == nil {
				continue
			}
			for _, p := range rule.HTTP.Paths {
				if p.Backend.Service != nil {
					ingressBacked[ing.Namespace+"/"+p.Backend.Service.Name] = true
				}
			}
		}
	}

	// Exposing selectors: LB/NodePort services + any Ingress-backed service.
	// Empty selectors (headless/external) select nothing and are skipped.
	type nsSelector struct {
		namespace string
		selector  map[string]string
	}
	var exposing []nsSelector
	for i := range services {
		s := &services[i]
		if len(s.Spec.Selector) == 0 {
			continue
		}
		if s.Spec.Type == corev1.ServiceTypeLoadBalancer ||
			s.Spec.Type == corev1.ServiceTypeNodePort ||
			ingressBacked[s.Namespace+"/"+s.Name] {
			exposing = append(exposing, nsSelector{namespace: s.Namespace, selector: s.Spec.Selector})
		}
	}

	exposed := map[string]bool{}
	for i := range pods {
		p := &pods[i]
		workload, _ := workloadIdentity(p)
		if workload == "" {
			continue
		}
		ref := p.Namespace + "/" + workload
		for _, sel := range exposing {
			if sel.namespace == p.Namespace && selectorMatches(sel.selector, p.Labels) {
				exposed[ref] = true
				break
			}
		}
	}
	return exposed
}

// selectorMatches reports whether every key/value in a non-empty selector is
// present in labels (label-selector subset match).
func selectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// controlPlaneComponents are short workload names matched EXACTLY — substring
// matching would wrongly catch add-ons (e.g. ccm-proportional-autoscaler).
var controlPlaneComponents = map[string]bool{"etcd": true, "ccm": true}

// isControlPlane marks the true Kubernetes control plane — API server, etcd,
// controller-manager, scheduler, and the cloud controller — by component name.
// Add-ons that merely live in kube-system (cert-manager, CNI, autoscalers, DNS,
// kube-proxy) are NOT control-plane.
func isControlPlane(workload string) bool {
	w := strings.ToLower(workload)
	for _, c := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler", "cloud-controller-manager"} {
		if strings.Contains(w, c) {
			return true
		}
	}
	return controlPlaneComponents[w]
}
