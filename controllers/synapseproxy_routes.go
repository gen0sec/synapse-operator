package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The two objects this controller writes for a SynapseProxy, and the proxy's
// pods mount.
func proxyUpstreamsName(proxy *synapsev1alpha1.SynapseProxy) types.NamespacedName {
	return types.NamespacedName{Namespace: proxy.Namespace, Name: proxy.Name + "-upstreams"}
}

func proxyCertsName(proxy *synapsev1alpha1.SynapseProxy) types.NamespacedName {
	return types.NamespacedName{Namespace: proxy.Namespace, Name: proxy.Name + "-certs"}
}

// SynapseRouteReconciler renders, for each SynapseProxy, the routes and TLS
// certificates of the Ingresses that belong to it: a ConfigMap
// `<name>-upstreams` and a Secret `<name>-certs`, both controlled by the
// proxy.
//
// An Ingress belongs to a proxy through its IngressClass. The class must be
// ours (spec.controller) and name the proxy in spec.parameters. The proxy
// does not get a say: its certificates Secret receives the TLS private keys
// of every Ingress it serves, from whatever namespace, so who serves a class
// is decided on the cluster-scoped object.
//
// The rendering itself is IngressReconciler's, run once per proxy.
//
// +kubebuilder:rbac:groups=synapse.gen0sec.com,resources=synapseproxies,verbs=get;list;watch
// +kubebuilder:rbac:groups=synapse.gen0sec.com,resources=synapseproxies/finalizers,verbs=update
type SynapseRouteReconciler struct {
	client.Client
	// ClusterDomain for backend FQDNs (default cluster.local).
	ClusterDomain string
	// Recorder emits Events on the Ingresses. May be nil.
	Recorder record.EventRecorder
}

func (r *SynapseRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var proxy synapsev1alpha1.SynapseProxy
	if err := r.Get(ctx, req.NamespacedName, &proxy); err != nil {
		if apierrors.IsNotFound(err) {
			forgetProxyGauges(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !proxy.DeletionTimestamp.IsZero() {
		// Both outputs go with the proxy; there is nothing left to render.
		return ctrl.Result{}, nil
	}

	var classes networkingv1.IngressClassList
	if err := r.List(ctx, &classes); err != nil {
		return ctrl.Result{}, err
	}
	bound := boundIngressClasses(classes.Items, &proxy)

	addresses, err := r.proxyAddresses(ctx, &proxy)
	if err != nil {
		return ctrl.Result{}, err
	}

	// A fresh value every time: IngressReconciler carries per-run state.
	render := &IngressReconciler{
		Client:                r.Client,
		IngressClassMatch:     func(name string) bool { return bound[name] },
		UpstreamsOutConfigMap: proxyUpstreamsName(&proxy),
		CertsOutSecret:        proxyCertsName(&proxy),
		OwnerRef:              metav1.NewControllerRef(&proxy, synapsev1alpha1.GroupVersion.WithKind("SynapseProxy")),
		ClusterDomain:         r.ClusterDomain,
		// The pods read the routes through a volume the kubelet refreshes
		// on its own schedule. A Service's cluster IP is still right a
		// minute later; a pod address may not be.
		ResolveBackendClusterIPs: true,
		StatusAddresses:          addresses,
		Recorder:                 r.Recorder,
	}
	_, _, _, err = render.render(ctx)
	return ctrl.Result{}, err
}

// boundIngressClasses returns the names of the IngressClasses that hand
// their Ingresses to proxy.
func boundIngressClasses(classes []networkingv1.IngressClass, proxy *synapsev1alpha1.SynapseProxy) map[string]bool {
	bound := map[string]bool{}
	for i := range classes {
		ic := &classes[i]
		p := ic.Spec.Parameters
		if ic.Spec.Controller != ControllerName || p == nil {
			continue
		}
		if p.APIGroup == nil || *p.APIGroup != synapsev1alpha1.GroupVersion.Group || p.Kind != "SynapseProxy" {
			continue
		}
		// A SynapseProxy is namespaced, so the reference has to say which
		// namespace. A cluster-scoped reference names no proxy at all.
		if p.Scope == nil || *p.Scope != networkingv1.IngressClassParametersReferenceScopeNamespace {
			continue
		}
		if p.Namespace == nil || *p.Namespace != proxy.Namespace || p.Name != proxy.Name {
			continue
		}
		bound[ic.Name] = true
	}
	return bound
}

// proxyAddresses returns where the proxy is reachable from outside: the
// load-balancer addresses of the Service the proxy controls, if any.
func (r *SynapseRouteReconciler) proxyAddresses(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) ([]string, error) {
	var svc corev1.Service
	if err := r.Get(ctx, client.ObjectKeyFromObject(proxy), &svc); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&svc, proxy) {
		return nil, nil
	}
	var out []string
	for _, in := range svc.Status.LoadBalancer.Ingress {
		switch {
		case in.IP != "":
			out = append(out, in.IP)
		case in.Hostname != "":
			out = append(out, in.Hostname)
		}
	}
	return out, nil
}

// forgetProxyGauges drops the per-proxy series of a proxy that is gone.
func forgetProxyGauges(namespace, name string) {
	mProxyHosts.DeleteLabelValues(namespace, name)
	mProxyRoutes.DeleteLabelValues(namespace, name)
	mProxyCerts.DeleteLabelValues(namespace, name)
}

// SetupWithManager registers the controller. A render is a function of
// cluster-wide state, so a change to any of its inputs re-renders every
// proxy; an unchanged render writes nothing.
func (r *SynapseRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	everyProxy := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		var proxies synapsev1alpha1.SynapseProxyList
		if err := r.List(ctx, &proxies); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list SynapseProxies")
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(proxies.Items))
		for i := range proxies.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&proxies.Items[i])})
		}
		return reqs
	})
	tlsSecrets := predicate.NewPredicateFuncs(func(o client.Object) bool {
		s, ok := o.(*corev1.Secret)
		return ok && s.Type == corev1.SecretTypeTLS
	})

	return ctrl.NewControllerManagedBy(mgr).
		// Named explicitly: the proxy's own controller is also `For` this kind.
		Named("synapse-proxy-routes").
		For(&synapsev1alpha1.SynapseProxy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// The outputs: put back what someone edited or deleted.
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Watches(&networkingv1.Ingress{}, everyProxy).
		Watches(&networkingv1.IngressClass{}, everyProxy).
		// Backend addresses, and the address of the proxy's own Service.
		Watches(&corev1.Service{}, everyProxy).
		// Certificate issuance and renewal.
		Watches(&corev1.Secret{}, everyProxy, builder.WithPredicates(tlsSecrets)).
		Complete(r)
}
