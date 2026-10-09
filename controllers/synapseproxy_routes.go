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
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

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
// What it needs to be allowed is in config/proxy-controller/rbac.yaml; see
// SynapseProxyReconciler.
type SynapseRouteReconciler struct {
	client.Client
	// ClusterDomain for backend FQDNs (default cluster.local).
	ClusterDomain string
	// Recorder emits Events on the Ingresses. May be nil.
	Recorder record.EventRecorder
	// GatewayAPI says the cluster has the Gateway API kinds, so that the
	// Gateways handed to a proxy are rendered too; see GatewayAPIServed.
	GatewayAPI bool
	// Namespace is the one the operator is limited to, or empty. What it
	// does not see outside it is not taken for gone.
	Namespace string
}

func (r *SynapseRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var proxy synapsev1alpha1.SynapseProxy
	if err := r.Get(ctx, req.NamespacedName, &proxy); err != nil {
		if apierrors.IsNotFound(err) {
			forgetProxyGauges(req.Namespace, req.Name)
			return ctrl.Result{}, r.sweepGateways(ctx)
		}
		return ctrl.Result{}, err
	}
	if !proxy.DeletionTimestamp.IsZero() {
		// Both outputs go with the proxy; there is nothing left to render.
		// What it said about its Gateways is not owned by it, and stays
		// until it is taken back.
		return ctrl.Result{}, r.sweepGateways(ctx)
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
		UpstreamsV2:           true,
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
	var (
		gatewayIn     gatewayInputs
		gatewayStatus gatewayStatuses
		lookups       = func() error { return nil }
	)
	if r.GatewayAPI {
		if gatewayIn, lookups, err = r.gatewayInputs(ctx, &proxy); err != nil {
			return ctrl.Result{}, err
		}
		render.RenderExtra = func(_ context.Context, m *renderModel) error {
			gatewayStatus = translateGateways(gatewayIn, m)
			// Rendered from a lookup that failed, the routes would lack a
			// backend that is there: nothing is written, and it is retried.
			return lookups()
		}
	}
	if _, _, _, err = render.render(ctx); err != nil {
		// It is retried, but until it succeeds the proxy serves the routes
		// and certificates of the last render that did, and says nothing.
		mProxyRenderErrors.WithLabelValues(proxy.Namespace, proxy.Name).Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(&proxy, corev1.EventTypeWarning, "RoutesNotRendered", "%v", err)
		}
		return ctrl.Result{}, err
	}
	if r.GatewayAPI {
		// After the routes are written: a status that says programmed is
		// about what the proxy has been given.
		if err := r.writeGatewayStatuses(ctx, gatewayIn, gatewayStatus); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// sweepGateways is sweepGatewayStatuses where the Gateway API is served.
func (r *SynapseRouteReconciler) sweepGateways(ctx context.Context) error {
	if !r.GatewayAPI {
		return nil
	}
	return r.sweepGatewayStatuses(ctx)
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
	mProxyRenderErrors.DeleteLabelValues(namespace, name)
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

	b := ctrl.NewControllerManagedBy(mgr).
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
		Watches(&corev1.Secret{}, everyProxy, builder.WithPredicates(tlsSecrets))
	if r.GatewayAPI {
		// What is written to them is their status, which is not a reason
		// to look at them again.
		spec := builder.WithPredicates(predicate.GenerationChangedPredicate{})
		b = b.Watches(&gwv1.GatewayClass{}, everyProxy, spec).
			Watches(&gwv1.Gateway{}, everyProxy, spec).
			Watches(&gwv1.HTTPRoute{}, everyProxy, spec).
			Watches(&gwv1beta1.ReferenceGrant{}, everyProxy).
			// A listener may choose by their labels which namespaces attach.
			Watches(&corev1.Namespace{}, everyProxy, builder.WithPredicates(predicate.LabelChangedPredicate{}))
	}
	return b.Complete(r)
}
