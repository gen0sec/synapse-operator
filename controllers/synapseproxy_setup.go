package controllers

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// proxyInstall is where what CheckProxyAccess finds missing comes from.
const proxyInstall = "config/proxy-controller"

// SetupProxyControllers registers the two controllers that run SynapseProxy
// resources: the one that renders each proxy's routes and certificates, and
// the one that runs the proxy itself.
//
// namespace is the one the operator is limited to, or empty.
func SetupProxyControllers(ctx context.Context, mgr ctrl.Manager, clusterDomain, namespace string) error {
	// Looked at once, here. CRDs installed later are found when the operator
	// next starts.
	gatewayAPI, err := GatewayAPIServed(mgr.GetRESTMapper())
	switch {
	case err != nil:
		return fmt.Errorf("whether the Gateway API is installed: %w", err)
	case !gatewayAPI:
		mgr.GetLogger().Info("Gateway API is not installed: only Ingresses are served; restart the operator once its CRDs are there")
	default:
		// Having the CRDs is not asking for them: many clusters come with
		// them. An operator whose role predates this one must go on serving
		// its Ingresses, and say what it is missing.
		err := CheckGatewayAccess(ctx, mgr.GetAPIReader(), namespace)
		switch {
		case err == nil:
			mgr.GetLogger().Info("Gateway API is installed: Gateways of a class that names a SynapseProxy are served")
		case apierrors.IsForbidden(err):
			gatewayAPI = false
			mgr.GetLogger().Error(err, "Gateway API is installed, but the operator is not allowed to read it: Gateways are NOT served, Ingresses are. The role that allows it is in "+proxyInstall)
		default:
			return fmt.Errorf("whether the Gateway API can be read: %w", err)
		}
	}
	routes := &SynapseRouteReconciler{
		Client:        mgr.GetClient(),
		ClusterDomain: clusterDomain,
		Recorder:      mgr.GetEventRecorderFor("synapse-proxy-routes"),
		GatewayAPI:    gatewayAPI,
		Namespace:     namespace,
	}
	if err := routes.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("route controller: %w", err)
	}
	proxies := &SynapseProxyReconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorderFor("synapse-proxy"),
	}
	if err := proxies.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("proxy controller: %w", err)
	}
	return nil
}

// CheckProxyAccess says why the SynapseProxy controllers cannot run as
// whoever c acts as, or returns nil. It is for use before there is a manager:
// the proxy controller indexes SynapseProxy resources, and a manager waits
// for the cache of an indexed kind before it starts anything. One that cannot
// read them starts no controller at all, and reports nothing but a failing
// watch in its log.
//
// Only that one read is checked. A controller that is refused anything else
// stops the manager with an error when its own cache does not fill.
func CheckProxyAccess(ctx context.Context, c client.Reader, namespace string) error {
	var proxies synapsev1alpha1.SynapseProxyList
	err := c.List(ctx, &proxies, client.InNamespace(namespace), client.Limit(1))
	switch {
	case err == nil:
		return nil
	case meta.IsNoMatchError(err):
		return fmt.Errorf("the SynapseProxy CRD is not installed; it is part of %s: %w", proxyInstall, err)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("not allowed to read SynapseProxy resources; the role that allows it is in %s: %w", proxyInstall, err)
	}
	return err
}
