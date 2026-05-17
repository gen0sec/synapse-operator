package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// IngressReconciler is a minimal Kubernetes Ingress controller that
// renders matching Ingresses into a synapse legacy `upstreams.yaml`
// written to a local path. It is designed to run as a SIDECAR in the
// synapse-proxy pod, writing to a shared emptyDir that synapse
// inotify-hot-reloads (~2s) — fast enough to serve cert-manager's
// ephemeral HTTP-01 solver Ingresses, which a ConfigMap mount
// (~60s kubelet propagation) could not.
//
// Backends are addressed by in-cluster DNS
// (svc.namespace.svc.cluster.local:port), so no Service/Endpoint
// watching or ClusterIP resolution is needed. TLS termination certs
// are handled out-of-band (the synapse-proxy mounts the cert-manager
// Secret directly), so this controller only programs L7 routes.
//
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingressclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
type IngressReconciler struct {
	client.Client
	// IngressClassName: only Ingresses whose spec.ingressClassName
	// equals this are programmed. cert-manager sets it on its solver
	// Ingress from the issuer's http01.ingress.ingressClassName.
	IngressClassName string
	// UpstreamsOutPath: where the rendered legacy upstreams.yaml is
	// written in place (the shared emptyDir synapse inotify-reloads).
	UpstreamsOutPath string
	// ClusterDomain for backend FQDNs (default cluster.local).
	ClusterDomain string
	// GatewayAPI: also reconcile Gateway API (GatewayClass/Gateway/
	// HTTPRoute) into the same upstreams.yaml. Requires the Gateway
	// API CRDs to be installed.
	GatewayAPI bool
}

func (r *IngressReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithName("ingress")
	changed, n, h, err := r.render(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		logger.Info("rendered synapse upstreams",
			"ingresses", n, "hosts", h, "path", r.UpstreamsOutPath)
	}
	return ctrl.Result{}, nil
}

// RenderOnce does a single list+render+write then returns — used by
// the initContainer so synapse starts with the real upstreams.yaml
// already in place (avoids synapse's 2s startup file-watch debounce
// dropping the first dynamic render).
func (r *IngressReconciler) RenderOnce(ctx context.Context) error {
	_, n, h, err := r.render(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("render-once: ingresses=%d hosts=%d -> %s\n", n, h, r.UpstreamsOutPath)
	return nil
}

// render lists all matching Ingresses and rewrites upstreams.yaml.
// Returns (changed, matchedIngresses, hosts, err).
func (r *IngressReconciler) render(ctx context.Context) (bool, int, int, error) {
	var list networkingv1.IngressList
	if err := r.List(ctx, &list); err != nil {
		return false, 0, 0, fmt.Errorf("list ingresses: %w", err)
	}

	// host -> (path -> backend). Whole config is rebuilt from scratch
	// every reconcile: idempotent and self-healing. ACME HTTP-01
	// challenge paths are special-cased (see acmeBackend).
	hosts := map[string]map[string]string{}
	// acmeBackend: backend for /.well-known/acme-challenge/ — must be
	// emitted as a synapse `internal_paths` override, because synapse
	// registers a built-in default `/.well-known/acme-challenge/* ->
	// (empty) internal ACME server` that is matched BEFORE host
	// `upstreams:` routes (gethosts.rs). cert-manager's ephemeral
	// HTTP-01 solver Ingress lands here.
	acmeBackend := ""
	matched := 0
	for i := range list.Items {
		ing := &list.Items[i]
		if !r.isOurs(ing) {
			continue
		}
		matched++
		for _, rule := range ing.Spec.Rules {
			host := rule.Host
			if host == "" || rule.HTTP == nil {
				continue
			}
			for _, p := range rule.HTTP.Paths {
				backend, ok := r.backendAddr(ctx, ing.Namespace, p.Backend)
				if !ok {
					continue
				}
				path := p.Path
				if path == "" {
					path = "/"
				}
				if strings.HasPrefix(path, acmeChallengePrefix) {
					acmeBackend = backend
					continue
				}
				if hosts[host] == nil {
					hosts[host] = map[string]string{}
				}
				hosts[host][path] = backend
			}
		}
	}

	if r.GatewayAPI {
		matched += r.renderGateways(ctx, hosts, &acmeBackend)
	}

	yaml := renderUpstreams(hosts, acmeBackend)
	changed, err := writeIfChanged(r.UpstreamsOutPath, yaml)
	if err != nil {
		return false, matched, len(hosts), fmt.Errorf("write %s: %w", r.UpstreamsOutPath, err)
	}
	return changed, matched, len(hosts), nil
}

// isOurs: ingress is ours iff spec.ingressClassName == r.IngressClassName.
func (r *IngressReconciler) isOurs(ing *networkingv1.Ingress) bool {
	if ing.Spec.IngressClassName == nil {
		return false
	}
	return *ing.Spec.IngressClassName == r.IngressClassName
}

// backendAddr resolves an Ingress backend to "fqdn:port". Port-by-name
// is resolved via a Service lookup; port-by-number is used directly.
func (r *IngressReconciler) backendAddr(ctx context.Context, ns string, b networkingv1.IngressBackend) (string, bool) {
	if b.Service == nil {
		return "", false
	}
	cd := r.ClusterDomain
	if cd == "" {
		cd = "cluster.local"
	}
	fqdn := fmt.Sprintf("%s.%s.svc.%s", b.Service.Name, ns, cd)
	if b.Service.Port.Number != 0 {
		return fmt.Sprintf("%s:%d", fqdn, b.Service.Port.Number), true
	}
	if b.Service.Port.Name != "" {
		var svc corev1.Service
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: b.Service.Name}, &svc); err != nil {
			return "", false
		}
		for _, sp := range svc.Spec.Ports {
			if sp.Name == b.Service.Port.Name {
				return fmt.Sprintf("%s:%d", fqdn, sp.Port), true
			}
		}
	}
	return "", false
}

// acmeChallengePrefix is the ACME HTTP-01 challenge path prefix.
// cert-manager's solver Ingress uses /.well-known/acme-challenge/<token>.
const acmeChallengePrefix = "/.well-known/acme-challenge"

// renderUpstreams emits the synapse legacy (v1) schema — the one
// parceyaml actually routes. The ACME challenge backend is emitted as
// an `internal_paths` entry keyed EXACTLY as synapse's built-in
// default (`/.well-known/acme-challenge/*`) so it REPLACES that
// default (parceyaml "will replace defaults if path matches") and the
// cert-manager solver is reachable. Deterministic key ordering keeps
// output stable so writeIfChanged avoids spurious reloads.
func renderUpstreams(hosts map[string]map[string]string, acmeBackend string) string {
	var b strings.Builder
	b.WriteString("# Generated by synapse-operator ingress controller. Do not edit.\n")
	if acmeBackend != "" {
		fmt.Fprintf(&b, "internal_paths:\n  \"/.well-known/acme-challenge/*\":\n    servers:\n      - %q\n    ssl_enabled: false\n", acmeBackend)
	}
	b.WriteString("upstreams:\n")
	hostKeys := make([]string, 0, len(hosts))
	for h := range hosts {
		hostKeys = append(hostKeys, h)
	}
	sort.Strings(hostKeys)
	for _, h := range hostKeys {
		fmt.Fprintf(&b, "  %q:\n    paths:\n", h)
		paths := hosts[h]
		pathKeys := make([]string, 0, len(paths))
		for p := range paths {
			pathKeys = append(pathKeys, p)
		}
		sort.Strings(pathKeys)
		for _, p := range pathKeys {
			fmt.Fprintf(&b, "      %q:\n        servers:\n          - %q\n        ssl_enabled: false\n", p, paths[p])
		}
	}
	return b.String()
}

// writeIfChanged writes IN PLACE (truncate same inode), NOT via
// tmp+rename. synapse's upstreams filewatch only reloads on inotify
// Modify(Data)/Create/Remove and IGNORES move/rename events, so an
// atomic rename would never trigger a hot-reload. An in-place
// truncate-write emits IN_MODIFY → Modify(Data) → synapse reloads.
// (A torn mid-write read is self-correcting: synapse debounces ~2s
// and re-reads on the next event.)
func writeIfChanged(path, content string) (bool, error) {
	if cur, err := os.ReadFile(path); err == nil && string(cur) == content {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return false, err
	}
	return true, nil
}

func (r *IngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Any Ingress change triggers a full rebuild; a single dummy
	// request key is enough (Reconcile ignores req and lists all).
	enqueueAll := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "rebuild"}}}
	})
	b := ctrl.NewControllerManagedBy(mgr).
		Named("synapse-ingress").
		Watches(&networkingv1.Ingress{}, enqueueAll)
	if r.GatewayAPI {
		b = b.Watches(gwHTTPRoute(), enqueueAll).
			Watches(gwGateway(), enqueueAll).
			Watches(gwGatewayClass(), enqueueAll)
	}
	return b.Complete(r)
}

// LogStartup is a tiny helper so main can announce the mode.
func (r *IngressReconciler) LogStartup(log logr.Logger) {
	log.Info("synapse ingress controller",
		"ingressClass", r.IngressClassName, "out", r.UpstreamsOutPath)
}
