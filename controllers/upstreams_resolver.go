// Package controllers — UpstreamsResolverReconciler.
//
// Why this exists
//
// synapse-proxy reverse-proxies traffic to backends declared in
// `upstreams.yaml`. Each backend entry today carries a k8s Service DNS
// name (`<svc>.<ns>.svc.cluster.local:<port>`). Inside synapse, pingora's
// `HttpPeer::new` calls `to_socket_addrs()` on that hostname-port pair —
// a blocking syscall against CoreDNS. Under any non-trivial load this
// routinely lands in the 100-500 ms band, so `upstream_peer_select_ms`
// shows up as the dominant latency floor on the data plane.
//
// The data-plane stop-gap (process-wide DNS cache in synapse-proxy,
// PR #382) flattens the steady-state penalty, but the root cause is
// that the proxy is doing DNS at all. The proxy already runs in k8s; it
// has authoritative knowledge of Service ClusterIPs via the API server
// without ever asking CoreDNS.
//
// This controller closes that loop:
//
//   1. Watches ConfigMaps labelled
//      `synapse.gen0sec.com/resolve-upstreams=true`.
//   2. Parses each source ConfigMap's `upstreams.yaml`.
//   3. Looks up every backend Service in the k8s API, substitutes the
//      hostname with the Service's ClusterIP.
//   4. Writes the rewritten upstreams.yaml to a sibling ConfigMap
//      (`<source-name>-resolved` by default; overridable via the
//      `synapse.gen0sec.com/resolved-configmap` annotation on the
//      source).
//   5. The output ConfigMap is owned by the source — `kubectl delete`
//      on the source garbage-collects the output. Synapse-proxy mounts
//      the OUTPUT ConfigMap, so it sees pre-resolved IPs and skips the
//      DNS hit entirely.
//
// Service add/update/delete events trigger a re-render across all
// labelled source ConfigMaps; the data plane converges on the new IP
// without manual intervention.
//
// Unresolvable backends (Service missing, headless, port-by-name with
// no matching named port) are passed through unchanged — synapse-proxy
// will still resolve them at request time via the in-process DNS cache.
// That degraded path is acceptable while the operator catches up; the
// alternative — failing the whole render — would be worse than a brief
// 100 ms hop.
//
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
package controllers

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/yaml"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Label that marks a ConfigMap as a source the resolver should
// process. Set on the source by the operator (humans, GitOps, etc.).
const ResolverSourceLabel = "synapse.gen0sec.com/resolve-upstreams"

// Annotation on the source ConfigMap that names the output ConfigMap.
// Optional — defaults to `<source-name>-resolved` in the same namespace.
const ResolvedConfigMapAnnotation = "synapse.gen0sec.com/resolved-configmap"

// Label applied to the output ConfigMap so other controllers (and
// humans) can identify operator-managed resolved configs at a glance,
// and so the controller can find its own outputs without an owner-ref
// walk.
const ResolvedOutputLabel = "synapse.gen0sec.com/resolved-upstreams-for"

// Key inside the ConfigMap that holds the synapse upstreams YAML. Both
// the source and the output use this key.
const UpstreamsKey = "upstreams.yaml"

// UpstreamsResolverReconciler watches labelled source ConfigMaps and
// resolves their backend Service DNS names to ClusterIPs, writing the
// resolved YAML to a sibling output ConfigMap.
type UpstreamsResolverReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// ClusterDomain matches the suffix on backend FQDNs we're willing
	// to rewrite. Defaults to "cluster.local". Backends outside this
	// domain (external services, non-k8s hosts) pass through unchanged.
	ClusterDomain string
}

// Reconcile is the entry point: triggered by a source ConfigMap event
// or by a Service event mapped through Services-watch.
func (r *UpstreamsResolverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("source", req.NamespacedName)

	var src corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &src); err != nil {
		if apierrors.IsNotFound(err) {
			// Owner reference garbage-collects the output ConfigMap
			// automatically — nothing for us to do.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !isResolverSource(&src) {
		// Label was removed since the watch fired. Don't touch.
		return ctrl.Result{}, nil
	}

	raw, ok := src.Data[UpstreamsKey]
	if !ok {
		logger.Info("source ConfigMap has no upstreams.yaml key, skipping",
			"key", UpstreamsKey)
		return ctrl.Result{}, nil
	}

	resolved, summary, err := r.resolveYAML(ctx, src.Namespace, raw)
	if err != nil {
		logger.Error(err, "failed to parse/resolve upstreams.yaml")
		return ctrl.Result{}, err
	}

	outName := resolvedOutputName(&src)
	out := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      outName,
			Namespace: src.Namespace,
		},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, out, func() error {
		if out.Labels == nil {
			out.Labels = map[string]string{}
		}
		out.Labels[ResolvedOutputLabel] = src.Name
		if out.Data == nil {
			out.Data = map[string]string{}
		}
		out.Data[UpstreamsKey] = resolved
		// Owner reference: tie the output's lifecycle to the source.
		return controllerutil.SetControllerReference(&src, out, r.Scheme)
	})
	if err != nil {
		logger.Error(err, "failed to write resolved ConfigMap", "output", outName)
		return ctrl.Result{}, err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("resolved upstreams written",
			"output", outName,
			"op", op,
			"backends_total", summary.total,
			"backends_resolved", summary.resolved,
			"backends_passthrough", summary.passthrough)
	}
	return ctrl.Result{}, nil
}

// resolutionSummary tracks per-render counters for the log line.
type resolutionSummary struct {
	total       int
	resolved    int
	passthrough int
}

// resolveYAML parses the input upstreams.yaml, substitutes resolvable
// backends with ClusterIPs, and re-serializes. The shape mirrors what
// synapse-proxy's parceyaml.rs expects:
//
//	upstreams:
//	  <host>:
//	    paths:
//	      <path-or-label>:
//	        servers:
//	          - <addr>:<port>
//	          ...
//	        ... other path fields preserved verbatim ...
//
// Only the leaf `servers` entries are inspected. Everything else (path
// configs, ssl_enabled, match_expr, etc.) is preserved as-is by using a
// generic `map[string]any` round-trip.
func (r *UpstreamsResolverReconciler) resolveYAML(
	ctx context.Context, defaultNamespace, in string,
) (string, resolutionSummary, error) {
	var root map[string]any
	if err := yaml.Unmarshal([]byte(in), &root); err != nil {
		return "", resolutionSummary{}, fmt.Errorf("unmarshal upstreams.yaml: %w", err)
	}

	logger := log.FromContext(ctx)
	summary := resolutionSummary{}

	upstreams, ok := root["upstreams"].(map[string]any)
	if !ok {
		// No upstreams block (or differently typed) — pass through.
		return in, summary, nil
	}

	for hostName, hostVal := range upstreams {
		hostMap, ok := hostVal.(map[string]any)
		if !ok {
			continue
		}
		paths, ok := hostMap["paths"].(map[string]any)
		if !ok {
			continue
		}
		for pathName, pathVal := range paths {
			pathMap, ok := pathVal.(map[string]any)
			if !ok {
				continue
			}
			servers, ok := pathMap["servers"].([]any)
			if !ok {
				continue
			}
			for i, sv := range servers {
				addr, ok := sv.(string)
				if !ok {
					continue
				}
				summary.total++
				rewritten, didResolve := r.rewriteServer(ctx, defaultNamespace, addr)
				servers[i] = rewritten
				if didResolve {
					summary.resolved++
				} else {
					summary.passthrough++
					logger.V(1).Info("backend passed through unchanged",
						"host", hostName, "path", pathName, "server", addr)
				}
			}
			pathMap["servers"] = servers
		}
	}

	out, err := yaml.Marshal(root)
	if err != nil {
		return "", summary, fmt.Errorf("marshal resolved upstreams: %w", err)
	}
	return string(out), summary, nil
}

// rewriteServer takes one entry from a `servers:` list and returns the
// version that should appear in the output. Returns (rewritten, true)
// when an actual substitution happened, (original, false) otherwise.
func (r *UpstreamsResolverReconciler) rewriteServer(
	ctx context.Context, defaultNamespace, addr string,
) (string, bool) {
	host, port, ok := splitHostPort(addr)
	if !ok {
		return addr, false
	}

	// Already an IP literal? Nothing to do.
	if isIPLiteral(host) {
		return addr, false
	}

	svcNamespace, svcName, isClusterFQDN := r.parseClusterFQDN(host)
	if !isClusterFQDN {
		// Short-form `<svc>` (single label, no dots) is taken as a
		// reference to a Service in the source's own namespace — this
		// matches what synapse-proxy already does when the resolver
		// applies the cluster suffix.
		if !strings.Contains(host, ".") {
			svcNamespace = defaultNamespace
			svcName = host
		} else {
			// External domain or some other shape; pass through.
			return addr, false
		}
	}

	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Namespace: svcNamespace, Name: svcName}, &svc); err != nil {
		// Service missing right now — passthrough; synapse-proxy will
		// resolve via DNS at request time. Next Service event will
		// re-render us.
		return addr, false
	}

	ip := svc.Spec.ClusterIP
	if ip == "" || ip == "None" {
		// Headless service: ClusterIP=None. Pass through; the proxy
		// must do its own selection across endpoints.
		return addr, false
	}
	return fmt.Sprintf("%s:%s", ip, port), true
}

// parseClusterFQDN returns (namespace, name, true) when host matches
// `<svc>.<ns>.svc.<ClusterDomain>` (with ClusterDomain defaulting to
// `cluster.local`). Anything else returns (..., false).
func (r *UpstreamsResolverReconciler) parseClusterFQDN(host string) (string, string, bool) {
	cd := r.ClusterDomain
	if cd == "" {
		cd = "cluster.local"
	}
	suffix := ".svc." + cd
	if !strings.HasSuffix(host, suffix) {
		return "", "", false
	}
	stem := strings.TrimSuffix(host, suffix)
	parts := strings.Split(stem, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[1], parts[0], true
}

func splitHostPort(addr string) (string, string, bool) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", "", false
	}
	host := addr[:idx]
	port := addr[idx+1:]
	// Strip IPv6 brackets if present so callers can probe IP-ness.
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	return host, port, host != "" && port != ""
}

func isIPLiteral(host string) bool {
	// Treat dotted-quad and any colon-containing string as an IP — the
	// only colons left after splitHostPort would be from an IPv6
	// address inside brackets, which we already unbracketed.
	if strings.Contains(host, ":") {
		return true
	}
	for _, c := range host {
		if c == '.' || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return strings.Count(host, ".") == 3
}

func isResolverSource(cm *corev1.ConfigMap) bool {
	if cm == nil {
		return false
	}
	v, ok := cm.Labels[ResolverSourceLabel]
	return ok && (v == "true" || v == "1")
}

func resolvedOutputName(src *corev1.ConfigMap) string {
	if n, ok := src.Annotations[ResolvedConfigMapAnnotation]; ok && n != "" {
		return n
	}
	return src.Name + "-resolved"
}

// SetupWithManager wires the controller: watches labelled source
// ConfigMaps directly, and Services (any change fans out to every
// labelled source).
func (r *UpstreamsResolverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueAllSources := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, _ client.Object) []reconcile.Request {
			var list corev1.ConfigMapList
			if err := mgr.GetClient().List(ctx, &list,
				client.MatchingLabelsSelector{
					Selector: labels.SelectorFromSet(labels.Set{
						ResolverSourceLabel: "true",
					}),
				}); err != nil {
				log.FromContext(ctx).Error(err,
					"failed to list resolver source ConfigMaps")
				return nil
			}
			out := make([]reconcile.Request, 0, len(list.Items))
			for _, cm := range list.Items {
				out = append(out, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: cm.Namespace,
						Name:      cm.Name,
					},
				})
			}
			return out
		},
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named("synapse-upstreams-resolver").
		For(&corev1.ConfigMap{}, builder.WithPredicates(sourcePredicate())).
		Watches(&corev1.Service{}, enqueueAllSources).
		Complete(r)
}

// sourcePredicate filters ConfigMap events down to those carrying the
// resolver source label. Without this we'd reconcile on every
// ConfigMap change cluster-wide.
func sourcePredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		cm, ok := obj.(*corev1.ConfigMap)
		return ok && isResolverSource(cm)
	})
}

// LogStartup mirrors IngressReconciler's startup banner so operators
// see at a glance which controllers are active in a given pod.
func (r *UpstreamsResolverReconciler) LogStartup(l logr.Logger) {
	cd := r.ClusterDomain
	if cd == "" {
		cd = "cluster.local"
	}
	l.Info("starting UpstreamsResolverReconciler",
		"sourceLabel", ResolverSourceLabel,
		"upstreamsKey", UpstreamsKey,
		"clusterDomain", cd,
		"defaultOutputSuffix", "-resolved")
}
