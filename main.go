package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"synapse-operator/controllers"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(networkingv1.AddToScheme(scheme))
	utilruntime.Must(gwv1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var watchedNamespace string
	var labelSelector string
	var configHashAnnotation string
	var ignoredConfigMapKeys string
	var ignoredSecretKeys string
	var ingressMode bool
	var upstreamsResolver bool
	var netvarsResolver bool
	var idsHotReloadHashExclude bool
	var perWorkloadConfigHash bool
	var renderOnce bool
	var ingressClass string
	var upstreamsOut string
	var upstreamsOutConfigMap string
	var resolveBackendClusterIPs bool
	var clusterDomain string
	var certsOut string
	var gatewayAPI bool
	var publishStatusAddress string
	var reloadProcessName string
	var reloadDebounce time.Duration
	var statusLeaderElection bool
	var statusLeaderElectionID string
	var leaderElectionNamespace string

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&watchedNamespace, "namespace", "", "Namespace to watch. Defaults to all namespaces.")
	flag.StringVar(&labelSelector, "label-selector", "app.kubernetes.io/name=synapse", "Label selector for config sources and workloads.")
	flag.StringVar(&configHashAnnotation, "config-hash-annotation", "synapse.gen0sec.com/config-hash", "Annotation key to store the config hash.")
	flag.StringVar(&ignoredConfigMapKeys, "ignore-configmap-keys", "upstreams.yaml", "Comma-separated ConfigMap keys to ignore when hashing.")
	flag.StringVar(&ignoredSecretKeys, "ignore-secret-keys", "", "Comma-separated Secret keys to ignore when hashing.")
	flag.BoolVar(&ingressMode, "ingress-mode", false, "Run as a Kubernetes Ingress + Gateway API controller (sidecar) instead of the config-hash controller: render class-matched Ingresses/HTTPRoutes into a synapse upstreams.yaml.")
	flag.BoolVar(&upstreamsResolver, "upstreams-resolver", false, "Enable the UpstreamsResolverReconciler: watch ConfigMaps labelled synapse.gen0sec.com/resolve-upstreams=true, substitute backend Service DNS names with their ClusterIPs, write the result to a sibling ConfigMap. Composable with other modes.")
	flag.BoolVar(&netvarsResolver, "netvars-resolver", false, "Enable the NetVarsResolverReconciler: watch synapse agent config ConfigMaps labelled synapse.gen0sec.com/resolve-netvars=true and fill ids.address_vars.HOME_NET/EXTERNAL_NET from cluster Node IPs + PodCIDRs + LoadBalancer VIPs + RFC1918 supernets (so inline IDS blocking never bans an internal IP). Honours a manual HOME_NET in the config or the synapse.gen0sec.com/home-net annotation. Composable with other modes.")
	flag.BoolVar(&idsHotReloadHashExclude, "ids-hot-reload-hash-exclude", false, "Exclude the hot-reloadable thalamus IDS fields (ids.address_vars/enforce_block/rule_paths/port_vars/flow_timeout_secs/max_flows) from the config-hash, so a change to ONLY those does not roll the workload (the agent hot-reloads them in process). Other config.yaml changes still roll. Only safe once all agents run a synapse image that hot-reloads these fields (r32+).")
	flag.BoolVar(&perWorkloadConfigHash, "per-workload-config-hash", false, "Stamp each workload with a hash of ONLY the labelled ConfigMaps/Secrets it actually references (volumes/envFrom/env), instead of one combined hash over every labelled source in the namespace. Stops an unrelated config change (e.g. agent rules) from rolling other workloads (e.g. the proxy). A workload that references no labelled source is left untouched.")
	flag.BoolVar(&renderOnce, "render-once", false, "Ingress-mode one-shot: render upstreams.yaml from current Ingresses/HTTPRoutes and exit (initContainer; primes the file before synapse starts).")
	flag.StringVar(&ingressClass, "ingress-class", "synapse", "spec.ingressClassName this controller serves (ingress-mode).")
	flag.StringVar(&upstreamsOut, "upstreams-out", "/shared/upstreams.yaml", "Path to write the rendered synapse upstreams.yaml (ingress-mode, sidecar layout: a shared volume synapse inotify-reloads). Ignored when --upstreams-out-configmap is set.")
	flag.StringVar(&upstreamsOutConfigMap, "upstreams-out-configmap", "", "Ingress-mode central layout: write the rendered upstreams.yaml to this ConfigMap (format namespace/name) instead of a file path. Disables SIGHUP signalling — synapse-proxy reloads via its own machinery on the ConfigMap mount.")
	flag.BoolVar(&resolveBackendClusterIPs, "resolve-backend-cluster-ips", false, "Ingress-mode: emit `<clusterIP>:port` instead of `<svc>.<ns>.svc.<cluster-domain>:port` for each backend, so synapse-proxy's HttpPeer skips DNS. Falls back to the FQDN for headless / ExternalName / not-yet-allocated Services.")
	flag.StringVar(&clusterDomain, "cluster-domain", "cluster.local", "Cluster DNS domain for backend FQDNs (ingress-mode).")
	flag.StringVar(&certsOut, "certs-out", "", "Ingress-mode: directory to project referenced Ingress/Gateway TLS Secrets into as <stem>.crt/<stem>.key (synapse's certificates dir; operator-owned, inotify-hot-reloaded). Empty = multi-cert disabled (legacy static mount).")
	flag.BoolVar(&gatewayAPI, "gateway-api", false, "Also reconcile Gateway API (GatewayClass/Gateway/HTTPRoute) into the same upstreams.yaml (ingress-mode; requires the Gateway API CRDs).")
	flag.StringVar(&publishStatusAddress, "publish-status-address", "", "Comma-separated IPs/hostnames to publish on matched Ingresses' .status.loadBalancer.ingress (ingress-mode). Empty = do not publish.")
	flag.StringVar(&reloadProcessName, "reload-process-name", "synapse", "argv0 basename of the co-located proxy process to SIGHUP on a changed render (ingress-mode).")
	flag.DurationVar(&reloadDebounce, "reload-debounce", 500*time.Millisecond, "Coalesce SIGHUP reload bursts within this window (ingress-mode; 0 = signal immediately on every changed render).")
	flag.BoolVar(&statusLeaderElection, "status-leader-election", false, "Ingress-mode: with >1 proxy replica, only the Lease holder writes shared cluster status (Gateway/HTTPRoute status, Ingress .status.loadBalancer). Per-pod render+SIGHUP is never gated. Off ⇒ every replica writes (single-replica default).")
	flag.StringVar(&statusLeaderElectionID, "status-leader-election-id", "synapse-ingress-status", "Lease name for the shared-status election (ingress-mode).")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "", "Namespace for the shared-status Lease (ingress-mode; defaults to $POD_NAMESPACE, then \"default\").")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	outCM, err := parseNamespacedName(upstreamsOutConfigMap)
	if err != nil {
		setupLog.Error(err, "--upstreams-out-configmap")
		os.Exit(1)
	}

	if ingressMode && renderOnce {
		cl, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
		if err != nil {
			setupLog.Error(err, "render-once: client")
			os.Exit(1)
		}
		ir := &controllers.IngressReconciler{
			Client:                   cl,
			IngressClassName:         ingressClass,
			UpstreamsOutPath:         upstreamsOut,
			UpstreamsOutConfigMap:    outCM,
			ResolveBackendClusterIPs: resolveBackendClusterIPs,
			CertsOutDir:              certsOut,
			ClusterDomain:            clusterDomain,
			GatewayAPI:               gatewayAPI,
		}
		if err := ir.RenderOnce(context.Background()); err != nil {
			setupLog.Error(err, "render-once failed")
			os.Exit(1)
		}
		os.Exit(0)
	}

	if strings.TrimSpace(configHashAnnotation) == "" {
		setupLog.Error(nil, "config-hash-annotation cannot be empty")
		os.Exit(1)
	}

	selector, err := parseLabelSelector(labelSelector)
	if err != nil {
		setupLog.Error(err, "invalid label selector", "selector", labelSelector)
		os.Exit(1)
	}

	ignoredConfigMapSet := parseKeySet(ignoredConfigMapKeys)
	ignoredSecretSet := parseKeySet(ignoredSecretKeys)

	mgrOptions := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "86a223f3.synapse.gen0sec.com",
	}

	if watchedNamespace != "" {
		mgrOptions.Cache.DefaultNamespaces = map[string]cache.Config{
			watchedNamespace: {},
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	var ingressReconciler *controllers.IngressReconciler
	if ingressMode {
		ingressReconciler = &controllers.IngressReconciler{
			Client:                   mgr.GetClient(),
			IngressClassName:         ingressClass,
			UpstreamsOutPath:         upstreamsOut,
			UpstreamsOutConfigMap:    outCM,
			ResolveBackendClusterIPs: resolveBackendClusterIPs,
			CertsOutDir:              certsOut,
			ClusterDomain:            clusterDomain,
			GatewayAPI:               gatewayAPI,
			// SignalReload is the sidecar-mode reload mechanism. The
			// reconciler also no-ops it internally in central mode (when
			// UpstreamsOutConfigMap is set), but we still gate the
			// constructor flag here for clarity: there's no co-located
			// synapse process to signal when we write to a ConfigMap.
			SignalReload:      outCM.Name == "",
			StatusAddresses:   parseCSV(publishStatusAddress),
			ReloadProcessName: reloadProcessName,
			ReloadDebounce:    reloadDebounce,
			Recorder:          mgr.GetEventRecorderFor("synapse-ingress"),
		}
		if statusLeaderElection {
			ns := leaderElectionNamespace
			if ns == "" {
				ns = os.Getenv("POD_NAMESPACE")
			}
			gate := &controllers.LeaderGate{}
			ingressReconciler.IsLeader = gate.IsLeader
			if err = mgr.Add(controllers.NewStatusLeaderElection(
				mgr.GetConfig(), ns, statusLeaderElectionID, os.Getenv("POD_NAME"), gate)); err != nil {
				setupLog.Error(err, "unable to add status leader election")
				os.Exit(1)
			}
		}
		if err = ingressReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "Ingress")
			os.Exit(1)
		}
		if err = mgr.Add(controllers.NewRenderPrimer(ingressReconciler)); err != nil {
			setupLog.Error(err, "unable to add render primer")
			os.Exit(1)
		}
		ingressReconciler.LogStartup(setupLog)
	} else if err = (&controllers.ConfigMapReconciler{
		Client:                    mgr.GetClient(),
		Scheme:                    mgr.GetScheme(),
		LabelSelector:             selector,
		ConfigHashAnnotation:      configHashAnnotation,
		IgnoredConfigMapKeys:      ignoredConfigMapSet,
		IgnoredSecretKeys:         ignoredSecretSet,
		ExcludeHotReloadIdsFields: idsHotReloadHashExclude,
		PerWorkloadConfigHash:     perWorkloadConfigHash,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ConfigMap")
		os.Exit(1)
	}

	if upstreamsResolver {
		ur := &controllers.UpstreamsResolverReconciler{
			Client:        mgr.GetClient(),
			Scheme:        mgr.GetScheme(),
			ClusterDomain: clusterDomain,
		}
		if err = ur.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "UpstreamsResolver")
			os.Exit(1)
		}
		ur.LogStartup(setupLog)
	}

	if netvarsResolver {
		nr := &controllers.NetVarsResolverReconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}
		if err = nr.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "NetVarsResolver")
			os.Exit(1)
		}
		nr.LogStartup(setupLog)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}

	readyCheck := healthz.Ping
	if ingressReconciler != nil {
		readyCheck = ingressReconciler.ReadyCheck
	}
	if err := mgr.AddReadyzCheck("readyz", readyCheck); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func parseLabelSelector(value string) (labels.Selector, error) {
	if strings.TrimSpace(value) == "" {
		return labels.Everything(), nil
	}
	return labels.Parse(value)
}

// parseNamespacedName accepts "namespace/name". Returns the zero value
// for an empty input — callers gate on Name == "" to detect "no output
// ConfigMap configured" (sidecar / file-only mode).
func parseNamespacedName(value string) (types.NamespacedName, error) {
	if strings.TrimSpace(value) == "" {
		return types.NamespacedName{}, nil
	}
	parts := strings.SplitN(value, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return types.NamespacedName{}, fmt.Errorf(
			"expected namespace/name, got %q", value)
	}
	return types.NamespacedName{
		Namespace: strings.TrimSpace(parts[0]),
		Name:      strings.TrimSpace(parts[1]),
	}, nil
}

func parseCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if s := strings.TrimSpace(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func parseKeySet(value string) map[string]struct{} {
	items := strings.Split(value, ",")
	if len(items) == 0 {
		return nil
	}
	entries := make(map[string]struct{})
	for _, item := range items {
		key := strings.TrimSpace(item)
		if key == "" {
			continue
		}
		entries[key] = struct{}{}
	}
	if len(entries) == 0 {
		return nil
	}
	return entries
}
