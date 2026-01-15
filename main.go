package main

import (
	"flag"
	"os"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

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

	if err = (&controllers.ConfigMapReconciler{
		Client:               mgr.GetClient(),
		Scheme:               mgr.GetScheme(),
		LabelSelector:        selector,
		ConfigHashAnnotation: configHashAnnotation,
		IgnoredConfigMapKeys: ignoredConfigMapSet,
		IgnoredSecretKeys:    ignoredSecretSet,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ConfigMap")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
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
