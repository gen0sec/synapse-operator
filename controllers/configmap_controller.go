package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// ConfigMapReconciler watches Synapse config ConfigMaps/Secrets and forces a rollout on the workload when the config changes.
type ConfigMapReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	LabelSelector        labels.Selector
	ConfigHashAnnotation string
	IgnoredConfigMapKeys map[string]struct{}
	IgnoredSecretKeys    map[string]struct{}
}

// Reconcile reacts to ConfigMap/Secret updates by updating the pod template annotation on Synapse workloads.
func (r *ConfigMapReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("resource", req.NamespacedName)

	var cfg corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &cfg); err == nil {
		logger = logger.WithValues("kind", "ConfigMap")
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	} else {
		var secret corev1.Secret
		if err := r.Get(ctx, req.NamespacedName, &secret); err == nil {
			logger = logger.WithValues("kind", "Secret")
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	hash, err := r.computeCombinedHash(ctx, req.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if hash == "" {
		logger.Info("No config sources found, skipping rollout")
		return ctrl.Result{}, nil
	}

	if err := r.patchDeployments(ctx, req.Namespace, hash, logger); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchDaemonSets(ctx, req.Namespace, hash, logger); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatefulSets(ctx, req.Namespace, hash, logger); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager configures the controller to watch ConfigMaps/Secrets that match the selector.
func (r *ConfigMapReconciler) SetupWithManager(mgr ctrl.Manager) error {
	selector := r.selector()
	matchesSelector := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		if obj == nil {
			return false
		}
		return selector.Matches(labels.Set(obj.GetLabels()))
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(
			&corev1.ConfigMap{},
			builder.WithPredicates(matchesSelector),
		).
		Watches(
			&corev1.Secret{},
			&handler.EnqueueRequestForObject{},
			builder.WithPredicates(matchesSelector),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(r)
}

func (r *ConfigMapReconciler) selector() labels.Selector {
	if r.LabelSelector == nil {
		return labels.Everything()
	}
	return r.LabelSelector
}

func (r *ConfigMapReconciler) computeCombinedHash(ctx context.Context, namespace string) (string, error) {
	configMaps := &corev1.ConfigMapList{}
	if err := r.List(
		ctx,
		configMaps,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return "", err
	}

	secrets := &corev1.SecretList{}
	if err := r.List(
		ctx,
		secrets,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return "", err
	}

	return hashConfigSources(configMaps.Items, secrets.Items, r.IgnoredConfigMapKeys, r.IgnoredSecretKeys), nil
}

func (r *ConfigMapReconciler) patchDeployments(ctx context.Context, namespace, hash string, logger logr.Logger) error {
	deployments := &appsv1.DeploymentList{}
	if err := r.List(
		ctx,
		deployments,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return err
	}

	for i := range deployments.Items {
		deploy := &deployments.Items[i]
		itemLogger := logger.WithValues("deployment", deploy.Name)
		updated, err := patchDeploymentHash(ctx, r.Client, deploy, r.ConfigHashAnnotation, hash)
		if err != nil {
			itemLogger.Error(err, "failed to update deployment with new config hash")
			return err
		}
		if updated {
			itemLogger.Info("Updated deployment pod template annotation to trigger restart", "configHash", hash)
		} else {
			itemLogger.V(1).Info("Deployment already up to date with config hash")
		}
	}

	return nil
}

func (r *ConfigMapReconciler) patchDaemonSets(ctx context.Context, namespace, hash string, logger logr.Logger) error {
	daemonSets := &appsv1.DaemonSetList{}
	if err := r.List(
		ctx,
		daemonSets,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return err
	}

	for i := range daemonSets.Items {
		daemonSet := &daemonSets.Items[i]
		itemLogger := logger.WithValues("daemonset", daemonSet.Name)
		updated, err := patchDaemonSetHash(ctx, r.Client, daemonSet, r.ConfigHashAnnotation, hash)
		if err != nil {
			itemLogger.Error(err, "failed to update daemonset with new config hash")
			return err
		}
		if updated {
			itemLogger.Info("Updated daemonset pod template annotation to trigger restart", "configHash", hash)
		} else {
			itemLogger.V(1).Info("DaemonSet already up to date with config hash")
		}
	}

	return nil
}

func (r *ConfigMapReconciler) patchStatefulSets(ctx context.Context, namespace, hash string, logger logr.Logger) error {
	statefulSets := &appsv1.StatefulSetList{}
	if err := r.List(
		ctx,
		statefulSets,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return err
	}

	for i := range statefulSets.Items {
		statefulSet := &statefulSets.Items[i]
		itemLogger := logger.WithValues("statefulset", statefulSet.Name)
		updated, err := patchStatefulSetHash(ctx, r.Client, statefulSet, r.ConfigHashAnnotation, hash)
		if err != nil {
			itemLogger.Error(err, "failed to update statefulset with new config hash")
			return err
		}
		if updated {
			itemLogger.Info("Updated statefulset pod template annotation to trigger restart", "configHash", hash)
		} else {
			itemLogger.V(1).Info("StatefulSet already up to date with config hash")
		}
	}

	return nil
}

func patchDeploymentHash(ctx context.Context, c client.Client, deploy *appsv1.Deployment, annotationKey, hash string) (bool, error) {
	original := deploy.DeepCopy()
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = map[string]string{}
	}
	if existing := deploy.Spec.Template.Annotations[annotationKey]; existing == hash {
		return false, nil
	}
	deploy.Spec.Template.Annotations[annotationKey] = hash
	return true, c.Patch(ctx, deploy, client.MergeFrom(original))
}

func patchDaemonSetHash(ctx context.Context, c client.Client, daemonSet *appsv1.DaemonSet, annotationKey, hash string) (bool, error) {
	original := daemonSet.DeepCopy()
	if daemonSet.Spec.Template.Annotations == nil {
		daemonSet.Spec.Template.Annotations = map[string]string{}
	}
	if existing := daemonSet.Spec.Template.Annotations[annotationKey]; existing == hash {
		return false, nil
	}
	daemonSet.Spec.Template.Annotations[annotationKey] = hash
	return true, c.Patch(ctx, daemonSet, client.MergeFrom(original))
}

func patchStatefulSetHash(ctx context.Context, c client.Client, statefulSet *appsv1.StatefulSet, annotationKey, hash string) (bool, error) {
	original := statefulSet.DeepCopy()
	if statefulSet.Spec.Template.Annotations == nil {
		statefulSet.Spec.Template.Annotations = map[string]string{}
	}
	if existing := statefulSet.Spec.Template.Annotations[annotationKey]; existing == hash {
		return false, nil
	}
	statefulSet.Spec.Template.Annotations[annotationKey] = hash
	return true, c.Patch(ctx, statefulSet, client.MergeFrom(original))
}

func hashConfigSources(configMaps []corev1.ConfigMap, secrets []corev1.Secret, ignoredConfigMapKeys, ignoredSecretKeys map[string]struct{}) string {
	type hashEntry struct {
		key  string
		hash string
	}

	entries := make([]hashEntry, 0, len(configMaps)+len(secrets))
	for i := range configMaps {
		cfg := &configMaps[i]
		hash := hashConfigMapContent(cfg, ignoredConfigMapKeys)
		if hash == "" {
			continue
		}
		entries = append(entries, hashEntry{
			key:  "configmap/" + cfg.Name,
			hash: hash,
		})
	}
	for i := range secrets {
		secret := &secrets[i]
		hash := hashSecretContent(secret, ignoredSecretKeys)
		if hash == "" {
			continue
		}
		entries = append(entries, hashEntry{
			key:  "secret/" + secret.Name,
			hash: hash,
		})
	}

	if len(entries) == 0 {
		return ""
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	hasher := sha256.New()
	for _, entry := range entries {
		hasher.Write([]byte(entry.key))
		hasher.Write([]byte{0})
		hasher.Write([]byte(entry.hash))
		hasher.Write([]byte{0})
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func hashConfigMapContent(cfg *corev1.ConfigMap, ignoredKeys map[string]struct{}) string {
	if len(cfg.Data) == 0 && len(cfg.BinaryData) == 0 {
		return ""
	}

	keys := make([]string, 0, len(cfg.Data)+len(cfg.BinaryData))
	for k := range cfg.Data {
		if shouldIgnoreKey(k, ignoredKeys) {
			continue
		}
		keys = append(keys, "s:"+k)
	}
	for k := range cfg.BinaryData {
		if shouldIgnoreKey(k, ignoredKeys) {
			continue
		}
		keys = append(keys, "b:"+k)
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)

	hasher := sha256.New()
	for _, k := range keys {
		switch {
		case len(k) > 2 && k[0:2] == "s:":
			key := k[2:]
			hasher.Write([]byte("s"))
			hasher.Write([]byte(key))
			hasher.Write([]byte{0})
			hasher.Write([]byte(cfg.Data[key]))
		case len(k) > 2 && k[0:2] == "b:":
			key := k[2:]
			hasher.Write([]byte("b"))
			hasher.Write([]byte(key))
			hasher.Write([]byte{0})
			hasher.Write(cfg.BinaryData[key])
		}
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func hashSecretContent(secret *corev1.Secret, ignoredKeys map[string]struct{}) string {
	if len(secret.Data) == 0 {
		return ""
	}

	keys := make([]string, 0, len(secret.Data))
	for k := range secret.Data {
		if shouldIgnoreKey(k, ignoredKeys) {
			continue
		}
		keys = append(keys, "d:"+k)
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)

	hasher := sha256.New()
	for _, k := range keys {
		key := k[2:]
		hasher.Write([]byte("d"))
		hasher.Write([]byte(key))
		hasher.Write([]byte{0})
		hasher.Write(secret.Data[key])
		hasher.Write([]byte{0})
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func shouldIgnoreKey(key string, ignoredKeys map[string]struct{}) bool {
	if len(ignoredKeys) == 0 {
		return false
	}
	_, ok := ignoredKeys[key]
	return ok
}
