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
	"sigs.k8s.io/yaml"
)

// ConfigMapReconciler watches Synapse config ConfigMaps/Secrets and forces a rollout on the workload when the config changes.
type ConfigMapReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	LabelSelector        labels.Selector
	ConfigHashAnnotation string
	IgnoredConfigMapKeys map[string]struct{}
	IgnoredSecretKeys    map[string]struct{}
	// ExcludeHotReloadIdsFields, when true, canonicalizes the hot-reloadable
	// thalamus IDS subtree out of config.yaml before hashing, so a change to
	// ONLY those fields (HOME_NET/EXTERNAL_NET, enforce_block, rule_paths,
	// port_vars, flow_timeout/max_flows) does NOT bump the config hash and thus
	// does NOT roll the workload — the agent hot-reloads them in process. Other
	// config.yaml changes still roll. Only safe once all agents run an image
	// that hot-reloads these fields (synapse r32+).
	ExcludeHotReloadIdsFields bool
	// PerWorkloadConfigHash, when true, stamps each workload with a hash of ONLY
	// the labelled ConfigMaps/Secrets it actually references (volumes, envFrom,
	// env valueFrom) instead of one combined hash over every labelled source in
	// the namespace. This stops an unrelated config change (e.g. the agent's
	// rules) from rolling other workloads (e.g. the proxy). A workload that
	// references no labelled source is left untouched.
	PerWorkloadConfigHash bool
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

	cms, secrets, err := r.listConfigSources(ctx, req.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	combined := hashConfigSources(cms, secrets, r.IgnoredConfigMapKeys, r.IgnoredSecretKeys, r.ExcludeHotReloadIdsFields)
	if combined == "" && !r.PerWorkloadConfigHash {
		logger.Info("No config sources found, skipping rollout")
		return ctrl.Result{}, nil
	}

	if err := r.patchDeployments(ctx, req.Namespace, cms, secrets, combined, logger); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchDaemonSets(ctx, req.Namespace, cms, secrets, combined, logger); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatefulSets(ctx, req.Namespace, cms, secrets, combined, logger); err != nil {
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

// listConfigSources returns all labelled ConfigMaps + Secrets in the namespace.
func (r *ConfigMapReconciler) listConfigSources(ctx context.Context, namespace string) ([]corev1.ConfigMap, []corev1.Secret, error) {
	configMaps := &corev1.ConfigMapList{}
	if err := r.List(
		ctx,
		configMaps,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return nil, nil, err
	}

	secrets := &corev1.SecretList{}
	if err := r.List(
		ctx,
		secrets,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: r.selector()},
	); err != nil {
		return nil, nil, err
	}

	return configMaps.Items, secrets.Items, nil
}

// hashForWorkload returns the config hash to stamp on a workload. In combined
// mode it's the namespace-wide hash. In per-workload mode it's a hash over only
// the labelled sources the workload references (volumes/envFrom/env) — so an
// unrelated config change doesn't roll it. Returns "" when per-workload mode
// finds no referenced labelled source (caller leaves the workload untouched).
func (r *ConfigMapReconciler) hashForWorkload(spec *corev1.PodSpec, cms []corev1.ConfigMap, secrets []corev1.Secret, combined string) string {
	if !r.PerWorkloadConfigHash {
		return combined
	}
	refCMs, refSecrets := referencedSources(spec)
	fcms := filterConfigMaps(cms, refCMs)
	fsecrets := filterSecrets(secrets, refSecrets)
	return hashConfigSources(fcms, fsecrets, r.IgnoredConfigMapKeys, r.IgnoredSecretKeys, r.ExcludeHotReloadIdsFields)
}

// referencedSources collects ConfigMap/Secret names a pod spec references via
// volumes (incl. projected), envFrom, and env valueFrom.
func referencedSources(spec *corev1.PodSpec) (map[string]struct{}, map[string]struct{}) {
	cms := map[string]struct{}{}
	secs := map[string]struct{}{}
	for i := range spec.Volumes {
		v := &spec.Volumes[i]
		if v.ConfigMap != nil {
			cms[v.ConfigMap.Name] = struct{}{}
		}
		if v.Secret != nil {
			secs[v.Secret.SecretName] = struct{}{}
		}
		if v.Projected != nil {
			for _, s := range v.Projected.Sources {
				if s.ConfigMap != nil {
					cms[s.ConfigMap.Name] = struct{}{}
				}
				if s.Secret != nil {
					secs[s.Secret.Name] = struct{}{}
				}
			}
		}
	}
	containers := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
	for i := range containers {
		c := &containers[i]
		for _, ef := range c.EnvFrom {
			if ef.ConfigMapRef != nil {
				cms[ef.ConfigMapRef.Name] = struct{}{}
			}
			if ef.SecretRef != nil {
				secs[ef.SecretRef.Name] = struct{}{}
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom == nil {
				continue
			}
			if e.ValueFrom.ConfigMapKeyRef != nil {
				cms[e.ValueFrom.ConfigMapKeyRef.Name] = struct{}{}
			}
			if e.ValueFrom.SecretKeyRef != nil {
				secs[e.ValueFrom.SecretKeyRef.Name] = struct{}{}
			}
		}
	}
	return cms, secs
}

func filterConfigMaps(cms []corev1.ConfigMap, names map[string]struct{}) []corev1.ConfigMap {
	out := make([]corev1.ConfigMap, 0, len(names))
	for i := range cms {
		if _, ok := names[cms[i].Name]; ok {
			out = append(out, cms[i])
		}
	}
	return out
}

func filterSecrets(secrets []corev1.Secret, names map[string]struct{}) []corev1.Secret {
	out := make([]corev1.Secret, 0, len(names))
	for i := range secrets {
		if _, ok := names[secrets[i].Name]; ok {
			out = append(out, secrets[i])
		}
	}
	return out
}

func (r *ConfigMapReconciler) patchDeployments(ctx context.Context, namespace string, cms []corev1.ConfigMap, secrets []corev1.Secret, combined string, logger logr.Logger) error {
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
		hash := r.hashForWorkload(&deploy.Spec.Template.Spec, cms, secrets, combined)
		if hash == "" {
			itemLogger.V(1).Info("Deployment references no labelled config, skipping")
			continue
		}
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

func (r *ConfigMapReconciler) patchDaemonSets(ctx context.Context, namespace string, cms []corev1.ConfigMap, secrets []corev1.Secret, combined string, logger logr.Logger) error {
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
		hash := r.hashForWorkload(&daemonSet.Spec.Template.Spec, cms, secrets, combined)
		if hash == "" {
			itemLogger.V(1).Info("DaemonSet references no labelled config, skipping")
			continue
		}
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

func (r *ConfigMapReconciler) patchStatefulSets(ctx context.Context, namespace string, cms []corev1.ConfigMap, secrets []corev1.Secret, combined string, logger logr.Logger) error {
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
		hash := r.hashForWorkload(&statefulSet.Spec.Template.Spec, cms, secrets, combined)
		if hash == "" {
			itemLogger.V(1).Info("StatefulSet references no labelled config, skipping")
			continue
		}
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

func hashConfigSources(configMaps []corev1.ConfigMap, secrets []corev1.Secret, ignoredConfigMapKeys, ignoredSecretKeys map[string]struct{}, excludeHotReloadIds bool) string {
	type hashEntry struct {
		key  string
		hash string
	}

	entries := make([]hashEntry, 0, len(configMaps)+len(secrets))
	for i := range configMaps {
		cfg := &configMaps[i]
		hash := hashConfigMapContent(cfg, ignoredConfigMapKeys, excludeHotReloadIds)
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

func hashConfigMapContent(cfg *corev1.ConfigMap, ignoredKeys map[string]struct{}, excludeHotReloadIds bool) string {
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
			value := cfg.Data[key]
			if excludeHotReloadIds && key == SynapseConfigKey {
				value = canonicalConfigSansHotReloadIds(value)
			}
			hasher.Write([]byte("s"))
			hasher.Write([]byte(key))
			hasher.Write([]byte{0})
			hasher.Write([]byte(value))
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

// hotReloadIdsFields are the thalamus IDS config fields the synapse agent
// applies in process (no restart) on a config reload. Stripping them from the
// hash means a change to ONLY these does not roll the workload. Keep in sync
// with synapse-idp's hot_reload_ids_runtime (ids_engine_fields_changed +
// enforce_block).
var hotReloadIdsFields = []string{
	"address_vars",
	"enforce_block",
	"rule_paths",
	"port_vars",
	"flow_timeout_secs",
	"max_flows",
}

// canonicalConfigSansHotReloadIds parses config.yaml, deletes the
// hot-reloadable IDS fields (under both `ids` and the deprecated
// `firewall.ids`), and re-marshals deterministically. If the content can't be
// parsed it is returned unchanged (fail-safe: the workload still rolls).
func canonicalConfigSansHotReloadIds(raw string) string {
	var root map[string]any
	if err := yaml.Unmarshal([]byte(raw), &root); err != nil || root == nil {
		return raw
	}
	stripHotReloadIdsFields(idsSubtree(root, "ids"))
	if fw, ok := root["firewall"].(map[string]any); ok {
		stripHotReloadIdsFields(idsSubtree(fw, "ids"))
	}
	out, err := yaml.Marshal(root)
	if err != nil {
		return raw
	}
	return string(out)
}

func idsSubtree(parent map[string]any, key string) map[string]any {
	ids, _ := parent[key].(map[string]any)
	return ids
}

func stripHotReloadIdsFields(ids map[string]any) {
	if ids == nil {
		return
	}
	for _, f := range hotReloadIdsFields {
		delete(ids, f)
	}
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
