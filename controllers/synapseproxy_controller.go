package controllers

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// Conditions of a SynapseProxy and the reasons they carry.
const (
	// ConditionConfigValid says whether spec renders to a config.yaml.
	ConditionConfigValid = "ConfigValid"
	// ConditionAvailable mirrors the Deployment's own Available condition.
	ConditionAvailable = "Available"
	// ConditionReady is the summary: the spec as written is what runs.
	ConditionReady = "Ready"

	ReasonRendered          = "Rendered"
	ReasonInvalidConfig     = "InvalidConfig"
	ReasonSecretNotFound    = "SecretNotFound"
	ReasonSecretKeyNotFound = "SecretKeyNotFound"

	ReasonApplied                  = "Applied"
	ReasonConfigInvalid            = "ConfigInvalid"
	ReasonWaitingForRoutes         = "WaitingForRoutes"
	ReasonNameCollision            = "NameCollision"
	ReasonRollingOut               = "RollingOut"
	ReasonProgressDeadlineExceeded = "ProgressDeadlineExceeded"
)

// proxyFieldOwner is the field manager the proxy's objects are applied as.
const proxyFieldOwner = "synapse-operator"

// proxyAPIKeySecretIndex indexes SynapseProxies by the Secret their API key
// comes from, so a change to that Secret finds the proxies that read it.
const proxyAPIKeySecretIndex = "spec.platform.apiKeySecretRef.name"

// collisionRetry is how often an object that is in the way is looked at
// again. Nothing watches objects the proxy does not own.
const collisionRetry = time.Minute

// SynapseProxyReconciler runs a SynapseProxy: it renders the config into a
// Secret and applies the ServiceAccount, Service and Deployment around it.
// The routes and certificates the pods also mount are written by
// SynapseRouteReconciler; this controller only waits for them.
//
// Every object is applied server-side as one field manager, so fields other
// writers set (an autoscaler, `kubectl rollout restart`) are left alone and
// fields this controller stops setting are removed.
//
// The pods are rolled by the config Secret's name, which is its content hash:
// this controller is the only thing that restarts them, and it has to cover
// everything they read at start.
//
// What it needs to be allowed is in config/proxy-controller/rbac.yaml, with
// SynapseRouteReconciler's. A change here that asks the API server for
// something new has to add it there, and one that stops asking has to take
// it out: the role's tests fail on both.
type SynapseProxyReconciler struct {
	client.Client
	// Recorder emits Events on the SynapseProxy. May be nil.
	Recorder record.EventRecorder
}

// proxyOutcome is what one reconcile concluded, to be written to status.
type proxyOutcome struct {
	configValid metav1.Condition
	ready       metav1.Condition
	configHash  string
	requeue     time.Duration
}

func (o *proxyOutcome) invalid(reason, message string) {
	o.configValid = metav1.Condition{Type: ConditionConfigValid, Status: metav1.ConditionFalse, Reason: reason, Message: message}
	o.notReady(ReasonConfigInvalid, message)
}

func (o *proxyOutcome) notReady(reason, message string) {
	o.ready = metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message}
}

func (r *SynapseProxyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var proxy synapsev1alpha1.SynapseProxy
	if err := r.Get(ctx, req.NamespacedName, &proxy); err != nil {
		if apierrors.IsNotFound(err) {
			mProxyReady.DeleteLabelValues(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !proxy.DeletionTimestamp.IsZero() {
		// Everything the proxy owns goes with it.
		return ctrl.Result{}, nil
	}

	outcome, err := r.reconcile(ctx, &proxy)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.writeStatus(ctx, &proxy, outcome); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: outcome.requeue}, nil
}

// reconcile brings the proxy's objects in line with its spec, as far as it
// can. A returned error is one worth retrying; anything the user has to fix
// is reported through the outcome instead, with the running objects left as
// they are.
func (r *SynapseProxyReconciler) reconcile(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) (*proxyOutcome, error) {
	o := &proxyOutcome{}

	apiKey, reason, message, err := r.apiKey(ctx, proxy)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		o.invalid(reason, message)
		return o, nil
	}

	cfg, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxy, APIKey: apiKey})
	if len(errs) > 0 {
		o.invalid(ReasonInvalidConfig, errs.ToAggregate().Error())
		return o, nil
	}
	o.configValid = metav1.Condition{Type: ConditionConfigValid, Status: metav1.ConditionTrue, Reason: ReasonRendered,
		Message: "spec renders to a Synapse configuration"}
	o.configHash = cfg.Hash

	// The pods mount the routes and the certificates, and Synapse does not
	// start watching a routes file that was missing when it started.
	if missing, err := r.missingRouteOutput(ctx, proxy); err != nil {
		return nil, err
	} else if missing != "" {
		o.notReady(ReasonWaitingForRoutes, missing)
		return o, nil
	}

	secret := buildProxyConfigSecret(proxy, cfg)
	objects := []client.Object{
		secret,
		buildProxyServiceAccount(proxy),
		buildProxyService(proxy),
		buildProxyDeployment(proxy, secret.Name),
	}
	// All of them, before any is written: half a proxy is worse than none.
	for _, obj := range objects {
		if inTheWay, err := r.inTheWay(ctx, proxy, obj); err != nil {
			return nil, err
		} else if inTheWay != "" {
			o.notReady(ReasonNameCollision, inTheWay)
			o.requeue = collisionRetry
			return o, nil
		}
	}
	for _, obj := range objects {
		if err := r.apply(ctx, obj); err != nil {
			return nil, fmt.Errorf("apply %T %s: %w", obj, obj.GetName(), err)
		}
	}
	if err := r.pruneConfigSecrets(ctx, proxy, secret.Name); err != nil {
		return nil, err
	}

	var deploy appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(proxy), &deploy); err != nil {
		return nil, err
	}
	reason, message = rolloutState(&deploy)
	if reason == ReasonApplied {
		o.ready = metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: reason, Message: message}
	} else {
		o.notReady(reason, message)
	}
	return o, nil
}

// apiKey resolves spec.platform.apiKeySecretRef. A non-empty reason means the
// reference cannot be resolved and says why.
func (r *SynapseProxyReconciler) apiKey(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) (key, reason, message string, err error) {
	if proxy.Spec.Platform == nil || proxy.Spec.Platform.APIKeySecretRef == nil {
		return "", "", "", nil
	}
	ref := proxy.Spec.Platform.APIKeySecretRef
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: proxy.Namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", ReasonSecretNotFound, fmt.Sprintf("spec.platform.apiKeySecretRef: Secret %q not found", ref.Name), nil
		}
		return "", "", "", err
	}
	value, ok := secret.Data[ref.Key]
	if !ok || len(value) == 0 {
		return "", ReasonSecretKeyNotFound, fmt.Sprintf("spec.platform.apiKeySecretRef: Secret %q has no key %q", ref.Name, ref.Key), nil
	}
	return string(value), "", "", nil
}

// missingRouteOutput names the first of the route controller's two objects
// that is not there yet, or returns "".
func (r *SynapseProxyReconciler) missingRouteOutput(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy) (string, error) {
	var routes corev1.ConfigMap
	name := proxyUpstreamsName(proxy)
	if err := r.Get(ctx, name, &routes); err != nil && !apierrors.IsNotFound(err) {
		return "", err
	} else if err != nil || !metav1.IsControlledBy(&routes, proxy) || routes.Data[UpstreamsKey] == "" {
		return fmt.Sprintf("the routes ConfigMap %q has not been rendered yet", name.Name), nil
	}
	var certs corev1.Secret
	name = proxyCertsName(proxy)
	if err := r.Get(ctx, name, &certs); err != nil && !apierrors.IsNotFound(err) {
		return "", err
	} else if err != nil || !metav1.IsControlledBy(&certs, proxy) {
		return fmt.Sprintf("the certificates Secret %q has not been rendered yet", name.Name), nil
	}
	return "", nil
}

// inTheWay reports an object that already exists under obj's name without
// being this proxy's. It is left untouched: applying over it would take it
// from whoever it belongs to.
func (r *SynapseProxyReconciler) inTheWay(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy, obj client.Object) (string, error) {
	existing := obj.DeepCopyObject().(client.Object)
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if metav1.IsControlledBy(existing, proxy) {
		return "", nil
	}
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	return fmt.Sprintf("%s %q already exists and does not belong to this SynapseProxy", kind, obj.GetName()), nil
}

// apply applies obj server-side. The empty status the conversion writes out
// is ignored by the API server: status is a subresource of everything here
// that has one.
func (r *SynapseProxyReconciler) apply(ctx context.Context, obj client.Object) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: content}
	return r.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(proxyFieldOwner), client.ForceOwnership)
}

// pruneConfigSecrets deletes the proxy's config Secrets that nothing mounts
// any more: every one but the current, and but those of ReplicaSets that
// still have pods, which are finishing a rollout.
func (r *SynapseProxyReconciler) pruneConfigSecrets(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy, current string) error {
	inUse := map[string]bool{current: true}
	var replicaSets appsv1.ReplicaSetList
	if err := r.List(ctx, &replicaSets, client.InNamespace(proxy.Namespace), client.MatchingLabels(proxySelector(proxy))); err != nil {
		return err
	}
	for i := range replicaSets.Items {
		rs := &replicaSets.Items[i]
		if rs.Status.Replicas == 0 && (rs.Spec.Replicas == nil || *rs.Spec.Replicas == 0) {
			continue
		}
		for _, v := range rs.Spec.Template.Spec.Volumes {
			if v.Secret != nil {
				inUse[v.Secret.SecretName] = true
			}
		}
	}

	var secrets corev1.SecretList
	selector := client.MatchingLabels{proxyLabel: proxy.Name, proxyPartLabel: proxyPartConfig}
	if err := r.List(ctx, &secrets, client.InNamespace(proxy.Namespace), selector); err != nil {
		return err
	}
	for i := range secrets.Items {
		s := &secrets.Items[i]
		if inUse[s.Name] || !metav1.IsControlledBy(s, proxy) {
			continue
		}
		if err := r.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// rolloutState says whether the Deployment runs what it was last told to,
// the way `kubectl rollout status` decides it.
func rolloutState(d *appsv1.Deployment) (reason, message string) {
	if d.Generation > d.Status.ObservedGeneration {
		return ReasonRollingOut, "waiting for the Deployment to be observed"
	}
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Reason == "ProgressDeadlineExceeded" {
			return ReasonProgressDeadlineExceeded, "the rollout made no progress within its deadline: " + c.Message
		}
	}
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	switch s := d.Status; {
	case s.UpdatedReplicas < want:
		return ReasonRollingOut, fmt.Sprintf("%d of %d pods run the current configuration and image", s.UpdatedReplicas, want)
	case s.Replicas > s.UpdatedReplicas:
		return ReasonRollingOut, fmt.Sprintf("%d old pods are still shutting down", s.Replicas-s.UpdatedReplicas)
	case s.AvailableReplicas < s.UpdatedReplicas:
		return ReasonRollingOut, fmt.Sprintf("%d of %d pods are available", s.AvailableReplicas, s.UpdatedReplicas)
	}
	return ReasonApplied, "the pods run the current configuration and image"
}

// writeStatus records the outcome and what the live objects show.
func (r *SynapseProxyReconciler) writeStatus(ctx context.Context, proxy *synapsev1alpha1.SynapseProxy, o *proxyOutcome) error {
	before := proxy.DeepCopy()
	status := &proxy.Status
	previousReady := meta.FindStatusCondition(status.Conditions, ConditionReady)
	previousReason := ""
	if previousReady != nil {
		previousReason = previousReady.Reason
	}

	status.ObservedGeneration = proxy.Generation
	status.Selector = labels.SelectorFromSet(proxySelector(proxy)).String()
	if o.configHash != "" {
		status.ConfigHash = o.configHash
	}
	for _, c := range []metav1.Condition{o.configValid, o.ready} {
		c.ObservedGeneration = proxy.Generation
		meta.SetStatusCondition(&status.Conditions, c)
	}

	// The running pods are reported whatever the outcome: an invalid spec
	// does not stop what is already there.
	var deploy appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(proxy), &deploy); err == nil && metav1.IsControlledBy(&deploy, proxy) {
		status.Replicas = deploy.Status.Replicas
		status.ReadyReplicas = deploy.Status.ReadyReplicas
		status.UpdatedReplicas = deploy.Status.UpdatedReplicas
		available := metav1.Condition{Type: ConditionAvailable, Status: metav1.ConditionFalse, Reason: "Unknown",
			Message: "the Deployment has not reported its availability", ObservedGeneration: proxy.Generation}
		for _, c := range deploy.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable {
				available.Status = metav1.ConditionStatus(c.Status)
				available.Reason, available.Message = c.Reason, c.Message
			}
		}
		meta.SetStatusCondition(&status.Conditions, available)
	} else if client.IgnoreNotFound(err) != nil {
		return err
	}
	var svc corev1.Service
	if err := r.Get(ctx, client.ObjectKeyFromObject(proxy), &svc); err == nil && metav1.IsControlledBy(&svc, proxy) {
		status.Addresses = serviceAddresses(&svc)
	} else if client.IgnoreNotFound(err) != nil {
		return err
	}

	ready := float64(0)
	if o.ready.Status == metav1.ConditionTrue {
		ready = 1
	}
	mProxyReady.WithLabelValues(proxy.Namespace, proxy.Name).Set(ready)

	if o.ready.Reason != previousReason && r.Recorder != nil {
		eventType := corev1.EventTypeNormal
		switch o.ready.Reason {
		case ReasonConfigInvalid, ReasonNameCollision, ReasonProgressDeadlineExceeded:
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(proxy, eventType, o.ready.Reason, o.ready.Message)
	}

	// A patch with nothing in it changes nothing on the server.
	return r.Status().Patch(ctx, proxy, client.MergeFrom(before))
}

// serviceAddresses returns where the Service can be reached: its
// load-balancer addresses once it has any, its cluster IP until then.
func serviceAddresses(svc *corev1.Service) []string {
	var out []string
	for _, in := range svc.Status.LoadBalancer.Ingress {
		switch {
		case in.IP != "":
			out = append(out, in.IP)
		case in.Hostname != "":
			out = append(out, in.Hostname)
		}
	}
	if len(out) == 0 && svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != corev1.ClusterIPNone {
		out = append(out, svc.Spec.ClusterIP)
	}
	return out
}

// SetupWithManager registers the controller.
func (r *SynapseProxyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(context.Background(), &synapsev1alpha1.SynapseProxy{}, proxyAPIKeySecretIndex,
		func(obj client.Object) []string {
			p := obj.(*synapsev1alpha1.SynapseProxy)
			if p.Spec.Platform == nil || p.Spec.Platform.APIKeySecretRef == nil {
				return nil
			}
			return []string{p.Spec.Platform.APIKeySecretRef.Name}
		})
	if err != nil {
		return err
	}
	// The Secret an API key comes from is the user's, not the proxy's, so it
	// is found through the index rather than through ownership.
	readers := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, secret client.Object) []reconcile.Request {
		var proxies synapsev1alpha1.SynapseProxyList
		if err := r.List(ctx, &proxies, client.InNamespace(secret.GetNamespace()),
			client.MatchingFields{proxyAPIKeySecretIndex: secret.GetName()}); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list SynapseProxies reading a Secret")
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(proxies.Items))
		for i := range proxies.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&proxies.Items[i])})
		}
		return reqs
	})

	return ctrl.NewControllerManagedBy(mgr).
		// Named explicitly: the route controller is also `For` this kind.
		Named("synapse-proxy").
		// Only the spec: this controller's own status writes are not news.
		For(&synapsev1alpha1.SynapseProxy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		// The config Secrets, and the route controller's two outputs, whose
		// arrival is what a waiting proxy waits for.
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&corev1.Secret{}, readers).
		Complete(r)
}
