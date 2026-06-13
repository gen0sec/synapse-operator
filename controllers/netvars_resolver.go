// Package controllers — NetVarsResolverReconciler.
//
// Fills the Thalamus IDS `address_vars.HOME_NET` / `EXTERNAL_NET` in a
// synapse agent config ConfigMap so that inline blocking (`enforce_block`)
// only bans genuinely-external source IPs.
//
// Why this exists: the engine's block path bans the offending source IP
// (`ban_ip /32`) and does NOT exempt RFC1918 / cluster-internal addresses.
// With the default `HOME_NET: any`, a `drop` rule that matches internal or
// cluster traffic would ban an internal/k8s IP — a self-inflicted outage.
// Scoping HOME_NET to "everything internal" makes `EXTERNAL_NET` mean "the
// internet", so `$EXTERNAL_NET -> $HOME_NET` drop rules can only ban remote
// attackers.
//
// Modes:
//   - Auto (HOME_NET unset / empty / "any"): HOME_NET = each Node's IPs +
//     each Node's PodCIDR(s) + every LoadBalancer Service's ingress/external
//     IPs + RFC1918/CGNAT/ULA supernets; EXTERNAL_NET = "!$HOME_NET".
//   - Manual override: a non-empty, non-"any" HOME_NET already in the config,
//     or the `synapse.gen0sec.com/home-net` annotation, is used verbatim
//     (EXTERNAL_NET still derived as "!$HOME_NET" unless also overridden).
//
// Why LoadBalancer IPs are HOME, not EXTERNAL: a source-rewriting / proxying
// edge LB (e.g. Hetzner LB with proxy-protocol) makes the agent's packet-level
// XDP IDS see `src = LB VIP` for every inbound connection (the real client is
// only in the PROXYv2 header, recovered at the proxy L7). If the LB VIP were in
// EXTERNAL_NET, an EXTERNAL->HOME `drop` rule would ban the LB's /32 and
// black-hole all ingress. The LB VIP is NOT a Node ExternalIP — it's a separate
// address (Service .status.loadBalancer.ingress / .spec.externalIPs /
// .spec.loadBalancerIP), so it must be collected explicitly.
//
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
package controllers

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/yaml"

	ctrl "sigs.k8s.io/controller-runtime"
)

// NetVarsSourceLabel marks a synapse agent config ConfigMap whose IDS
// HOME_NET/EXTERNAL_NET the operator should manage.
const NetVarsSourceLabel = "synapse.gen0sec.com/resolve-netvars"

// HomeNetOverrideAnnotation, when set on the source ConfigMap, pins HOME_NET
// to its value verbatim (auto-discovery is skipped).
const HomeNetOverrideAnnotation = "synapse.gen0sec.com/home-net"

// NetVarsManagedAnnotation records how the operator is currently managing
// HOME_NET on this ConfigMap, so a value the operator auto-derived is not
// later mistaken for a user-pinned one. Without this marker, the first
// auto-fill writes a concrete (non-"any") HOME_NET, and the next reconcile
// would see that value and treat it as manual — freezing auto-discovery so
// Node/Service changes never re-derive. Values: "auto" | "manual".
const NetVarsManagedAnnotation = "synapse.gen0sec.com/netvars-managed"

const (
	managedAuto   = "auto"
	managedManual = "manual"
)

// SynapseConfigKey is the ConfigMap key holding the synapse YAML config.
const SynapseConfigKey = "config.yaml"

// internalSupernets are always treated as HOME (never a remote attacker):
// RFC1918, CGNAT (100.64/10), and IPv6 ULA/link-local. k8s pod/service
// CIDRs almost always fall inside these; Node PodCIDRs are added on top to
// cover non-RFC1918 cluster setups.
var internalSupernets = []string{
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"100.64.0.0/10",
	"fc00::/7",
	"fe80::/10",
}

// NetVarsResolverReconciler fills IDS HOME_NET/EXTERNAL_NET in labelled
// synapse agent config ConfigMaps.
type NetVarsResolverReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile fills HOME_NET/EXTERNAL_NET in the source ConfigMap's config.yaml.
func (r *NetVarsResolverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("source", req.NamespacedName)

	var cm corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !isNetVarsSource(&cm) {
		return ctrl.Result{}, nil
	}
	raw, ok := cm.Data[SynapseConfigKey]
	if !ok {
		logger.Info("source ConfigMap has no config.yaml key, skipping", "key", SynapseConfigKey)
		return ctrl.Result{}, nil
	}

	homeNet, manual, err := r.desiredHomeNet(ctx, &cm, raw)
	if err != nil {
		logger.Error(err, "failed to compute HOME_NET")
		return ctrl.Result{}, err
	}
	if homeNet == "" {
		// Auto mode found no internal ranges (shouldn't happen: supernets are
		// constant) — leave the config untouched rather than emit an empty var.
		return ctrl.Result{}, nil
	}

	newRaw, configChanged, err := setIdsAddressVars(raw, homeNet, "!$HOME_NET")
	if err != nil {
		logger.Error(err, "failed to rewrite config.yaml address_vars")
		return ctrl.Result{}, err
	}

	// Record the management mode so a future reconcile re-derives an auto value
	// (instead of mistaking it for a manual pin). See NetVarsManagedAnnotation.
	desiredManaged := managedAuto
	if manual {
		desiredManaged = managedManual
	}
	annotationChanged := cm.Annotations[NetVarsManagedAnnotation] != desiredManaged

	if !configChanged && !annotationChanged {
		return ctrl.Result{}, nil
	}

	cm.Data[SynapseConfigKey] = newRaw
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations[NetVarsManagedAnnotation] = desiredManaged
	if err := r.Update(ctx, &cm); err != nil {
		logger.Error(err, "failed to write HOME_NET/EXTERNAL_NET into ConfigMap")
		return ctrl.Result{}, err
	}
	logger.Info("filled IDS network vars", "manual", manual, "home_net", homeNet)
	return ctrl.Result{}, nil
}

// desiredHomeNet decides HOME_NET. Precedence:
//  1. the home-net override annotation -> manual (verbatim);
//  2. operator-managed auto (the netvars-managed=auto marker is present) ->
//     re-derive from the cluster, even though the current value is non-"any";
//  3. an empty / "any" value -> adopt auto;
//  4. a non-"any" value with no auto marker -> a genuine user pin -> manual.
//
// Cases 2 and 4 are what the managed marker disambiguates: both have a concrete
// HOME_NET in the config, but only case 4 was put there by a human.
func (r *NetVarsResolverReconciler) desiredHomeNet(ctx context.Context, cm *corev1.ConfigMap, raw string) (string, bool, error) {
	if ov, ok := cm.Annotations[HomeNetOverrideAnnotation]; ok && strings.TrimSpace(ov) != "" {
		return strings.TrimSpace(ov), true, nil
	}
	cur := currentHomeNet(raw)
	operatorManagedAuto := cm.Annotations[NetVarsManagedAnnotation] == managedAuto
	userPinned := cur != "" && !strings.EqualFold(cur, "any") && !operatorManagedAuto
	if userPinned {
		// Concrete value the operator never wrote — respect it verbatim.
		return cur, true, nil
	}
	auto, err := r.autoHomeNet(ctx)
	if err != nil {
		return "", false, err
	}
	return auto, false, nil
}

// autoHomeNet builds the HOME_NET list from cluster nodes + LoadBalancer
// Service VIPs + internal supernets.
func (r *NetVarsResolverReconciler) autoHomeNet(ctx context.Context) (string, error) {
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}

	set := make(map[string]struct{})
	for _, c := range internalSupernets {
		set[c] = struct{}{}
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		for _, addr := range n.Status.Addresses {
			if addr.Type != corev1.NodeInternalIP && addr.Type != corev1.NodeExternalIP {
				continue
			}
			if cidr := hostCIDR(addr.Address); cidr != "" {
				set[cidr] = struct{}{}
			}
		}
		// PodCIDR(s) cover the cluster pod network when it's not in RFC1918.
		if n.Spec.PodCIDR != "" {
			if _, _, err := net.ParseCIDR(n.Spec.PodCIDR); err == nil {
				set[n.Spec.PodCIDR] = struct{}{}
			}
		}
		for _, c := range n.Spec.PodCIDRs {
			if _, _, err := net.ParseCIDR(c); err == nil {
				set[c] = struct{}{}
			}
		}
	}

	// LoadBalancer Service VIPs: a source-rewriting edge LB makes the agent's
	// packet IDS see the LB IP as the source of all ingress, so it must be HOME
	// (never bannable). These are distinct from any Node ExternalIP.
	if err := r.addLoadBalancerIPs(ctx, set); err != nil {
		return "", err
	}

	cidrs := make([]string, 0, len(set))
	for c := range set {
		cidrs = append(cidrs, c)
	}
	sort.Strings(cidrs)
	return "[" + strings.Join(cidrs, ",") + "]", nil
}

// addLoadBalancerIPs adds every Service's LoadBalancer ingress IP(s),
// .spec.externalIPs, and .spec.loadBalancerIP to the HOME set as /32 or /128.
func (r *NetVarsResolverReconciler) addLoadBalancerIPs(ctx context.Context, set map[string]struct{}) error {
	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs); err != nil {
		return fmt.Errorf("list services: %w", err)
	}
	add := func(ip string) {
		if cidr := hostCIDR(strings.TrimSpace(ip)); cidr != "" {
			set[cidr] = struct{}{}
		}
	}
	for i := range svcs.Items {
		s := &svcs.Items[i]
		for _, ing := range s.Status.LoadBalancer.Ingress {
			add(ing.IP)
		}
		for _, ip := range s.Spec.ExternalIPs {
			add(ip)
		}
		if s.Spec.LoadBalancerIP != "" {
			add(s.Spec.LoadBalancerIP)
		}
	}
	return nil
}

// hostCIDR turns a bare IP into a single-host CIDR (/32 or /128).
func hostCIDR(ip string) string {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	if parsed.To4() != nil {
		return ip + "/32"
	}
	return ip + "/128"
}

// currentHomeNet extracts ids.address_vars.HOME_NET from the raw config, or "".
func currentHomeNet(raw string) string {
	var root map[string]any
	if yaml.Unmarshal([]byte(raw), &root) != nil {
		return ""
	}
	av := nestedMap(root, "ids", "address_vars")
	if av == nil {
		return ""
	}
	if v, ok := av["HOME_NET"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// setIdsAddressVars sets ids.address_vars.HOME_NET/EXTERNAL_NET, creating the
// nested maps if absent. Returns (newYAML, changed?).
func setIdsAddressVars(raw, homeNet, externalNet string) (string, bool, error) {
	var root map[string]any
	if err := yaml.Unmarshal([]byte(raw), &root); err != nil {
		return "", false, fmt.Errorf("unmarshal config.yaml: %w", err)
	}
	if root == nil {
		root = map[string]any{}
	}
	ids, _ := root["ids"].(map[string]any)
	if ids == nil {
		ids = map[string]any{}
		root["ids"] = ids
	}
	av, _ := ids["address_vars"].(map[string]any)
	if av == nil {
		av = map[string]any{}
		ids["address_vars"] = av
	}

	curHome, _ := av["HOME_NET"].(string)
	curExt, _ := av["EXTERNAL_NET"].(string)
	if curHome == homeNet && curExt == externalNet {
		return raw, false, nil
	}
	av["HOME_NET"] = homeNet
	av["EXTERNAL_NET"] = externalNet

	out, err := yaml.Marshal(root)
	if err != nil {
		return "", false, fmt.Errorf("marshal config.yaml: %w", err)
	}
	return string(out), true, nil
}

// nestedMap walks a map[string]any by keys, returning the final map or nil.
func nestedMap(root map[string]any, keys ...string) map[string]any {
	cur := root
	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

func isNetVarsSource(cm *corev1.ConfigMap) bool {
	return cm.Labels[NetVarsSourceLabel] == "true"
}

// SetupWithManager wires the controller: reconcile on labelled ConfigMap
// changes, and re-resolve every source when a Node changes (IPs/PodCIDRs).
func (r *NetVarsResolverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueAllSources := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, _ client.Object) []reconcile.Request {
			var list corev1.ConfigMapList
			if err := mgr.GetClient().List(ctx, &list,
				client.MatchingLabelsSelector{
					Selector: labels.SelectorFromSet(labels.Set{NetVarsSourceLabel: "true"}),
				}); err != nil {
				log.FromContext(ctx).Error(err, "failed to list netvars source ConfigMaps")
				return nil
			}
			out := make([]reconcile.Request, 0, len(list.Items))
			for i := range list.Items {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
					Namespace: list.Items[i].Namespace, Name: list.Items[i].Name,
				}})
			}
			return out
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		Named("synapse-netvars-resolver").
		For(&corev1.ConfigMap{}, builder.WithPredicates(netVarsSourcePredicate())).
		Watches(&corev1.Node{}, enqueueAllSources).
		Watches(&corev1.Service{}, enqueueAllSources).
		Complete(r)
}

func netVarsSourcePredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		cm, ok := obj.(*corev1.ConfigMap)
		return ok && isNetVarsSource(cm)
	})
}

// LogStartup mirrors the other reconcilers' startup banner.
func (r *NetVarsResolverReconciler) LogStartup(l logr.Logger) {
	l.Info("starting NetVarsResolverReconciler",
		"sourceLabel", NetVarsSourceLabel,
		"overrideAnnotation", HomeNetOverrideAnnotation,
		"configKey", SynapseConfigKey)
}
