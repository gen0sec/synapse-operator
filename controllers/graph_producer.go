package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GraphProducerReconciler uploads the east-west service graph (workload
// identities + NetworkPolicy declared edges) to service-graph-api over REST,
// which writes it into the Apache AGE service_graph. The operator stays
// REST-only — like the Identity and Edge producers it never connects to a
// database; service-graph-api is the sole graph writer.
//
// Runs as a leader-elected manager.Runnable (a ticking full-snapshot upload),
// so a multi-replica operator uploads from one place only.
type GraphProducerReconciler struct {
	client.Client
	Log logr.Logger
	// UploadURL is the service-graph-api base, e.g.
	// http://service-graph-api.services.svc.cluster.local:9999
	UploadURL  string
	APIKey     string
	Interval   time.Duration
	HTTPClient *http.Client

	lastVersion string
}

// graphPayload mirrors service-graph-api's GraphPayload (the upload contract).
type graphPayload struct {
	Version   string          `json:"version"`
	Workloads []graphWorkload `json:"workloads"`
	Edges     []declaredEdge  `json:"edges"`
}

// graphWorkload is a vertex: one workload identity, keyed by "namespace/name".
// Role/InternetExposed/ControlPlane are classification derived from pure
// Kubernetes state (Services/Ingress + control-plane heuristics) — they drive
// the visualizer's node colours/badges.
type graphWorkload struct {
	Ref             string   `json:"ref"`
	Namespace       string   `json:"namespace"`
	Workload        string   `json:"workload"`
	App             string   `json:"app"`
	Role            string   `json:"role,omitempty"`
	InternetExposed bool     `json:"internet_exposed,omitempty"`
	ControlPlane    bool     `json:"control_plane,omitempty"`
	// IPs are this workload's pod IPs. The agent owns the observed layer but has
	// no k8s identity (it must work on-prem), so it publishes observed edges by
	// raw IP; service-graph-api resolves those IPs to this ref through an index
	// built from these IPs. The operator is the only producer that knows the
	// IP->workload mapping, so it must ship it here.
	IPs []string `json:"ips,omitempty"`
}

// declaredEdge is a directed src->dst edge between workload refs.
type declaredEdge struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// Start implements manager.Runnable: an initial upload then a periodic loop.
func (r *GraphProducerReconciler) Start(ctx context.Context) error {
	if r.HTTPClient == nil {
		r.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	if r.Interval <= 0 {
		r.Interval = 60 * time.Second
	}
	r.buildAndUpload(ctx)
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.buildAndUpload(ctx)
		}
	}
}

// LogStartup announces the producer configuration at boot.
func (r *GraphProducerReconciler) LogStartup(log logr.Logger) {
	log.Info("GraphProducer enabled: uploading workload identities + declared edges to service-graph-api",
		"uploadURL", r.UploadURL, "interval", r.Interval.String())
}

func (r *GraphProducerReconciler) buildAndUpload(ctx context.Context) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "graph-producer: list pods failed")
		return
	}
	var nps networkingv1.NetworkPolicyList
	if err := r.List(ctx, &nps); err != nil {
		r.Log.Error(err, "graph-producer: list networkpolicies failed")
		return
	}
	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList); err != nil {
		r.Log.Error(err, "graph-producer: list namespaces failed")
		return
	}
	// Services + Ingresses drive internet-exposed classification. A failure
	// here is non-fatal — we still upload identities + declared edges.
	var svcList corev1.ServiceList
	if err := r.List(ctx, &svcList); err != nil {
		r.Log.Error(err, "graph-producer: list services failed (classification degraded)")
	}
	var ingList networkingv1.IngressList
	if err := r.List(ctx, &ingList); err != nil {
		r.Log.Error(err, "graph-producer: list ingresses failed (classification degraded)")
	}

	workloads := collectGraphWorkloads(pods.Items)
	if len(workloads) == 0 {
		return
	}
	classifyWorkloads(workloads, pods.Items, svcList.Items, ingList.Items)
	edges := collectDeclaredEdges(nps.Items, pods.Items, nsList.Items)

	version := graphVersion(workloads, edges)
	if version == r.lastVersion {
		return // graph unchanged; skip the upload
	}
	if err := r.upload(ctx, graphPayload{Version: version, Workloads: workloads, Edges: edges}); err != nil {
		r.Log.Error(err, "graph-producer: upload failed", "version", version)
		return
	}
	r.lastVersion = version
	r.Log.Info("graph-producer: uploaded service graph",
		"version", version, "workloads", len(workloads), "edges", len(edges))
}

// upload PUTs the full graph snapshot to service-graph-api (mirrors the
// Identity/Edge producers' REST upload).
func (r *GraphProducerReconciler) upload(ctx context.Context, p graphPayload) error {
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	url := strings.TrimSuffix(r.UploadURL, "/") + "/v1/service-graph"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.APIKey)
	}
	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("upload returned status %d", resp.StatusCode)
	}
	return nil
}

// collectGraphWorkloads dedups the per-IP identity entries down to one vertex
// per workload (namespace/name), reusing the identity producer's resolution.
func collectGraphWorkloads(pods []corev1.Pod) []graphWorkload {
	seen := map[string]graphWorkload{}
	// Aggregate every IP that resolves to a ref so the observed layer can map
	// traffic back to this workload. dedup keeps the IP list stable/idempotent.
	ips := map[string]map[string]struct{}{}
	for _, e := range resolveIdentityEntries(pods) {
		ref := e.namespace + "/" + e.workload
		if _, ok := seen[ref]; !ok {
			seen[ref] = graphWorkload{Ref: ref, Namespace: e.namespace, Workload: e.workload, App: e.app}
			ips[ref] = map[string]struct{}{}
		}
		if e.ip != "" {
			ips[ref][e.ip] = struct{}{}
		}
	}
	out := make([]graphWorkload, 0, len(seen))
	for ref, w := range seen {
		w.IPs = make([]string, 0, len(ips[ref]))
		for ip := range ips[ref] {
			w.IPs = append(w.IPs, ip)
		}
		sort.Strings(w.IPs)
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out
}

// classifyWorkloads sets Role / InternetExposed / ControlPlane on each workload
// from pure Kubernetes state: control-plane via namespace/component heuristics,
// internet-exposed via LoadBalancer/NodePort Services or Ingress-backed Services
// whose selector matches the workload's pods. Mutates workloads in place.
func classifyWorkloads(workloads []graphWorkload, pods []corev1.Pod, services []corev1.Service, ingresses []networkingv1.Ingress) {
	// Services referenced by an Ingress backend expose their (usually ClusterIP)
	// Service to the internet.
	ingressBacked := map[string]bool{}
	for i := range ingresses {
		ing := &ingresses[i]
		if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
			ingressBacked[ing.Namespace+"/"+ing.Spec.DefaultBackend.Service.Name] = true
		}
		for _, rule := range ing.Spec.Rules {
			if rule.HTTP == nil {
				continue
			}
			for _, p := range rule.HTTP.Paths {
				if p.Backend.Service != nil {
					ingressBacked[ing.Namespace+"/"+p.Backend.Service.Name] = true
				}
			}
		}
	}

	// Exposing selectors: LB/NodePort services + any Ingress-backed service.
	// Empty selectors (headless/external) select nothing and are skipped.
	type nsSelector struct {
		namespace string
		selector  map[string]string
	}
	var exposing []nsSelector
	for i := range services {
		s := &services[i]
		if len(s.Spec.Selector) == 0 {
			continue
		}
		if s.Spec.Type == corev1.ServiceTypeLoadBalancer ||
			s.Spec.Type == corev1.ServiceTypeNodePort ||
			ingressBacked[s.Namespace+"/"+s.Name] {
			exposing = append(exposing, nsSelector{namespace: s.Namespace, selector: s.Spec.Selector})
		}
	}

	internetExposed := map[string]bool{}
	for i := range pods {
		p := &pods[i]
		workload, _ := workloadIdentity(p)
		if workload == "" {
			continue
		}
		ref := p.Namespace + "/" + workload
		for _, sel := range exposing {
			if sel.namespace == p.Namespace && selectorMatches(sel.selector, p.Labels) {
				internetExposed[ref] = true
				break
			}
		}
	}

	for i := range workloads {
		w := &workloads[i]
		w.ControlPlane = isControlPlane(w.Workload)
		w.InternetExposed = internetExposed[w.Ref]
		switch {
		case w.ControlPlane:
			w.Role = "control-plane"
		case w.InternetExposed:
			w.Role = "internet-exposed"
		default:
			w.Role = "internal"
		}
	}
}

// selectorMatches reports whether every key/value in a non-empty selector is
// present in labels (label-selector subset match).
func selectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// controlPlaneComponents are short workload names matched EXACTLY — substring
// matching would wrongly catch add-ons (e.g. ccm-proportional-autoscaler).
var controlPlaneComponents = map[string]bool{"etcd": true, "ccm": true}

// isControlPlane marks the true Kubernetes control plane — API server, etcd,
// controller-manager, scheduler, and the cloud controller — by component name.
// Add-ons that merely live in kube-system (cert-manager, CNI, autoscalers,
// DNS, kube-proxy) are NOT control-plane.
func isControlPlane(workload string) bool {
	w := strings.ToLower(workload)
	for _, c := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler", "cloud-controller-manager"} {
		if strings.Contains(w, c) {
			return true
		}
	}
	return controlPlaneComponents[w]
}

// collectDeclaredEdges derives deduped src->dst workload edges from
// NetworkPolicy, reusing walkPolicyEdges. Wildcard (allow-all) peers and
// self-edges are dropped — they aren't concrete workload-to-workload edges.
func collectDeclaredEdges(nps []networkingv1.NetworkPolicy, pods []corev1.Pod, namespaces []corev1.Namespace) []declaredEdge {
	set := map[declaredEdge]struct{}{}
	walkPolicyEdges(nps, pods, namespaces,
		func(src, dst string, _ []string) {
			if src == "*/*" || dst == "*/*" || src == dst {
				return
			}
			set[declaredEdge{Src: src, Dst: dst}] = struct{}{}
		},
		func(string) {}, func(string) {},
	)
	out := make([]declaredEdge, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Src != out[j].Src {
			return out[i].Src < out[j].Src
		}
		return out[i].Dst < out[j].Dst
	})
	return out
}

// graphVersion is a content hash of the desired graph, so an unchanged cluster
// skips the upload (mirrors the Identity/Edge producers' lastVersion gate).
func graphVersion(workloads []graphWorkload, edges []declaredEdge) string {
	h := sha256.New()
	for _, w := range workloads {
		fmt.Fprintf(h, "W %s %s %s %t %t %v\n", w.Ref, w.App, w.Role, w.InternetExposed, w.ControlPlane, w.IPs)
	}
	for _, e := range edges {
		fmt.Fprintf(h, "E %s>%s\n", e.Src, e.Dst)
	}
	return "graph-" + hex.EncodeToString(h.Sum(nil))[:16]
}
