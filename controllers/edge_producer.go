package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EdgeProducerReconciler compiles cluster NetworkPolicy into a declared-edge
// allow-list (workload->workload:port) + the set of policy-governed destination
// workloads, and uploads it to the download-api so agents can evaluate the
// `edge.*` microsegmentation fields. Like the identity producer, the operator
// is the only component with the cluster state (NetworkPolicies + pod labels).
//
// Scope (v1, alert-first): Ingress rules only; podSelector/namespaceSelector
// matchLabels+matchExpressions via the standard selector; an empty `from` =>
// allow-from-all (`*/*`); numeric ports (named ports and ipBlock peers widen to
// any-port `*` / are skipped, conservatively avoiding false violations). Egress
// rules are a follow-up.
type EdgeProducerReconciler struct {
	client.Client
	Log        logr.Logger
	UploadURL  string
	APIKey     string
	Interval   time.Duration
	HTTPClient *http.Client

	lastVersion string
}

func (r *EdgeProducerReconciler) Start(ctx context.Context) error {
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

func (r *EdgeProducerReconciler) LogStartup(log logr.Logger) {
	log.Info("EdgeProducer enabled: compiling NetworkPolicy -> declared-edge allow-list and uploading to download-api",
		"uploadURL", r.UploadURL, "interval", r.Interval.String())
}

func (r *EdgeProducerReconciler) buildAndUpload(ctx context.Context) {
	var nps networkingv1.NetworkPolicyList
	if err := r.List(ctx, &nps); err != nil {
		r.Log.Error(err, "edge-producer: list networkpolicies failed")
		return
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "edge-producer: list pods failed")
		return
	}
	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList); err != nil {
		r.Log.Error(err, "edge-producer: list namespaces failed")
		return
	}

	doc, version, edges := buildEdgeDoc(nps.Items, pods.Items, nsList.Items)
	if version == r.lastVersion {
		return
	}
	if err := r.upload(ctx, []byte(doc), version); err != nil {
		r.Log.Error(err, "edge-producer: upload failed", "version", version)
		return
	}
	r.lastVersion = version
	r.Log.Info("edge-producer: uploaded edge-set", "version", version, "edges", edges, "policies", len(nps.Items))
}

// buildEdgeDoc returns the serialized allow-list, a content-hash version, and
// the edge count. The doc format matches the agent's edge_set parser:
//
//	E <srcns>/<srcwl> > <dstns>/<dstwl> : <port>
//	G <dstns>/<dstwl>
func buildEdgeDoc(nps []networkingv1.NetworkPolicy, pods []corev1.Pod, namespaces []corev1.Namespace) (string, string, int) {
	nsLabels := map[string]labels.Set{}
	for i := range namespaces {
		nsLabels[namespaces[i].Name] = labels.Set(namespaces[i].Labels)
	}

	edges := map[string]struct{}{}
	governedDst := map[string]struct{}{}
	governedSrc := map[string]struct{}{}
	addEdge := func(src, dst string, ports []string) {
		for _, port := range ports {
			edges["E "+src+" > "+dst+" : "+port] = struct{}{}
		}
	}

	for i := range nps {
		np := &nps[i]
		sel, err := metav1.LabelSelectorAsSelector(&np.Spec.PodSelector)
		if err != nil {
			continue
		}
		// The policy's pods are the DESTINATION for its ingress rules and the
		// SOURCE for its egress rules.
		selPods := podsMatching(pods, np.Namespace, sel)
		selWls := workloadSet(selPods)
		if len(selWls) == 0 {
			continue
		}

		if policyHasIngress(np) {
			for wl := range selWls {
				governedDst[np.Namespace+"/"+wl] = struct{}{}
			}
			for ri := range np.Spec.Ingress {
				rule := &np.Spec.Ingress[ri]
				// Ingress named ports resolve against the destination (selected) pods.
				ports := portStrings(rule.Ports, selPods)
				var srcRefs []string
				if len(rule.From) == 0 {
					srcRefs = []string{"*/*"} // empty from = allow-from-all
				} else {
					for pi := range rule.From {
						srcRefs = append(srcRefs, peerWorkloadRefs(&rule.From[pi], np.Namespace, pods, nsLabels)...)
					}
				}
				for dstWl := range selWls {
					dst := np.Namespace + "/" + dstWl
					for _, src := range srcRefs {
						addEdge(src, dst, ports)
					}
				}
			}
		}

		if policyHasEgress(np) {
			for wl := range selWls {
				governedSrc[np.Namespace+"/"+wl] = struct{}{}
			}
			for ri := range np.Spec.Egress {
				rule := &np.Spec.Egress[ri]
				var dstRefs []string
				var toPods []corev1.Pod
				if len(rule.To) == 0 {
					dstRefs = []string{"*/*"} // empty to = allow-to-all
				} else {
					for pi := range rule.To {
						toPods = append(toPods, peerPods(&rule.To[pi], np.Namespace, pods, nsLabels)...)
						dstRefs = append(dstRefs, peerWorkloadRefs(&rule.To[pi], np.Namespace, pods, nsLabels)...)
					}
				}
				// Egress named ports resolve against the `to` peer (destination)
				// pods; numeric ports stay exact, unresolved named ports widen.
				ports := portStrings(rule.Ports, toPods)
				for srcWl := range selWls {
					src := np.Namespace + "/" + srcWl
					for _, dst := range dstRefs {
						addEdge(src, dst, ports)
					}
				}
			}
		}
	}

	lines := make([]string, 0, len(edges)+len(governedDst)+len(governedSrc))
	for e := range edges {
		lines = append(lines, e)
	}
	for g := range governedDst {
		lines = append(lines, "G "+g)
	}
	for s := range governedSrc {
		lines = append(lines, "S "+s)
	}
	sort.Strings(lines)

	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	version := "edges-" + hex.EncodeToString(h[:])[:16]

	var b strings.Builder
	b.WriteString("# NetworkPolicy-derived declared-edge allow-list\n")
	for _, l := range lines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	return b.String(), version, len(edges)
}

func policyHasEgress(np *networkingv1.NetworkPolicy) bool {
	// Egress applies only when explicitly listed in policyTypes (unlike Ingress,
	// which is the default). The presence of an Egress rule also implies it.
	for _, pt := range np.Spec.PolicyTypes {
		if pt == networkingv1.PolicyTypeEgress {
			return true
		}
	}
	return len(np.Spec.Egress) > 0
}

func policyHasIngress(np *networkingv1.NetworkPolicy) bool {
	if len(np.Spec.PolicyTypes) == 0 {
		return true // default applies to Ingress
	}
	for _, pt := range np.Spec.PolicyTypes {
		if pt == networkingv1.PolicyTypeIngress {
			return true
		}
	}
	return false
}

// peerPods resolves one NetworkPolicy peer (`from`/`to`) to the matching pods.
// ipBlock peers are skipped (not workload-addressable).
func peerPods(peer *networkingv1.NetworkPolicyPeer, policyNs string, pods []corev1.Pod, nsLabels map[string]labels.Set) []corev1.Pod {
	if peer.IPBlock != nil {
		return nil
	}
	// Namespace scope: nil namespaceSelector -> the policy's namespace; set ->
	// every namespace whose labels match.
	var namespaces []string
	if peer.NamespaceSelector == nil {
		namespaces = []string{policyNs}
	} else {
		nsSel, err := metav1.LabelSelectorAsSelector(peer.NamespaceSelector)
		if err != nil {
			return nil
		}
		for ns, lbls := range nsLabels {
			if nsSel.Matches(lbls) {
				namespaces = append(namespaces, ns)
			}
		}
	}

	podSel := labels.Everything()
	if peer.PodSelector != nil {
		s, err := metav1.LabelSelectorAsSelector(peer.PodSelector)
		if err != nil {
			return nil
		}
		podSel = s
	}

	var out []corev1.Pod
	for _, ns := range namespaces {
		out = append(out, podsMatching(pods, ns, podSel)...)
	}
	return out
}

// peerWorkloadRefs resolves one peer to distinct `ns/workload` refs.
func peerWorkloadRefs(peer *networkingv1.NetworkPolicyPeer, policyNs string, pods []corev1.Pod, nsLabels map[string]labels.Set) []string {
	seen := map[string]struct{}{}
	var refs []string
	for _, p := range peerPods(peer, policyNs, pods, nsLabels) {
		wl, _ := workloadIdentity(&p)
		if wl == "" {
			continue
		}
		ref := p.Namespace + "/" + wl
		if _, ok := seen[ref]; !ok {
			seen[ref] = struct{}{}
			refs = append(refs, ref)
		}
	}
	return refs
}

// podsMatching returns the pods in ns whose labels match sel (host-network
// excluded — those share the node IP, not a workload identity).
func podsMatching(pods []corev1.Pod, ns string, sel labels.Selector) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Namespace != ns || p.Spec.HostNetwork {
			continue
		}
		if sel.Matches(labels.Set(p.Labels)) {
			out = append(out, *p)
		}
	}
	return out
}

// workloadSet maps a pod list to the set of workload names it covers.
func workloadSet(pods []corev1.Pod) map[string]struct{} {
	out := map[string]struct{}{}
	for i := range pods {
		if wl, _ := workloadIdentity(&pods[i]); wl != "" {
			out[wl] = struct{}{}
		}
	}
	return out
}

// workloadsMatching returns the set of workload names in ns whose pods match sel.
func workloadsMatching(pods []corev1.Pod, ns string, sel labels.Selector) map[string]struct{} {
	return workloadSet(podsMatching(pods, ns, sel))
}

// portStrings renders the rule's ports. Empty -> any (`*`); a numeric port ->
// its number; a NAMED port -> the containerPort number(s) it resolves to on the
// destination pods (so the edge is constrained to the real port, not widened);
// a named port that resolves to nothing -> any (`*`), conservatively so a flow
// on the real port is never a false violation.
func portStrings(ports []networkingv1.NetworkPolicyPort, dstPods []corev1.Pod) []string {
	if len(ports) == 0 {
		return []string{"*"}
	}
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	for i := range ports {
		p := &ports[i]
		if p.Port == nil {
			add("*")
			continue
		}
		if p.Port.Type == intstr.Int {
			add(fmt.Sprintf("%d", p.Port.IntValue()))
			continue
		}
		nums := resolveNamedPort(dstPods, p.Port.StrVal)
		if len(nums) == 0 {
			add("*") // unresolved named port -> conservative any
			continue
		}
		for _, n := range nums {
			add(fmt.Sprintf("%d", n))
		}
	}
	return out
}

// resolveNamedPort returns the distinct containerPort numbers that carry `name`
// across the destination pods (a NetworkPolicy named port matches the target
// pod's named containerPort).
func resolveNamedPort(pods []corev1.Pod, name string) []int32 {
	seen := map[int32]struct{}{}
	var out []int32
	for i := range pods {
		for ci := range pods[i].Spec.Containers {
			for _, cp := range pods[i].Spec.Containers[ci].Ports {
				if cp.Name == name {
					if _, ok := seen[cp.ContainerPort]; !ok {
						seen[cp.ContainerPort] = struct{}{}
						out = append(out, cp.ContainerPort)
					}
				}
			}
		}
	}
	return out
}

func (r *EdgeProducerReconciler) upload(ctx context.Context, body []byte, version string) error {
	base := strings.TrimSuffix(r.UploadURL, "/")
	url := fmt.Sprintf("%s/policy-edges/upload?version=%s", base, version)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
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
