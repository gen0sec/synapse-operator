package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IdentityProducerReconciler builds a workload-identity MMDB (pod IP ->
// {workload, namespace, app}) from the cluster's Pods and uploads it to the
// download-api so agents can pull it for east-west / lateral-movement
// detection. The operator is the only component with live pod->workload state,
// so it is the producer (core/platform is multi-tenant SaaS with no direct
// cluster access).
//
// It is event-driven off the Pod informer: on any pod add/update/delete it emits
// a small coalesced *delta* (changed IP->identity upserts + removed IPs) so
// agents stay fresh in ~sub-second without rebuilding/redistributing the whole
// table. The full MMDB is still uploaded, but only as an infrequent cold-start /
// resync *baseline* (Interval, e.g. 5m) — it is no longer the per-change unit of
// work. Deltas carry an (epoch, seq): epoch is the operator-start nonce, seq is
// monotonic; an agent that sees a gap re-pulls the baseline.
type IdentityProducerReconciler struct {
	client.Client
	Cache         cache.Cache // Pod informer source for event-driven deltas
	Log           logr.Logger
	UploadURL     string // download-api base, e.g. https://api.gen0sec.com/v1
	APIKey        string
	Interval      time.Duration // full-MMDB baseline interval (cold-start/resync)
	DeltaDebounce time.Duration // coalescing window for deltas
	HTTPClient    *http.Client

	epoch uint64                   // operator-start nonce (agents resync on change)
	seq   uint64                   // monotonic delta sequence within the epoch
	index map[string]identityEntry // last-emitted IP -> identity (delta diff base)

	lastVersion string
}

// Start implements manager.Runnable: an initial baseline, then an event-driven
// delta loop with a periodic baseline refresh.
func (r *IdentityProducerReconciler) Start(ctx context.Context) error {
	if r.HTTPClient == nil {
		r.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	if r.Interval <= 0 {
		r.Interval = 5 * time.Minute
	}
	if r.DeltaDebounce <= 0 {
		r.DeltaDebounce = 200 * time.Millisecond
	}
	r.epoch = uint64(time.Now().UnixNano())
	r.index = map[string]identityEntry{}

	// Initial full baseline (also seeds r.index so the first delta diffs against it).
	r.buildAndUpload(ctx)

	// Any Pod change signals a coalesced delta flush.
	dirty := make(chan struct{}, 1)
	signal := func() {
		select {
		case dirty <- struct{}{}:
		default:
		}
	}
	if r.Cache != nil {
		inf, err := r.Cache.GetInformer(ctx, &corev1.Pod{})
		if err != nil {
			return fmt.Errorf("identity-producer: get pod informer: %w", err)
		}
		if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc:    func(interface{}) { signal() },
			UpdateFunc: func(_, _ interface{}) { signal() },
			DeleteFunc: func(interface{}) { signal() },
		}); err != nil {
			return fmt.Errorf("identity-producer: add pod event handler: %w", err)
		}
	}

	baseline := time.NewTicker(r.Interval)
	defer baseline.Stop()
	var debounce <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-baseline.C:
			r.buildAndUpload(ctx) // periodic cold-start/resync baseline
		case <-dirty:
			if debounce == nil {
				debounce = time.After(r.DeltaDebounce)
			}
		case <-debounce:
			debounce = nil
			r.flushDelta(ctx)
		}
	}
}

// LogStartup announces the producer configuration at boot.
func (r *IdentityProducerReconciler) LogStartup(log logr.Logger) {
	log.Info("IdentityProducer enabled: event-driven identity deltas + periodic MMDB baseline to download-api",
		"uploadURL", r.UploadURL, "baselineInterval", r.Interval.String())
}

// identityEntry is one resolved IP -> identity row.
type identityEntry struct {
	ip        string
	workload  string
	namespace string
	app       string
	labels    map[string]string
}

// filterLabels drops the k8s-internal churn labels (pod-template-hash and
// friends) that change on every rollout and would thrash the identity delta
// stream, keeping only user-defined labels for identity.k8s.*_label rules.
func filterLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch k {
		case "pod-template-hash",
			"controller-revision-hash",
			"pod-template-generation",
			"statefulset.kubernetes.io/pod-name",
			"apps.kubernetes.io/pod-index",
			"controller-uid",
			"batch.kubernetes.io/controller-uid",
			"batch.kubernetes.io/job-name",
			"job-name":
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// equalTo compares two identity entries (the labels map makes the struct
// non-comparable with ==), so the delta diff can detect real changes.
func (e identityEntry) equalTo(o identityEntry) bool {
	if e.ip != o.ip || e.workload != o.workload || e.namespace != o.namespace || e.app != o.app {
		return false
	}
	if len(e.labels) != len(o.labels) {
		return false
	}
	for k, v := range e.labels {
		if o.labels[k] != v {
			return false
		}
	}
	return true
}

func (r *IdentityProducerReconciler) buildAndUpload(ctx context.Context) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "identity-producer: list pods failed")
		return
	}
	entries := resolveIdentityEntries(pods.Items)
	// Reseed the delta diff-base from the baseline snapshot; deltas emitted after
	// this baseline diff against the current full state (self-heals any drift).
	r.index = indexEntries(entries)
	if len(entries) == 0 {
		return
	}
	version := identityVersion(entries)
	if version == r.lastVersion {
		return // cluster identity unchanged; skip redundant upload
	}
	mmdb, err := buildIdentityMMDB(entries)
	if err != nil {
		r.Log.Error(err, "identity-producer: build mmdb failed")
		return
	}
	if err := r.upload(ctx, mmdb, version); err != nil {
		r.Log.Error(err, "identity-producer: upload failed", "version", version)
		return
	}
	r.lastVersion = version
	r.Log.Info("identity-producer: uploaded identity mmdb",
		"version", version, "entries", len(entries), "bytes", len(mmdb))
}

// IdentityEntry is one wire-format IP -> identity row in a delta.
type IdentityEntry struct {
	IP        string            `json:"ip"`
	Workload  string            `json:"workload"`
	Namespace string            `json:"namespace"`
	App       string            `json:"app"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// IdentityDelta is an incremental change to the IP->identity table: upserted rows
// plus removed IPs, tagged with (epoch, seq) for agent-side gap detection.
type IdentityDelta struct {
	Epoch   uint64          `json:"epoch"`
	Seq     uint64          `json:"seq"`
	Upserts []IdentityEntry `json:"upserts,omitempty"`
	Removes []string        `json:"removes,omitempty"`
}

// indexEntries keys the resolved entries by IP for diffing.
func indexEntries(entries []identityEntry) map[string]identityEntry {
	m := make(map[string]identityEntry, len(entries))
	for _, e := range entries {
		m[e.ip] = e
	}
	return m
}

// flushDelta re-resolves the current identity table from the informer cache,
// diffs it against the last-emitted state, and POSTs a delta of just the
// changes. r.index/r.seq advance only on a successful POST, so a failed relay
// re-emits the same diff (with the same next seq) on the following flush.
func (r *IdentityProducerReconciler) flushDelta(ctx context.Context) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "identity-producer: list pods failed (delta)")
		return
	}
	newIndex := indexEntries(resolveIdentityEntries(pods.Items))

	var upserts []IdentityEntry
	for ip, e := range newIndex {
		if prev, ok := r.index[ip]; !ok || !prev.equalTo(e) {
			upserts = append(upserts, IdentityEntry{IP: e.ip, Workload: e.workload, Namespace: e.namespace, App: e.app, Labels: e.labels})
		}
	}
	var removes []string
	for ip := range r.index {
		if _, ok := newIndex[ip]; !ok {
			removes = append(removes, ip)
		}
	}
	if len(upserts) == 0 && len(removes) == 0 {
		return
	}

	delta := IdentityDelta{Epoch: r.epoch, Seq: r.seq + 1, Upserts: upserts, Removes: removes}
	if err := r.postDelta(ctx, delta); err != nil {
		r.Log.Error(err, "identity-producer: post delta failed", "seq", delta.Seq)
		return
	}
	r.seq = delta.Seq
	r.index = newIndex
	r.Log.Info("identity-producer: delta emitted",
		"epoch", r.epoch, "seq", r.seq, "upserts", len(upserts), "removes", len(removes))
}

// postDelta relays a delta to the download-api, which fans it to agents over SSE.
func (r *IdentityProducerReconciler) postDelta(ctx context.Context, d IdentityDelta) error {
	base := strings.TrimSuffix(r.UploadURL, "/")
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/identity/delta", bytes.NewReader(body))
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
		return fmt.Errorf("delta returned status %d", resp.StatusCode)
	}
	return nil
}

// resolveIdentityEntries maps each pod's IP(s) to a workload identity.
//
// Entries are keyed by IP so a recycled IP resolves to a single owner. Only
// serving pods claim an IP, and on conflict the most-recently-started pod wins
// (last-writer-by-startTime). This is the pod-restart / IP-recycle guard: k8s
// hands a terminating pod's IP to a new pod, and a dying pod that still reports
// that IP must not shadow the live owner (observed in prod: a demo pod's IP
// resolved to a terminating CronJob pod).
func resolveIdentityEntries(pods []corev1.Pod) []identityEntry {
	byIP := map[string]identityEntry{}
	start := map[string]time.Time{}
	for i := range pods {
		p := &pods[i]
		// Host-network pods share the node IP; their identity would be the
		// node, not a workload — skip to avoid mislabeling node traffic.
		if p.Spec.HostNetwork {
			continue
		}
		if !podServing(p) {
			continue
		}
		workload, app := workloadIdentity(p)
		if workload == "" {
			continue
		}
		var st time.Time
		if p.Status.StartTime != nil {
			st = p.Status.StartTime.Time
		}
		for _, ip := range podIPs(p) {
			// A pod that started at the same time or later owns the IP; an
			// equal/earlier pod (e.g. still-terminating) does not overwrite it.
			if prev, ok := start[ip]; ok && !st.After(prev) {
				continue
			}
			byIP[ip] = identityEntry{
				ip:        ip,
				workload:  workload,
				namespace: p.Namespace,
				app:       app,
				labels:    filterLabels(p.Labels),
			}
			start[ip] = st
		}
	}
	out := make([]identityEntry, 0, len(byIP))
	for _, e := range byIP {
		out = append(out, e)
	}
	return out
}

// podServing reports whether a pod should own its IP for identity resolution: it
// must be Running, Ready, and not terminating. A Terminating/Pending/Completed
// pod that still reports an IP must not win that IP over the live pod that
// recycled it — this closes the IP-recycle race.
func podServing(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podIPs(p *corev1.Pod) []string {
	seen := map[string]struct{}{}
	var ips []string
	add := func(ip string) {
		ip = strings.TrimSpace(ip)
		if ip == "" {
			return
		}
		if _, ok := seen[ip]; ok {
			return
		}
		seen[ip] = struct{}{}
		ips = append(ips, ip)
	}
	add(p.Status.PodIP)
	for _, pip := range p.Status.PodIPs {
		add(pip.IP)
	}
	return ips
}

// workloadIdentity resolves a stable workload name + app for a pod. It prefers
// the owning controller name (stripping the ReplicaSet hash a Deployment adds),
// and reads the app from the well-known labels.
func workloadIdentity(p *corev1.Pod) (workload, app string) {
	app = p.Labels["app.kubernetes.io/name"]
	if app == "" {
		app = p.Labels["app"]
	}
	for _, o := range p.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			name := o.Name
			if o.Kind == "ReplicaSet" {
				// Deployment replicas are interchangeable — aggregate them under
				// the deployment name (strip the pod-template hash).
				name = trimReplicaSetHash(name)
			} else if isOrdinalPod(p.Name, o.Name) {
				// Ordinal-stable controllers (StatefulSet, Strimzi's StrimziPodSet,
				// …) name pods "<controller>-<ordinal>" and the replicas have stable
				// identities that talk to EACH OTHER (DB streaming replication,
				// inter-broker traffic). Key them per-pod (core-0/core-1/core-2) so
				// that traffic is real edges, not a self-loop on the controller name.
				name = p.Name
			}
			workload = name
			break
		}
	}
	if workload == "" {
		// No controller (bare pod): use the app label or the pod name.
		if app != "" {
			workload = app
		} else {
			workload = p.Name
		}
	}
	if app == "" {
		app = workload
	}
	return workload, app
}

// isOrdinalPod reports whether podName is "<controllerName>-<ordinal>" (a stable
// ordinal pod, as StatefulSets and StrimziPodSets create). Such pods are keyed
// per-pod so their mutual traffic isn't collapsed into a self-loop. A Deployment
// pod ("<rs>-<random>") fails this (the suffix isn't all digits) and aggregates.
func isOrdinalPod(podName, controllerName string) bool {
	suffix := strings.TrimPrefix(podName, controllerName+"-")
	if suffix == podName || suffix == "" {
		return false
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// trimReplicaSetHash removes the trailing "-<hash>" a Deployment's ReplicaSet
// name carries (e.g. "frontend-7d9f8c6b5" -> "frontend"). Only trims a segment
// that actually looks like a pod-template hash, never a meaningful name part.
func trimReplicaSetHash(name string) string {
	i := strings.LastIndex(name, "-")
	if i <= 0 {
		return name
	}
	suffix := name[i+1:]
	if len(suffix) < 5 || len(suffix) > 10 {
		return name
	}
	for _, c := range suffix {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return name
		}
	}
	return name[:i]
}

// identityVersion is a deterministic content hash of the resolved entry set, so
// an unchanged cluster produces an unchanged version (no redundant uploads).
func identityVersion(entries []identityEntry) string {
	rows := make([]string, len(entries))
	for i, e := range entries {
		rows[i] = e.ip + "|" + e.namespace + "|" + e.workload + "|" + e.app
	}
	sort.Strings(rows)
	h := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return "id-" + hex.EncodeToString(h[:])[:16]
}

// buildIdentityMMDB writes an MMDB keyed by pod IP /32 (or /128) -> identity.
// Field names (workload/namespace/app) mirror the agent's MmdbIdentityRecord.
func buildIdentityMMDB(entries []identityEntry) ([]byte, error) {
	writer, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType: "Workload-Identity",
		Description: map[string]string{
			"en": "Pod/host IP -> workload identity for east-west detection",
		},
		RecordSize: 28,
		// Pod IPs are intentionally RFC1918 (e.g. 192.168.0.0/16, 10.0.0.0/8),
		// which mmdbwriter treats as reserved and rejects by default. Identity
		// is precisely about those internal ranges, so allow them.
		IncludeReservedNetworks: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create mmdb writer: %w", err)
	}
	for _, e := range entries {
		ip := net.ParseIP(e.ip)
		if ip == nil {
			continue
		}
		var ipNet *net.IPNet
		// Use the 4-byte form for IPv4 so the IP and the /32 mask agree in
		// length; a 16-byte IPv4 with a 4-byte mask confuses the tree insert.
		if v4 := ip.To4(); v4 != nil {
			ipNet = &net.IPNet{IP: v4, Mask: net.CIDRMask(32, 32)}
		} else {
			ipNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
		}
		rec := mmdbtype.Map{
			"workload":  mmdbtype.String(e.workload),
			"namespace": mmdbtype.String(e.namespace),
			"app":       mmdbtype.String(e.app),
		}
		if len(e.labels) > 0 {
			lm := mmdbtype.Map{}
			for k, v := range e.labels {
				lm[mmdbtype.String(k)] = mmdbtype.String(v)
			}
			rec["labels"] = lm
		}
		if err := writer.Insert(ipNet, rec); err != nil {
			return nil, fmt.Errorf("insert %s: %w", e.ip, err)
		}
	}
	var buf bytes.Buffer
	if _, err := writer.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("write mmdb: %w", err)
	}
	return buf.Bytes(), nil
}

// upload PUTs the MMDB to the download-api identity upload endpoint.
func (r *IdentityProducerReconciler) upload(ctx context.Context, mmdb []byte, version string) error {
	base := strings.TrimSuffix(r.UploadURL, "/")
	url := fmt.Sprintf("%s/identity/upload?version=%s", base, version)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(mmdb))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
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
