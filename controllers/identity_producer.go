package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IdentityProducerReconciler builds a workload-identity MMDB (pod IP ->
// {workload, namespace, app}) from the cluster's Pods and uploads it to the
// download-api so agents can pull it for east-west / lateral-movement
// detection. The operator is the only component with live pod->workload state,
// so it is the producer (core/platform is multi-tenant SaaS with no direct
// cluster access).
//
// It runs as a manager.Runnable on an interval — identity changes don't need
// instant propagation (agents poll version.txt on their own refresh interval) —
// and only re-uploads when the resolved IP->identity set actually changes
// (content-hashed version), so a steady cluster produces no churn.
type IdentityProducerReconciler struct {
	client.Client
	Log        logr.Logger
	UploadURL  string // download-api base, e.g. https://api.gen0sec.com/v1
	APIKey     string
	Interval   time.Duration
	HTTPClient *http.Client

	lastVersion string
}

// Start implements manager.Runnable: an initial build then a periodic loop.
func (r *IdentityProducerReconciler) Start(ctx context.Context) error {
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
func (r *IdentityProducerReconciler) LogStartup(log logr.Logger) {
	log.Info("IdentityProducer enabled: building pod->workload identity MMDB and uploading to download-api",
		"uploadURL", r.UploadURL, "interval", r.Interval.String())
}

// identityEntry is one resolved IP -> identity row.
type identityEntry struct {
	ip        string
	workload  string
	namespace string
	app       string
}

func (r *IdentityProducerReconciler) buildAndUpload(ctx context.Context) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "identity-producer: list pods failed")
		return
	}
	entries := resolveIdentityEntries(pods.Items)
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

// resolveIdentityEntries maps each pod's IP(s) to a workload identity.
func resolveIdentityEntries(pods []corev1.Pod) []identityEntry {
	var out []identityEntry
	for i := range pods {
		p := &pods[i]
		// Host-network pods share the node IP; their identity would be the
		// node, not a workload — skip to avoid mislabeling node traffic.
		if p.Spec.HostNetwork {
			continue
		}
		workload, app := workloadIdentity(p)
		if workload == "" {
			continue
		}
		for _, ip := range podIPs(p) {
			out = append(out, identityEntry{
				ip:        ip,
				workload:  workload,
				namespace: p.Namespace,
				app:       app,
			})
		}
	}
	return out
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
