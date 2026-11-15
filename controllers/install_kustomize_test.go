package controllers

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// The install directories are applied together: the operator, then what each
// optional controller takes on top. A resource two of them both carry cannot
// be built into one kustomization, as the end-to-end test does.
func TestInstallDirs_ComposeInOneKustomization(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("no kubectl to build a kustomization with")
	}
	overlay := t.TempDir()
	var resources []string
	for _, dir := range []string{"../config", "../config/proxy-controller", "../config/agent-controller"} {
		abs, err := filepath.Abs(dir)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(overlay, abs)
		if err != nil {
			t.Fatal(err)
		}
		resources = append(resources, "  - "+rel)
	}
	kustomization := "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n" + strings.Join(resources, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(overlay, "kustomization.yaml"), []byte(kustomization), 0o600); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(kubectl, "kustomize", overlay).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize: %v\n%s", err, out)
	}
	for _, want := range []string{
		"name: synapseproxies.synapse.gen0sec.com",
		"name: synapseagents.synapse.gen0sec.com",
		"name: synapse-operator-agent-controller",
	} {
		if strings.Count(string(out), want) < 1 {
			t.Errorf("the combined install lacks %q", want)
		}
	}
}

// The default install runs the east-west producers next to the config-hash
// controller. They hold their own lease, so that each Deployment elects among
// its own replicas, and they start without the API-key Secret, idle.
func TestDefaultInstall_ShipsProducers(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("no kubectl to build a kustomization with")
	}
	cmd := exec.Command(kubectl, "kustomize", "../config")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl kustomize: %v\n%s", err, stderr.String())
	}

	deployments := map[string]*appsv1.Deployment{}
	clusterRoles := map[string]*rbacv1.ClusterRole{}
	bindings := map[string]*rbacv1.ClusterRoleBinding{}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		raw, err := yaml.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		switch obj["kind"] {
		case "Deployment":
			d := &appsv1.Deployment{}
			if err := yaml.Unmarshal(raw, d); err != nil {
				t.Fatal(err)
			}
			deployments[d.Name] = d
		case "ClusterRole":
			r := &rbacv1.ClusterRole{}
			if err := yaml.Unmarshal(raw, r); err != nil {
				t.Fatal(err)
			}
			clusterRoles[r.Name] = r
		case "ClusterRoleBinding":
			b := &rbacv1.ClusterRoleBinding{}
			if err := yaml.Unmarshal(raw, b); err != nil {
				t.Fatal(err)
			}
			bindings[b.Name] = b
		}
	}
	if len(deployments) != 2 {
		names := make([]string, 0, len(deployments))
		for n := range deployments {
			names = append(names, n)
		}
		t.Fatalf("the default install has %d Deployments %v, want 2", len(deployments), names)
	}
	controller, producers := deployments["synapse-operator"], deployments["synapse-operator-producers"]
	if controller == nil || producers == nil {
		t.Fatal("want synapse-operator and synapse-operator-producers")
	}

	arg := func(d *appsv1.Deployment, prefix string) string {
		for _, a := range d.Spec.Template.Spec.Containers[0].Args {
			if strings.HasPrefix(a, prefix) {
				return strings.TrimPrefix(a, prefix)
			}
		}
		return ""
	}
	pid := arg(producers, "--leader-election-id=")
	if pid == "" {
		t.Fatal("the producers Deployment sets no --leader-election-id")
	}
	if cid := arg(controller, "--leader-election-id="); cid == pid || pid == "86a223f3.synapse.gen0sec.com" {
		t.Errorf("the producers lease %q is not distinct from the controller's", pid)
	}
	if got := arg(producers, "--config-hash-controller="); got != "false" {
		t.Errorf("--config-hash-controller on the producers = %q, want false", got)
	}
	if c, p := controller.Spec.Template.Spec.Containers[0].Image, producers.Spec.Template.Spec.Containers[0].Image; c != p {
		t.Errorf("the images differ (%q, %q): one images: entry has to cover both", c, p)
	}

	has := func(d *appsv1.Deployment, flag string) bool {
		for _, a := range d.Spec.Template.Spec.Containers[0].Args {
			if a == flag {
				return true
			}
		}
		return false
	}
	for _, flag := range []string{"--identity-producer", "--edge-producer", "--leader-elect"} {
		if !has(producers, flag) {
			t.Errorf("the producers Deployment lacks %s", flag)
		}
	}
	for _, flag := range []string{"--identity-producer", "--edge-producer"} {
		if has(controller, flag) {
			t.Errorf("the controller Deployment has %s: both Deployments would upload", flag)
		}
	}

	if sa := producers.Spec.Template.Spec.ServiceAccountName; sa != "synapse-operator-producers" {
		t.Errorf("the producers run as %q, want their own synapse-operator-producers", sa)
	}
	role := clusterRoles["synapse-operator-producers"]
	if role == nil {
		t.Fatal("no ClusterRole synapse-operator-producers")
	}
	for _, rule := range role.Rules {
		for _, v := range rule.Verbs {
			if v != "get" && v != "list" && v != "watch" {
				t.Errorf("the producers' ClusterRole grants %q on %v", v, rule.Resources)
			}
		}
		for _, r := range rule.Resources {
			if r == "secrets" || r == "*" {
				t.Errorf("the producers' ClusterRole reaches %q", r)
			}
		}
	}
	if b := bindings["synapse-operator-producers"]; b == nil || b.RoleRef.Name != "synapse-operator-producers" ||
		len(b.Subjects) != 1 || b.Subjects[0].Name != "synapse-operator-producers" {
		t.Errorf("the producers' ClusterRole is not bound to their ServiceAccount: %+v", b)
	}

	found := false
	for _, e := range producers.Spec.Template.Spec.Containers[0].Env {
		if e.Name != "SYNAPSE_API_KEY" {
			continue
		}
		found = true
		ref := e.ValueFrom.SecretKeyRef
		if ref == nil || ref.Optional == nil || !*ref.Optional {
			t.Error("the API-key Secret reference is not optional")
		}
	}
	if !found {
		t.Error("the producers Deployment has no SYNAPSE_API_KEY")
	}
}

// One cluster id feeds both Deployments, through one ConfigMap, so the
// controller's agents and the producers' uploads cannot name different clusters.
func TestDefaultInstall_OneClusterID(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("no kubectl to build a kustomization with")
	}
	cmd := exec.Command(kubectl, "kustomize", "../config")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl kustomize: %v\n%s", err, stderr.String())
	}

	var deployments []*appsv1.Deployment
	var clusterMaps []*corev1.ConfigMap
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		raw, err := yaml.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		switch obj["kind"] {
		case "Deployment":
			d := &appsv1.Deployment{}
			if err := yaml.Unmarshal(raw, d); err != nil {
				t.Fatal(err)
			}
			deployments = append(deployments, d)
		case "ConfigMap":
			c := &corev1.ConfigMap{}
			if err := yaml.Unmarshal(raw, c); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(c.Name, "synapse-operator-cluster") {
				clusterMaps = append(clusterMaps, c)
			}
		}
	}
	if len(clusterMaps) != 1 {
		t.Fatalf("the default install has %d cluster ConfigMaps, want 1", len(clusterMaps))
	}
	cm := clusterMaps[0]
	if _, ok := cm.Data["cluster-id"]; !ok {
		t.Errorf("the cluster ConfigMap has no cluster-id key: %v", cm.Data)
	}
	if len(deployments) != 2 {
		t.Fatalf("the default install has %d Deployments, want 2", len(deployments))
	}
	for _, d := range deployments {
		var found *corev1.EnvVar
		for i, e := range d.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "SYNAPSE_CLUSTER_ID" {
				found = &d.Spec.Template.Spec.Containers[0].Env[i]
			}
		}
		if found == nil || found.ValueFrom == nil || found.ValueFrom.ConfigMapKeyRef == nil {
			t.Errorf("%s does not read SYNAPSE_CLUSTER_ID from a ConfigMap", d.Name)
			continue
		}
		ref := found.ValueFrom.ConfigMapKeyRef
		if ref.Name != cm.Name || ref.Key != "cluster-id" {
			t.Errorf("%s reads SYNAPSE_CLUSTER_ID from %s/%s, want %s/cluster-id (the name is hashed, so the reference has to be rewritten)", d.Name, ref.Name, ref.Key, cm.Name)
		}
		if ref.Optional != nil && !*ref.Optional {
			t.Errorf("%s: the reference is mandatory, so the pod would not start without the ConfigMap", d.Name)
		}
		for _, a := range d.Spec.Template.Spec.Containers[0].Args {
			if strings.Contains(a, "$(SYNAPSE_CLUSTER_ID)") {
				t.Errorf("%s passes the id as an argument (%q): it stays literal when the ConfigMap is missing", d.Name, a)
			}
		}
	}
}
