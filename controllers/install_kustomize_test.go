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
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if obj["kind"] != "Deployment" {
			continue
		}
		raw, err := yaml.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		d := &appsv1.Deployment{}
		if err := yaml.Unmarshal(raw, d); err != nil {
			t.Fatal(err)
		}
		deployments[d.Name] = d
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
