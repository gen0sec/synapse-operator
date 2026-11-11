package controllers

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
