// Package testenv starts a local Kubernetes API server with the operator's
// CRDs installed, for tests that need real schema validation, defaulting and
// subresources. The fake client implements none of those.
package testenv

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// KubernetesVersion is the control-plane version the tests run against unless
// ENVTEST_K8S_VERSION names another one.
const KubernetesVersion = "1.37.0"

// Start boots the API server and returns its client configuration and a
// function that stops it.
//
// The binaries come from KUBEBUILDER_ASSETS when it is set. Otherwise the
// pinned version is downloaded once into the directory setup-envtest uses.
func Start() (*rest.Config, func() error, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, nil, err
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	// Off by default, and on in some distributions, OpenShift among them:
	// whoever makes an object depend on an owner must be allowed to update
	// that owner's finalizers. The operator has to work with it on.
	env.ControlPlane.GetAPIServer().Configure().
		Append("enable-admission-plugins", "OwnerReferencesPermissionEnforcement")
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dir, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory()
		if err != nil {
			return nil, nil, fmt.Errorf("locate the envtest binaries directory: %w", err)
		}
		version := os.Getenv("ENVTEST_K8S_VERSION")
		if version == "" {
			version = KubernetesVersion
		}
		env.DownloadBinaryAssets = true
		env.DownloadBinaryAssetsVersion = "v" + strings.TrimPrefix(version, "v")
		env.BinaryAssetsDirectory = dir
	}
	cfg, err := env.Start()
	if err != nil {
		return nil, nil, fmt.Errorf("start the test API server: %w", err)
	}

	// The Gateway API's CRDs, from the module the operator is built against,
	// so that the types it reads and the server's cannot differ. They use
	// validation an older API server does not have, and such a server
	// refuses them: it is then a cluster without the Gateway API, which is
	// one the operator has to work on too. Tests that need it ask whether
	// it is there.
	gatewayAPI, err := moduleDir(root, "sigs.k8s.io/gateway-api")
	if err != nil {
		_ = env.Stop()
		return nil, nil, err
	}
	_, _ = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths: []string{filepath.Join(gatewayAPI, "config", "crd", "standard")},
	})
	return cfg, env.Stop, nil
}

// moduleDir returns where the source of a module this one requires is kept.
func moduleDir(root, module string) (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("locate module %s: %w", module, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// moduleRoot walks up from the working directory, which is the package
// directory under `go test`, to the directory holding go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the working directory")
		}
		dir = parent
	}
}
