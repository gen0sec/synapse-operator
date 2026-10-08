// Package testenv starts a local Kubernetes API server with the operator's
// CRDs installed, for tests that need real schema validation, defaulting and
// subresources. The fake client implements none of those.
package testenv

import (
	"errors"
	"fmt"
	"os"
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
	return cfg, env.Stop, nil
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
