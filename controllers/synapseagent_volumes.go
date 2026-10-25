package controllers

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	corev1 "k8s.io/api/core/v1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// ReasonInvalidVolumes is the ConfigValid reason of an agent whose
// spec.volumes or spec.volumeMounts break a rule.
const ReasonInvalidVolumes = "InvalidVolumes"

// agentReservedVolumes are the volume names the operator sets up itself.
var agentReservedVolumes = []string{"config", "data", "bpffs", "debugfs", "modules", "eve-log"}

// agentReservedPaths are the paths the operator mounts. A user mount may not
// equal, contain or sit inside one of them.
var agentReservedPaths = []string{
	agentConfigDir, agentDataDir, "/sys/fs/bpf", "/sys/kernel/debug", "/lib/modules", "/var/log/synapse-eve",
}

// validateAgentVolumes returns what is wrong with the user's volumes and
// mounts, or "" when they are fine.
func validateAgentVolumes(spec *synapsev1alpha1.SynapseAgentSpec) string {
	declared := make(map[string]bool, len(spec.Volumes))
	for _, v := range spec.Volumes {
		for _, r := range agentReservedVolumes {
			if v.Name == r {
				return fmt.Sprintf("spec.volumes: the name %q is used by the operator", v.Name)
			}
		}
		if errs := validation.IsDNS1123Label(v.Name); len(errs) > 0 {
			return fmt.Sprintf("spec.volumes: the name %q is not valid: %s", v.Name, errs[0])
		}
		if n := volumeSources(v.VolumeSource); n != 1 {
			return fmt.Sprintf("spec.volumes: %q sets %d volume sources, want exactly 1", v.Name, n)
		}
		declared[v.Name] = true
	}
	for _, m := range spec.VolumeMounts {
		if !declared[m.Name] {
			return fmt.Sprintf("spec.volumeMounts: %q names no volume of spec.volumes", m.Name)
		}
		if m.SubPath != "" || m.SubPathExpr != "" {
			field := "subPath"
			if m.SubPath == "" {
				field = "subPathExpr"
			}
			return fmt.Sprintf("spec.volumeMounts: %q sets %s, which is not supported: kubelet does not refresh such a mount", m.Name, field)
		}
		if !path.IsAbs(m.MountPath) {
			return fmt.Sprintf("spec.volumeMounts: %q has mountPath %q, which is not an absolute path", m.Name, m.MountPath)
		}
		p := path.Clean(m.MountPath)
		for _, r := range agentReservedPaths {
			if pathsOverlap(p, r) {
				return fmt.Sprintf("spec.volumeMounts: %q mounts at %q, which overlaps %s of the operator", m.Name, m.MountPath, r)
			}
		}
	}
	return ""
}

// pathsOverlap is true when a and b are the same clean path, or one lies
// inside the other.
func pathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, strings.TrimSuffix(b, "/")+"/") || strings.HasPrefix(b, strings.TrimSuffix(a, "/")+"/")
}

// volumeSources counts the sources a volume sets. Every field of
// VolumeSource is a pointer with omitempty, so the JSON form holds the set ones.
func volumeSources(src corev1.VolumeSource) int {
	raw, err := json.Marshal(src)
	if err != nil {
		return 0
	}
	var set map[string]json.RawMessage
	if err := json.Unmarshal(raw, &set); err != nil {
		return 0
	}
	return len(set)
}
