package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cmNamed(name, data string) corev1.ConfigMap {
	return corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "synapse-os"},
		Data:       map[string]string{"config.yaml": data},
	}
}

func podSpecMountingCM(cmName string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Volumes: []corev1.Volume{{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				},
			},
		}},
	}
}

func TestReferencedSources_AllRefTypes(t *testing.T) {
	spec := &corev1.PodSpec{
		Volumes: []corev1.Volume{
			{VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "vol-cm"}}}},
			{VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "vol-sec"}}},
			{VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "proj-cm"}}},
				{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "proj-sec"}}},
			}}}},
		},
		Containers: []corev1.Container{{
			EnvFrom: []corev1.EnvFromSource{
				{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "envfrom-cm"}}},
				{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "envfrom-sec"}}},
			},
			Env: []corev1.EnvVar{{ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "envkey-cm"}},
			}}},
		}},
	}
	cms, secs := referencedSources(spec)
	for _, want := range []string{"vol-cm", "proj-cm", "envfrom-cm", "envkey-cm"} {
		if _, ok := cms[want]; !ok {
			t.Errorf("missing referenced cm %q; got %v", want, cms)
		}
	}
	for _, want := range []string{"vol-sec", "proj-sec", "envfrom-sec"} {
		if _, ok := secs[want]; !ok {
			t.Errorf("missing referenced secret %q; got %v", want, secs)
		}
	}
}

func TestPerWorkload_IsolatesWorkloads(t *testing.T) {
	r := &ConfigMapReconciler{PerWorkloadConfigHash: true}
	agentSpec := podSpecMountingCM("synapse-agent")
	proxySpec := podSpecMountingCM("synapse-proxy")

	cms := []corev1.ConfigMap{cmNamed("synapse-agent", "agent-v1"), cmNamed("synapse-proxy", "proxy-v1")}
	combined := hashConfigSources(cms, nil, nil, nil, false)

	proxyHash1 := r.hashForWorkload(proxySpec, cms, nil, combined)
	agentHash1 := r.hashForWorkload(agentSpec, cms, nil, combined)
	if proxyHash1 == "" || agentHash1 == "" {
		t.Fatal("per-workload hashes should be non-empty")
	}
	if proxyHash1 == agentHash1 {
		t.Error("proxy and agent should hash differently (different CMs)")
	}

	// Change the AGENT cm only — proxy hash must NOT change; agent hash must.
	cms2 := []corev1.ConfigMap{cmNamed("synapse-agent", "agent-v2-RULES-CHANGED"), cmNamed("synapse-proxy", "proxy-v1")}
	combined2 := hashConfigSources(cms2, nil, nil, nil, false)
	proxyHash2 := r.hashForWorkload(proxySpec, cms2, nil, combined2)
	agentHash2 := r.hashForWorkload(agentSpec, cms2, nil, combined2)

	if proxyHash2 != proxyHash1 {
		t.Errorf("proxy hash changed on an AGENT-only config change (this is the bug being fixed): %s -> %s", proxyHash1, proxyHash2)
	}
	if agentHash2 == agentHash1 {
		t.Error("agent hash should change when the agent CM changes")
	}
}

func TestPerWorkload_CombinedModeUnchanged(t *testing.T) {
	r := &ConfigMapReconciler{PerWorkloadConfigHash: false}
	cms := []corev1.ConfigMap{cmNamed("synapse-agent", "a"), cmNamed("synapse-proxy", "p")}
	combined := hashConfigSources(cms, nil, nil, nil, false)
	// In combined mode, both workloads get the SAME combined hash regardless of refs.
	if got := r.hashForWorkload(podSpecMountingCM("synapse-agent"), cms, nil, combined); got != combined {
		t.Errorf("combined mode should return the combined hash; got %s want %s", got, combined)
	}
	if got := r.hashForWorkload(podSpecMountingCM("synapse-proxy"), cms, nil, combined); got != combined {
		t.Errorf("combined mode should return the combined hash; got %s want %s", got, combined)
	}
}

func TestPerWorkload_NoReferences_EmptyHash(t *testing.T) {
	r := &ConfigMapReconciler{PerWorkloadConfigHash: true}
	cms := []corev1.ConfigMap{cmNamed("synapse-agent", "a")}
	combined := hashConfigSources(cms, nil, nil, nil, false)
	// A workload that references a CM NOT in the labelled set -> empty hash (skip).
	got := r.hashForWorkload(podSpecMountingCM("some-unrelated-cm"), cms, nil, combined)
	if got != "" {
		t.Errorf("workload referencing no labelled source should hash to empty; got %q", got)
	}
}
