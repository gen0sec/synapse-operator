package controllers

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

func emptyDirVolume(name string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
}

func rulesConfigMapVolume(name string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "my-rules"}},
	}}
}

func withVolumes(vols []corev1.Volume, mounts []corev1.VolumeMount) func(*synapsev1alpha1.SynapseAgentSpec) {
	return func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Volumes = vols
		s.VolumeMounts = mounts
	}
}

func TestAgentDaemonSet_UserVolumesAppendedInOrder(t *testing.T) {
	plain := buildAgentDaemonSet(nodeAgent(), "h")
	a := nodeAgent(withVolumes(
		[]corev1.Volume{emptyDirVolume("zeta"), rulesConfigMapVolume("alpha")},
		[]corev1.VolumeMount{
			{Name: "zeta", MountPath: "/mnt/zeta"},
			{Name: "alpha", MountPath: "/etc/synapse/rules", ReadOnly: true},
		}))
	ds := buildAgentDaemonSet(a, "h")

	vols, base := agentPodSpec(ds).Volumes, agentPodSpec(plain).Volumes
	if len(vols) != len(base)+2 {
		t.Fatalf("%d volumes, want %d", len(vols), len(base)+2)
	}
	for i := range base {
		if vols[i].Name != base[i].Name {
			t.Errorf("operator volume %d = %q, want %q", i, vols[i].Name, base[i].Name)
		}
	}
	if vols[len(base)].Name != "zeta" || vols[len(base)+1].Name != "alpha" {
		t.Errorf("user volumes out of spec order: %v", vols[len(base):])
	}

	mounts, baseMounts := agentContainer(t, ds).VolumeMounts, agentContainer(t, plain).VolumeMounts
	if len(mounts) != len(baseMounts)+2 {
		t.Fatalf("%d mounts, want %d", len(mounts), len(baseMounts)+2)
	}
	for i := range baseMounts {
		if mounts[i].Name != baseMounts[i].Name || mounts[i].MountPath != baseMounts[i].MountPath {
			t.Errorf("operator mount %d = %+v, want %+v", i, mounts[i], baseMounts[i])
		}
	}
	if m := mounts[len(baseMounts)+1]; m.Name != "alpha" || m.MountPath != "/etc/synapse/rules" || !m.ReadOnly {
		t.Errorf("last mount = %+v", m)
	}
}

func TestValidateAgentVolumes(t *testing.T) {
	vol := func(name string) corev1.Volume { return emptyDirVolume(name) }
	mount := func(name, p string) corev1.VolumeMount { return corev1.VolumeMount{Name: name, MountPath: p} }

	cases := []struct {
		name   string
		vols   []corev1.Volume
		mounts []corev1.VolumeMount
		want   string // "" means valid; else a substring of the message
	}{
		{name: "none"},
		{name: "valid", vols: []corev1.Volume{vol("rules")}, mounts: []corev1.VolumeMount{mount("rules", "/etc/synapse/rules")}},
		{name: "volume without a mount", vols: []corev1.Volume{vol("rules")}},
		{name: "sibling of an operator path", vols: []corev1.Volume{vol("x")}, mounts: []corev1.VolumeMount{mount("x", "/var/lib/synapse-extra")}},
		{name: "reserved config", vols: []corev1.Volume{vol("config")}, want: `"config"`},
		{name: "reserved data", vols: []corev1.Volume{vol("data")}, want: `"data"`},
		{name: "reserved bpffs", vols: []corev1.Volume{vol("bpffs")}, want: `"bpffs"`},
		{name: "reserved debugfs", vols: []corev1.Volume{vol("debugfs")}, want: `"debugfs"`},
		{name: "reserved modules", vols: []corev1.Volume{vol("modules")}, want: `"modules"`},
		{name: "reserved eve-log", vols: []corev1.Volume{vol("eve-log")}, want: `"eve-log"`},
		{name: "mount names no volume", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("b", "/mnt/b")}, want: `"b"`},
		{name: "relative path", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "mnt/a")}, want: "absolute"},
		{name: "empty path", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "")}, want: "absolute"},
		{name: "equals config dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/etc/synapse/config")}, want: "/etc/synapse/config"},
		{name: "equals data dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/var/lib/synapse")}, want: "/var/lib/synapse"},
		{name: "equals bpffs", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/sys/fs/bpf")}, want: "/sys/fs/bpf"},
		{name: "equals debugfs", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/sys/kernel/debug")}, want: "/sys/kernel/debug"},
		{name: "equals modules", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/lib/modules")}, want: "/lib/modules"},
		{name: "equals eve dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/var/log/synapse-eve")}, want: "/var/log/synapse-eve"},
		{name: "equals, trailing slash", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/var/lib/synapse/")}, want: "/var/lib/synapse"},
		{name: "equals, dot dot", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/tmp/../var/lib/synapse")}, want: "/var/lib/synapse"},
		{name: "parent of config dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/etc/synapse")}, want: "/etc/synapse/config"},
		{name: "parent of bpffs", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/sys")}, want: "/sys/fs/bpf"},
		{name: "root", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/")}, want: "/etc/synapse/config"},
		{name: "child of data dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/var/lib/synapse/rules")}, want: "/var/lib/synapse"},
		{name: "child of config dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/etc/synapse/config/extra")}, want: "/etc/synapse/config"},
		{name: "child of eve dir", vols: []corev1.Volume{vol("a")}, mounts: []corev1.VolumeMount{mount("a", "/var/log/synapse-eve/x")}, want: "/var/log/synapse-eve"},
		{name: "subPath", vols: []corev1.Volume{vol("a")},
			mounts: []corev1.VolumeMount{{Name: "a", MountPath: "/mnt/a", SubPath: "f"}}, want: "subPath"},
		{name: "subPathExpr", vols: []corev1.Volume{vol("a")},
			mounts: []corev1.VolumeMount{{Name: "a", MountPath: "/mnt/a", SubPathExpr: "$(POD_NAME)"}}, want: "subPathExpr"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := validateAgentVolumes(&synapsev1alpha1.SynapseAgentSpec{Volumes: c.vols, VolumeMounts: c.mounts})
			if c.want == "" {
				if got != "" {
					t.Fatalf("refused a valid spec: %s", got)
				}
				return
			}
			if got == "" {
				t.Fatalf("accepted; want a message containing %q", c.want)
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("message %q does not contain %q", got, c.want)
			}
		})
	}
}

func TestAgent_VolumesRoundTrip(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running(withVolumes(
		[]corev1.Volume{rulesConfigMapVolume("rules")},
		[]corev1.VolumeMount{{Name: "rules", MountPath: "/etc/synapse/rules", ReadOnly: true}}))

	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	ds := e.daemonSet()
	cm := agentVolume(t, ds, "rules").ConfigMap
	if cm == nil || cm.Name != "my-rules" {
		t.Errorf("rules volume = %+v", cm)
	}
	if m := agentMountAt(t, agentContainer(t, ds), "/etc/synapse/rules"); m.Name != "rules" || !m.ReadOnly {
		t.Errorf("rules mount = %+v", m)
	}
}

func TestAgent_InvalidVolumes(t *testing.T) {
	e := newAgentEnv(t)
	a := e.agent(withVolumes([]corev1.Volume{emptyDirVolume("data")}, nil))
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonInvalidVolumes)
	if !strings.Contains(c.Message, `"data"`) {
		t.Errorf("message does not name the volume: %s", c.Message)
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	e.wantNothingApplied()

	// Once running, an invalid edit leaves what runs alone.
	e = newAgentEnv(t)
	a = e.running()
	secret, ds := e.configSecret(), e.daemonSet()
	hash := a.Status.ConfigHash
	e.edit(a, withVolumes([]corev1.Volume{emptyDirVolume("x")},
		[]corev1.VolumeMount{{Name: "x", MountPath: "/var/lib/synapse/x"}}))
	e.reconcile(a)
	c = wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonInvalidVolumes)
	if !strings.Contains(c.Message, "/var/lib/synapse/x") {
		t.Errorf("message does not name the path: %s", c.Message)
	}
	if e.configSecret().ResourceVersion != secret.ResourceVersion || e.daemonSet().ResourceVersion != ds.ResourceVersion {
		t.Error("the running objects were touched")
	}
	if a.Status.ConfigHash != hash {
		t.Errorf("configHash = %q, want the running config's %q", a.Status.ConfigHash, hash)
	}
	if !strings.Contains(e.events(), ReasonConfigInvalid) {
		t.Error("no event")
	}
}

func TestAgent_AddingAVolumeRollsTheTemplate(t *testing.T) {
	e := newAgentEnv(t)
	a := e.running()
	before := e.daemonSet()

	e.edit(a, withVolumes([]corev1.Volume{emptyDirVolume("scratch")},
		[]corev1.VolumeMount{{Name: "scratch", MountPath: "/mnt/scratch"}}))
	e.reconcile(a)

	after := e.daemonSet()
	if after.Generation == before.Generation {
		t.Error("the DaemonSet generation did not change: the pods will not roll")
	}
	agentVolume(t, after, "scratch")
}
