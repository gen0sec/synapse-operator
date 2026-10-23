package v1alpha1_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/discovery"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"synapse-operator/api/v1alpha1"
)

// validAgent is the smallest SynapseAgent the API accepts. Every rejection
// case below starts from it and breaks exactly one thing.
func validAgent() *v1alpha1.SynapseAgent {
	return &v1alpha1.SynapseAgent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("agent-%d", nameSeq.Add(1)),
			Namespace: "default",
		},
		Spec: v1alpha1.SynapseAgentSpec{Image: "example.test/synapse:1.0.0"},
	}
}

func mustCreateAgent(t *testing.T, a *v1alpha1.SynapseAgent) {
	t.Helper()
	if err := k8s.Create(context.Background(), a); err != nil {
		t.Fatalf("create %s: %v", a.Name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), a) })
}

func TestSynapseAgent_Rejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*v1alpha1.SynapseAgent)
		want   string // substring of the API server's message
	}{
		{"no image",
			func(a *v1alpha1.SynapseAgent) { a.Spec.Image = "" },
			"spec.image"},
		{"a name longer than 48 characters",
			func(a *v1alpha1.SynapseAgent) { a.Name = strings.Repeat("a", 49) },
			"at most 48 characters"},
		{"a name that is not a DNS label",
			func(a *v1alpha1.SynapseAgent) { a.Name = "Bad_Name" },
			"metadata.name"},
		{"a name that starts with a digit",
			func(a *v1alpha1.SynapseAgent) { a.Name = "9-starts-with-a-digit" },
			"DNS-1035"},
		{"a node selector for a Windows node",
			func(a *v1alpha1.SynapseAgent) {
				a.Spec.NodeSelector = map[string]string{"kubernetes.io/os": "windows"}
			},
			"agents run on Linux nodes only"},
		{"an unknown log level",
			func(a *v1alpha1.SynapseAgent) { a.Spec.Logging = &v1alpha1.LoggingSpec{Level: "verbose"} },
			"spec.logging.level"},
		{"an unknown os",
			func(a *v1alpha1.SynapseAgent) { a.Spec.OS = "freebsd" },
			"spec.os"},
		{"a duplicate homeNet entry",
			func(a *v1alpha1.SynapseAgent) {
				a.Spec.IDS = &v1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8", "10.0.0.0/8"}}
			},
			"Duplicate value"},
		{"an empty capture interface",
			func(a *v1alpha1.SynapseAgent) { a.Spec.Capture = &v1alpha1.AgentCaptureSpec{Interfaces: []string{""}} },
			"spec.capture.interfaces"},
		{"an API key reference without a key",
			func(a *v1alpha1.SynapseAgent) {
				a.Spec.Platform = &v1alpha1.AgentPlatformSpec{APIKeySecretRef: &v1alpha1.SecretKeyReference{Name: "creds"}}
			},
			"spec.platform.apiKeySecretRef.key"},
		{"a config that is not an object",
			func(a *v1alpha1.SynapseAgent) { a.Spec.Config = &runtime.RawExtension{Raw: []byte(`"mode: agent"`)} },
			"spec.config"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := validAgent()
			tc.mutate(a)
			err := k8s.Create(context.Background(), a)
			if err == nil {
				_ = k8s.Delete(context.Background(), a)
				t.Fatal("accepted, want it rejected")
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("error is not Invalid: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("message does not mention %q:\n%v", tc.want, err)
			}
		})
	}
}

func TestSynapseAgent_Defaults(t *testing.T) {
	a := validAgent()
	mustCreateAgent(t, a)

	if want := []corev1.Toleration{{Operator: corev1.TolerationOpExists}}; !reflect.DeepEqual(a.Spec.Tolerations, want) {
		t.Errorf("tolerations = %+v, want %+v", a.Spec.Tolerations, want)
	}
	if a.Spec.HostPID != nil {
		t.Errorf("hostPID = %v, want it left unset: the builder defaults it per OS", *a.Spec.HostPID)
	}
	if a.Spec.OS != "linux" {
		t.Errorf("os = %q, want it defaulted to linux", a.Spec.OS)
	}
}

func TestSynapseAgent_ExplicitEmptyTolerationsAreKept(t *testing.T) {
	a := validAgent()
	a.Spec.Tolerations = []corev1.Toleration{}
	mustCreateAgent(t, a)

	var got v1alpha1.SynapseAgent
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(a), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Tolerations == nil || len(got.Spec.Tolerations) != 0 {
		t.Errorf("tolerations = %#v, want the explicit empty list kept", got.Spec.Tolerations)
	}
}

func TestSynapseAgent_FullSpecIsAccepted(t *testing.T) {
	maxUnavailable := intstr.FromInt32(2)
	a := validAgent()
	a.Name = strings.Repeat("a", 48) // the longest name allowed
	a.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
	a.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
	}
	a.Spec.NodeSelector = map[string]string{"kubernetes.io/os": "linux", "pool": "edge"}
	a.Spec.Tolerations = []corev1.Toleration{{Key: "edge", Operator: corev1.TolerationOpExists}}
	a.Spec.Affinity = &corev1.Affinity{}
	a.Spec.PriorityClassName = "system-node-critical"
	a.Spec.UpdateStrategy = &appsv1.DaemonSetUpdateStrategy{
		Type:          appsv1.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDaemonSet{MaxUnavailable: &maxUnavailable},
	}
	a.Spec.HostPID = ptr.To(false)
	a.Spec.OS = "linux"
	a.Spec.Logging = &v1alpha1.LoggingSpec{Level: "debug"}
	a.Spec.Platform = &v1alpha1.AgentPlatformSpec{
		APIKeySecretRef: &v1alpha1.SecretKeyReference{Name: "synapse-credentials", Key: "API_KEY"},
		Identity:        &v1alpha1.AgentIdentitySpec{Enabled: ptr.To(true), Cluster: "prod-eu"},
	}
	a.Spec.Capture = &v1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0", "eth1"}}
	a.Spec.IDS = &v1alpha1.AgentIDSSpec{
		Enabled:      ptr.To(true),
		EnforceBlock: ptr.To(false),
		HomeNet:      []string{"10.0.0.0/8", "fd00::/8"},
	}
	a.Spec.Config = &runtime.RawExtension{Raw: []byte(`{"firewall":{"ids":{"rule_paths":["/rules"]}}}`)}
	mustCreateAgent(t, a)

	if a.Spec.HostPID == nil || *a.Spec.HostPID {
		t.Errorf("hostPID = %v, want the explicit false kept", a.Spec.HostPID)
	}
	if a.Spec.IDS == nil || len(a.Spec.IDS.HomeNet) != 2 {
		t.Errorf("ids = %+v, want it kept", a.Spec.IDS)
	}
}

// agentCRD reads the generated manifest, which is what ships.
func agentCRD(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../config/crd/bases/synapse.gen0sec.com_synapseagents.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	return crd
}

func agentVersion(t *testing.T) map[string]any {
	t.Helper()
	versions := agentCRD(t)["spec"].(map[string]any)["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("%d versions, want 1", len(versions))
	}
	return versions[0].(map[string]any)
}

// Synapse never acts on network.ip_version, so the typed field is gone.
func TestSynapseAgent_CaptureHasNoIPVersion(t *testing.T) {
	spec := agentVersion(t)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)
	capture := spec["properties"].(map[string]any)["capture"].(map[string]any)["properties"].(map[string]any)
	if _, ok := capture["ipVersion"]; ok {
		t.Error("spec.capture.ipVersion is in the CRD schema")
	}
	if _, ok := capture["interfaces"]; !ok {
		t.Error("spec.capture.interfaces is missing: the probe is reading the wrong path")
	}
}

func TestSynapseAgent_HasNoScaleSubresource(t *testing.T) {
	sub, _ := agentVersion(t)["subresources"].(map[string]any)
	if _, ok := sub["status"]; !ok {
		t.Error("no status subresource")
	}
	if _, ok := sub["scale"]; ok {
		t.Error("the CRD declares a scale subresource: a DaemonSet has no replica count")
	}

	a := validAgent()
	mustCreateAgent(t, a)
	err := k8s.SubResource("scale").Get(context.Background(), a, &autoscalingv1.Scale{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("get scale: %v, want NotFound", err)
	}
}

// Same trap as the proxy's: a wrong JSONPath is an empty column, not an
// error. So check the columns in the manifest and read the table.
func TestSynapseAgent_PrinterColumns(t *testing.T) {
	ctx := context.Background()

	var declared []string
	for _, c := range agentVersion(t)["additionalPrinterColumns"].([]any) {
		declared = append(declared, c.(map[string]any)["name"].(string))
	}
	wantDeclared := []string{"Ready", "Reason", "Nodes", "Ready-Pods", "Updated", "Image", "Config", "Age"}
	if !reflect.DeepEqual(declared, wantDeclared) {
		t.Errorf("printer columns = %v, want %v", declared, wantDeclared)
	}

	a := validAgent()
	a.Namespace = "columns-" + a.Name
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: a.Namespace}}); err != nil {
		t.Fatal(err)
	}
	mustCreateAgent(t, a)
	a.Status = v1alpha1.SynapseAgentStatus{
		ConfigHash:             "abc123",
		DesiredNumberScheduled: 5,
		CurrentNumberScheduled: 4,
		UpdatedNumberScheduled: 3,
		NumberReady:            2,
		NumberAvailable:        1,
		NumberMisscheduled:     6,
		Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Applied",
			LastTransitionTime: metav1.Now(),
		}},
	}
	if err := k8s.Status().Update(ctx, a); err != nil {
		t.Fatalf("status update: %v", err)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := dc.RESTClient().Get().
		AbsPath("/apis", v1alpha1.GroupVersion.Group, v1alpha1.GroupVersion.Version, "namespaces", a.Namespace, "synapseagents").
		SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io").
		Do(ctx).Raw()
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	var table metav1.Table
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("%d rows, want 1", len(table.Rows))
	}

	got := map[string]string{}
	wide := map[string]bool{}
	for i, col := range table.ColumnDefinitions {
		got[col.Name] = fmt.Sprint(table.Rows[0].Cells[i])
		wide[col.Name] = col.Priority != 0
	}
	want := map[string]string{
		"Name":       a.Name,
		"Ready":      "True",
		"Reason":     "Applied",
		"Nodes":      "5",
		"Ready-Pods": "2",
		"Updated":    "3",
		"Image":      "example.test/synapse:1.0.0",
		"Config":     "abc123",
	}
	for name, cell := range want {
		if got[name] != cell {
			t.Errorf("column %s = %q, want %q", name, got[name], cell)
		}
	}
	if _, ok := got["Age"]; !ok {
		t.Error("no Age column")
	}
	for name, isWide := range wide {
		if want := name == "Image" || name == "Config"; isWide != want {
			t.Errorf("column %s: shown only with -o wide = %v, want %v", name, isWide, want)
		}
	}
}
