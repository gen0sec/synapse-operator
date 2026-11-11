package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

func TestAgentRuleFileName(t *testing.T) {
	a := agentRuleFileName(0, "web.rules", []byte("alert tcp any any -> any 80 (sid:1;)\n"))
	if again := agentRuleFileName(0, "web.rules", []byte("alert tcp any any -> any 80 (sid:1;)\n")); again != a {
		t.Errorf("the same content gave %q and %q", a, again)
	}
	if changed := agentRuleFileName(0, "web.rules", []byte("alert tcp any any -> any 80 (sid:2;)\n")); changed == a {
		t.Errorf("a different content kept the name %q", a)
	}
	if moved := agentRuleFileName(1, "web.rules", []byte("alert tcp any any -> any 80 (sid:1;)\n")); moved == a {
		t.Errorf("another source index kept the name %q", a)
	}
	if !strings.HasPrefix(a, "rules.0.web.") || !strings.HasSuffix(a, ".rules") {
		t.Errorf("name = %q, want rules.0.web.<hash>.rules", a)
	}
	if got := len(strings.TrimSuffix(strings.TrimPrefix(a, "rules.0.web."), ".rules")); got != 12 {
		t.Errorf("the hash in %q is %d characters, want 12", a, got)
	}
	// A key that does not end in .rules keeps its whole name as the stem.
	if got := agentRuleFileName(2, "custom", []byte("x")); !strings.HasPrefix(got, "rules.2.custom.") {
		t.Errorf("name = %q", got)
	}
	// Secret keys may be 253 characters long; the name has to stay a valid key.
	if got := agentRuleFileName(0, strings.Repeat("k", 253), []byte("x")); len(got) > 253 {
		t.Errorf("name is %d characters long, a Secret key may have 253", len(got))
	}
}

func rulesGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "agentconfig", name+".yaml")
	if *updateGolden {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update-golden to create it)", err)
	}
	if string(got) != string(want) {
		t.Errorf("output differs from %s (run with -update-golden if the change is intended)\n--- got ---\n%s--- want ---\n%s", path, got, want)
	}
}

func TestRenderAgentConfig_Rules(t *testing.T) {
	files := []string{"rules.0.web.0123456789ab.rules", "rules.0.zz.ba9876543210.rules", "rules.1.core.00ffeeddccbb.rules"}
	in := AgentConfigInput{Agent: agentForConfig(), RuleFiles: files}
	out := renderAgentOK(t, in)
	rulesGolden(t, "rules", out.YAML)

	tree := agentTree(t, in)
	paths, _ := tree["ids"].(map[string]any)["rule_paths"].([]any)
	want := []any{"/etc/synapse/rules/*.rules",
		"/etc/synapse/config/" + files[0], "/etc/synapse/config/" + files[1], "/etc/synapse/config/" + files[2]}
	if len(paths) != len(want) {
		t.Fatalf("rule_paths = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Errorf("rule_paths[%d] = %v, want %v", i, paths[i], want[i])
		}
	}
}

func TestRenderAgentConfig_RulesFollowTheUsersRulePaths(t *testing.T) {
	in := AgentConfigInput{
		Agent:     agentForConfig(agentRawConfig(`{"ids": {"rule_paths": ["/etc/mine/*.rules"]}}`)),
		RuleFiles: []string{"rules.0.web.0123456789ab.rules"},
	}
	paths, _ := agentTree(t, in)["ids"].(map[string]any)["rule_paths"].([]any)
	if len(paths) != 2 || paths[0] != "/etc/mine/*.rules" || paths[1] != "/etc/synapse/config/rules.0.web.0123456789ab.rules" {
		t.Errorf("rule_paths = %v", paths)
	}
}

func TestRenderAgentConfig_RulesAreHot(t *testing.T) {
	render := func(files ...string) AgentConfigOutput {
		return renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(), RuleFiles: files})
	}
	none, one, two := render(), render("rules.0.a.000000000001.rules"), render("rules.0.a.000000000002.rules")
	if none.Hash == one.Hash || one.Hash == two.Hash {
		t.Error("a change of rule file did not change the config, so Synapse would not reload")
	}
	if none.RestartHash != one.RestartHash || one.RestartHash != two.RestartHash {
		t.Error("a change of rule file moved RestartHash, so it would roll the pods")
	}
}

func TestRenderAgentConfig_RulePathsThatAreNotAList(t *testing.T) {
	_, err := RenderAgentConfig(AgentConfigInput{
		Agent:     agentForConfig(agentRawConfig(`{"ids": {"rule_paths": "/etc/mine"}}`)),
		RuleFiles: []string{"rules.0.web.0123456789ab.rules"},
	})
	if err == nil || !strings.Contains(err.Error(), "spec.config.ids.rule_paths") {
		t.Errorf("err = %v, want it to name spec.config.ids.rule_paths", err)
	}
}

func rulesFrom(sources ...synapsev1alpha1.AgentIDSRuleSource) func(*synapsev1alpha1.SynapseAgentSpec) {
	return func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{Rules: sources}
	}
}

func fromConfigMap(name string, keys ...string) synapsev1alpha1.AgentIDSRuleSource {
	return synapsev1alpha1.AgentIDSRuleSource{ConfigMapRef: &synapsev1alpha1.ObjectNameReference{Name: name}, Keys: keys}
}

func fromSecret(name string, keys ...string) synapsev1alpha1.AgentIDSRuleSource {
	return synapsev1alpha1.AgentIDSRuleSource{SecretRef: &synapsev1alpha1.ObjectNameReference{Name: name}, Keys: keys}
}

func (e *agentEnv) ruleConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	e.t.Helper()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: name}, Data: data}
	e.create(cm)
	return cm
}

func (e *agentEnv) ruleSecret(name string, data map[string]string) *corev1.Secret {
	e.t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: name}, Data: map[string][]byte{}}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	e.create(s)
	return s
}

// ruleKeys are the keys of the config Secret that are rule files.
func ruleKeys(s *corev1.Secret) []string {
	var keys []string
	for k := range s.Data {
		if k != agentConfigKey {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestAgent_RulesFromConfigMap(t *testing.T) {
	e := newAgentEnv(t)
	cm := e.ruleConfigMap("local-rules", map[string]string{
		"web.rules": "alert http any any -> any any (msg:\"a\"; sid:1;)\n",
		"notes.txt": "not a rule file",
	})
	a := e.running(rulesFrom(fromConfigMap("local-rules")))
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)

	secret, ds := e.configSecret(), e.daemonSet()
	keys := ruleKeys(secret)
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "rules.0.web.") || !strings.HasSuffix(keys[0], ".rules") {
		t.Fatalf("rule keys of the Secret = %v, want one rules.0.web.<hash>.rules", keys)
	}
	if got := string(secret.Data[keys[0]]); got != cm.Data["web.rules"] {
		t.Errorf("rule file holds %q", got)
	}
	if !strings.Contains(e.config(), "/etc/synapse/config/"+keys[0]) {
		t.Errorf("rule_paths does not list the file:\n%s", e.config())
	}

	// A new content is a new name, in the same Secret, and rolls nothing.
	change(t, e.k8s, cm, func() { cm.Data["web.rules"] = "alert http any any -> any any (msg:\"b\"; sid:2;)\n" })
	e.reconcile(a)
	now := e.configSecret()
	if now.UID != secret.UID {
		t.Error("the config Secret was replaced")
	}
	changed := ruleKeys(now)
	if len(changed) != 1 || changed[0] == keys[0] {
		t.Fatalf("rule keys after the edit = %v, were %v: the old key must be gone and a new one there", changed, keys)
	}
	if !strings.Contains(e.config(), "/etc/synapse/config/"+changed[0]) || strings.Contains(e.config(), keys[0]) {
		t.Errorf("rule_paths does not follow the edit:\n%s", e.config())
	}
	after := e.daemonSet()
	if after.Spec.Template.Annotations[agentRestartHashAnnotation] != ds.Spec.Template.Annotations[agentRestartHashAnnotation] {
		t.Error("a rule change changed the template annotation")
	}
	if after.ResourceVersion != ds.ResourceVersion {
		t.Error("a rule change touched the DaemonSet")
	}
}

func TestAgent_RulesFromSecret(t *testing.T) {
	e := newAgentEnv(t)
	e.ruleSecret("private-rules", map[string]string{"a.rules": "alert one", "b.rules": "alert two"})
	a := e.running(rulesFrom(fromSecret("private-rules", "b.rules")))
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)

	keys := ruleKeys(e.configSecret())
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "rules.0.b.") {
		t.Fatalf("rule keys = %v, want only the named key b.rules", keys)
	}
	if got := string(e.configSecret().Data[keys[0]]); got != "alert two" {
		t.Errorf("rule file holds %q", got)
	}
}

func TestAgent_RulesKeepSourceAndKeyOrder(t *testing.T) {
	e := newAgentEnv(t)
	e.ruleConfigMap("second", map[string]string{"z.rules": "z", "a.rules": "a"})
	e.ruleSecret("first", map[string]string{"m.rules": "m"})
	e.running(rulesFrom(fromSecret("first"), fromConfigMap("second")))

	cfg := e.config()
	var seen []int
	for _, stem := range []string{"rules.0.m.", "rules.1.a.", "rules.1.z."} {
		i := strings.Index(cfg, "/etc/synapse/config/"+stem)
		if i < 0 {
			t.Fatalf("rule_paths lacks %s:\n%s", stem, cfg)
		}
		seen = append(seen, i)
	}
	if !(seen[0] < seen[1] && seen[1] < seen[2]) {
		t.Errorf("paths are not in source, then key order:\n%s", cfg)
	}
}

func TestAgent_RulesSourceWithNoMatchingKeyAddsNothing(t *testing.T) {
	e := newAgentEnv(t)
	e.ruleConfigMap("notes", map[string]string{"readme": "x"})
	a := e.running(rulesFrom(fromConfigMap("notes")))
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	if keys := ruleKeys(e.configSecret()); len(keys) != 0 {
		t.Errorf("rule keys = %v, want none", keys)
	}
}

func TestAgent_RulesSourceNotFound(t *testing.T) {
	e := newAgentEnv(t)
	a := e.agent(rulesFrom(fromConfigMap("late")))
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesSourceNotFound)
	if !strings.Contains(c.Message, `"late"`) || !strings.Contains(c.Message, "ConfigMap") {
		t.Errorf("message: %s", c.Message)
	}
	wantAgentCondition(t, a, ConditionReady, metav1.ConditionFalse, ReasonConfigInvalid)
	e.wantNothingApplied()

	e.ruleConfigMap("late", map[string]string{"a.rules": "alert"})
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	if keys := ruleKeys(e.configSecret()); len(keys) != 1 {
		t.Errorf("rule keys = %v after the source arrived", keys)
	}

	b := newAgentEnvIn(t, e.ns, "other")
	b.agent(rulesFrom(fromSecret("late")))
	ab := &synapsev1alpha1.SynapseAgent{}
	b.reconcile(ab)
	c = wantAgentCondition(t, ab, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesSourceNotFound)
	if !strings.Contains(c.Message, "Secret") {
		t.Errorf("message: %s", c.Message)
	}
}

func TestAgent_RulesSourceDeletedKeepsTheLastConfig(t *testing.T) {
	e := newAgentEnv(t)
	cm := e.ruleConfigMap("local-rules", map[string]string{"a.rules": "alert"})
	a := e.running(rulesFrom(fromConfigMap("local-rules")))
	secret := e.configSecret()

	if err := e.k8s.Delete(e.ctx, cm); err != nil {
		t.Fatal(err)
	}
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesSourceNotFound)
	kept := e.configSecret()
	if kept.ResourceVersion != secret.ResourceVersion {
		t.Error("the config Secret was written although a source is missing")
	}
	if len(ruleKeys(kept)) != 1 {
		t.Errorf("the last rules are gone: %v", ruleKeys(kept))
	}
}

func TestAgent_RulesKeyNotFound(t *testing.T) {
	e := newAgentEnv(t)
	e.ruleConfigMap("local-rules", map[string]string{"a.rules": "alert"})
	a := e.agent(rulesFrom(fromConfigMap("local-rules", "a.rules", "missing.rules")))
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesKeyNotFound)
	if !strings.Contains(c.Message, `"missing.rules"`) || !strings.Contains(c.Message, `"local-rules"`) {
		t.Errorf("message: %s", c.Message)
	}
	e.wantNothingApplied()
}

func TestAgent_RulesTooLarge(t *testing.T) {
	e := newAgentEnv(t)
	big := strings.Repeat("x", 500<<10)
	e.ruleConfigMap("one", map[string]string{"a.rules": big})
	e.ruleSecret("two", map[string]string{"b.rules": big})
	a := e.agent(rulesFrom(fromConfigMap("one"), fromSecret("two")))
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesTooLarge)
	if !strings.Contains(c.Message, "900 KiB") {
		t.Errorf("message: %s", c.Message)
	}
	e.wantNothingApplied()
}

func TestAgent_RulesTooManyFiles(t *testing.T) {
	e := newAgentEnv(t)
	files := map[string]string{}
	for i := 0; i < 257; i++ {
		files[fmt.Sprintf("r%03d.rules", i)] = "x"
	}
	e.ruleConfigMap("many", files)
	a := e.agent(rulesFrom(fromConfigMap("many")))
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesTooLarge)
	if !strings.Contains(c.Message, "257") || !strings.Contains(c.Message, "256") {
		t.Errorf("message names neither the count nor the limit: %s", c.Message)
	}
	e.wantNothingApplied()
}

// The Secret holds config.yaml as well, and the API server caps all of it.
func TestAgent_RulesAndConfigTogetherAreTooLarge(t *testing.T) {
	e := newAgentEnv(t)
	e.ruleConfigMap("big", map[string]string{"a.rules": strings.Repeat("x", 800<<10)})
	a := e.agent(rulesFrom(fromConfigMap("big")), func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(`{"ids":{"address_vars":{"CUSTOM":"` + strings.Repeat("y", 250000) + `"}}}`)}
	})
	e.reconcile(a)

	c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesTooLarge)
	if !strings.Contains(c.Message, "config.yaml") || !strings.Contains(c.Message, "rule files (819200 bytes)") {
		t.Errorf("message does not give both sizes: %s", c.Message)
	}
	e.wantNothingApplied()
}

func TestAgent_RulesMayNotReadAnAgentsConfigSecret(t *testing.T) {
	e := newAgentEnv(t)
	other := newAgentEnvIn(t, e.ns, "other")
	other.running()
	a := e.running()

	for _, name := range []string{e.name + "-config", "other-config"} {
		e.edit(a, rulesFrom(fromSecret(name)))
		e.reconcile(a)
		c := wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionFalse, ReasonRulesSourceInvalid)
		if !strings.Contains(c.Message, name) {
			t.Errorf("message does not name %s: %s", name, c.Message)
		}
	}
}

func TestAgent_RulesFromBinaryData(t *testing.T) {
	e := newAgentEnv(t)
	cm := e.ruleConfigMap("local-rules", map[string]string{"text.rules": "t"})
	change(t, e.k8s, cm, func() { cm.BinaryData = map[string][]byte{"bin.rules": []byte("b")} })

	a := e.running(rulesFrom(fromConfigMap("local-rules")))
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	if keys := ruleKeys(e.configSecret()); len(keys) != 2 {
		t.Errorf("rule keys = %v, want the text and the binary one", keys)
	}

	e.edit(a, rulesFrom(fromConfigMap("local-rules", "bin.rules")))
	e.reconcile(a)
	wantAgentCondition(t, a, ConditionConfigValid, metav1.ConditionTrue, ReasonRendered)
	keys := ruleKeys(e.configSecret())
	if len(keys) != 1 || string(e.configSecret().Data[keys[0]]) != "b" {
		t.Errorf("a named binaryData key was not found: %v", keys)
	}
}

type getCountingReader struct {
	client.Reader
	configMaps int
}

func (c *getCountingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok {
		c.configMaps++
	}
	return c.Reader.Get(ctx, key, obj, opts...)
}

// ConfigMaps are many and not the agent's, so they are not cached whole.
func TestAgent_RuleConfigMapsAreReadLive(t *testing.T) {
	e := newAgentEnv(t)
	live := &getCountingReader{Reader: e.k8s}
	e.agents.RuleReader = live
	e.ruleConfigMap("local-rules", map[string]string{"a.rules": "alert"})
	e.running(rulesFrom(fromConfigMap("local-rules")))
	if live.configMaps != 1 {
		t.Errorf("rule ConfigMap reads through the live reader = %d, want 1", live.configMaps)
	}
}
