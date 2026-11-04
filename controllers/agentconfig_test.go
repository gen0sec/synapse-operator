package controllers

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// agentForConfig is a SynapseAgent with the smallest spec the API accepts.
func agentForConfig(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) *synapsev1alpha1.SynapseAgent {
	a := &synapsev1alpha1.SynapseAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "node-agent", Namespace: "synapse-os"},
		Spec:       synapsev1alpha1.SynapseAgentSpec{Image: "example.test/synapse:1.0.0"},
	}
	for _, m := range mutate {
		m(&a.Spec)
	}
	return a
}

func withEve(e synapsev1alpha1.EveLogSpec) func(*synapsev1alpha1.SynapseAgentSpec) {
	return func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Logging = &synapsev1alpha1.AgentLoggingSpec{Eve: &e}
	}
}

func agentRawConfig(json string) func(*synapsev1alpha1.SynapseAgentSpec) {
	return func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(json)}
	}
}

func renderAgentOK(t *testing.T, in AgentConfigInput) AgentConfigOutput {
	t.Helper()
	out, err := RenderAgentConfig(in)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(out.YAML) == 0 || out.Hash == "" || out.RestartHash == "" {
		t.Fatalf("render returned no config (yaml %d bytes, hash %q, restart hash %q)", len(out.YAML), out.Hash, out.RestartHash)
	}
	return out
}

func agentTree(t *testing.T, in AgentConfigInput) map[string]any {
	t.Helper()
	var tree map[string]any
	if err := yaml.Unmarshal(renderAgentOK(t, in).YAML, &tree); err != nil {
		t.Fatalf("output is not YAML: %v", err)
	}
	return tree
}

func TestRenderAgentConfig_Golden(t *testing.T) {
	cases := map[string]AgentConfigInput{
		"minimal": {Agent: agentForConfig()},
		"typed-fields": {
			APIKey: "k-123",
			Agent: agentForConfig(func(s *synapsev1alpha1.SynapseAgentSpec) {
				s.Logging = &synapsev1alpha1.AgentLoggingSpec{Level: "debug"}
				s.Platform = &synapsev1alpha1.AgentPlatformSpec{
					APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "synapse-credentials", Key: "API_KEY"},
					Identity:        &synapsev1alpha1.AgentIdentitySpec{Enabled: boolPtr(true), Cluster: "prod-eu"},
				}
				s.Capture = &synapsev1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0", "eth1"}}
				s.IDS = &synapsev1alpha1.AgentIDSSpec{Enabled: boolPtr(true), EnforceBlock: boolPtr(true)}
			}),
		},
		"raw-config": {Agent: agentForConfig(agentRawConfig(`{
			"telemetry": {"enabled": true, "otlp": {"endpoint": "http://collector:4317"}},
			"ids": {"rule_paths": ["/etc/rules/*.rules"], "address_vars": {"CUSTOM": "10.1.0.0/16"}},
			"daemon": {"working_directory": null},
			"platform": {"identity": {"url": "https://example.test/identity.mmdb"}}
		}`))},
		"home-net": {
			HomeNet: "[10.42.0.0/16,192.168.1.5/32]",
			Agent: agentForConfig(func(s *synapsev1alpha1.SynapseAgentSpec) {
				s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8", "fd00::/8"}}
			}),
		},
		"eve-stdout": {Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true}))},
		"eve-stderr": {Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true, Output: "stderr"}))},
		"eve-file": {Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{
			Enabled: true, Output: "file", HostPath: "/var/log/synapse-agents", EventTypes: []string{"stats", "alert"},
		}))},
		"scalars-survive": {Agent: agentForConfig(agentRawConfig(`{
			"ids": {"max_flows": 18446744073709551615, "flow_timeout_secs": 9007199254740993},
			"telemetry": {"resource": {"attributes": {"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""}}}
		}`))},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			got := renderAgentOK(t, in).YAML
			path := filepath.Join("testdata", "agentconfig", name+".yaml")
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
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
		})
	}
}

func TestRenderAgentConfig_ScalarsKeepTheirValueAndType(t *testing.T) {
	in := AgentConfigInput{Agent: agentForConfig(agentRawConfig(`{
		"ids": {"max_flows": 18446744073709551615, "flow_timeout_secs": 9007199254740993},
		"telemetry": {"resource": {"attributes": {"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""}}}
	}`))}
	text := string(renderAgentOK(t, in).YAML)
	for _, line := range []string{"max_flows: 18446744073709551615\n", "flow_timeout_secs: 9007199254740993\n"} {
		if !strings.Contains(text, line) {
			t.Errorf("output lacks %q:\n%s", line, text)
		}
	}
	tree := agentTree(t, in)
	for key, want := range map[string]string{"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""} {
		wantAt(t, tree, "telemetry.resource.attributes."+key, want)
	}
}

func TestRenderAgentConfig_TypedFieldsAndInputs(t *testing.T) {
	tree := agentTree(t, AgentConfigInput{Agent: agentForConfig()})
	wantAt(t, tree, "mode", "agent")
	wantAt(t, tree, "daemon.enabled", false)
	wantAt(t, tree, "network.iface", "auto")
	wantAt(t, tree, "logging.level", "info")
	wantAbsent(t, tree, "ids.address_vars.HOME_NET")
	wantAbsent(t, tree, "platform.api_key")
	wantAbsent(t, tree, "geoip")
	wantAbsent(t, tree, "proxy")

	// capture.interfaces replaces iface, so only one key names the interfaces.
	tree = agentTree(t, AgentConfigInput{Agent: agentForConfig(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Capture = &synapsev1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0"}}
	})})
	wantAbsent(t, tree, "network.iface")
	if got, _ := at(tree, "network.ifaces"); fmt.Sprint(got) != "[eth0]" {
		t.Errorf("network.ifaces = %v, want [eth0]", got)
	}

	// The typed homeNet wins over the computed one.
	tree = agentTree(t, AgentConfigInput{HomeNet: "[1.2.3.4/32]", Agent: agentForConfig(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8"}}
	})})
	wantAt(t, tree, "ids.address_vars.HOME_NET", "[10.0.0.0/8]")
	tree = agentTree(t, AgentConfigInput{HomeNet: "[1.2.3.4/32]", Agent: agentForConfig()})
	wantAt(t, tree, "ids.address_vars.HOME_NET", "[1.2.3.4/32]")
	wantAt(t, tree, "ids.address_vars.EXTERNAL_NET", "!$HOME_NET")
}

func TestRenderAgentConfig_Eve(t *testing.T) {
	t.Run("disabled gives the bytes of no block", func(t *testing.T) {
		none := renderAgentOK(t, AgentConfigInput{Agent: agentForConfig()})
		off := renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: false, Output: "file"}))})
		if string(none.YAML) != string(off.YAML) || none.Hash != off.Hash || none.RestartHash != off.RestartHash {
			t.Errorf("a disabled EVE block changes the render:\n%s", off.YAML)
		}
	})
	t.Run("types render sorted and only when set", func(t *testing.T) {
		tree := agentTree(t, AgentConfigInput{Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true, EventTypes: []string{"stats", "alert"}}))})
		if got, _ := at(tree, "logging.eve_log.event_types"); fmt.Sprint(got) != "[alert stats]" {
			t.Errorf("event_types = %v, want [alert stats]", got)
		}
		tree = agentTree(t, AgentConfigInput{Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true}))})
		wantAbsent(t, tree, "logging.eve_log.event_types")
	})
	t.Run("stdout to stderr moves RestartHash", func(t *testing.T) {
		out := renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true, Output: "stdout"}))})
		err := renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(withEve(synapsev1alpha1.EveLogSpec{Enabled: true, Output: "stderr"}))})
		if out.RestartHash == err.RestartHash || out.Hash == err.Hash {
			t.Error("switching the EVE output did not change the hashes")
		}
	})
}

func TestRenderAgentConfig_OwnedKeys(t *testing.T) {
	cases := map[string]struct{ raw, path, hint string }{
		"mode":                      {`{"mode": "proxy"}`, "spec.config.mode", "agent mode"},
		"daemon.enabled":            {`{"daemon": {"enabled": true}}`, "spec.config.daemon.enabled", "foreground"},
		"proxy":                     {`{"proxy": {"h2c": true}}`, "spec.config.proxy", "SynapseProxy"},
		"pingora":                   {`{"pingora": {}}`, "spec.config.pingora", "SynapseProxy"},
		"arxignis":                  {`{"arxignis": {}}`, "spec.config.arxignis", "platform"},
		"firewall.ids":              {`{"firewall": {"ids": {"enabled": true}}}`, "spec.config.firewall.ids", "ids"},
		"logging.level":             {`{"logging": {"level": "debug"}}`, "spec.config.logging.level", "spec.logging.level"},
		"logging.eve_log":           {`{"logging": {"eve_log": {"file": "/tmp/x"}}}`, "spec.config.logging.eve_log", "spec.logging.eve"},
		"logging.eve_log.enabled":   {`{"logging": {"eve_log": {"enabled": true}}}`, "spec.config.logging.eve_log", "spec.logging.eve"},
		"platform.api_key":          {`{"platform": {"api_key": "k"}}`, "spec.config.platform.api_key", "apiKeySecretRef"},
		"platform.identity.enabled": {`{"platform": {"identity": {"enabled": true}}}`, "spec.config.platform.identity.enabled", "spec.platform.identity.enabled"},
		"platform.identity.cluster": {`{"platform": {"identity": {"cluster": "c"}}}`, "spec.config.platform.identity.cluster", "spec.platform.identity.cluster"},
		"network.ifaces":            {`{"network": {"ifaces": ["eth0"]}}`, "spec.config.network.ifaces", "spec.capture.interfaces"},
		"ids.enabled":               {`{"ids": {"enabled": false}}`, "spec.config.ids.enabled", "spec.ids.enabled"},
		"ids.enforce_block":         {`{"ids": {"enforce_block": true}}`, "spec.config.ids.enforce_block", "spec.ids.enforceBlock"},
		"ids.address_vars.HOME_NET": {`{"ids": {"address_vars": {"HOME_NET": "[10.0.0.0/8]"}}}`, "spec.config.ids.address_vars.HOME_NET", "spec.ids.homeNet"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(agentRawConfig(c.raw))})
			if err == nil {
				t.Fatal("render accepted an owned key")
			}
			if !strings.Contains(err.Error(), c.path) || !strings.Contains(err.Error(), c.hint) {
				t.Errorf("err = %v, want it to name %s and say %q", err, c.path, c.hint)
			}
		})
	}

	t.Run("network.ip_version passes through", func(t *testing.T) {
		tree := agentTree(t, AgentConfigInput{Agent: agentForConfig(agentRawConfig(`{"network": {"ip_version": "ipv4"}}`))})
		wantAt(t, tree, "network.ip_version", "ipv4")
	})

	t.Run("ids.address_vars.CUSTOM is allowed", func(t *testing.T) {
		tree := agentTree(t, AgentConfigInput{Agent: agentForConfig(agentRawConfig(`{"ids": {"address_vars": {"CUSTOM": "10.1.0.0/16"}}}`))})
		wantAt(t, tree, "ids.address_vars.CUSTOM", "10.1.0.0/16")
		wantAt(t, tree, "ids.address_vars.EXTERNAL_NET", "!$HOME_NET")
	})
}

func TestRenderAgentConfig_APIKey(t *testing.T) {
	withRef := func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Platform = &synapsev1alpha1.AgentPlatformSpec{
			APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
		}
	}
	for name, key := range map[string]string{"empty": "", "whitespace": " \n\t", "not a token": "abc def"} {
		t.Run(name, func(t *testing.T) {
			_, err := RenderAgentConfig(AgentConfigInput{APIKey: key, Agent: agentForConfig(withRef)})
			if err == nil || !strings.Contains(err.Error(), "spec.platform.apiKeySecretRef") {
				t.Fatalf("err = %v, want it to name spec.platform.apiKeySecretRef", err)
			}
			if key != "" && strings.Contains(err.Error(), "abc") {
				t.Errorf("err repeats the key: %v", err)
			}
		})
	}

	t.Run("trailing newline is trimmed", func(t *testing.T) {
		tree := agentTree(t, AgentConfigInput{APIKey: "k-123\n", Agent: agentForConfig(withRef)})
		wantAt(t, tree, "platform.api_key", "k-123")
	})
	t.Run("ignored without a reference", func(t *testing.T) {
		wantAbsent(t, agentTree(t, AgentConfigInput{APIKey: "k-123", Agent: agentForConfig()}), "platform.api_key")
	})
}

func TestRenderAgentConfig_RestartHash(t *testing.T) {
	render := func(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) AgentConfigOutput {
		return renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(mutate...)})
	}
	base := render()

	if again := render(); string(again.YAML) != string(base.YAML) || again.Hash != base.Hash || again.RestartHash != base.RestartHash {
		t.Error("two renders of the same input differ")
	}
	if base.Hash == base.RestartHash {
		t.Error("RestartHash equals Hash on a config that holds hot fields")
	}

	hot := map[string]func(*synapsev1alpha1.SynapseAgentSpec){
		"ids.rule_paths":        agentRawConfig(`{"ids": {"rule_paths": ["/other/*.rules"]}}`),
		"ids.address_vars":      agentRawConfig(`{"ids": {"address_vars": {"CUSTOM": "10.1.0.0/16"}}}`),
		"ids.port_vars":         agentRawConfig(`{"ids": {"port_vars": {"HTTP_PORTS": "80"}}}`),
		"ids.flow_timeout_secs": agentRawConfig(`{"ids": {"flow_timeout_secs": 99}}`),
		"ids.max_flows":         agentRawConfig(`{"ids": {"max_flows": 99}}`),
		"ids.enforce_block": func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.IDS = &synapsev1alpha1.AgentIDSSpec{EnforceBlock: boolPtr(true)}
		},
		"ids.address_vars.HOME_NET": func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: []string{"10.0.0.0/8"}}
		},
	}
	for name, mutate := range hot {
		t.Run("hot "+name, func(t *testing.T) {
			got := render(mutate)
			if got.Hash == base.Hash {
				t.Fatal("Hash did not change")
			}
			if got.RestartHash != base.RestartHash {
				t.Error("RestartHash changed on a hot-reloadable field")
			}
		})
	}

	// Synapse reads the same six fields from the older firewall.ids too. The
	// render refuses that key, so the rendered file is edited instead.
	withFirewallIDS := func(field, value string) string {
		var tree map[string]any
		if err := yaml.Unmarshal(base.YAML, &tree); err != nil {
			t.Fatal(err)
		}
		fw, _ := tree["firewall"].(map[string]any)
		fw["ids"] = map[string]any{field: value}
		out, err := yaml.Marshal(tree)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	for _, f := range hotReloadIdsFields {
		t.Run("hot firewall.ids."+f, func(t *testing.T) {
			one, two := withFirewallIDS(f, "1"), withFirewallIDS(f, "2")
			if one == two {
				t.Fatal("the two inputs are the same")
			}
			if canonicalConfigSansHotReloadIds(one) != canonicalConfigSansHotReloadIds(two) {
				t.Errorf("firewall.ids.%s counts as a restart field", f)
			}
		})
	}

	restart := map[string]func(*synapsev1alpha1.SynapseAgentSpec){
		"logging.level": func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.Logging = &synapsev1alpha1.AgentLoggingSpec{Level: "debug"}
		},
		"capture.interfaces": func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.Capture = &synapsev1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0"}}
		},
		"ids.enabled": func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.IDS = &synapsev1alpha1.AgentIDSSpec{Enabled: boolPtr(false)}
		},
		"ids.snaplen": agentRawConfig(`{"ids": {"snaplen": 1500}}`),
		"telemetry":   agentRawConfig(`{"telemetry": {"enabled": true}}`),
	}
	for name, mutate := range restart {
		t.Run("restart "+name, func(t *testing.T) {
			got := render(mutate)
			if got.Hash == base.Hash || got.RestartHash == base.RestartHash {
				t.Errorf("hash changed %v, restart hash changed %v; want both", got.Hash != base.Hash, got.RestartHash != base.RestartHash)
			}
		})
	}
}

// Fields under firewall.ids and ids that are not hot must still move
// RestartHash, or the strip would eat more than the six fields.
func TestRenderAgentConfig_RestartHashStripsOnlyTheHotSet(t *testing.T) {
	a := canonicalConfigSansHotReloadIds("firewall:\n  ids:\n    enabled: true\n    max_flows: 1\n")
	b := canonicalConfigSansHotReloadIds("firewall:\n  ids:\n    enabled: false\n    max_flows: 2\n")
	if a == b {
		t.Error("firewall.ids.enabled is stripped; it is not hot")
	}
}

func TestAgentConfigDefaults_UseOnlyKnownKeys(t *testing.T) {
	var defaults map[string]any
	if err := yaml.Unmarshal(agentConfigDefaults, &defaults); err != nil {
		t.Fatal(err)
	}
	// Top-level sections of Synapse's Config that the defaults may set.
	known := map[string]map[string]bool{
		"mode":         nil,
		"multi_thread": nil,
		"daemon":       {"enabled": true, "working_directory": true},
		"network":      {"iface": true},
		"firewall":     {"mode": true},
		"ids":          {"enabled": true, "enforce_block": true, "rule_paths": true, "address_vars": true},
		"logging":      {"level": true, "file_logging_enabled": true},
		"platform":     {"threat": true, "geoip": true},
	}
	for key, value := range defaults {
		keys, ok := known[key]
		if !ok {
			t.Errorf("defaults set top-level key %q, which Synapse ignores or the agent does not use", key)
			continue
		}
		if keys == nil {
			continue
		}
		section, _ := value.(map[string]any)
		for sub := range section {
			if !keys[sub] {
				t.Errorf("defaults set %s.%s, which is not a known key", key, sub)
			}
		}
	}
	if net, _ := defaults["network"].(map[string]any); net != nil {
		if _, has := net["disable_xdp"]; has {
			t.Error("network.disable_xdp is ignored by Synapse")
		}
	}
	for _, banned := range []string{"geoip", "proxy", "pingora", "arxignis"} {
		if _, has := defaults[banned]; has {
			t.Errorf("defaults set %q", banned)
		}
	}
}

func TestRenderAgentConfig_HomeNetEntries(t *testing.T) {
	render := func(entries ...string) (AgentConfigOutput, error) {
		return RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.IDS = &synapsev1alpha1.AgentIDSSpec{HomeNet: entries}
		})})
	}

	// A bare address is written as it is, not widened to /32 or /128.
	out, err := render("10.0.0.1", "10.0.0.0/8", "fd00::1", "fd00::/8")
	if err != nil {
		t.Fatalf("valid entries refused: %v", err)
	}
	if !strings.Contains(string(out.YAML), "HOME_NET: '[10.0.0.1,10.0.0.0/8,fd00::1,fd00::/8]'") {
		t.Errorf("HOME_NET not as written:\n%s", out.YAML)
	}

	for _, bad := range []string{"10.0.0.0/33", "a,b", "10.0.0.0/8,any", "[x]", "[10.0.0.0/8]", "!10.0.0.0/8", "", "any", " 10.0.0.0/8", "10.0.0.0/8 ", "fe80::1%eth0"} {
		t.Run(fmt.Sprintf("%q", bad), func(t *testing.T) {
			_, err := render("192.168.0.0/16", bad)
			if err == nil {
				t.Fatal("render accepted the entry")
			}
			if !strings.Contains(err.Error(), "spec.ids.homeNet[1]") {
				t.Errorf("err = %v, want it to name spec.ids.homeNet[1]", err)
			}
		})
	}
}

func TestRenderAgentConfig_RawIfaceWithCaptureInterfaces(t *testing.T) {
	withInterfaces := func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.Capture = &synapsev1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0"}}
	}
	raw := agentRawConfig(`{"network": {"iface": "eth1"}}`)

	_, err := RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(raw, withInterfaces)})
	if err == nil {
		t.Fatal("render accepted network.iface beside spec.capture.interfaces")
	}
	for _, want := range []string{"spec.config.network.iface", "spec.capture.interfaces"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to name %s", err, want)
		}
	}

	tree := agentTree(t, AgentConfigInput{Agent: agentForConfig(raw)})
	wantAt(t, tree, "network.iface", "eth1")
	wantAbsent(t, tree, "network.ifaces")
}

// With the IDS off nothing reloads the file in a running agent, so every
// change has to roll the pods.
func TestRenderAgentConfig_RestartHashWithIDSOff(t *testing.T) {
	off := func(homeNet ...string) func(*synapsev1alpha1.SynapseAgentSpec) {
		return func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.IDS = &synapsev1alpha1.AgentIDSSpec{Enabled: boolPtr(false), HomeNet: homeNet}
		}
	}
	render := func(mutate ...func(*synapsev1alpha1.SynapseAgentSpec)) AgentConfigOutput {
		return renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(mutate...)})
	}
	base := render(off())
	changed := render(off("10.0.0.0/8"))
	if changed.RestartHash == base.RestartHash {
		t.Error("a HOME_NET change with the IDS off did not move RestartHash")
	}
	if changed.RestartHash != changed.Hash {
		t.Error("with the IDS off RestartHash should equal Hash")
	}
	hot := render(off(), agentRawConfig(`{"ids": {"max_flows": 99}}`))
	if hot.RestartHash == base.RestartHash {
		t.Error("a hot field change with the IDS off did not move RestartHash")
	}

	on := render(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{Enabled: boolPtr(true), HomeNet: []string{"10.0.0.0/8"}}
	})
	onBase := render(func(s *synapsev1alpha1.SynapseAgentSpec) {
		s.IDS = &synapsev1alpha1.AgentIDSSpec{Enabled: boolPtr(true)}
	})
	if on.RestartHash != onBase.RestartHash {
		t.Error("a HOME_NET change with the IDS on moved RestartHash")
	}
}

// The restart hash is computed from the tree, not by a float64 round trip.
func TestRenderAgentConfig_RestartHashKeepsBigIntegers(t *testing.T) {
	render := func(raw string) AgentConfigOutput {
		return renderAgentOK(t, AgentConfigInput{Agent: agentForConfig(agentRawConfig(raw))})
	}
	for name, pair := range map[string][2]string{
		"ids.poll_timeout_ms": {`{"ids": {"poll_timeout_ms": 9007199254740993}}`, `{"ids": {"poll_timeout_ms": 9007199254740992}}`},
		"u64 max":             {`{"telemetry": {"limit": 18446744073709551615}}`, `{"telemetry": {"limit": 18446744073709551614}}`},
	} {
		t.Run(name, func(t *testing.T) {
			if render(pair[0]).RestartHash == render(pair[1]).RestartHash {
				t.Error("two different integers give the same RestartHash")
			}
		})
	}
}

func TestAgentRestartHash_StripsOnlyTheHotSet(t *testing.T) {
	tree := func(ids, fwIDS map[string]any) map[string]any {
		return map[string]any{"ids": ids, "firewall": map[string]any{"ids": fwIDS}}
	}
	for _, f := range hotReloadIdsFields {
		one := agentRestartHash(tree(map[string]any{f: "1"}, map[string]any{f: "1"}))
		two := agentRestartHash(tree(map[string]any{f: "2"}, map[string]any{f: "2"}))
		if one != two {
			t.Errorf("%s counts as a restart field", f)
		}
	}
	a := agentRestartHash(tree(map[string]any{"enabled": true, "max_flows": 1}, map[string]any{"enabled": true}))
	b := agentRestartHash(tree(map[string]any{"enabled": false, "max_flows": 2}, map[string]any{"enabled": true}))
	if a == b {
		t.Error("ids.enabled is stripped; it is not hot")
	}
	c := agentRestartHash(tree(map[string]any{"max_flows": 1}, map[string]any{"enabled": true}))
	d := agentRestartHash(tree(map[string]any{"max_flows": 1}, map[string]any{"enabled": false}))
	if c == d {
		t.Error("firewall.ids.enabled is stripped; it is not hot")
	}
	in := map[string]any{"max_flows": 1, "x": 2}
	agentRestartHash(tree(in, nil))
	if _, ok := in["max_flows"]; !ok {
		t.Error("agentRestartHash edited its input")
	}
}

func TestRenderAgentConfig_RefusesControlCharacters(t *testing.T) {
	cases := map[string]struct {
		mutate func(*synapsev1alpha1.SynapseAgentSpec)
		path   string
	}{
		"identity.cluster": {func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.Platform = &synapsev1alpha1.AgentPlatformSpec{Identity: &synapsev1alpha1.AgentIdentitySpec{Cluster: "prod\x01eu"}}
		}, "spec.platform.identity.cluster"},
		"capture.interfaces": {func(s *synapsev1alpha1.SynapseAgentSpec) {
			s.Capture = &synapsev1alpha1.AgentCaptureSpec{Interfaces: []string{"eth0", "eth\x011"}}
		}, "spec.capture.interfaces[1]"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(c.mutate)})
			if err == nil || !strings.Contains(err.Error(), c.path) {
				t.Errorf("err = %v, want it to name %s", err, c.path)
			}
		})
	}
}

func TestRenderAgentConfig_MergeErrors(t *testing.T) {
	t.Run("a null over a default object", func(t *testing.T) {
		_, err := RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(agentRawConfig(`{"ids": null}`))})
		if err == nil || !strings.Contains(err.Error(), "spec.config.ids") {
			t.Errorf("err = %v, want it to name spec.config.ids", err)
		}
	})
	t.Run("a key with a control character", func(t *testing.T) {
		_, err := RenderAgentConfig(AgentConfigInput{Agent: agentForConfig(agentRawConfig(`{"telemetry": {"a\u0001": 1}}`))})
		if err == nil || !strings.Contains(err.Error(), "spec.config.telemetry") {
			t.Errorf("err = %v, want it to name spec.config.telemetry", err)
		}
	})
	t.Run("the order of several errors is stable", func(t *testing.T) {
		raw := `{"ids": {"a\u0001": 1}, "logging": {"b\u0001": 1}, "platform": {"c\u0001": 1}, "network": {"d\u0001": 1}, "telemetry": {"e\u0001": 1}}`
		in := AgentConfigInput{Agent: agentForConfig(agentRawConfig(raw))}
		_, first := RenderAgentConfig(in)
		if first == nil {
			t.Fatal("render accepted the keys")
		}
		for i := 0; i < 30; i++ {
			if _, err := RenderAgentConfig(in); err == nil || err.Error() != first.Error() {
				t.Fatalf("render %d: %v, want %v", i, err, first)
			}
		}
	})
}
