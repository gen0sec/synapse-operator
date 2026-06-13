package controllers

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cmWithConfig(yaml string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "synapse-agent", Namespace: "synapse-os"},
		Data:       map[string]string{SynapseConfigKey: yaml},
	}
}

const baseAgentConfig = `mode: agent
ids:
  enabled: true
  capture_mode: xdp
  snaplen: 512
  rule_paths:
    - /etc/synapse/rules/a.rules
  address_vars:
    HOME_NET: "any"
    EXTERNAL_NET: "any"
  enforce_block: false
`

// Only HOME_NET/EXTERNAL_NET differ — a hot-reloadable change.
const homeNetChangedConfig = `mode: agent
ids:
  enabled: true
  capture_mode: xdp
  snaplen: 512
  rule_paths:
    - /etc/synapse/rules/a.rules
  address_vars:
    HOME_NET: "[10.0.0.0/8,167.235.217.46/32]"
    EXTERNAL_NET: "!$HOME_NET"
  enforce_block: true
`

// capture_mode differs — NOT hot-reloadable, must still roll.
const captureModeChangedConfig = `mode: agent
ids:
  enabled: true
  capture_mode: afpacket
  snaplen: 512
  rule_paths:
    - /etc/synapse/rules/a.rules
  address_vars:
    HOME_NET: "any"
    EXTERNAL_NET: "any"
  enforce_block: false
`

func TestConfigHash_ExcludeHotReloadIds_HomeNetChangeNoRoll(t *testing.T) {
	base := hashConfigMapContent(cmWithConfig(baseAgentConfig), nil, true)
	changed := hashConfigMapContent(cmWithConfig(homeNetChangedConfig), nil, true)
	if base == "" || changed == "" {
		t.Fatalf("empty hash: base=%q changed=%q", base, changed)
	}
	if base != changed {
		t.Errorf("HOME_NET/enforce_block change should NOT change the hash when excluded;\n base=%s\n chg =%s", base, changed)
	}
}

func TestConfigHash_ExcludeHotReloadIds_CaptureModeStillRolls(t *testing.T) {
	base := hashConfigMapContent(cmWithConfig(baseAgentConfig), nil, true)
	changed := hashConfigMapContent(cmWithConfig(captureModeChangedConfig), nil, true)
	if base == changed {
		t.Errorf("capture_mode change MUST change the hash (not hot-reloadable)")
	}
}

func TestConfigHash_ExcludeDisabled_HomeNetChangeRolls(t *testing.T) {
	// With the flag off, any config.yaml change (incl. HOME_NET) rolls.
	base := hashConfigMapContent(cmWithConfig(baseAgentConfig), nil, false)
	changed := hashConfigMapContent(cmWithConfig(homeNetChangedConfig), nil, false)
	if base == changed {
		t.Errorf("with exclusion disabled, HOME_NET change must change the hash")
	}
}

func TestCanonicalConfigSansHotReloadIds_StripsFields(t *testing.T) {
	out := canonicalConfigSansHotReloadIds(homeNetChangedConfig)
	for _, gone := range []string{"HOME_NET", "EXTERNAL_NET", "address_vars", "enforce_block", "rule_paths"} {
		if strings.Contains(out, gone) {
			t.Errorf("canonical form should not contain %q:\n%s", gone, out)
		}
	}
	// Non-hot-reloadable fields remain.
	for _, keep := range []string{"capture_mode", "snaplen"} {
		if !strings.Contains(out, keep) {
			t.Errorf("canonical form should retain %q:\n%s", keep, out)
		}
	}
}

func TestCanonicalConfig_UnparseableReturnedUnchanged(t *testing.T) {
	bad := "\tthis: : is not yaml: ["
	if got := canonicalConfigSansHotReloadIds(bad); got != bad {
		t.Errorf("unparseable config must be returned unchanged (fail-safe roll)")
	}
}
