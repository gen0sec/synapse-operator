package controllers

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// agentConfigDefaults are the settings the operator applies to a SynapseAgent
// unless spec.config says otherwise.
//
//go:embed agentconfig_defaults.yaml
var agentConfigDefaults []byte

// agentConfigDefaultsJSON is the same document, converted once. A defaults
// file that does not parse stops the operator when it starts, not at the
// first reconcile.
var agentConfigDefaultsJSON = func() []byte {
	out, err := yaml.YAMLToJSON(agentConfigDefaults)
	if err != nil {
		panic(fmt.Sprintf("agentconfig_defaults.yaml: %v", err))
	}
	if _, err := decodeConfigObject(out); err != nil {
		panic(fmt.Sprintf("agentconfig_defaults.yaml: %v", err))
	}
	return out
}()

// AgentConfigInput is everything a SynapseAgent's config.yaml is rendered
// from.
type AgentConfigInput struct {
	Agent *synapsev1alpha1.SynapseAgent
	// APIKey is the value spec.platform.apiKeySecretRef resolves to. It is
	// used only when that reference is set.
	APIKey string
	// HomeNet is the HOME_NET the operator computed from the cluster, in
	// Synapse's "[a,b]" form. spec.ids.homeNet wins over it. It comes from the
	// operator, not from a user, so it is not validated here.
	HomeNet string
}

// AgentConfigOutput is a rendered config.yaml. It can carry the platform API
// key, so it belongs in a Secret.
type AgentConfigOutput struct {
	YAML []byte
	// Hash identifies YAML.
	Hash string
	// RestartHash identifies YAML without the settings Synapse reloads from a
	// changed file. A change to those alone leaves it as it was, so it is
	// what the pod template carries.
	RestartHash string
}

// agentOwnedConfigKeys have exactly one writer each: a typed field of the
// spec, or the operator itself, which builds the pod around them. A key
// listed here is refused in spec.config rather than overridden.
var agentOwnedConfigKeys = []ownedConfigKey{
	{"mode", "the operator always runs a SynapseAgent in agent mode"},
	{"daemon.enabled", "the operator runs Synapse in the foreground"},
	{"proxy", "a SynapseAgent runs no proxy; use a SynapseProxy"},
	// Aliases Synapse accepts for the sections above and below. Beside the
	// operator's own `platform` they would be a duplicate key.
	{"pingora", "a SynapseAgent runs no proxy; use a SynapseProxy"},
	{"arxignis", "use the key platform"},
	// The older place for the IDS settings. Synapse ignores it whenever the
	// top-level `ids` section is there, and here it always is.
	{"firewall.ids", "use the top-level key ids"},
	{"logging.level", "use spec.logging.level"},
	{"platform.api_key", "use spec.platform.apiKeySecretRef"},
	{"platform.identity.enabled", "use spec.platform.identity.enabled"},
	{"platform.identity.cluster", "use spec.platform.identity.cluster"},
	{"network.ifaces", "use spec.capture.interfaces"},
	{"ids.enabled", "use spec.ids.enabled"},
	{"ids.enforce_block", "use spec.ids.enforceBlock"},
	{"ids.address_vars.HOME_NET", "use spec.ids.homeNet"},
}

// RenderAgentConfig renders the config.yaml of a SynapseAgent.
//
// Three layers, lowest first: the operator's defaults, spec.config, then the
// typed fields and the values the controller passes in. It is a pure
// function of its input, and the output is byte-for-byte stable.
//
// Any error means no config: the caller leaves what is running alone.
func RenderAgentConfig(in AgentConfigInput) (AgentConfigOutput, error) {
	spec := &in.Agent.Spec
	specPath := field.NewPath("spec")

	var rawJSON []byte
	if spec.Config != nil {
		rawJSON = spec.Config.Raw
	}
	raw, err := decodeConfigObject(rawJSON)
	if err != nil {
		return AgentConfigOutput{}, field.ErrorList{field.Invalid(specPath.Child("config"), "<config>", err.Error())}.ToAggregate()
	}

	errs := checkOwnedConfigKeysIn(agentOwnedConfigKeys, raw, specPath.Child("config"))

	apiKey := ""
	if spec.Platform != nil && spec.Platform.APIKeySecretRef != nil {
		apiKey = strings.TrimSpace(in.APIKey)
		ref := specPath.Child("platform", "apiKeySecretRef")
		switch {
		case apiKey == "":
			errs = append(errs, field.Invalid(ref, "", "the referenced Secret key is empty"))
		case !isPlainToken(apiKey):
			// The value is not repeated: it is, or was meant to be, a secret.
			errs = append(errs, field.Invalid(ref, "<redacted>", "the referenced Secret key holds whitespace or control characters, which an API key cannot"))
		}
	}

	homeNet := in.HomeNet
	if spec.IDS != nil && len(spec.IDS.HomeNet) > 0 {
		// HOME_NET is hot-swapped into running agents, so an entry that is
		// not an address or a CIDR (a comma, a bracket, "any") must not
		// reach the file. A bare address is written as it is.
		for i, entry := range spec.IDS.HomeNet {
			if !isAddressOrPrefix(entry) {
				errs = append(errs, field.Invalid(specPath.Child("ids", "homeNet").Index(i), entry, "must be an IP address or a CIDR"))
			}
		}
		homeNet = "[" + strings.Join(spec.IDS.HomeNet, ",") + "]"
	}
	if spec.Platform != nil && spec.Platform.Identity != nil && !isConfigText(spec.Platform.Identity.Cluster) {
		errs = append(errs, field.Invalid(specPath.Child("platform", "identity", "cluster"), "<text>", "holds control characters"))
	}
	if spec.Capture != nil {
		if network, ok := raw["network"].(map[string]any); ok && len(spec.Capture.Interfaces) > 0 && network["iface"] != nil {
			errs = append(errs, field.Forbidden(specPath.Child("config", "network", "iface"),
				"spec.capture.interfaces is set, and Synapse ignores network.iface beside network.ifaces; use spec.capture.interfaces, or remove network.iface"))
		}
		for i, name := range spec.Capture.Interfaces {
			if !isConfigText(name) {
				errs = append(errs, field.Invalid(specPath.Child("capture", "interfaces").Index(i), "<text>", "holds control characters"))
			}
		}
	}

	cfg, err := decodeConfigObject(agentConfigDefaultsJSON)
	if err != nil {
		return AgentConfigOutput{}, field.ErrorList{field.InternalError(specPath, err)}.ToAggregate()
	}
	errs = append(errs, mergeConfig(cfg, raw, specPath.Child("config"))...)
	if len(errs) > 0 {
		// A stable order: the errors are written to the resource's status,
		// which must not change from one render of the same spec to the next.
		sort.SliceStable(errs, func(i, j int) bool {
			if errs[i].Field != errs[j].Field {
				return errs[i].Field < errs[j].Field
			}
			return errs[i].Detail < errs[j].Detail
		})
		return AgentConfigOutput{}, errs.ToAggregate()
	}

	setConfig(cfg, "mode", "agent")
	setConfig(cfg, "daemon.enabled", false)
	if spec.Logging != nil && spec.Logging.Level != "" {
		setConfig(cfg, "logging.level", spec.Logging.Level)
	}
	if apiKey != "" {
		setConfig(cfg, "platform.api_key", apiKey)
	}
	if id := spec.Platform; id != nil && id.Identity != nil {
		if id.Identity.Enabled != nil {
			setConfig(cfg, "platform.identity.enabled", *id.Identity.Enabled)
		}
		if id.Identity.Cluster != "" {
			setConfig(cfg, "platform.identity.cluster", id.Identity.Cluster)
		}
	}
	if c := spec.Capture; c != nil {
		if len(c.Interfaces) > 0 {
			setConfig(cfg, "network.ifaces", c.Interfaces)
			// Synapse reads ifaces first and ignores iface when both are
			// set. Dropping iface leaves one place that names the interfaces.
			if network, ok := cfg["network"].(map[string]any); ok {
				delete(network, "iface")
			}
		}
	}
	if ids := spec.IDS; ids != nil {
		if ids.Enabled != nil {
			setConfig(cfg, "ids.enabled", *ids.Enabled)
		}
		if ids.EnforceBlock != nil {
			setConfig(cfg, "ids.enforce_block", *ids.EnforceBlock)
		}
	}
	if homeNet != "" {
		setConfig(cfg, "ids.address_vars.HOME_NET", homeNet)
	}

	body, err := yaml.Marshal(cfg)
	if err != nil {
		return AgentConfigOutput{}, errors.New(field.InternalError(specPath, err).Error())
	}
	out := append([]byte("# Generated by synapse-operator from a SynapseAgent. Do not edit.\n"), body...)
	sum := sha256.Sum256(out)
	hash := hex.EncodeToString(sum[:])
	restartHash := agentRestartHash(cfg)
	// With the IDS off no worker reloads the file, so nothing is hot.
	if ids, _ := cfg["ids"].(map[string]any); ids["enabled"] == false {
		restartHash = hash
	}
	return AgentConfigOutput{YAML: out, Hash: hash, RestartHash: restartHash}, nil
}

// agentRestartHash hashes the config without the hot-reloadable IDS fields.
// It works on the tree, not on the text: a round trip through float64 would
// give two integers above 2^53 the same hash.
func agentRestartHash(cfg map[string]any) string {
	cp := copyConfigTree(cfg).(map[string]any)
	if ids, ok := cp["ids"].(map[string]any); ok {
		stripHotReloadIdsFields(ids)
	}
	if fw, ok := cp["firewall"].(map[string]any); ok {
		stripHotReloadIdsFields(idsSubtree(fw, "ids"))
	}
	body, err := yaml.Marshal(cp)
	if err != nil {
		// The same tree marshalled for the output, so this does not happen;
		// an unstable hash only costs a restart.
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func copyConfigTree(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[k] = copyConfigTree(e)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = copyConfigTree(e)
		}
		return out
	default:
		return v
	}
}
