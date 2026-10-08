package controllers

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// Paths inside the proxy container. The rendered config points Synapse at
// them and the pod mounts them, so both sides take them from here.
const (
	proxyUpstreamsFile = "/etc/synapse/upstreams/upstreams.yaml"
	proxyCertsDir      = "/etc/synapse/certs"
)

// defaultInternalServicesPort is Synapse's own default for
// proxy.internal_services.port.
const defaultInternalServicesPort = 9180

// proxyConfigDefaults are the settings the operator applies unless
// spec.config says otherwise.
//
//go:embed proxyconfig_defaults.yaml
var proxyConfigDefaults []byte

// ProxyConfigInput is everything a SynapseProxy's config.yaml is rendered
// from.
type ProxyConfigInput struct {
	Proxy *synapsev1alpha1.SynapseProxy
	// APIKey is the value spec.platform.apiKeySecretRef resolves to. It is
	// used only when that reference is set.
	APIKey string
}

// ProxyConfigOutput is a rendered config.yaml. It can carry the platform API
// key, so it belongs in a Secret.
type ProxyConfigOutput struct {
	YAML []byte
	// Hash identifies YAML. It changes when, and only when, the pods need
	// the new file.
	Hash string
}

// ownedConfigKey is a Synapse config key spec.config may not set.
type ownedConfigKey struct {
	path string // dotted
	hint string // what to do instead
}

// ownedConfigKeys have exactly one writer each: a typed field of the spec, or
// the operator itself, which builds the pod around them. A key listed here is
// refused in spec.config rather than overridden, so the resource never says
// one thing while the proxy does another.
var ownedConfigKeys = []ownedConfigKey{
	{"mode", "the operator always runs a SynapseProxy in proxy mode"},
	{"daemon.enabled", "the operator runs Synapse in the foreground"},
	{"proxy.listeners", "use spec.listeners"},
	{"proxy.address_http", "use spec.listeners"},
	{"proxy.address_tls", "use spec.listeners"},
	{"proxy.certificates", "the operator mounts the certificates"},
	{"proxy.upstream.conf", "the operator mounts the routes"},
	// The whole section: a SynapseProxy takes its certificates from the
	// mounted Secret and never runs Synapse's built-in ACME client.
	{"proxy.acme", "a SynapseProxy does not use Synapse's built-in ACME client; issue certificates with cert-manager"},
	{"proxy.internal_services.enabled", "the health probes depend on the internal services"},
	{"proxy.tls_grade", "use spec.tls.grade"},
	{"proxy.trusted_proxies", "use spec.trustedProxies"},
	{"logging.level", "use spec.logging.level"},
	{"platform.api_key", "use spec.platform.apiKeySecretRef"},
	// Aliases Synapse accepts for the two sections above. Beside the
	// operator's own `proxy` and `platform` they would be a duplicate key.
	{"pingora", "use the key proxy"},
	{"arxignis", "use the key platform"},
}

// RenderProxyConfig renders the config.yaml of a SynapseProxy.
//
// Three layers, lowest first: the operator's defaults, spec.config, then the
// typed fields and the keys the operator owns. It is a pure function of its
// input, and the output is byte-for-byte stable: map keys are sorted, and
// numbers and strings come out exactly as spec.config wrote them.
//
// Any error means no config: the caller leaves what is running alone.
func RenderProxyConfig(in ProxyConfigInput) (ProxyConfigOutput, field.ErrorList) {
	spec := &in.Proxy.Spec
	specPath := field.NewPath("spec")

	raw, err := decodeConfigObject(rawConfigBytes(spec))
	if err != nil {
		return ProxyConfigOutput{}, field.ErrorList{field.Invalid(specPath.Child("config"), "<config>", err.Error())}
	}

	errs := checkOwnedConfigKeys(raw, specPath.Child("config"))
	for i, entry := range spec.TrustedProxies {
		if !isAddressOrPrefix(entry) {
			errs = append(errs, field.Invalid(specPath.Child("trustedProxies").Index(i), entry, "must be an IP address or a CIDR"))
		}
	}

	cfg, err := decodeConfigObject(mustYAMLToJSON(proxyConfigDefaults))
	if err != nil {
		panic(fmt.Sprintf("proxyconfig_defaults.yaml: %v", err))
	}
	mergeConfig(cfg, raw)

	internalPort := internalServicesPort(cfg)
	listeners := make([]any, 0, len(spec.Listeners))
	for i, l := range spec.Listeners {
		if int64(l.Port) == internalPort {
			errs = append(errs, field.Invalid(specPath.Child("listeners").Index(i).Child("port"), l.Port,
				"is the port of Synapse's internal services (proxy.internal_services.port)"))
		}
		listeners = append(listeners, map[string]any{
			"name":    l.Name,
			"address": fmt.Sprintf("0.0.0.0:%d", l.Port),
			"mode":    strings.ToLower(l.Protocol),
		})
	}
	if len(errs) > 0 {
		return ProxyConfigOutput{}, errs
	}

	setConfig(cfg, "mode", "proxy")
	setConfig(cfg, "daemon.enabled", false)
	setConfig(cfg, "proxy.listeners", listeners)
	setConfig(cfg, "proxy.certificates", proxyCertsDir)
	setConfig(cfg, "proxy.upstream.conf", proxyUpstreamsFile)
	setConfig(cfg, "proxy.internal_services.enabled", true)
	if spec.TLS != nil && spec.TLS.Grade != "" {
		setConfig(cfg, "proxy.tls_grade", spec.TLS.Grade)
	}
	if len(spec.TrustedProxies) > 0 {
		setConfig(cfg, "proxy.trusted_proxies", spec.TrustedProxies)
	}
	if spec.Logging != nil && spec.Logging.Level != "" {
		setConfig(cfg, "logging.level", spec.Logging.Level)
	}
	if spec.Platform != nil && spec.Platform.APIKeySecretRef != nil {
		setConfig(cfg, "platform.api_key", in.APIKey)
	}

	body, err := yaml.Marshal(cfg)
	if err != nil {
		return ProxyConfigOutput{}, field.ErrorList{field.InternalError(specPath, err)}
	}
	out := append([]byte("# Generated by synapse-operator from a SynapseProxy. Do not edit.\n"), body...)
	sum := sha256.Sum256(out)
	return ProxyConfigOutput{YAML: out, Hash: hex.EncodeToString(sum[:])}, nil
}

func rawConfigBytes(spec *synapsev1alpha1.SynapseProxySpec) []byte {
	if spec.Config == nil {
		return nil
	}
	return spec.Config.Raw
}

// decodeConfigObject decodes one JSON object, keeping numbers as written: a
// float64 would silently round a 64-bit byte limit.
func decodeConfigObject(data []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("is not valid JSON: %w", err)
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("must be an object")
	}
	return obj, nil
}

func mustYAMLToJSON(doc []byte) []byte {
	out, err := yaml.YAMLToJSON(doc)
	if err != nil {
		panic(err)
	}
	return out
}

// checkOwnedConfigKeys reports every owned key raw sets, and every place raw
// puts something other than an object where an owned key has to live.
func checkOwnedConfigKeys(raw map[string]any, base *field.Path) field.ErrorList {
	var errs field.ErrorList
	notObjects := map[string]bool{}
	for _, owned := range ownedConfigKeys {
		parts := strings.Split(owned.path, ".")
		cur, path := raw, base
		for i, part := range parts {
			path = path.Child(part)
			value, present := cur[part]
			if !present {
				break
			}
			if i == len(parts)-1 {
				errs = append(errs, field.Forbidden(path, owned.hint))
				break
			}
			if value == nil {
				break
			}
			next, isObject := value.(map[string]any)
			if !isObject {
				if !notObjects[path.String()] {
					notObjects[path.String()] = true
					errs = append(errs, field.Invalid(path, "<config>", "must be an object"))
				}
				break
			}
			cur = next
		}
	}
	return errs
}

// mergeConfig overlays src on dst. Objects merge key by key; anything else
// replaces. A null removes the key: Synapse has no use for a null, and for
// most keys it is a type error that stops the proxy from starting.
func mergeConfig(dst, src map[string]any) {
	for key, value := range src {
		switch v := value.(type) {
		case nil:
			delete(dst, key)
		case map[string]any:
			into, isObject := dst[key].(map[string]any)
			if !isObject {
				into = map[string]any{}
				dst[key] = into
			}
			mergeConfig(into, v)
		default:
			dst[key] = value
		}
	}
}

// setConfig sets a dotted key, creating the objects on the way. The owned-key
// check has already refused anything else in their place.
func setConfig(cfg map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		next, isObject := cfg[part].(map[string]any)
		if !isObject {
			next = map[string]any{}
			cfg[part] = next
		}
		cfg = next
	}
	cfg[parts[len(parts)-1]] = value
}

// internalServicesPort is the port the merged config gives the internal
// services, or Synapse's default.
func internalServicesPort(cfg map[string]any) int64 {
	proxy, _ := cfg["proxy"].(map[string]any)
	services, _ := proxy["internal_services"].(map[string]any)
	if n, isNumber := services["port"].(json.Number); isNumber {
		if port, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
			return port
		}
	}
	return defaultInternalServicesPort
}

func isAddressOrPrefix(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}
	_, err := netip.ParseAddr(s)
	return err == nil
}
