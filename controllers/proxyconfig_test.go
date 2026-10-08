package controllers

import (
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite the files under testdata/proxyconfig from the current output")

// proxyForConfig is a SynapseProxy with the smallest spec the API accepts.
func proxyForConfig(mutate ...func(*synapsev1alpha1.SynapseProxySpec)) *synapsev1alpha1.SynapseProxy {
	p := &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "synapse-os"},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:     "example.test/synapse:1.0.0",
			Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
		},
	}
	for _, m := range mutate {
		m(&p.Spec)
	}
	return p
}

func rawConfig(json string) func(*synapsev1alpha1.SynapseProxySpec) {
	return func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Config = &runtime.RawExtension{Raw: []byte(json)}
	}
}

func renderOK(t *testing.T, in ProxyConfigInput) ProxyConfigOutput {
	t.Helper()
	out, errs := RenderProxyConfig(in)
	if len(errs) > 0 {
		t.Fatalf("render: %v", errs.ToAggregate())
	}
	if len(out.YAML) == 0 || out.Hash == "" {
		t.Fatalf("render returned no config (yaml %d bytes, hash %q)", len(out.YAML), out.Hash)
	}
	return out
}

// renderedTree parses the output the way Synapse would see it.
func renderedTree(t *testing.T, in ProxyConfigInput) map[string]any {
	t.Helper()
	var tree map[string]any
	if err := yaml.Unmarshal(renderOK(t, in).YAML, &tree); err != nil {
		t.Fatalf("output is not YAML: %v", err)
	}
	return tree
}

func at(tree map[string]any, path string) (any, bool) {
	var cur any = tree
	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[key]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func wantAt(t *testing.T, tree map[string]any, path string, want any) {
	t.Helper()
	got, ok := at(tree, path)
	if !ok {
		t.Errorf("%s: absent, want %v", path, want)
		return
	}
	if got != want {
		t.Errorf("%s = %v (%T), want %v (%T)", path, got, got, want, want)
	}
}

func wantAbsent(t *testing.T, tree map[string]any, path string) {
	t.Helper()
	if got, ok := at(tree, path); ok {
		t.Errorf("%s = %v, want it absent", path, got)
	}
}

// errorAt reports whether errs holds an error of the given type at path.
func errorAt(errs field.ErrorList, typ field.ErrorType, path string) bool {
	return slices.ContainsFunc(errs, func(e *field.Error) bool { return e.Type == typ && e.Field == path })
}

func TestRenderProxyConfig_Golden(t *testing.T) {
	cases := map[string]ProxyConfigInput{
		"minimal": {Proxy: proxyForConfig()},
		"typed-fields": {
			APIKey: "k-123",
			Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
				s.Listeners = []synapsev1alpha1.Listener{
					{Name: "http", Port: 80, Protocol: "HTTP"},
					{Name: "https", Port: 443, Protocol: "TLS"},
				}
				s.TLS = &synapsev1alpha1.TLSSpec{Grade: "high"}
				s.TrustedProxies = []string{"10.0.0.0/8", "fd00::/8"}
				s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"}
				s.Platform = &synapsev1alpha1.PlatformSpec{
					APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "synapse-credentials", Key: "API_KEY"},
				}
			}),
		},
		"raw-config": {Proxy: proxyForConfig(rawConfig(`{
			"proxy": {"h2c": false, "redis": {"prefix": "edge"}, "max_downstream_connections": 20000},
			"telemetry": {"enabled": true, "otlp": {"endpoint": "http://collector:4317"}},
			"daemon": {"working_directory": null}
		}`))},
		// Values that a careless YAML round trip would change.
		"scalars-survive": {Proxy: proxyForConfig(rawConfig(`{
			"proxy": {"tunnel_max_bytes": 18446744073709551615, "tunnel_max_lifetime_secs": 9007199254740993},
			"telemetry": {"resource": {"attributes": {"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""}}}
		}`))},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			got := renderOK(t, in).YAML
			path := filepath.Join("testdata", "proxyconfig", name+".yaml")
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

// spec.config is JSON by the time the operator sees it, and config.yaml is
// YAML. Nothing may change meaning on the way: not a 64-bit limit, and not a
// string that YAML would read as something else.
func TestRenderProxyConfig_ScalarsKeepTheirValueAndType(t *testing.T) {
	in := ProxyConfigInput{Proxy: proxyForConfig(rawConfig(`{
		"proxy": {"tunnel_max_bytes": 18446744073709551615, "tunnel_max_lifetime_secs": 9007199254740993},
		"telemetry": {"resource": {"attributes": {"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""}}}
	}`))}

	text := string(renderOK(t, in).YAML)
	for _, line := range []string{"tunnel_max_bytes: 18446744073709551615\n", "tunnel_max_lifetime_secs: 9007199254740993\n"} {
		if !strings.Contains(text, line) {
			t.Errorf("output lacks %q:\n%s", line, text)
		}
	}

	tree := renderedTree(t, in)
	for key, want := range map[string]string{"a": "yes", "b": "null", "c": "1e3", "d": "0x10", "e": "007", "f": ""} {
		wantAt(t, tree, "telemetry.resource.attributes."+key, want)
	}
}

// Without these the pod the operator builds does not work: the mounts, the
// ports and the probes all assume them.
func TestRenderProxyConfig_SetsTheKeysThePodDependsOn(t *testing.T) {
	tree := renderedTree(t, ProxyConfigInput{Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Listeners = []synapsev1alpha1.Listener{
			{Name: "web", Port: 8080, Protocol: "HTTP"},
			{Name: "secure", Port: 8443, Protocol: "TLS"},
			{Name: "http", Port: 80, Protocol: "HTTP"},
		}
	})})

	wantAt(t, tree, "mode", "proxy")
	wantAt(t, tree, "daemon.enabled", false)
	wantAt(t, tree, "proxy.certificates", "/etc/synapse/certs")
	wantAt(t, tree, "proxy.upstream.conf", "/etc/synapse/upstreams/upstreams.yaml")
	// Certificates come from the mounted Secret. The built-in ACME client is
	// off by default in Synapse, so the section is not rendered at all.
	wantAbsent(t, tree, "proxy.acme")
	wantAt(t, tree, "proxy.internal_services.enabled", true)
	wantAbsent(t, tree, "proxy.address_http")
	wantAbsent(t, tree, "proxy.address_tls")

	// In spec order: a redirect to HTTPS targets the first TLS listener.
	got, _ := at(tree, "proxy.listeners")
	want := []any{
		map[string]any{"name": "web", "address": "0.0.0.0:8080", "mode": "http"},
		map[string]any{"name": "secure", "address": "0.0.0.0:8443", "mode": "tls"},
		map[string]any{"name": "http", "address": "0.0.0.0:80", "mode": "http"},
	}
	gotYAML, _ := yaml.Marshal(got)
	wantYAML, _ := yaml.Marshal(want)
	if string(gotYAML) != string(wantYAML) {
		t.Errorf("proxy.listeners =\n%s\nwant\n%s", gotYAML, wantYAML)
	}
}

func TestRenderProxyConfig_TypedFields(t *testing.T) {
	t.Run("unset fields leave Synapse's defaults alone", func(t *testing.T) {
		tree := renderedTree(t, ProxyConfigInput{Proxy: proxyForConfig()})
		wantAbsent(t, tree, "proxy.tls_grade")
		wantAbsent(t, tree, "proxy.trusted_proxies")
		wantAbsent(t, tree, "logging.level")
		wantAbsent(t, tree, "platform.api_key")
	})

	// An empty string is not "the default" to Synapse; it is a value.
	t.Run("a section given but left empty renders nothing", func(t *testing.T) {
		tree := renderedTree(t, ProxyConfigInput{Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
			s.TLS = &synapsev1alpha1.TLSSpec{}
			s.Logging = &synapsev1alpha1.LoggingSpec{}
			s.Platform = &synapsev1alpha1.PlatformSpec{}
			s.TrustedProxies = []string{}
		})})
		wantAbsent(t, tree, "proxy.tls_grade")
		wantAbsent(t, tree, "proxy.trusted_proxies")
		wantAbsent(t, tree, "logging.level")
		wantAbsent(t, tree, "platform.api_key")
	})

	t.Run("set fields are rendered", func(t *testing.T) {
		tree := renderedTree(t, ProxyConfigInput{
			APIKey: "k-123",
			Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
				s.TLS = &synapsev1alpha1.TLSSpec{Grade: "high"}
				s.TrustedProxies = []string{"10.0.0.0/8", "192.0.2.7"}
				s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"}
				s.Platform = &synapsev1alpha1.PlatformSpec{
					APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
				}
			}),
		})
		wantAt(t, tree, "proxy.tls_grade", "high")
		wantAt(t, tree, "logging.level", "debug")
		wantAt(t, tree, "platform.api_key", "k-123")
		got, _ := at(tree, "proxy.trusted_proxies")
		if list, ok := got.([]any); !ok || len(list) != 2 || list[0] != "10.0.0.0/8" || list[1] != "192.0.2.7" {
			t.Errorf("proxy.trusted_proxies = %v", got)
		}
	})

	// The key is only ever taken from the referenced Secret.
	t.Run("no API key without a reference", func(t *testing.T) {
		tree := renderedTree(t, ProxyConfigInput{APIKey: "stray", Proxy: proxyForConfig()})
		wantAbsent(t, tree, "platform.api_key")
	})
}

// A SynapseProxy with nothing but listeners has to behave like a default
// install of the chart did. These are the chart settings that differ from
// Synapse's built-in defaults; the list is literal on purpose.
func TestRenderProxyConfig_Defaults(t *testing.T) {
	tree := renderedTree(t, ProxyConfigInput{Proxy: proxyForConfig()})

	wantAt(t, tree, "daemon.working_directory", "/var/lib/synapse")
	wantAt(t, tree, "ids.enabled", true)
	wantAt(t, tree, "ids.enforce_block", true)
	wantAt(t, tree, "logging.file_logging_enabled", false)
	wantAt(t, tree, "platform.threat.url", "https://download.gen0sec.com/v1")
	wantAt(t, tree, "platform.threat.path", "/var/lib/synapse")
	wantAt(t, tree, "platform.geoip.country.url", "https://github.com/gen0sec/geoip-databases/raw/download/ipinfo_lite.mmdb")
	wantAt(t, tree, "platform.geoip.country.path", "/var/lib/synapse/ipinfo_lite.mmdb")
	wantAt(t, tree, "proxy.default_certificate", "default")
	wantAt(t, tree, "proxy.h2c", true)
	wantAt(t, tree, "proxy.captcha.token_ttl", float64(7200))
	wantAt(t, tree, "proxy.captcha.cache_ttl", float64(300))
	if got, _ := at(tree, "ids.rule_paths"); !slices.Equal(stringsOf(got), []string{"/etc/synapse/rules/*.rules"}) {
		t.Errorf("ids.rule_paths = %v", got)
	}

	// Synapse does not apply its per-key defaults to a section that is
	// missing altogether, so these two must be rendered whatever they hold.
	for _, section := range []string{"platform", "logging"} {
		if _, ok := at(tree, section); !ok {
			t.Errorf("no %s section", section)
		}
	}

	// Chart settings deliberately not carried over: a section only some
	// builds of Synapse know, a flag Synapse overwrites at start, and a
	// placeholder address for a Redis that is not there.
	wantAbsent(t, tree, "classifier")
	wantAbsent(t, tree, "platform.include_response_body")
	wantAbsent(t, tree, "proxy.redis")
}

func stringsOf(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func TestRenderProxyConfig_RawConfigMergesOverDefaults(t *testing.T) {
	tree := renderedTree(t, ProxyConfigInput{Proxy: proxyForConfig(rawConfig(`{
		"ids": {"enforce_block": false, "rule_paths": ["/custom/*.rules"]},
		"proxy": {"redis": {"url": "redis://cache:6379/0"}},
		"daemon": {"working_directory": null},
		"telemetry": {"enabled": true, "tracing": null}
	}`))})

	wantAt(t, tree, "ids.enforce_block", false)                // a default, overridden
	wantAt(t, tree, "ids.enabled", true)                       // its sibling default survives
	wantAt(t, tree, "proxy.redis.url", "redis://cache:6379/0") // a key the operator has no default for
	wantAt(t, tree, "proxy.h2c", true)                         // and the defaults beside it survive
	wantAbsent(t, tree, "daemon.working_directory")            // null removes a default
	wantAt(t, tree, "telemetry.enabled", true)                 // a section the operator knows nothing about
	wantAbsent(t, tree, "telemetry.tracing")                   // null is never passed on to Synapse
	wantAt(t, tree, "mode", "proxy")
	// A list replaces the default list; the two are not concatenated.
	if got, _ := at(tree, "ids.rule_paths"); !slices.Equal(stringsOf(got), []string{"/custom/*.rules"}) {
		t.Errorf("ids.rule_paths = %v", got)
	}
}

// The literal list is the contract. A key added to or dropped from the
// renderer's own table without a matching change here fails the test.
func TestRenderProxyConfig_RefusesKeysItOwns(t *testing.T) {
	owned := map[string]string{
		"mode":                            `"proxy"`,
		"daemon.enabled":                  `false`,
		"proxy.listeners":                 `[]`,
		"proxy.address_http":              `"0.0.0.0:80"`,
		"proxy.address_tls":               `"0.0.0.0:443"`,
		"proxy.certificates":              `"/certs"`,
		"proxy.upstream.conf":             `"/x/upstreams.yaml"`,
		"proxy.acme":                      `{"enabled": true}`,
		"proxy.internal_services.enabled": `false`,
		"proxy.tls_grade":                 `"unsafe"`,
		"proxy.trusted_proxies":           `["0.0.0.0/0"]`,
		"logging.level":                   `"trace"`,
		"platform.api_key":                `"plaintext"`,
		"pingora":                         `{}`,
		"arxignis":                        `{}`,
	}

	var tablePaths []string
	for _, k := range ownedConfigKeys {
		tablePaths = append(tablePaths, k.path)
	}
	slices.Sort(tablePaths)
	var listed []string
	for path := range owned {
		listed = append(listed, path)
	}
	slices.Sort(listed)
	if !slices.Equal(tablePaths, listed) {
		t.Fatalf("renderer owns %v\nthis test covers %v", tablePaths, listed)
	}

	nested := func(path, value string) string {
		parts := strings.Split(path, ".")
		doc := value
		for i := len(parts) - 1; i >= 0; i-- {
			doc = `{"` + parts[i] + `": ` + doc + `}`
		}
		return doc
	}

	for path, value := range owned {
		t.Run(path, func(t *testing.T) {
			out, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(rawConfig(nested(path, value)))})
			if !errorAt(errs, field.ErrorTypeForbidden, "spec.config."+path) {
				t.Errorf("no Forbidden error at spec.config.%s; got %v", path, errs)
			}
			if len(out.YAML) != 0 || out.Hash != "" {
				t.Error("a config was rendered despite the error")
			}
		})
	}

	// Setting the very value the operator would set is refused all the same:
	// the key has one owner, whatever the value.
	t.Run("even with the operator's own value", func(t *testing.T) {
		_, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(rawConfig(`{"mode": "proxy"}`))})
		if !errorAt(errs, field.ErrorTypeForbidden, "spec.config.mode") {
			t.Errorf("got %v", errs)
		}
	})

	// One pass reports every offending key, not just the first.
	t.Run("all at once", func(t *testing.T) {
		_, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(rawConfig(`{
			"mode": "agent",
			"proxy": {"certificates": "/certs", "tls_grade": "unsafe", "h2c": true},
			"pingora": {}
		}`))})
		for _, path := range []string{"mode", "proxy.certificates", "proxy.tls_grade", "pingora"} {
			if !errorAt(errs, field.ErrorTypeForbidden, "spec.config."+path) {
				t.Errorf("no error for %s", path)
			}
		}
		if len(errs) != 4 {
			t.Errorf("%d errors, want 4: %v", len(errs), errs)
		}
	})
}

func TestRenderProxyConfig_RefusesAScalarWhereItNeedsAnObject(t *testing.T) {
	for _, doc := range []string{`{"proxy": "x"}`, `{"proxy": {"upstream": 5}}`, `{"daemon": []}`} {
		t.Run(doc, func(t *testing.T) {
			out, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(rawConfig(doc))})
			if len(errs) != 1 || errs[0].Type != field.ErrorTypeInvalid || !strings.HasPrefix(errs[0].Field, "spec.config.") {
				t.Errorf("got %v, want one Invalid error under spec.config", errs)
			}
			if len(out.YAML) != 0 {
				t.Error("a config was rendered despite the error")
			}
		})
	}
}

func TestRenderProxyConfig_RefusesConfigThatIsNotAnObject(t *testing.T) {
	for _, doc := range []string{`"mode: proxy"`, `[1]`, `{`} {
		t.Run(doc, func(t *testing.T) {
			_, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(rawConfig(doc))})
			if !errorAt(errs, field.ErrorTypeInvalid, "spec.config") {
				t.Errorf("got %v, want an Invalid error at spec.config", errs)
			}
		})
	}
}

func TestRenderProxyConfig_TrustedProxies(t *testing.T) {
	t.Run("addresses and prefixes of both families", func(t *testing.T) {
		renderOK(t, ProxyConfigInput{Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
			s.TrustedProxies = []string{"10.0.0.0/8", "192.0.2.7", "fd00::/8", "2001:db8::1"}
		})})
	})

	// Synapse skips an entry it cannot parse and only logs it, which turns a
	// typo into a proxy that silently trusts nobody.
	t.Run("anything else is refused", func(t *testing.T) {
		_, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(func(s *synapsev1alpha1.SynapseProxySpec) {
			s.TrustedProxies = []string{"10.0.0.0/8", "10.0.0.0/33", "lb.example.test", "10.0.0.0/8 "}
		})})
		for _, path := range []string{"spec.trustedProxies[1]", "spec.trustedProxies[2]", "spec.trustedProxies[3]"} {
			if !errorAt(errs, field.ErrorTypeInvalid, path) {
				t.Errorf("no Invalid error at %s; got %v", path, errs)
			}
		}
		if len(errs) != 3 {
			t.Errorf("%d errors, want 3: %v", len(errs), errs)
		}
	})
}

// The internal services listen on loopback inside the same pod, so a public
// listener on that port cannot bind.
func TestRenderProxyConfig_ListenerMayNotTakeTheInternalServicesPort(t *testing.T) {
	listener := func(port int32) func(*synapsev1alpha1.SynapseProxySpec) {
		return func(s *synapsev1alpha1.SynapseProxySpec) { s.Listeners[0].Port = port }
	}

	_, errs := RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(listener(9180))})
	if !errorAt(errs, field.ErrorTypeInvalid, "spec.listeners[0].port") {
		t.Errorf("default internal port: got %v", errs)
	}

	moved := rawConfig(`{"proxy": {"internal_services": {"port": 9999}}}`)
	renderOK(t, ProxyConfigInput{Proxy: proxyForConfig(listener(9180), moved)})

	_, errs = RenderProxyConfig(ProxyConfigInput{Proxy: proxyForConfig(listener(9999), moved)})
	if !errorAt(errs, field.ErrorTypeInvalid, "spec.listeners[0].port") {
		t.Errorf("moved internal port: got %v", errs)
	}
}

func TestRenderProxyConfig_IsDeterministic(t *testing.T) {
	// The same document with its keys in two different orders.
	a := `{"telemetry": {"enabled": true, "otlp": {"endpoint": "e", "headers": {"x": "1", "y": "2"}}}, "proxy": {"h2c": false, "redis": {"prefix": "p"}}}`
	b := `{"proxy": {"redis": {"prefix": "p"}, "h2c": false}, "telemetry": {"otlp": {"headers": {"y": "2", "x": "1"}, "endpoint": "e"}, "enabled": true}}`

	first := renderOK(t, ProxyConfigInput{Proxy: proxyForConfig(rawConfig(a))})
	for range 20 {
		for _, doc := range []string{a, b} {
			out := renderOK(t, ProxyConfigInput{Proxy: proxyForConfig(rawConfig(doc))})
			if string(out.YAML) != string(first.YAML) || out.Hash != first.Hash {
				t.Fatalf("output changed between renders of the same config:\n%s\nvs\n%s", first.YAML, out.YAML)
			}
		}
	}
}

// The hash names the Secret the pods mount, so it decides when they roll.
func TestRenderProxyConfig_HashTracksTheRenderedConfigOnly(t *testing.T) {
	withKey := func(s *synapsev1alpha1.SynapseProxySpec) {
		s.Platform = &synapsev1alpha1.PlatformSpec{
			APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
		}
	}
	base := renderOK(t, ProxyConfigInput{APIKey: "k-1", Proxy: proxyForConfig(withKey)}).Hash

	moves := map[string]ProxyConfigInput{
		"a listener port": {APIKey: "k-1", Proxy: proxyForConfig(withKey, func(s *synapsev1alpha1.SynapseProxySpec) { s.Listeners[0].Port = 8080 })},
		"a typed field":   {APIKey: "k-1", Proxy: proxyForConfig(withKey, func(s *synapsev1alpha1.SynapseProxySpec) { s.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"} })},
		"raw config":      {APIKey: "k-1", Proxy: proxyForConfig(withKey, rawConfig(`{"proxy": {"h2c": false}}`))},
		"the API key":     {APIKey: "k-2", Proxy: proxyForConfig(withKey)},
	}
	for name, in := range moves {
		if got := renderOK(t, in).Hash; got == base {
			t.Errorf("changing %s left the hash at %s", name, got)
		}
	}

	replicas := int32(5)
	stays := map[string]func(*synapsev1alpha1.SynapseProxySpec){
		"replicas":  func(s *synapsev1alpha1.SynapseProxySpec) { s.Replicas = &replicas },
		"the image": func(s *synapsev1alpha1.SynapseProxySpec) { s.Image = "example.test/synapse:2.0.0" },
		"the Service": func(s *synapsev1alpha1.SynapseProxySpec) {
			s.Service = &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer}
		},
		"scheduling": func(s *synapsev1alpha1.SynapseProxySpec) {
			s.NodeSelector = map[string]string{"edge": "true"}
			s.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		},
	}
	for name, mutate := range stays {
		if got := renderOK(t, ProxyConfigInput{APIKey: "k-1", Proxy: proxyForConfig(withKey, mutate)}).Hash; got != base {
			t.Errorf("changing %s moved the hash; pods would roll for nothing", name)
		}
	}

	other := proxyForConfig(withKey)
	other.Name, other.Namespace = "other", "elsewhere"
	if got := renderOK(t, ProxyConfigInput{APIKey: "k-1", Proxy: other}).Hash; got != base {
		t.Error("the hash depends on the object's name")
	}
}
