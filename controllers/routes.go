package controllers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Annotation support for synapse upstream settings.
//
// Annotations are read from the Ingress / HTTPRoute object and applied
// to every route that object contributes (the standard per-object
// ingress-controller convention). Recognized keys (prefix
// `synapse.gen0sec.com/`), with a small `nginx.ingress.kubernetes.io/`
// compatibility subset:
//
//   backend-protocol     HTTP|HTTPS  -> ssl_enabled (TLS to upstream)
//   http2                bool        -> http2_enabled
//   force-https|ssl-redirect bool    -> https_proxy_enabled (force_https)
//   connect-timeout      uint(sec)   -> connection_timeout
//   read-timeout         uint(sec)   -> read_timeout
//   write-timeout        uint(sec)   -> write_timeout
//   idle-timeout         uint(sec)   -> idle_timeout
//   healthcheck          bool        -> healthcheck
//   disable-access-log   bool        -> disable_access_log
//   request-headers      csv         -> request_headers[]
//   response-headers     csv         -> response_headers[]
//   sticky-sessions      bool        -> top-level sticky_sessions
//   proxy-body-size      size        -> max_body_size (per-route 413 cap)
//   server-alias         csv(hosts)  -> duplicate route under each alias
//   ssl-passthrough      bool        -> v2 tls.passthrough (SNI-route TCP)
//   permanent-redirect   url         -> redirect { status: 301, location }
//   permanent-redirect-code u16      -> override 301 (e.g. 308)
//   temporal-redirect    url         -> redirect { status: 302, location }
//   temporal-redirect-code  u16      -> override 302 (e.g. 307)
//
// ssl-passthrough switches the emitted file from v1 to v2 schema for
// the WHOLE FILE (the v1 schema has no passthrough representation; the
// v2 parser is schema-version-strict). Every other Ingress in the
// cluster is then re-rendered in v2 form too. The v1 → v2 fidelity
// for terminate hosts depends on synapse-utils carrying per-route
// ssl_enabled, http2_enabled, disable_access_log, and max_body_size
// in its v2 RawRoute (synapse PR feat(upstreams_v2): per-route v1-
// compat knobs).
//
// Sizes for proxy-body-size accept nginx-style suffixes: "50m" = 50MiB,
// "1g" = 1GiB, "1024k" = 1024KiB, bare bytes (e.g. "1048576"). Synapse
// enforces with a Content-Length pre-check and a streaming counter; over
// the cap returns 413 Payload Too Large. nginx-compat key
// `nginx.ingress.kubernetes.io/proxy-body-size` is accepted as a fallback.
//
// All map onto fields the synapse legacy v1 upstreams schema already
// supports (synapse-utils structs.rs). HTTPRoute backendRef weights
// and Request/ResponseHeaderModifier filters are translated too;
// URLRewrite has no equivalent in the v1 per-path schema and is left
// unmodified (not silently faked).

const (
	annPrefix   = "synapse.gen0sec.com/"
	nginxPrefix = "nginx.ingress.kubernetes.io/"
)

// backend is one upstream server. weight 0 ⇒ unweighted (rendered as
// the bare "addr" string form); >0 ⇒ weighted object form.
type backend struct {
	addr   string
	weight uint32
}

// routeCfg is the resolved per-(host,path) upstream configuration.
type routeCfg struct {
	servers          []backend
	ssl              *bool
	http2            *bool
	forceHTTPS       *bool
	healthcheck      *bool
	disableAccessLog *bool
	connectTimeout   *uint64
	readTimeout      *uint64
	writeTimeout     *uint64
	idleTimeout      *uint64
	maxBodySize      *uint64
	reqHeaders       []string
	respHeaders      []string
	// redirect, when set, makes this route a 3xx short-circuit. Status is
	// 301 (permanent) or 302 (temporal) by default; *-code annotations
	// override. Mutually exclusive with rewriting/forwarding at the proxy:
	// when set, synapse writes the redirect and never contacts upstream.
	redirectStatus   *uint64
	redirectLocation string
}

// annSettings is the subset parsed from an object's annotations,
// applied onto each routeCfg that object contributes.
type annSettings struct {
	ssl              *bool
	http2            *bool
	forceHTTPS       *bool
	healthcheck      *bool
	disableAccessLog *bool
	connectTimeout   *uint64
	readTimeout      *uint64
	writeTimeout     *uint64
	idleTimeout      *uint64
	maxBodySize      *uint64
	reqHeaders       []string
	respHeaders      []string
	// serverAliases are extra hostnames the route should also answer on,
	// programmed under each alias with the same backend + settings (first-
	// writer-wins on alias collisions, same as primary-host conflicts).
	// Matches nginx-ingress server-alias semantics: cert binding is NOT
	// inferred — list the alias in spec.tls[].hosts to get TLS for it.
	serverAliases []string
	// passthrough, when true, makes every rule host on this Ingress an
	// SNI-routed TCP passthrough host (synapse terminates nothing for
	// these hosts). Triggers v2 schema emission for the whole file.
	passthrough bool
	// redirectStatus + redirectLocation drive the per-route redirect
	// short-circuit. Populated by parseAnnotations from
	// permanent-redirect / temporal-redirect (+ -code variants). When
	// non-empty, addRoute writes a redirect-only route block.
	redirectStatus   *uint64
	redirectLocation string
	sticky           bool
}

// certProjection is one TLS Secret to materialize into the synapse
// certificates dir as <stem>.crt/<stem>.key.
type certProjection struct {
	stem string // file-stem == synapse cert name (usually the host)
	ns   string // Secret namespace
	name string // Secret name
}

type renderModel struct {
	hosts map[string]map[string]*routeCfg
	acme  string
	// hostCert maps a host to a cert file-stem, emitted as the
	// per-host `certificate:` in upstreams.yaml (synapse SNI
	// precedence #1: upstreams_cert_map).
	hostCert map[string]string
	// certProjections is the set of Secrets to write into the
	// certificates dir (keyed by stem, first-writer-wins).
	certProjections map[string]certProjection
	// passthroughHosts maps SNI → upstream "addr:port". A non-empty
	// map triggers v2 schema emission for the whole file (v1 has no
	// passthrough representation). FIRST-WRITER-WINS against both
	// terminate hosts in `hosts` and other passthrough entries.
	passthroughHosts map[string]string
	sticky           bool
}

func newRenderModel() *renderModel {
	return &renderModel{
		hosts:            map[string]map[string]*routeCfg{},
		hostCert:         map[string]string{},
		certProjections:  map[string]certProjection{},
		passthroughHosts: map[string]string{},
	}
}

// addCert records a host→Secret TLS binding: it schedules the Secret
// for projection as <stem>.crt/<stem>.key and (when host != "")
// binds the host to that cert name in upstreams.yaml. First-writer-
// wins per stem and per host so output is deterministic.
func (m *renderModel) addCert(host, stem, ns, name string) {
	if stem == "" || ns == "" || name == "" {
		return
	}
	if _, ok := m.certProjections[stem]; !ok {
		m.certProjections[stem] = certProjection{stem: stem, ns: ns, name: name}
	}
	if host != "" {
		if _, ok := m.hostCert[host]; !ok {
			m.hostCert[host] = stem
		}
	}
}

// addRoute records host/path → servers with the object's annotation
// settings. FIRST-WRITER-WINS: if (host,path) is already set it is
// NOT overwritten; returns false so the caller can log a deterministic
// conflict (sources are iterated in a stable order — Ingresses, then
// HTTPRoutes, each sorted by namespace/name — so Ingress beats Gateway
// and the result is reproducible regardless of informer ordering).
// A host already claimed for SNI passthrough is also "taken" — adding
// a terminate route on top of it would silently lose either side.
func (m *renderModel) addRoute(host, path string, servers []backend, a annSettings, extraReq, extraResp []string) bool {
	if _, claimed := m.passthroughHosts[host]; claimed {
		return false
	}
	if m.hosts[host] == nil {
		m.hosts[host] = map[string]*routeCfg{}
	}
	if _, exists := m.hosts[host][path]; exists {
		return false
	}
	rc := &routeCfg{
		servers:          servers,
		ssl:              a.ssl,
		http2:            a.http2,
		forceHTTPS:       a.forceHTTPS,
		healthcheck:      a.healthcheck,
		disableAccessLog: a.disableAccessLog,
		connectTimeout:   a.connectTimeout,
		readTimeout:      a.readTimeout,
		writeTimeout:     a.writeTimeout,
		idleTimeout:      a.idleTimeout,
		maxBodySize:      a.maxBodySize,
		reqHeaders:       append(append([]string{}, a.reqHeaders...), extraReq...),
		respHeaders:      append(append([]string{}, a.respHeaders...), extraResp...),
		redirectStatus:   a.redirectStatus,
		redirectLocation: a.redirectLocation,
	}
	m.hosts[host][path] = rc
	if a.sticky {
		m.sticky = true
	}
	return true
}

// addPassthroughHost claims `host` as an SNI-routed TCP passthrough
// upstream. FIRST-WRITER-WINS: returns false if any terminate route
// already exists for the host, or another passthrough is already
// registered. Triggers v2 schema emission for the whole file.
func (m *renderModel) addPassthroughHost(host, upstream string) bool {
	if host == "" || upstream == "" {
		return false
	}
	if _, claimed := m.passthroughHosts[host]; claimed {
		return false
	}
	if existing, ok := m.hosts[host]; ok && len(existing) > 0 {
		return false
	}
	m.passthroughHosts[host] = upstream
	return true
}

func parseBool(s string) *bool {
	v := strings.EqualFold(strings.TrimSpace(s), "true")
	if !v && !strings.EqualFold(strings.TrimSpace(s), "false") {
		return nil
	}
	return &v
}

func parseUint(s string) *uint64 {
	if n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err == nil {
		return &n
	}
	return nil
}

// parseSize parses an nginx-style size string: bare bytes ("1048576"),
// or with a single suffix k/K, m/M, g/G (case-insensitive) for KiB/MiB/GiB.
// Whitespace tolerated. Returns nil on any malformed input — same contract
// as parseUint / parseBool, so an invalid annotation silently fails to
// register (matching the rest of the parser's "best-effort" semantics).
// Overflow on the multiplier is treated as malformed.
func parseSize(s string) *uint64 {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil
	}
	mul := uint64(1)
	last := t[len(t)-1]
	switch last {
	case 'k', 'K':
		mul = 1024
		t = t[:len(t)-1]
	case 'm', 'M':
		mul = 1024 * 1024
		t = t[:len(t)-1]
	case 'g', 'G':
		mul = 1024 * 1024 * 1024
		t = t[:len(t)-1]
	}
	n, err := strconv.ParseUint(strings.TrimSpace(t), 10, 64)
	if err != nil {
		return nil
	}
	if mul > 1 && n > (^uint64(0))/mul {
		// would overflow the multiplication; treat as malformed
		return nil
	}
	out := n * mul
	return &out
}

func csv(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// parseAnnotations maps recognized annotations to upstream settings.
// `synapse.gen0sec.com/<k>` takes precedence over the nginx-compat key.
func parseAnnotations(ann map[string]string) annSettings {
	if ann == nil {
		return annSettings{}
	}
	get := func(k string) (string, bool) {
		if v, ok := ann[annPrefix+k]; ok {
			return v, true
		}
		return "", false
	}
	var s annSettings

	// backend-protocol: HTTPS ⇒ TLS to upstream. synapse-compat key
	// first, then nginx-compat.
	bp := ""
	if v, ok := get("backend-protocol"); ok {
		bp = v
	} else if v, ok := ann[nginxPrefix+"backend-protocol"]; ok {
		bp = v
	}
	if bp != "" {
		t := strings.EqualFold(bp, "HTTPS")
		s.ssl = &t
	}
	if v, ok := get("http2"); ok {
		s.http2 = parseBool(v)
	}
	if v, ok := get("force-https"); ok {
		s.forceHTTPS = parseBool(v)
	} else if v, ok := get("ssl-redirect"); ok {
		s.forceHTTPS = parseBool(v)
	} else if v, ok := ann[nginxPrefix+"ssl-redirect"]; ok {
		s.forceHTTPS = parseBool(v)
	}
	if v, ok := get("healthcheck"); ok {
		s.healthcheck = parseBool(v)
	}
	if v, ok := get("disable-access-log"); ok {
		s.disableAccessLog = parseBool(v)
	}
	if v, ok := get("connect-timeout"); ok {
		s.connectTimeout = parseUint(v)
	}
	if v, ok := get("read-timeout"); ok {
		s.readTimeout = parseUint(v)
	}
	if v, ok := get("write-timeout"); ok {
		s.writeTimeout = parseUint(v)
	}
	if v, ok := get("idle-timeout"); ok {
		s.idleTimeout = parseUint(v)
	}
	// proxy-body-size: per-route 413 cap (synapse enforces in synapse-proxy).
	// synapse-compat key first, then the nginx-compat key as a fallback so
	// existing ingress-nginx annotations migrate cleanly.
	if v, ok := get("proxy-body-size"); ok {
		s.maxBodySize = parseSize(v)
	} else if v, ok := ann[nginxPrefix+"proxy-body-size"]; ok {
		s.maxBodySize = parseSize(v)
	}
	if v, ok := get("request-headers"); ok {
		s.reqHeaders = csv(v)
	}
	if v, ok := get("response-headers"); ok {
		s.respHeaders = csv(v)
	}
	// server-alias: extra hostnames the route also answers on. nginx-compat
	// fallback honored. Bare commas, whitespace tolerated (csv() trims).
	if v, ok := get("server-alias"); ok {
		s.serverAliases = csv(v)
	} else if v, ok := ann[nginxPrefix+"server-alias"]; ok {
		s.serverAliases = csv(v)
	}
	// ssl-passthrough: route TLS connections straight to a TCP upstream
	// without termination. Triggers v2 schema emission. synapse-compat
	// key first, then nginx-compat.
	if v, ok := get("ssl-passthrough"); ok {
		if b := parseBool(v); b != nil {
			s.passthrough = *b
		}
	} else if v, ok := ann[nginxPrefix+"ssl-passthrough"]; ok {
		if b := parseBool(v); b != nil {
			s.passthrough = *b
		}
	}
	// permanent-redirect / temporal-redirect (+ -code variants). nginx
	// semantics: permanent-redirect default 301; temporal-redirect
	// default 302; -code overrides the default. If both permanent and
	// temporal are set on the same Ingress, permanent wins (stronger
	// commitment). synapse-prefixed keys take precedence over nginx-
	// compat keys via the get() helper.
	pickRedirect := func(urlKey, codeKey string, defaultStatus uint64) {
		var loc string
		if v, ok := get(urlKey); ok {
			loc = strings.TrimSpace(v)
		} else if v, ok := ann[nginxPrefix+urlKey]; ok {
			loc = strings.TrimSpace(v)
		}
		if loc == "" {
			return
		}
		status := defaultStatus
		var codeStr string
		if v, ok := get(codeKey); ok {
			codeStr = v
		} else if v, ok := ann[nginxPrefix+codeKey]; ok {
			codeStr = v
		}
		if codeStr != "" {
			if n := parseUint(codeStr); n != nil {
				status = *n
			}
		}
		s.redirectLocation = loc
		s.redirectStatus = &status
	}
	// temporal first so permanent can clobber it (permanent wins on tie)
	pickRedirect("temporal-redirect", "temporal-redirect-code", 302)
	pickRedirect("permanent-redirect", "permanent-redirect-code", 301)
	if v, ok := get("sticky-sessions"); ok {
		if b := parseBool(v); b != nil {
			s.sticky = *b
		}
	}
	return s
}

// renderUpstreams emits the synapse legacy v1 schema. The ACME
// challenge backend is an `internal_paths` override (plain HTTP, no
// knobs — it is cert-manager's solver). Deterministic ordering keeps
// output stable so writeIfChanged avoids spurious reloads.
func renderUpstreams(m *renderModel) string {
	var b strings.Builder
	b.WriteString("# Generated by synapse-operator ingress controller. Do not edit.\n")
	if m.acme != "" {
		fmt.Fprintf(&b, "internal_paths:\n  \"/.well-known/acme-challenge/*\":\n    servers:\n      - %q\n    ssl_enabled: false\n", m.acme)
	}
	if m.sticky {
		b.WriteString("sticky_sessions: true\n")
	}
	b.WriteString("upstreams:\n")
	hostKeys := make([]string, 0, len(m.hosts))
	for h := range m.hosts {
		hostKeys = append(hostKeys, h)
	}
	sort.Strings(hostKeys)
	for _, h := range hostKeys {
		fmt.Fprintf(&b, "  %q:\n", h)
		if stem := m.hostCert[h]; stem != "" {
			fmt.Fprintf(&b, "    certificate: %q\n", stem)
		}
		b.WriteString("    paths:\n")
		paths := m.hosts[h]
		pathKeys := make([]string, 0, len(paths))
		for p := range paths {
			pathKeys = append(pathKeys, p)
		}
		sort.Strings(pathKeys)
		for _, p := range pathKeys {
			rc := paths[p]
			fmt.Fprintf(&b, "      %q:\n        servers:\n", p)
			for _, sv := range rc.servers {
				if sv.weight > 0 {
					fmt.Fprintf(&b, "          - { address: %q, weight: %d }\n", sv.addr, sv.weight)
				} else {
					fmt.Fprintf(&b, "          - %q\n", sv.addr)
				}
			}
			// ssl_enabled: explicit annotation wins; default false
			// (plain HTTP to backend — unchanged prior behavior).
			ssl := false
			if rc.ssl != nil {
				ssl = *rc.ssl
			}
			fmt.Fprintf(&b, "        ssl_enabled: %t\n", ssl)
			if rc.http2 != nil {
				fmt.Fprintf(&b, "        http2_enabled: %t\n", *rc.http2)
			}
			if rc.forceHTTPS != nil {
				fmt.Fprintf(&b, "        https_proxy_enabled: %t\n", *rc.forceHTTPS)
			}
			if rc.healthcheck != nil {
				fmt.Fprintf(&b, "        healthcheck: %t\n", *rc.healthcheck)
			}
			if rc.disableAccessLog != nil {
				fmt.Fprintf(&b, "        disable_access_log: %t\n", *rc.disableAccessLog)
			}
			if rc.connectTimeout != nil {
				fmt.Fprintf(&b, "        connection_timeout: %d\n", *rc.connectTimeout)
			}
			if rc.readTimeout != nil {
				fmt.Fprintf(&b, "        read_timeout: %d\n", *rc.readTimeout)
			}
			if rc.writeTimeout != nil {
				fmt.Fprintf(&b, "        write_timeout: %d\n", *rc.writeTimeout)
			}
			if rc.idleTimeout != nil {
				fmt.Fprintf(&b, "        idle_timeout: %d\n", *rc.idleTimeout)
			}
			if rc.maxBodySize != nil {
				fmt.Fprintf(&b, "        max_body_size: %d\n", *rc.maxBodySize)
			}
			if rc.redirectStatus != nil && rc.redirectLocation != "" {
				fmt.Fprintf(&b, "        redirect:\n          status: %d\n          location: %q\n",
					*rc.redirectStatus, rc.redirectLocation)
			}
			writeHeaderList(&b, "request_headers", rc.reqHeaders)
			writeHeaderList(&b, "response_headers", rc.respHeaders)
		}
	}
	return b.String()
}

func writeHeaderList(b *strings.Builder, key string, hs []string) {
	if len(hs) == 0 {
		return
	}
	fmt.Fprintf(b, "        %s:\n", key)
	for _, h := range hs {
		fmt.Fprintf(b, "          - %q\n", h)
	}
}

// renderUpstreamsV2 emits the v2 synapse upstreams schema. Used when
// at least one host is SNI passthrough — v1 has no passthrough
// representation, so the whole file switches to v2. Terminate hosts
// are emitted with full per-route fidelity (ssl_enabled, http2_enabled,
// disable_access_log, max_body_size, force_https, timeouts, headers),
// which requires synapse's RawRoute to carry those four knobs (see
// synapse feat(upstreams_v2): per-route v1-compat knobs).
func renderUpstreamsV2(m *renderModel) string {
	var b strings.Builder
	b.WriteString("# Generated by synapse-operator ingress controller. Do not edit.\n")
	b.WriteString("version: 2\n")

	if m.sticky {
		b.WriteString("proxy:\n  sticky_sessions:\n    enabled: true\n")
	}

	// ACME HTTP-01 challenge backend: v2 expresses internal paths as a
	// top-level `internal:` list (the v1 `internal_paths:` override).
	if m.acme != "" {
		fmt.Fprintf(&b, "internal:\n  - path: \"/.well-known/acme-challenge/*\"\n    upstream: %q\n", m.acme)
	}

	// Stable iteration: terminate hosts first (sorted), then passthrough
	// hosts (sorted). Single emission per host. Deterministic output
	// keeps writeIfChanged from triggering spurious reloads.
	hostKeys := make([]string, 0, len(m.hosts)+len(m.passthroughHosts))
	for h := range m.hosts {
		hostKeys = append(hostKeys, h)
	}
	for h := range m.passthroughHosts {
		hostKeys = append(hostKeys, h)
	}
	sort.Strings(hostKeys)

	if len(hostKeys) == 0 {
		return b.String()
	}
	b.WriteString("hosts:\n")
	for _, h := range hostKeys {
		fmt.Fprintf(&b, "  %q:\n", h)
		if upstream, ok := m.passthroughHosts[h]; ok {
			b.WriteString("    tls:\n      passthrough: true\n")
			fmt.Fprintf(&b, "    upstream: %q\n", upstream)
			continue
		}
		// terminate path
		b.WriteString("    tls:\n      terminate:\n")
		if stem := m.hostCert[h]; stem != "" {
			fmt.Fprintf(&b, "        cert: %q\n", stem)
		}
		paths := m.hosts[h]
		pathKeys := make([]string, 0, len(paths))
		for p := range paths {
			pathKeys = append(pathKeys, p)
		}
		sort.Strings(pathKeys)
		b.WriteString("    paths:\n")
		for _, p := range pathKeys {
			rc := paths[p]
			fmt.Fprintf(&b, "      %q:\n", p)
			writeRouteV2(&b, rc)
		}
	}
	return b.String()
}

// writeRouteV2 emits one v2 route block. Field ordering and indentation
// match the v2 schema (synapse-utils RawRoute) — anything else fails
// the deny_unknown_fields parser.
func writeRouteV2(b *strings.Builder, rc *routeCfg) {
	if len(rc.servers) == 1 && rc.servers[0].weight == 0 {
		fmt.Fprintf(b, "        upstream: %q\n", rc.servers[0].addr)
	} else {
		b.WriteString("        upstreams:\n")
		for _, sv := range rc.servers {
			if sv.weight > 0 {
				fmt.Fprintf(b, "          - { addr: %q, weight: %d }\n", sv.addr, sv.weight)
			} else {
				fmt.Fprintf(b, "          - %q\n", sv.addr)
			}
		}
	}
	if rc.ssl != nil {
		fmt.Fprintf(b, "        ssl_enabled: %t\n", *rc.ssl)
	}
	if rc.http2 != nil {
		fmt.Fprintf(b, "        http2_enabled: %t\n", *rc.http2)
	}
	if rc.forceHTTPS != nil && *rc.forceHTTPS {
		b.WriteString("        force_https: true\n")
	}
	if rc.healthcheck != nil && *rc.healthcheck {
		// v2 requires a structured health_check. v1 had only a boolean,
		// so fall back to the simplest type: TCP. Operators that need
		// HTTP health checks can hand-edit upstreams.yaml or extend
		// the annotation set later.
		b.WriteString("        health_check:\n          type: tcp\n")
	}
	if rc.disableAccessLog != nil {
		fmt.Fprintf(b, "        disable_access_log: %t\n", *rc.disableAccessLog)
	}
	if rc.maxBodySize != nil {
		fmt.Fprintf(b, "        max_body_size: %d\n", *rc.maxBodySize)
	}
	if rc.redirectStatus != nil && rc.redirectLocation != "" {
		fmt.Fprintf(b, "        redirect:\n          status: %d\n          location: %q\n",
			*rc.redirectStatus, rc.redirectLocation)
	}
	if rc.connectTimeout != nil || rc.readTimeout != nil || rc.writeTimeout != nil || rc.idleTimeout != nil {
		b.WriteString("        timeouts:\n")
		if rc.connectTimeout != nil {
			fmt.Fprintf(b, "          connect: %d\n", *rc.connectTimeout)
		}
		if rc.readTimeout != nil {
			fmt.Fprintf(b, "          read: %d\n", *rc.readTimeout)
		}
		if rc.writeTimeout != nil {
			fmt.Fprintf(b, "          write: %d\n", *rc.writeTimeout)
		}
		if rc.idleTimeout != nil {
			fmt.Fprintf(b, "          idle: %d\n", *rc.idleTimeout)
		}
	}
	if len(rc.reqHeaders) > 0 || len(rc.respHeaders) > 0 {
		b.WriteString("        headers:\n")
		if len(rc.reqHeaders) > 0 {
			b.WriteString("          request:\n")
			for _, h := range rc.reqHeaders {
				fmt.Fprintf(b, "            - %q\n", h)
			}
		}
		if len(rc.respHeaders) > 0 {
			b.WriteString("          response:\n")
			for _, h := range rc.respHeaders {
				fmt.Fprintf(b, "            - %q\n", h)
			}
		}
	}
}
