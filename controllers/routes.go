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
	reqHeaders       []string
	respHeaders      []string
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
	reqHeaders       []string
	respHeaders      []string
	sticky           bool
}

type renderModel struct {
	hosts  map[string]map[string]*routeCfg
	acme   string
	sticky bool
}

func newRenderModel() *renderModel {
	return &renderModel{hosts: map[string]map[string]*routeCfg{}}
}

// addRoute records host/path → servers with the object's annotation
// settings. FIRST-WRITER-WINS: if (host,path) is already set it is
// NOT overwritten; returns false so the caller can log a deterministic
// conflict (sources are iterated in a stable order — Ingresses, then
// HTTPRoutes, each sorted by namespace/name — so Ingress beats Gateway
// and the result is reproducible regardless of informer ordering).
func (m *renderModel) addRoute(host, path string, servers []backend, a annSettings, extraReq, extraResp []string) bool {
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
		reqHeaders:       append(append([]string{}, a.reqHeaders...), extraReq...),
		respHeaders:      append(append([]string{}, a.respHeaders...), extraResp...),
	}
	m.hosts[host][path] = rc
	if a.sticky {
		m.sticky = true
	}
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
	if v, ok := get("request-headers"); ok {
		s.reqHeaders = csv(v)
	}
	if v, ok := get("response-headers"); ok {
		s.respHeaders = csv(v)
	}
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
		fmt.Fprintf(&b, "  %q:\n    paths:\n", h)
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
