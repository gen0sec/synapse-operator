package controllers

import (
	"fmt"
	"regexp/syntax"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
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
	// Headers the route adds a value to, as "Name: value" lines, and
	// names of headers it removes. Only the v2 schema has these; the v1
	// writer leaves them out.
	reqAdd, respAdd       []string
	reqRemove, respRemove []string
	// redirect, when set, makes this route a 3xx short-circuit. Status is
	// 301 (permanent) or 302 (temporal) by default; *-code annotations
	// override. Mutually exclusive with rewriting/forwarding at the proxy:
	// when set, synapse writes the redirect and never contacts upstream.
	redirectStatus   *uint64
	redirectLocation string
	// matchExpr, when non-empty, makes this a regex route: synapse matches
	// it via `match_expr` (a wirefilter expression) instead of by the path
	// key, which becomes just a unique label. Populated from a Gateway
	// RegularExpression path match or an nginx `use-regex` Ingress path.
	matchExpr string
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
	// useRegex mirrors nginx `nginx.ingress.kubernetes.io/use-regex: "true"`:
	// the Ingress's paths are POSIX regexes, rendered as synapse `match_expr`
	// regex routes rather than longest-prefix paths.
	useRegex bool
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
	// solvers are the paths of cert-manager's HTTP-01 solvers, by host,
	// each with where it is answered. They are no routes of the host as
	// far as anything else goes: a solver does not take a host from what
	// may route it. See renderUpstreamsV2.
	solvers map[string]map[string]string
	// sameAsV1 has the v2 writer say what Synapse made of the v1 file the
	// same routes used to be written in, where the two schemas do not
	// mean the same by saying nothing: see renderUpstreamsV2. A
	// SynapseProxy's routes are written so. The older modes, which write
	// a v2 file only beside a passthrough host, write it as they did.
	sameAsV1 bool
}

func newRenderModel() *renderModel {
	return &renderModel{
		hosts:            map[string]map[string]*routeCfg{},
		hostCert:         map[string]string{},
		certProjections:  map[string]certProjection{},
		passthroughHosts: map[string]string{},
		solvers:          map[string]map[string]string{},
	}
}

// addSolver records the path of an HTTP-01 solver on host and the address
// it is answered at. The first one for a path keeps it.
func (m *renderModel) addSolver(host, path, addr string) {
	if m.solvers[host] == nil {
		m.solvers[host] = map[string]string{}
	}
	if _, ok := m.solvers[host][path]; !ok {
		m.solvers[host][path] = addr
	}
}

// hostKeyProblem says why Synapse would not take name as a host of a v2
// file, which it then refuses as a whole; "" when it would.
func hostKeyProblem(name string) string {
	name = strings.TrimRight(name, ".")
	switch {
	case name == "":
		return "it is empty"
	case strings.Contains(strings.TrimPrefix(name, "*."), "*"):
		return "a `*` can only stand for the labels in front, as in `*.example.com`"
	}
	return ""
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
	// Normalize away a trailing slash (except the bare root "/"). synapse's
	// longest-prefix router (synapse-proxy gethosts.rs) truncates the request
	// path at "/" boundaries and looks up each candidate by EXACT key, so a key
	// stored WITH a trailing slash (e.g. "/docs/x/swagger/") can only ever be
	// hit by an exact request to that slash — it never prefix-matches sub-paths
	// like ".../index.html" (the truncation yields "/docs/x/swagger", which
	// != the slashed key). Storing the de-slashed key lets the route cover its
	// whole subtree. Regex routes (addRegexRoute) keep their literal form.
	path = plainPathKey(path)
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

// regexRouteKey is the unique (host-scoped) path-map label for a regex
// route. synapse ignores the key for matching when match_expr is set, but
// it must be unique and stable; the regex itself makes it deterministic and
// collision-free (same regex twice ⇒ same key ⇒ first-writer-wins).
func regexRouteKey(regex string) string {
	return "expr:" + regex
}

// pathRegexExpr renders the synapse wirefilter match expression for a path
// regex, e.g. `http.request.path matches "^/api/runs/[^/]+/stream$"`. The
// regex is anchored at the start (`^`) when not already, because an
// Ingress/HTTPRoute path regex matches from the beginning of the request
// path (nginx use-regex / Gateway semantics) — k8s also requires the stored
// path to begin with `/`, so the `^` is supplied here rather than in the spec.
func pathRegexExpr(regex string) string {
	if !strings.HasPrefix(regex, "^") {
		regex = "^" + regex
	}
	return "http.request.path matches " + wfRegex(regex)
}

// wfString writes s as a string literal of Synapse's expression language.
// The language has three escapes, `\"`, `\\` and `\xHH`, and refuses any
// other, so a general-purpose quoting function writes strings it cannot
// read.
func wfString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20 || c > 0x7e:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// wfRegex writes a regular expression as a regex literal of that language,
// which is not a string literal: a backslash and the character after it go
// to the regex engine as they are. Doubled, as quoting a string would, `\.`
// becomes a backslash and any character.
//
// The one thing to take care of is the quote, which ends the literal. `\"`
// is a quote outside a character class only, so it is written `\x22`, which
// the engine reads as a quote everywhere.
func wfRegex(re string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(re); i++ {
		switch {
		case re[i] == '"':
			b.WriteString(`\x22`)
		case re[i] == '\\' && i+1 < len(re) && re[i+1] == '"':
			b.WriteString(`\x22`)
			i++
		case re[i] == '\\' && i+1 < len(re):
			b.WriteByte(re[i])
			b.WriteByte(re[i+1])
			i++
		default:
			b.WriteByte(re[i])
		}
	}
	b.WriteByte('"')
	return b.String()
}

// maxRegexLen bounds a regular expression as it is written out. A class is
// written as the ranges it stands for, and one like `\pL` is tens of
// kilobytes of them.
const maxRegexLen = 1024

// canonicalRegex returns re as Go's own parser writes it back, or why it
// cannot be used.
//
// Synapse's regex engine is not Go's, and the two do not read every
// expression alike: braces that are no repetition are literal here and an
// error there, `\Q...\E` exists only here, `\<` is a character here and a
// word boundary there. An expression Synapse refuses costs its route, and
// with it every route that was written to exclude it. Written back from the
// parse tree it is in the small common core of the two: classes as explicit
// ranges, literals escaped, groups and flags spelled out.
func canonicalRegex(re string) (string, error) {
	parsed, err := syntax.Parse(re, syntax.Perl)
	if err != nil {
		return "", err
	}
	out := parsed.String()
	if len(out) > maxRegexLen {
		return "", fmt.Errorf("it is %d bytes with its classes written out, and the limit is %d", len(out), maxRegexLen)
	}
	if err := asciiRegex(parsed, out); err != nil {
		return "", err
	}
	return out, nil
}

// asciiRegex says what in a regular expression Synapse's engine does not
// take, or would take to mean something else: it is run without Unicode.
// written is the expression as parsed.String() gives it.
//
//   - A class with a character outside ASCII in it is an error there.
//   - A letter outside ASCII has no other case there.
//   - A character written by its number is one byte there, and above 0x7f
//     not the character it is here.
//
// A character outside ASCII that is simply to be matched is fine: it is the
// same bytes on both sides.
func asciiRegex(parsed *syntax.Regexp, written string) error {
	var folded func(re *syntax.Regexp) bool
	folded = func(re *syntax.Regexp) bool {
		if re.Op == syntax.OpLiteral && re.Flags&syntax.FoldCase != 0 &&
			slices.ContainsFunc(re.Rune, func(r rune) bool { return r > unicode.MaxASCII }) {
			return true
		}
		return slices.ContainsFunc(re.Sub, folded)
	}
	if folded(parsed) {
		return fmt.Errorf("a letter outside ASCII cannot be matched without regard to case")
	}
	inClass := false
	for i := 0; i < len(written); {
		r, size := utf8.DecodeRuneInString(written[i:])
		switch {
		case r == '\\' && strings.HasPrefix(written[i:], `\x`):
			// `\x{10ffff}`, or two digits without the braces.
			digits := written[min(i+2, len(written)):min(i+4, len(written))]
			size = 4
			if end := strings.IndexByte(written[i:], '}'); strings.HasPrefix(digits, "{") && end > 0 {
				digits, size = written[i+3:i+end], end+1
			}
			if n, err := strconv.ParseUint(digits, 16, 32); err != nil || n > unicode.MaxASCII {
				return fmt.Errorf("a character outside ASCII cannot be written by its number")
			}
		case r == '\\':
			size++ // and whatever it escapes
		case r == '[' && !inClass:
			inClass = true
		case r == ']':
			inClass = false
		case r > unicode.MaxASCII && inClass:
			return fmt.Errorf("a character class cannot hold a character outside ASCII")
		}
		i += size
	}
	return nil
}

// addRegexRoute records a regex path route: it is matched by `match_expr`
// (a wirefilter expression over the request path) rather than by longest-
// prefix. Same FIRST-WRITER-WINS semantics as addRoute, keyed by the regex
// so a host can carry multiple distinct regex routes deterministically.
func (m *renderModel) addRegexRoute(host, regex string, servers []backend, a annSettings, extraReq, extraResp []string) bool {
	if regex == "" {
		return false
	}
	if _, claimed := m.passthroughHosts[host]; claimed {
		return false
	}
	key := regexRouteKey(regex)
	if m.hosts[host] == nil {
		m.hosts[host] = map[string]*routeCfg{}
	}
	if _, exists := m.hosts[host][key]; exists {
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
		matchExpr:        pathRegexExpr(regex),
	}
	m.hosts[host][key] = rc
	if a.sticky {
		m.sticky = true
	}
	return true
}

// addExprRoute records a route that Synapse matches by the given expression,
// under a label that only has to be unique on the host. It is addRegexRoute
// for an expression that is more than one regular expression on the path.
func (m *renderModel) addExprRoute(host, label, expr string, servers []backend, extraReq, extraResp []string) bool {
	if expr == "" || label == "" {
		return false
	}
	if _, claimed := m.passthroughHosts[host]; claimed {
		return false
	}
	if m.hosts[host] == nil {
		m.hosts[host] = map[string]*routeCfg{}
	}
	if _, exists := m.hosts[host][label]; exists {
		return false
	}
	m.hosts[host][label] = &routeCfg{
		servers:     servers,
		reqHeaders:  slices.Clone(extraReq),
		respHeaders: slices.Clone(extraResp),
		matchExpr:   expr,
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
	// use-regex: treat this Ingress's paths as POSIX regexes and render them
	// as synapse `match_expr` regex routes (ingress-nginx parity). synapse-
	// compat key first, then the nginx-compat key.
	if v, ok := get("use-regex"); ok {
		if b := parseBool(v); b != nil {
			s.useRegex = *b
		}
	} else if v, ok := ann[nginxPrefix+"use-regex"]; ok {
		if b := parseBool(v); b != nil {
			s.useRegex = *b
		}
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
			if rc.matchExpr != "" {
				fmt.Fprintf(&b, "        match_expr: %q\n", rc.matchExpr)
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
			writeHeaderLists(&b, rc)
		}
	}
	return b.String()
}

// writeHeaderLists writes a v1 route's two header lists.
//
// A route with request headers also says what its response headers are,
// none included. Synapse reads one that has request headers and not a word
// on response headers as the older single list, and sends the same headers
// back to the client: a credential set for the backend would go out in
// every response.
func writeHeaderLists(b *strings.Builder, rc *routeCfg) {
	write := func(key string, hs []string, always bool) {
		switch {
		case len(hs) > 0:
			fmt.Fprintf(b, "        %s:\n", key)
			for _, h := range hs {
				fmt.Fprintf(b, "          - %q\n", h)
			}
		case always:
			fmt.Fprintf(b, "        %s: []\n", key)
		}
	}
	write("request_headers", rc.reqHeaders, false)
	write("response_headers", rc.respHeaders, len(rc.reqHeaders) > 0)
}

// plainPathKey is the key a plain path is stored under: without a trailing
// slash, except the root. See addRoute.
func plainPathKey(path string) string {
	if path != "/" {
		if trimmed := strings.TrimRight(path, "/"); trimmed != "" {
			return trimmed
		}
	}
	return path
}

// renderUpstreamsV2 emits the v2 synapse upstreams schema. It is what a
// SynapseProxy's routes are written in: the v2 schema has what the Gateway
// API needs of the proxy, and the v1 schema does not. The older modes use
// v2 when at least one host is SNI passthrough — v1 has no passthrough
// representation, so the whole file switches to v2.
// Terminate hosts are emitted with full per-route fidelity (ssl_enabled,
// http2_enabled, disable_access_log, max_body_size, force_https, timeouts,
// headers), which requires synapse's RawRoute to carry those four knobs
// (see synapse feat(upstreams_v2): per-route v1-compat knobs).
//
// The two schemas do not mean the same by saying nothing. With
// renderModel.sameAsV1 the file says what Synapse made of the v1 file:
//
//   - how a backend is reached: plain HTTP unless the route asks for TLS.
//     Unsaid, a v2 route goes by the backend's port.
//   - whether it is probed: unless the route says not to, and then with
//     the global settings. That is a v2 route without `health_check:`;
//     one that is not probed says `type: none`.
//   - how long a backend may take to send: 120 seconds. A v2 file that
//     sets no read timeout gives a route 60.
//   - what a backend is told about the client: nothing. A v2 file has the
//     client's fingerprints sent on in request headers unless it says no.
//
// One thing is not carried over. In a v1 file a path without a list of
// headers takes the one of the nearest path above it that has one. In a v2
// file a route's headers are its own, each way.
func renderUpstreamsV2(m *renderModel) string {
	var b strings.Builder
	b.WriteString("# Generated by synapse-operator ingress controller. Do not edit.\n")
	b.WriteString("version: 2\n")
	if m.sameAsV1 {
		fmt.Fprintf(&b, "timeouts:\n  read: %d\n", v1ReadTimeoutSeconds)
	}

	if m.sameAsV1 || m.sticky {
		b.WriteString("proxy:\n")
		if m.sameAsV1 {
			b.WriteString("  fingerprints:\n    forward: false\n")
		}
		if m.sticky {
			b.WriteString("  sticky_sessions:\n    enabled: true\n")
		}
	}

	// ACME HTTP-01 challenge backend: v2 expresses internal paths as a
	// top-level `internal:` list (the v1 `internal_paths:` override).
	// Synapse reads that list and does not route by it, so a proxy's file
	// has each solver's path as a route of its host instead: see below.
	if m.acme != "" && !m.sameAsV1 {
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
	if m.sameAsV1 {
		// A host that has a solver and nothing else is written for it.
		for h := range m.solvers {
			if _, routed := m.hosts[h]; !routed {
				if _, passed := m.passthroughHosts[h]; !passed {
					hostKeys = append(hostKeys, h)
				}
			}
		}
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
		// terminate path. A host with no certificate bound to it is
		// served in plain HTTP, and over TLS with whatever the listener
		// has for its name. That has to be said with an empty block: a
		// `terminate:` with nothing after it is no `tls:` block at all to
		// Synapse, which refuses a host without one, and the file with it.
		if stem := m.hostCert[h]; stem != "" {
			fmt.Fprintf(&b, "    tls:\n      terminate:\n        cert: %q\n", stem)
		} else {
			b.WriteString("    tls:\n      terminate: {}\n")
		}
		paths := m.hosts[h]
		pathKeys := make([]string, 0, len(paths))
		for p := range paths {
			pathKeys = append(pathKeys, p)
		}
		sort.Strings(pathKeys)
		// A solver's path, as a plain path of the host: the whole path of
		// the challenge, which Synapse tries before the host's
		// expressions and, among plain paths, is the longest. A route the
		// host has for that very path keeps it.
		solvers := map[string]*routeCfg{}
		if m.sameAsV1 {
			for p, addr := range m.solvers[h] {
				if _, routed := paths[p]; !routed {
					solvers[p] = &routeCfg{servers: []backend{{addr: addr}}}
					pathKeys = append(pathKeys, p)
				}
			}
			sort.Strings(pathKeys)
		}
		b.WriteString("    paths:\n")
		for _, p := range pathKeys {
			rc := paths[p]
			if rc == nil {
				rc = solvers[p]
			}
			fmt.Fprintf(&b, "      %q:\n", p)
			writeRouteV2(&b, rc, m.sameAsV1)
		}
	}
	return b.String()
}

// v1ReadTimeoutSeconds is how long a backend may take to send, for a route
// of a v1 file that does not say.
const v1ReadTimeoutSeconds = 120

// writeRouteV2 emits one v2 route block. Field ordering and indentation
// match the v2 schema (synapse-utils RawRoute) — anything else fails
// the deny_unknown_fields parser. sameAsV1 is renderModel.sameAsV1.
func writeRouteV2(b *strings.Builder, rc *routeCfg, sameAsV1 bool) {
	// A route of a v1 file is probed unless it says `healthcheck: false`.
	// Synapse does not probe a route chosen by an expression at all, and
	// refuses a file that says how to.
	unprobed := sameAsV1 && rc.matchExpr == "" && rc.healthcheck != nil && !*rc.healthcheck
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
	if rc.matchExpr != "" {
		fmt.Fprintf(b, "        match_expr: %q\n", rc.matchExpr)
	}
	switch {
	case unprobed:
		// A backend that is never probed is reached by its port, whatever
		// the route says of TLS: in a v1 file as well. A v2 file that
		// says otherwise beside `type: none` is refused, so nothing is
		// said.
	case rc.ssl != nil:
		fmt.Fprintf(b, "        ssl_enabled: %t\n", *rc.ssl)
	case sameAsV1:
		b.WriteString("        ssl_enabled: false\n")
	}
	if rc.http2 != nil {
		fmt.Fprintf(b, "        http2_enabled: %t\n", *rc.http2)
	}
	if rc.forceHTTPS != nil && *rc.forceHTTPS {
		b.WriteString("        force_https: true\n")
	}
	switch {
	case unprobed:
		b.WriteString("        health_check:\n          type: none\n")
	case sameAsV1:
		// Probed with the global settings, which is what `healthcheck:
		// true` asked for in a v1 file, and what saying nothing does.
	case rc.healthcheck != nil && *rc.healthcheck && rc.matchExpr == "":
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
	writeTransformsV2(b, rc)
}

// writeTransformsV2 writes the headers a route adds a value to and the ones
// it removes: one transform rule for requests and one for responses. Within
// a rule Synapse removes first and adds after. `headers:`, which sets, runs
// before both.
func writeTransformsV2(b *strings.Builder, rc *routeCfg) {
	rule := func(key string, remove, add []string) {
		if len(remove) == 0 && len(add) == 0 {
			return
		}
		fmt.Fprintf(b, "          %s:\n", key)
		lead := "            - "
		if len(remove) > 0 {
			b.WriteString(lead + "remove:\n")
			lead = "              "
			for _, name := range remove {
				fmt.Fprintf(b, "                - %q\n", name)
			}
		}
		if len(add) > 0 {
			b.WriteString(lead + "add:\n")
			// A header's values, in the order they were given, under
			// its name; the names in the order they first appear.
			var names []string
			values := map[string][]string{}
			for _, line := range add {
				name, value, _ := strings.Cut(line, ": ")
				if _, seen := values[name]; !seen {
					names = append(names, name)
				}
				values[name] = append(values[name], value)
			}
			for _, name := range names {
				fmt.Fprintf(b, "                %q:\n", name)
				for _, value := range values[name] {
					fmt.Fprintf(b, "                  - %q\n", value)
				}
			}
		}
	}
	if len(rc.reqRemove)+len(rc.reqAdd)+len(rc.respRemove)+len(rc.respAdd) == 0 {
		return
	}
	b.WriteString("        transforms:\n")
	rule("request_headers", rc.reqRemove, rc.reqAdd)
	rule("response_headers", rc.respRemove, rc.respAdd)
}
