package controllers

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ownedHost = "owned.example.com"

// ownerIngress is the Ingress of namespace ns that has host at path, created
// at created, of the class that is bound to the proxy under test.
func ownerIngress(ns, name, host, path string, created time.Time) *networkingv1.Ingress {
	ing := ingressIn(ns, name, host, path)
	ing.Spec.IngressClassName = ptr("public")
	ing.CreationTimestamp = metav1.NewTime(created)
	return ing
}

// renderOwned renders objs for a proxy whose class is "public".
func renderOwned(t *testing.T, objs ...client.Object) (routes string, certs map[string][]byte, events *record.FakeRecorder) {
	t.Helper()
	proxy := testProxy("synapse-os", "edge")
	r := newRouteReconciler(t, append([]client.Object{proxy, classFor("public", proxy)}, objs...)...)
	events = record.NewFakeRecorder(64)
	r.Recorder = events
	mustReconcileRoutes(t, r, proxy)
	return upstreamsOf(t, r, proxy), certsOf(t, r, proxy).Data, events
}

func eventsOf(events *record.FakeRecorder, reason string) []string {
	var out []string
	for len(events.Events) > 0 {
		if e := <-events.Events; strings.Contains(e, " "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

// owner is zzz, the namespace that sorts last, created at T. The newcomer is
// aaa, which sorts first, created an hour later. Namespace order alone would
// give the host to aaa.
func ownerAndNewcomer(t *testing.T, owner, newcomer *networkingv1.Ingress, more ...client.Object) (string, map[string][]byte, *record.FakeRecorder) {
	t.Helper()
	return renderOwned(t, append([]client.Object{owner, newcomer}, more...)...)
}

func TestOwnership_OwnerOutputIsTheSameWithAndWithoutTheNewcomer(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	alone, _, _ := renderOwned(t, owner)
	with, _, _ := ownerAndNewcomer(t, owner, ownerIngress("aaa", "intruder", ownedHost, "/intruder", t0.Add(time.Hour)))
	if with != alone {
		t.Errorf("the newcomer changed the owner's routes\n--- with ---\n%s--- alone ---\n%s", with, alone)
	}
	if strings.Contains(with, "/intruder") {
		t.Errorf("the newcomer's path is served:\n%s", with)
	}
}

func TestOwnership_RefusalIsSaidCountedAndNamesTheOwner(t *testing.T) {
	before := counterValue(t, mHostConflicts)
	_, _, events := ownerAndNewcomer(t,
		ownerIngress("zzz", "shop", ownedHost, "/", t0),
		ownerIngress("aaa", "intruder", ownedHost, "/intruder", t0.Add(time.Hour)))
	got := eventsOf(events, "HostnameConflict")
	if len(got) != 1 || !strings.HasPrefix(got[0], corev1.EventTypeWarning+" ") || !strings.Contains(got[0], ownedHost) || !strings.Contains(got[0], "zzz") {
		t.Errorf("events = %q, want one warning naming the host and namespace zzz", got)
	}
	if d := counterValue(t, mHostConflicts) - before; d != 1 {
		t.Errorf("host conflict counter went up by %v, want 1", d)
	}
}

func TestOwnership_RegexRouteOfANewcomerIsRefused(t *testing.T) {
	intruder := annotated(ownerIngress("aaa", "intruder", ownedHost, "/x.*", t0.Add(time.Hour)), "use-regex", "true")
	out, _, _ := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), intruder)
	if strings.Contains(out, "expr:") || strings.Contains(out, "aaa.svc") {
		t.Errorf("the newcomer's regex route is served:\n%s", out)
	}
}

func TestOwnership_PassthroughOfANewcomerIsRefused(t *testing.T) {
	intruder := annotated(ownerIngress("aaa", "intruder", ownedHost, "/", t0.Add(time.Hour)), "ssl-passthrough", "true")
	out, _, _ := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), intruder)
	if strings.Contains(out, "passthrough") || strings.Contains(out, "aaa.svc") {
		t.Errorf("the newcomer takes the host over:\n%s", out)
	}
}

func TestOwnership_ServerAliasOnAnOwnedHostIsRefused(t *testing.T) {
	intruder := annotated(ownerIngress("aaa", "intruder", "mine.example.com", "/", t0.Add(time.Hour)), "server-alias", ownedHost)
	out, _, events := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), intruder)
	wantHosts(t, out, []string{"mine.example.com", ownedHost}, nil)
	if strings.Count(out, "aaa.svc") != 1 {
		t.Errorf("the alias is served for the newcomer:\n%s", out)
	}
	if got := eventsOf(events, "HostnameConflict"); len(got) != 1 {
		t.Errorf("events = %q, want one HostnameConflict", got)
	}
}

func TestOwnership_TLSHostOfANewcomerGetsNoCertificate(t *testing.T) {
	intruder := ownerIngress("aaa", "intruder", "mine.example.com", "/", t0.Add(time.Hour))
	intruder.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{ownedHost, "mine.example.com"}, SecretName: "aaa-tls"}}
	_, certs, _ := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), intruder,
		tlsSecret("aaa", "aaa-tls", "CRT-aaa", "KEY-aaa"))
	if _, ok := certs["mine.example.com.crt"]; !ok {
		t.Errorf("the newcomer's own host has no certificate: %v", keysOf(certs))
	}
	if _, ok := certs[ownedHost+".crt"]; ok {
		t.Errorf("the newcomer's certificate for the owned host is projected: %v", keysOf(certs))
	}
}

func TestOwnership_SolverOfANewcomerIsRefused(t *testing.T) {
	solver := ownerIngress("aaa", "cm-acme-http-solver-1", ownedHost, "/.well-known/acme-challenge/tok", t0.Add(time.Hour))
	out, _, _ := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), solver)
	if strings.Contains(out, "acme-challenge") {
		t.Errorf("the newcomer's solver is served:\n%s", out)
	}
}

func TestOwnership_ANamespaceMaySpreadAHostOverIngresses(t *testing.T) {
	out, _, events := renderOwned(t,
		ownerIngress("zzz", "one", ownedHost, "/one", t0),
		ownerIngress("zzz", "two", ownedHost, "/two", t0.Add(time.Hour)))
	if !strings.Contains(out, "/one") || !strings.Contains(out, "/two") {
		t.Errorf("a namespace lost a path of its own host:\n%s", out)
	}
	if got := eventsOf(events, "HostnameConflict"); len(got) != 0 {
		t.Errorf("events = %q, want none", got)
	}
}

func TestOwnership_AnIngressThatCannotBeServedStillOwnsItsHost(t *testing.T) {
	broken := ownerIngress("zzz", "broken", ownedHost, "/", t0)
	broken.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	foreign := ownerIngress("bbb", "foreign", ownedHost, "/", t0.Add(-time.Hour))
	foreign.Spec.IngressClassName = ptr("nginx")
	out, _, _ := ownerAndNewcomer(t, broken, ownerIngress("aaa", "intruder", ownedHost, "/intruder", t0.Add(time.Hour)), foreign)
	if strings.Contains(out, "/intruder") {
		t.Errorf("the owner's host passed to a newcomer:\n%s", out)
	}
}

func TestOwnership_TheHostPassesToTheNextOldestWhenTheOwnerIsGone(t *testing.T) {
	out, _, _ := renderOwned(t,
		ownerIngress("bbb", "second", ownedHost, "/second", t0.Add(30*time.Minute)),
		ownerIngress("aaa", "third", ownedHost, "/third", t0.Add(time.Hour)))
	if !strings.Contains(out, "/second") || strings.Contains(out, "/third") {
		t.Errorf("host did not pass to the next oldest:\n%s", out)
	}
}

func TestOwnership_OffLetsNamespacesShareAsBefore(t *testing.T) {
	out := renderToFile(t, &IngressReconciler{IngressClassName: "synapse"},
		ingressIn("zzz", "shop", ownedHost, "/"), ingressIn("aaa", "other", ownedHost, "/other"))
	if !strings.Contains(out, "/other") {
		t.Errorf("ownership is off and the second namespace is refused:\n%s", out)
	}
}

func TestOwnership_ExtraClaimsAreResolvedWithTheIngresses(t *testing.T) {
	shop := ingressIn("zzz", "shop", ownedHost, "/")
	shop.CreationTimestamp = metav1.NewTime(t0)
	older := claim(ownedHost, "ccc", "gw", t0.Add(-time.Hour))
	r := &IngressReconciler{IngressClassName: "synapse", HostOwnership: true,
		ExtraClaims: func() []hostClaim { return []hostClaim{older} }}
	out := renderToFile(t, r, shop)
	if strings.Contains(out, ownedHost) {
		t.Errorf("an older claim from the hook does not hold the host:\n%s", out)
	}
	r = &IngressReconciler{IngressClassName: "synapse", HostOwnership: true}
	if out := renderToFile(t, r, shop); !strings.Contains(out, ownedHost) {
		t.Errorf("a nil ExtraClaims refuses the host:\n%s", out)
	}
}

// A younger namespace's wildcard must not serve a host that an older claim
// holds and the file has no entry for: a backend that cannot be resolved
// leaves the host out, and Synapse then matches the wildcard.
func TestOwnership_AYoungerWildcardDoesNotServeAHeldHostWithoutRoutes(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	owner.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, wild)
	doc := parseV2(t, out)
	h, ok := doc.Hosts[ownedHost]
	if !ok {
		t.Fatalf("%s has no entry, so aaa's *.example.com serves it:\n%s", ownedHost, out)
	}
	if h.Paths == nil || len(h.Paths) != 0 {
		t.Errorf("paths of %s = %v, want an empty map:\n%s", ownedHost, h.Paths, out)
	}
	if _, ok := doc.Hosts["*.example.com"]; !ok {
		t.Errorf("the wildcard is gone:\n%s", out)
	}
}

func TestOwnership_AYoungerWildcardDoesNotServeATLSOnlyClaim(t *testing.T) {
	owner := ownerIngress("zzz", "shop", "other.zzz.test", "/", t0)
	owner.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{ownedHost}, SecretName: "zzz-tls"}}
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, certs, _ := renderOwned(t, owner, wild, tlsSecret("zzz", "zzz-tls", "CRT-zzz", "KEY-zzz"))
	doc := parseV2(t, out)
	h, ok := doc.Hosts[ownedHost]
	if !ok {
		t.Fatalf("%s has no entry, so aaa's *.example.com serves it:\n%s", ownedHost, out)
	}
	if h.TLS.Terminate == nil || h.TLS.Terminate.Cert != ownedHost || len(h.Paths) != 0 {
		t.Errorf("entry = %+v, want the owner's certificate and no paths:\n%s", h, out)
	}
	if _, ok := certs[ownedHost+".crt"]; !ok {
		t.Errorf("the owner's certificate is not projected: %v", keysOf(certs))
	}
}

// An owned wildcard under a foreign wildcard is held as well.
func TestOwnership_AnOwnedWildcardUnderAForeignWildcardIsHeld(t *testing.T) {
	owner := ownerIngress("zzz", "shop", "*.a.example.com", "/", t0)
	owner.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, wild)
	doc := parseV2(t, out)
	h, ok := doc.Hosts["*.a.example.com"]
	if !ok || h.Paths == nil || len(h.Paths) != 0 {
		t.Errorf("*.a.example.com = %+v (present %v), want an entry with empty paths:\n%s", h, ok, out)
	}
}

func TestOwnership_NothingIsReservedWithoutAForeignWildcard(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	owner.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	other := ownerIngress("aaa", "other", "aaa.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, other)
	wantHosts(t, out, []string{"aaa.example.com"}, []string{ownedHost})
}

func TestOwnership_TheOwnersOwnWildcardReservesNothing(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	owner.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	wild := ownerIngress("zzz", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, wild)
	wantHosts(t, out, []string{"*.example.com"}, []string{ownedHost})
}

// The owner's solver for a held name is answered under the name's own entry,
// not under the wildcard of another namespace.
func TestOwnership_ASolverOfAHeldNameLandsOnItsEntry(t *testing.T) {
	owner := ownerIngress("zzz", "shop", ownedHost, "/", t0)
	owner.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	solver := ownerIngress("zzz", "cm-acme-http-solver-1", ownedHost, "/.well-known/acme-challenge/tok", t0)
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, solver, wild)
	doc := parseV2(t, out)
	if _, ok := doc.Hosts[ownedHost].Paths["/.well-known/acme-challenge/tok"]; !ok {
		t.Errorf("the solver is not under %s:\n%s", ownedHost, out)
	}
	if _, ok := doc.Hosts["*.example.com"].Paths["/.well-known/acme-challenge/tok"]; ok {
		t.Errorf("the solver is under the wildcard:\n%s", out)
	}
}

// A newcomer that names a refused host in a rule and again through the
// fallback of a spec.tls without hosts is told of it once.
func TestOwnership_ARefusedHostNamedByARuleAndTheTLSFallbackIsSaidOnce(t *testing.T) {
	before := counterValue(t, mHostConflicts)
	intruder := ownerIngress("aaa", "intruder", ownedHost, "/intruder", t0.Add(time.Hour))
	intruder.Spec.TLS = []networkingv1.IngressTLS{{SecretName: "aaa-tls"}}
	_, _, events := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), intruder,
		tlsSecret("aaa", "aaa-tls", "CRT-aaa", "KEY-aaa"))
	if got := eventsOf(events, "HostnameConflict"); len(got) != 1 {
		t.Errorf("events = %q, want one HostnameConflict", got)
	}
	if d := counterValue(t, mHostConflicts) - before; d != 1 {
		t.Errorf("host conflict counter went up by %v, want 1", d)
	}
}

// An Ingress that names one host it may not use and one it may keeps the
// second.
func TestOwnership_APartlyRefusedIngressKeepsItsOtherHosts(t *testing.T) {
	before := counterValue(t, mHostConflicts)
	mixed := ownerIngress("aaa", "mixed", ownedHost, "/", t0.Add(time.Hour))
	free := mixed.Spec.Rules[0].DeepCopy()
	free.Host = "aaa.example.com"
	mixed.Spec.Rules = append(mixed.Spec.Rules, *free)
	mixed.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{ownedHost, "aaa.example.com"}, SecretName: "aaa-tls"}}
	out, certs, events := ownerAndNewcomer(t, ownerIngress("zzz", "shop", ownedHost, "/", t0), mixed,
		tlsSecret("aaa", "aaa-tls", "CRT-aaa", "KEY-aaa"))
	wantHosts(t, out, []string{"aaa.example.com", ownedHost}, nil)
	if n := strings.Count(out, "aaa.svc"); n != 1 {
		t.Errorf("the backend of aaa is written %d times, want once (for the free host):\n%s", n, out)
	}
	if _, ok := certs["aaa.example.com.crt"]; !ok {
		t.Errorf("the free host has no certificate: %v", keysOf(certs))
	}
	if _, ok := certs[ownedHost+".crt"]; ok {
		t.Errorf("a certificate for the refused host is projected: %v", keysOf(certs))
	}
	if got := eventsOf(events, "HostnameConflict"); len(got) != 1 {
		t.Errorf("conflict events = %q, want one", got)
	}
	if d := counterValue(t, mHostConflicts) - before; d != 1 {
		t.Errorf("host conflict counter went up by %v, want 1", d)
	}
}

// A passthrough host of the owner is already written, and gets no second
// entry: a repeated key makes Synapse refuse the file.
func TestOwnership_AnOwnersPassthroughHostUnderAForeignWildcardIsWrittenOnce(t *testing.T) {
	owner := annotated(ownerIngress("zzz", "raw", ownedHost, "/", t0), "ssl-passthrough", "true")
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, wild)
	if n := strings.Count(out, `"`+ownedHost+`":`); n != 1 {
		t.Errorf("%s is written %d times, want once:\n%s", ownedHost, n, out)
	}
	if !parseV2(t, out).Hosts[ownedHost].TLS.Passthrough {
		t.Errorf("%s is not a passthrough host:\n%s", ownedHost, out)
	}
}

// The entry is written under the spelling the claim used, and carries the
// certificate bound under the spelling of the TLS host.
func TestOwnership_AHeldEntryInAnotherSpellingKeepsTheCertificate(t *testing.T) {
	owner := broken(ownerIngress("zzz", "shop", "other.zzz.test", "/", t0))
	annotated(owner, "server-alias", "OWNED.example.com")
	owner.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{ownedHost}, SecretName: "zzz-tls"}}
	wild := ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour))
	out, _, _ := renderOwned(t, owner, wild, tlsSecret("zzz", "zzz-tls", "CRT-zzz", "KEY-zzz"))
	h, ok := parseV2(t, out).Hosts["OWNED.example.com"]
	if !ok || h.TLS.Terminate == nil || h.TLS.Terminate.Cert != ownedHost {
		t.Errorf("OWNED.example.com = %+v (present %v), want the certificate %s:\n%s", h, ok, ownedHost, out)
	}
}

// A wildcard of the owner's own namespace that another wildcard of that
// namespace covers is the owner's too.
func TestOwnership_AWildcardOfTheOwnersNamespaceIsNotForeign(t *testing.T) {
	out, _, _ := renderOwned(t,
		broken(ownerIngress("bbb", "x", "x.a.example.com", "/", t0)),
		ownerIngress("bbb", "w1", "*.example.com", "/w1", t0.Add(time.Minute)),
		ownerIngress("bbb", "w2", "*.a.example.com", "/w2", t0.Add(2*time.Minute)))
	wantHosts(t, out, []string{"*.example.com", "*.a.example.com"}, []string{"x.a.example.com"})
}

// A held name's solver sits under the name's own entry, so it is answered,
// and nobody is told it is not.
func TestOwnership_ASolverOfAHeldNameUnderAForeignPassthroughWildcardIsAnswered(t *testing.T) {
	solver := ownerIngress("zzz", "cm-acme-http-solver-1", ownedHost, "/.well-known/acme-challenge/tok", t0)
	wild := annotated(ownerIngress("aaa", "wild", "*.example.com", "/", t0.Add(time.Hour)), "ssl-passthrough", "true")
	out, _, ev := renderOwned(t, solver, wild)
	if got := eventsOf(ev, "SolverUnreachable"); len(got) != 0 {
		t.Errorf("events = %q, want none", got)
	}
	if _, ok := parseV2(t, out).Hosts[ownedHost].Paths["/.well-known/acme-challenge/tok"]; !ok {
		t.Errorf("the solver is not under %s:\n%s", ownedHost, out)
	}
}

// broken makes the backend of ing unresolvable, so that its host has no
// routes.
func broken(ing *networkingv1.Ingress) *networkingv1.Ingress {
	ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port = networkingv1.ServiceBackendPort{Name: "nosuchport"}
	return ing
}
