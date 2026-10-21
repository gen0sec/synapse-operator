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
