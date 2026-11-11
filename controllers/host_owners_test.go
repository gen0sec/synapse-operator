package controllers

import (
	"math/rand"
	"testing"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func claim(host, ns, name string, created time.Time) hostClaim {
	ing := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: metav1.NewTime(created)}}
	return hostClaim{host: host, ns: ns, created: created, who: ing}
}

func wantOwner(t *testing.T, o *hostOwners, host, want string) {
	t.Helper()
	got, ok := o.owner(host)
	if want == "" {
		if ok {
			t.Errorf("owner(%q) = %q, want none", host, got)
		}
		return
	}
	if !ok || got != want {
		t.Errorf("owner(%q) = %q, %v, want %q", host, got, ok, want)
	}
}

func TestResolveHostOwners_TheOldestClaimWins(t *testing.T) {
	o := resolveHostOwners([]hostClaim{
		claim("a.example.com", "aaa", "x", t0.Add(time.Hour)),
		claim("a.example.com", "zzz", "x", t0),
		claim("b.example.com", "aaa", "x", t0),
	})
	wantOwner(t, o, "a.example.com", "zzz")
	wantOwner(t, o, "b.example.com", "aaa")
	wantOwner(t, o, "c.example.com", "")
	if o.allows("a.example.com", "aaa") || !o.allows("a.example.com", "zzz") || !o.allows("c.example.com", "aaa") {
		t.Error("allows does not follow owner")
	}
}

func TestResolveHostOwners_EqualAgeIsDecidedByNamespaceThenName(t *testing.T) {
	o := resolveHostOwners([]hostClaim{
		claim("a.example.com", "zzz", "x", t0),
		claim("a.example.com", "bbb", "x", t0),
	})
	wantOwner(t, o, "a.example.com", "bbb")
}

func TestResolveHostOwners_AWildcardIsHeldInOneDirectionOnly(t *testing.T) {
	// An older wildcard holds what it covers.
	o := resolveHostOwners([]hostClaim{
		claim("*.example.com", "zzz", "w", t0),
		claim("shop.example.com", "aaa", "s", t0.Add(time.Hour)),
	})
	wantOwner(t, o, "shop.example.com", "zzz")
	wantOwner(t, o, "*.example.com", "zzz")
	if o.allows("shop.example.com", "aaa") {
		t.Error("a newer host under an older wildcard is allowed")
	}

	// A newer wildcard does not take the host an older claim holds, but
	// holds the rest.
	o = resolveHostOwners([]hostClaim{
		claim("shop.example.com", "zzz", "s", t0),
		claim("*.example.com", "aaa", "w", t0.Add(time.Hour)),
	})
	wantOwner(t, o, "shop.example.com", "zzz")
	wantOwner(t, o, "other.example.com", "aaa")
	if !o.allows("*.example.com", "aaa") {
		t.Error("the newer wildcard is refused")
	}
}

func TestResolveHostOwners_CaseAndTrailingDotFold(t *testing.T) {
	o := resolveHostOwners([]hostClaim{
		claim("Shop.Example.COM.", "zzz", "s", t0),
		claim("shop.example.com", "aaa", "s", t0.Add(time.Hour)),
	})
	wantOwner(t, o, "SHOP.example.com", "zzz")
	if o.allows("shop.example.com.", "aaa") {
		t.Error("a spelling of the host takes it from its owner")
	}
}

func TestResolveHostOwners_OrderOfTheClaimsDoesNotMatter(t *testing.T) {
	base := []hostClaim{
		claim("a.example.com", "zzz", "x", t0),
		claim("a.example.com", "aaa", "x", t0.Add(time.Hour)),
		claim("*.example.com", "mmm", "w", t0.Add(30*time.Minute)),
		claim("b.example.com", "bbb", "x", t0.Add(2*time.Hour)),
		claim("b.example.com", "ccc", "x", t0.Add(2*time.Hour)),
		claim("c.example.com", "aaa", "x", t0),
	}
	hosts := []string{"a.example.com", "b.example.com", "c.example.com", "d.example.com", "*.example.com"}
	want := map[string]string{}
	for _, h := range hosts {
		want[h], _ = resolveHostOwners(base).owner(h)
	}
	rng := rand.New(rand.NewSource(1))
	for range 50 {
		shuffled := append([]hostClaim(nil), base...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		o := resolveHostOwners(shuffled)
		for _, h := range hosts {
			if got, _ := o.owner(h); got != want[h] {
				t.Fatalf("owner(%q) = %q after a shuffle, want %q", h, got, want[h])
			}
		}
	}
}

func TestHostOwners_NilAllowsEverything(t *testing.T) {
	var o *hostOwners
	if !o.allows("a.example.com", "aaa") {
		t.Error("a nil hostOwners refuses")
	}
}

func TestIngressClaims(t *testing.T) {
	ing := routedIngress("x", ptr("synapse"), "Shop.example.com")
	ing.Namespace = "zzz"
	ing.CreationTimestamp = metav1.NewTime(t0)
	ing.Spec.Rules = append(ing.Spec.Rules, networkingv1.IngressRule{Host: "rule2.example.com"}) // no HTTP: no route
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"tls.example.com"}, SecretName: "s"}}
	a := annSettings{serverAliases: []string{"alias.example.com", "*", "a*b.example.com", ""}}

	var got []string
	for _, c := range ingressClaims(ing, a) {
		if c.ns != "zzz" || !c.created.Equal(t0) || c.who != ing {
			t.Errorf("claim %+v is not the Ingress's", c)
		}
		got = append(got, c.host)
	}
	want := []string{"Shop.example.com", "rule2.example.com", "alias.example.com", "tls.example.com"}
	if len(got) != len(want) {
		t.Fatalf("claims = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claims = %v, want %v", got, want)
		}
	}
}
