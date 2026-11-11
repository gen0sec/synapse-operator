package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// renderAliasOwnershipOff renders one Ingress with the given server-alias,
// with host ownership off, and returns the routes file and the events.
func renderAliasOwnershipOff(t *testing.T, alias string) (string, *record.FakeRecorder) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "u.yaml")
	ing := routedIngress("aliased", ptr("synapse"), "www.example.com")
	ing.Annotations = map[string]string{"synapse.gen0sec.com/server-alias": alias}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ing).Build()
	events := record.NewFakeRecorder(32)
	r := &IngressReconciler{Client: c, IngressClassName: "synapse", UpstreamsOutPath: out,
		CertsOutDir: t.TempDir(), ClusterDomain: "cluster.local", Recorder: events}
	if _, _, _, err := r.render(context.Background()); err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered), events
}

func TestRender_AnAliasThatIsNoHostIsRefusedWithOwnershipOff(t *testing.T) {
	cases := []struct {
		name, alias string
		served      []string
		refused     []string
	}{
		{"a space", "ok.example.com, bad host.example.com", []string{"ok.example.com"}, []string{"bad host.example.com"}},
		{"a Kelvin sign", "ok.example.com, \u212Aelvin.example.com", []string{"ok.example.com"}, []string{"\u212A"}},
		{"a dotted capital I", "ok.example.com, \u0130nfo.example.com", []string{"ok.example.com"}, []string{"\u0130"}},
		{"a newline", "x.example.com\ny.example.com", nil, []string{"x.example.com", "y.example.com"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rendered, events := renderAliasOwnershipOff(t, tc.alias)
			for _, host := range append([]string{"www.example.com"}, tc.served...) {
				if !strings.Contains(rendered, fmt.Sprintf("\n  %q:\n", host)) {
					t.Errorf("the file has no host %q\n%s", host, rendered)
				}
			}
			for _, host := range tc.refused {
				if strings.Contains(rendered, host) {
					t.Errorf("the file has the refused alias %q\n%s", host, rendered)
				}
			}
			if got := eventsOf(events, "InvalidServerAlias"); len(got) == 0 {
				t.Error("no InvalidServerAlias event")
			}
		})
	}
}

// With ownership on the rule was already there; this pins that it stays.
func TestOwnership_AnAliasThatIsNoHostIsStillRefused(t *testing.T) {
	ing := annotated(ownerIngress("aaa", "shop", ownedHost, "/", t0), "server-alias", "ok.example.com, bad host.example.com")
	out, _, events := renderOwned(t, ing)
	if !strings.Contains(out, "ok.example.com") || strings.Contains(out, "bad host") {
		t.Errorf("the routes are:\n%s", out)
	}
	if got := eventsOf(events, "InvalidServerAlias"); len(got) == 0 {
		t.Error("no InvalidServerAlias event")
	}
}

func TestHostCharsOK(t *testing.T) {
	for host, want := range map[string]bool{
		"www.example.com": true, "WWW.Example.COM.": true, "*.wild.example.com": true, "svc_a-1.ns": true,
		"": false, "bad host": false, "a\nb": false, "\u212Aelvin.example.com": false,
		"\u0130nfo.example.com": false, "caf\u00e9.example.com": false,
	} {
		if got := hostCharsOK(host); got != want {
			t.Errorf("hostCharsOK(%q) = %v, want %v", host, got, want)
		}
	}
}
