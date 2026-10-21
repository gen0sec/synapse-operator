package controllers

import (
	"fmt"
	"strings"
	"testing"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func claimOf(ns, host string, created time.Time) hostClaim {
	return hostClaim{host: host, ns: ns, created: created, who: &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "x"}}}
}

func firstSeenOf(t *testing.T, l *hostClaims, ns, host string) time.Time {
	t.Helper()
	e, ok := l.entries[claimKey{ns: ns, host: host}]
	if !ok {
		t.Fatalf("no entry for %s %s in\n%s", ns, host, l.body())
	}
	return e.firstSeen
}

func TestHostClaims_ParseAndBodyRoundTrip(t *testing.T) {
	body := hostClaimsHeader + "\n" +
		"a h.example.com " + t0.Format(time.RFC3339Nano) + "\n" +
		"b h.example.com " + t0.Add(time.Hour+1).Format(time.RFC3339Nano) + " " + t0.Add(2*time.Hour).Format(time.RFC3339Nano) + "\n"
	l, bad := parseHostClaims(body, true)
	if bad != 0 || len(l.entries) != 2 || !l.exists {
		t.Fatalf("bad=%d entries=%d exists=%v", bad, len(l.entries), l.exists)
	}
	if got := l.body(); got != body {
		t.Errorf("round trip changed the body\n got: %q\nwant: %q", got, body)
	}
	if e := l.entries[claimKey{"b", "h.example.com"}]; !e.absentSince.Equal(t0.Add(2 * time.Hour)) {
		t.Errorf("absentSince = %v", e.absentSince)
	}
}

func TestHostClaims_MalformedLinesAreSkipped(t *testing.T) {
	body := "# a comment\n\n" +
		"a h.example.com " + t0.Format(time.RFC3339Nano) + "\n" +
		"only-two fields\n" +
		"a b not-a-time\n" +
		"a b " + t0.Format(time.RFC3339Nano) + " not-a-time\n" +
		"a b c d e\n"
	l, bad := parseHostClaims(body, true)
	if bad != 4 {
		t.Errorf("malformed = %d, want 4", bad)
	}
	if len(l.entries) != 1 {
		t.Errorf("entries = %d, want only the good line", len(l.entries))
	}
}

func TestHostClaims_RecordSeedsFromCreationWhenAbsent(t *testing.T) {
	l, _ := parseHostClaims("", false)
	now := t0.Add(48 * time.Hour)
	changed, dropped := l.record([]hostClaim{
		claimOf("a", "H.Example.com.", t0),
		claimOf("b", "zero.example.com", time.Time{}),
	}, now)
	if !changed || dropped != 0 {
		t.Fatalf("changed=%v dropped=%d", changed, dropped)
	}
	if got := firstSeenOf(t, l, "a", "h.example.com"); !got.Equal(t0) {
		t.Errorf("seeded firstSeen = %v, want the creation time", got)
	}
	if got := firstSeenOf(t, l, "b", "zero.example.com"); !got.Equal(now) {
		t.Errorf("a claim with no creation time = %v, want now", got)
	}
}

func TestHostClaims_RecordAgesANewClaimNow(t *testing.T) {
	l, _ := parseHostClaims(hostClaimsHeader+"\n", true)
	now := t0.Add(24 * time.Hour)
	l.record([]hostClaim{claimOf("a", "h.example.com", t0.Add(-time.Hour))}, now)
	if got := firstSeenOf(t, l, "a", "h.example.com"); !got.Equal(now) {
		t.Errorf("a claim new to an existing ledger = %v, want now", got)
	}
}

func TestHostClaims_RecordNeverMovesAFirstSeen(t *testing.T) {
	l, _ := parseHostClaims("a h.example.com "+t0.Format(time.RFC3339Nano)+"\n", true)
	claims := []hostClaim{claimOf("a", "h.example.com", t0.Add(-100*time.Hour))}
	changed, _ := l.record(claims, t0.Add(time.Hour))
	if changed {
		t.Error("recording a claim already there changed the ledger")
	}
	if got := firstSeenOf(t, l, "a", "h.example.com"); !got.Equal(t0) {
		t.Errorf("firstSeen moved to %v", got)
	}
}

func TestHostClaims_AnAbsentClaimIsMarkedThenRemovedAfterTheGrace(t *testing.T) {
	l, _ := parseHostClaims("a h.example.com "+t0.Format(time.RFC3339Nano)+"\n", true)
	k := claimKey{"a", "h.example.com"}
	t1 := t0.Add(time.Minute)
	if changed, _ := l.record(nil, t1); !changed || !l.entries[k].absentSince.Equal(t1) {
		t.Fatalf("not marked absent: %+v", l.entries[k])
	}
	if changed, _ := l.record(nil, t1.Add(hostClaimGrace-time.Nanosecond)); changed {
		t.Error("removed before the grace")
	}
	if _, ok := l.entries[k]; !ok {
		t.Fatal("removed before the grace")
	}
	// The mark is made once.
	if !l.entries[k].absentSince.Equal(t1) {
		t.Errorf("absentSince moved to %v", l.entries[k].absentSince)
	}
	if changed, _ := l.record(nil, t1.Add(hostClaimGrace+time.Second)); !changed {
		t.Error("removal not reported as a change")
	}
	if _, ok := l.entries[k]; ok {
		t.Error("still there after the grace")
	}
}

func TestHostClaims_AClaimThatReturnsIsNoLongerAbsent(t *testing.T) {
	l, _ := parseHostClaims("a h.example.com "+t0.Format(time.RFC3339Nano)+" "+t0.Add(time.Minute).Format(time.RFC3339Nano)+"\n", true)
	changed, _ := l.record([]hostClaim{claimOf("a", "h.example.com", t0)}, t0.Add(10*time.Minute))
	if !changed {
		t.Error("clearing absentSince is a change")
	}
	e := l.entries[claimKey{"a", "h.example.com"}]
	if !e.absentSince.IsZero() || !e.firstSeen.Equal(t0) {
		t.Errorf("entry = %+v", e)
	}
}

func TestHostClaims_NowIsClampedToTheNewestEntry(t *testing.T) {
	l, _ := parseHostClaims(
		"a h "+t0.Add(time.Hour).Format(time.RFC3339Nano)+"\n"+
			"b h "+t0.Format(time.RFC3339Nano)+"\n", true)
	if got := l.now(t0); !got.Equal(t0.Add(time.Hour + time.Nanosecond)) {
		t.Errorf("a clock before the newest entry gave %v", got)
	}
	if got := l.now(t0.Add(3 * time.Hour)); !got.Equal(t0.Add(3 * time.Hour)) {
		t.Errorf("a clock after every entry gave %v", got)
	}
}

func TestHostClaims_AgeUsesTheLedgerAndNowForTheRest(t *testing.T) {
	l, _ := parseHostClaims("a h.example.com "+t0.Format(time.RFC3339Nano)+"\n", true)
	now := t0.Add(time.Hour)
	in := []hostClaim{claimOf("a", "H.example.com", t0.Add(30*time.Minute)), claimOf("b", "h.example.com", t0.Add(-time.Hour))}
	out := l.age(in, now)
	if !out[0].created.Equal(t0) || !out[1].created.Equal(now) {
		t.Errorf("aged = %v, %v", out[0].created, out[1].created)
	}
	if in[0].created.Equal(t0) {
		t.Error("age changed its input")
	}
}

func TestHostClaims_BodyIsCappedAndTheNewestEntriesAreDroppedFirst(t *testing.T) {
	l, _ := parseHostClaims("", true)
	var claims []hostClaim
	for i := 0; i < 14000; i++ {
		host := fmt.Sprintf("host-%06d.%s.example.com", i, strings.Repeat("x", 20))
		claims = append(claims, claimOf("ns", host, t0))
	}
	// Two later entries, to be dropped before any of the older ones.
	older := t0.Add(-time.Hour)
	l.record(claims, older)
	newest := claimOf("zz", "newest.example.com", t0)
	l.entries[claimKey{"zz", "newest.example.com"}] = claimEntry{firstSeen: t0.Add(time.Hour)}
	_, dropped := l.record(append(claims, newest), t0.Add(2*time.Hour))
	if len(l.body()) > maxHostClaimsBytes {
		t.Fatalf("body is %d bytes, over the cap", len(l.body()))
	}
	if dropped == 0 {
		t.Fatalf("nothing dropped from %d entries", len(claims)+1)
	}
	if _, ok := l.entries[claimKey{"zz", "newest.example.com"}]; ok {
		t.Error("the newest entry was kept while the cap dropped lines")
	}
	if _, ok := l.entries[claimKey{"ns", claims[0].host}]; !ok {
		t.Error("an old entry was dropped")
	}
	if _, ok := l.entries[claimKey{"ns", claims[len(claims)-1].host}]; ok {
		t.Error("the last of the equally new entries was kept")
	}
}

func TestHostClaims_BodyIsSortedAndStable(t *testing.T) {
	a, _ := parseHostClaims("", true)
	b, _ := parseHostClaims("", true)
	claims := []hostClaim{
		claimOf("b", "z.example.com", t0), claimOf("a", "z.example.com", t0),
		claimOf("a", "a.example.com", t0), claimOf("b", "a.example.com", t0),
	}
	a.record(claims, t0)
	rev := []hostClaim{claims[3], claims[2], claims[1], claims[0]}
	b.record(rev, t0)
	if a.body() != b.body() {
		t.Errorf("order of the claims changed the body:\n%s\n%s", a.body(), b.body())
	}
	want := []string{"a a.example.com", "a z.example.com", "b a.example.com", "b z.example.com"}
	lines := strings.Split(strings.TrimSpace(a.body()), "\n")[1:]
	for i, w := range want {
		if !strings.HasPrefix(lines[i], w+" ") {
			t.Errorf("line %d = %q, want prefix %q", i, lines[i], w)
		}
	}
	if changed, _ := a.record(claims, t0.Add(time.Hour)); changed {
		t.Error("recording the same claims again changed the body")
	}
}

func TestHostClaims_AKeyWithWhitespaceIsNotWritten(t *testing.T) {
	l, _ := parseHostClaims("", true)
	now := t0.Add(time.Hour)
	l.record([]hostClaim{
		{ns: "a", host: "ok.example.com"},
		{ns: "b", host: "x.example.com\naaa owned.example.com"},
		{ns: "c d", host: "z.example.com"},
	}, now)
	body := l.body()
	if strings.Contains(body, "aaa owned") || strings.Contains(body, "c d") || !strings.Contains(body, "a ok.example.com") {
		t.Errorf("body:\n%s", body)
	}
	if _, malformed := parseHostClaims(body, true); malformed != 0 {
		t.Errorf("the body written does not parse cleanly: %d", malformed)
	}
}

func TestHostClaims_ADuplicateKeyKeepsTheOldestAndIsCounted(t *testing.T) {
	body := "a h.example.com 2025-01-01T12:00:00Z\na h.example.com 2025-01-01T15:00:00Z\na h.example.com 2025-01-01T09:00:00Z\n"
	l, malformed := parseHostClaims(body, true)
	if malformed != 2 {
		t.Errorf("malformed = %d, want 2", malformed)
	}
	want := time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC)
	if got := l.entries[claimKey{"a", "h.example.com"}].firstSeen; !got.Equal(want) {
		t.Errorf("firstSeen = %v, want the oldest %v", got, want)
	}
}

func TestHostClaims_ANewClaimIsNeverAsOldAsTheNewestLine(t *testing.T) {
	future := t0.Add(5 * time.Hour)
	l, _ := parseHostClaims(claimLine(claimKey{"a", "h.example.com"}, claimEntry{firstSeen: future})+"\n", true)
	l.record([]hostClaim{{ns: "a", host: "h.example.com"}, {ns: "b", host: "g.example.com"}}, l.now(t0))
	if got := l.entries[claimKey{"b", "g.example.com"}].firstSeen; !got.After(future) {
		t.Errorf("a claim from a later render is aged %v, not after %v", got, future)
	}
}
