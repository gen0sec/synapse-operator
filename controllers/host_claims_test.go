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
	body := "a h.example.com 2025-01-01T15:00:00Z\na h.example.com 2025-01-01T09:00:00Z\na h.example.com 2025-01-01T12:00:00Z\n"
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

func manyClaims(ns string, n int, created time.Time, pad int) []hostClaim {
	var claims []hostClaim
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("h-%06d.%s.example.com", i, strings.Repeat("x", pad))
		claims = append(claims, claimOf(ns, host, created))
	}
	return claims
}

func linesOf(l *hostClaims, ns string) int {
	n := 0
	for k := range l.entries {
		if k.ns == ns {
			n++
		}
	}
	return n
}

func TestHostClaims_AQuotaDropsTheNewestLinesOfANamespace(t *testing.T) {
	l, _ := parseHostClaims("", true)
	n := maxHostClaimsPerNamespace + 10
	var claims []hostClaim
	for i := 0; i < n; i++ {
		// Older with a lower number, so the last ten are the newest.
		claims = append(claims, claimOf("ns", fmt.Sprintf("h-%06d.example.com", i), t0.Add(time.Duration(i)*time.Second)))
	}
	l.record(claims, t0.Add(24*time.Hour))
	if got := linesOf(l, "ns"); got != maxHostClaimsPerNamespace {
		t.Fatalf("namespace holds %d lines, want %d", got, maxHostClaimsPerNamespace)
	}
	if l.quotaDropped["ns"] != 10 {
		t.Errorf("quotaDropped = %v", l.quotaDropped)
	}
	// Seeded from creation: the newest ten are dropped.
	if _, ok := l.entries[claimKey{"ns", fmt.Sprintf("h-%06d.example.com", n-1)}]; ok {
		t.Error("the newest line was kept")
	}
	if _, ok := l.entries[claimKey{"ns", "h-000000.example.com"}]; !ok {
		t.Error("the oldest line was dropped")
	}
}

func TestHostClaims_ExistingLinesBeatNewClaimsAtTheQuota(t *testing.T) {
	l, _ := parseHostClaims("", true)
	old := manyClaims("ns", maxHostClaimsPerNamespace, t0, 1)
	l.record(old, t0)
	more := append(manyClaims("ns", 5, t0, 2), old...)
	l.record(more, t0.Add(time.Hour))
	if got := linesOf(l, "ns"); got != maxHostClaimsPerNamespace {
		t.Fatalf("namespace holds %d lines", got)
	}
	for _, c := range old {
		if _, ok := l.entries[claimKey{"ns", c.host}]; !ok {
			t.Fatalf("an existing line %s was evicted by a new claim", c.host)
		}
	}
	if l.quotaDropped["ns"] != 5 {
		t.Errorf("quotaDropped = %v", l.quotaDropped)
	}
}

func TestHostClaims_TheByteCapDropsFromTheLargestNamespaceFirst(t *testing.T) {
	l, _ := parseHostClaims("", true)
	var claims []hostClaim
	// Each under the quota; together over the byte cap.
	for _, ns := range []string{"n1", "n2", "n3", "n4", "n5", "n6"} {
		claims = append(claims, manyClaims(ns, maxHostClaimsPerNamespace, t0, 150)...)
	}
	claims = append(claims, manyClaims("small", 3, t0.Add(time.Hour), 150)...)
	_, dropped := l.record(claims, t0.Add(2*time.Hour))
	if len(l.body()) > maxHostClaimsBytes {
		t.Fatalf("body is %d bytes, over the cap", len(l.body()))
	}
	if dropped == 0 {
		t.Fatal("nothing dropped")
	}
	if got := linesOf(l, "small"); got != 3 {
		t.Errorf("a small namespace lost lines to larger ones: %d left", got)
	}
	lo, hi := maxHostClaimsPerNamespace, 0
	for _, ns := range []string{"n1", "n2", "n3", "n4", "n5", "n6"} {
		n := linesOf(l, ns)
		lo, hi = min(lo, n), max(hi, n)
	}
	if hi-lo > 1 {
		t.Errorf("the large namespaces were cut unevenly: %d to %d lines", lo, hi)
	}
}

func TestHostClaims_ANamespaceUnderItsShareIsNotDroppedWhileALargerOneHasLines(t *testing.T) {
	l, _ := parseHostClaims("", true)
	claims := manyClaims("mid", 300, t0.Add(time.Hour), 150)
	for _, ns := range []string{"a", "b", "c", "d", "e", "f"} {
		claims = append(claims, manyClaims(ns, maxHostClaimsPerNamespace, t0, 150)...)
	}
	l.record(claims, t0.Add(2*time.Hour))
	if got := linesOf(l, "mid"); got != 300 {
		t.Errorf("the namespace under the share lost lines: %d left of 300", got)
	}
}

// put fills the ledger with n lines of a namespace, hosts padded to length.
func put(l *hostClaims, ns string, n, pad int, firstSeen, absent time.Time) []claimKey {
	var keys []claimKey
	for i := 0; i < n; i++ {
		k := claimKey{ns, fmt.Sprintf("h-%06d.%s.example.com", i, strings.Repeat("x", pad))}
		l.entries[k] = claimEntry{firstSeen: firstSeen, absentSince: absent}
		keys = append(keys, k)
	}
	return keys
}

func bodySize(l *hostClaims) int { return len(l.body()) }

func TestHostClaims_TheByteCapDropsNewLinesBeforeAnyLineThatWasThere(t *testing.T) {
	l, _ := parseHostClaims("", true)
	for _, ns := range []string{"x1", "x2", "x3"} {
		put(l, ns, 1024, 170, t0, time.Time{})
	}
	isNew := map[claimKey]bool{}
	for _, k := range put(l, "n", 650, 170, t0.Add(time.Hour), time.Time{}) {
		isNew[k] = true
	}
	if bodySize(l) <= maxHostClaimsBytes {
		t.Fatalf("setup: %d bytes is under the cap", bodySize(l))
	}
	if l.capSize(isNew) == 0 || bodySize(l) > maxHostClaimsBytes {
		t.Fatalf("not capped: %d bytes", bodySize(l))
	}
	for _, ns := range []string{"x1", "x2", "x3"} {
		if got := linesOf(l, ns); got != 1024 {
			t.Errorf("%s lost lines to new ones: %d left", ns, got)
		}
	}
	if linesOf(l, "n") >= 650 {
		t.Error("no new line was dropped")
	}
}

func TestHostClaims_TheByteCapDropsAbsentLinesBeforeEstablishedOnes(t *testing.T) {
	l, _ := parseHostClaims("", true)
	for _, ns := range []string{"x1", "x2", "x3"} {
		put(l, ns, 1024, 170, t0, time.Time{})
	}
	put(l, "n", 650, 170, t0, t0.Add(time.Minute))
	if bodySize(l) <= maxHostClaimsBytes {
		t.Fatalf("setup: %d bytes is under the cap", bodySize(l))
	}
	l.capSize(nil)
	for _, ns := range []string{"x1", "x2", "x3"} {
		if got := linesOf(l, ns); got != 1024 {
			t.Errorf("%s lost lines to absent ones: %d left", ns, got)
		}
	}
	if linesOf(l, "n") >= 650 {
		t.Error("no absent line was dropped")
	}
}

func TestHostClaims_TheByteCapCountsBytesNotLines(t *testing.T) {
	l, _ := parseHostClaims("", true)
	put(l, "z", maxHostClaimsPerNamespace, 10, t0, time.Time{})
	put(l, "q", 1000, 232, t0, time.Time{})
	for _, ns := range []string{"r1", "r2", "r3"} {
		put(l, ns, 700, 170, t0, time.Time{})
	}
	if bodySize(l) <= maxHostClaimsBytes {
		t.Fatalf("setup: %d bytes is under the cap", bodySize(l))
	}
	l.capSize(nil)
	if got := linesOf(l, "z"); got != maxHostClaimsPerNamespace {
		t.Errorf("the namespace with the most lines but few bytes lost lines: %d left", got)
	}
	if linesOf(l, "q") >= 1000 {
		t.Error("the namespace with the most bytes lost nothing")
	}
}

func TestHostClaims_ATieInBytesDropsFromTheLastNamespaceByName(t *testing.T) {
	l, _ := parseHostClaims("", true)
	isNew := map[claimKey]bool{}
	for _, ns := range []string{"m", "n"} {
		for _, k := range put(l, ns, 1000, 150, t0, time.Time{}) {
			isNew[k] = true
		}
	}
	// Short filler, until the body is over the cap by less than one line.
	size := len(hostClaimsHeader) + 1
	for k, e := range l.entries {
		size += reservedLen(k, e)
	}
	for i := 0; size <= maxHostClaimsBytes; i++ {
		k := claimKey{"f", fmt.Sprintf("f%06d.example.com", i)}
		l.entries[k] = claimEntry{firstSeen: t0}
		size += reservedLen(k, l.entries[k])
	}
	if got := l.capSize(isNew); got != 1 {
		t.Fatalf("dropped %d lines, want 1", got)
	}
	if linesOf(l, "n") != 999 || linesOf(l, "m") != 1000 {
		t.Errorf("m has %d lines and n %d: the tie must go to the last name", linesOf(l, "m"), linesOf(l, "n"))
	}
}

func TestHostClaims_AFloodCannotEvictAnEstablishedHostFromAnotherNamespace(t *testing.T) {
	l, _ := parseHostClaims("", true)
	victim := manyClaims("victim", 699, t0, 1)
	victim = append(victim, claimOf("victim", "shop.example.com", t0))
	l.record(victim, t0.Add(time.Hour))
	l.record(append(victim, claimOf("x1", "shop.example.com", t0)), t0.Add(2*time.Hour))
	all := append([]hostClaim{claimOf("x1", "shop.example.com", t0)}, victim...)
	for _, ns := range []string{"x1", "x2", "x3", "x4", "x5"} {
		all = append(all, manyClaims(ns, 650, t0, 232)...)
	}
	_, dropped := l.record(all, t0.Add(5*time.Hour))
	if dropped == 0 {
		t.Fatal("setup: nothing dropped")
	}
	if got := linesOf(l, "victim"); got != 700 {
		t.Errorf("victim has %d lines of 700", got)
	}
	if got := firstSeenOf(t, l, "victim", "shop.example.com"); !got.Equal(t0.Add(time.Hour)) {
		t.Errorf("victim's shop is aged %v", got)
	}
}

func TestHostClaims_AReadOnlyAgeTreatsAnExpiredLineAsNew(t *testing.T) {
	l, _ := parseHostClaims(claimLine(claimKey{"a", "h.example.com"}, claimEntry{firstSeen: t0, absentSince: t0.Add(time.Minute)})+"\n", true)
	now := t0.Add(3 * time.Hour)
	out := l.age([]hostClaim{claimOf("a", "h.example.com", t0)}, now)
	if !out[0].created.Equal(now) {
		t.Errorf("a claim returned after the grace is aged %v, not %v", out[0].created, now)
	}
}

func tinyClaims(ns string, n int, created time.Time) []hostClaim {
	var claims []hostClaim
	for i := 0; i < n; i++ {
		claims = append(claims, claimOf(ns, fmt.Sprintf("t%04d.example.com", i), created))
	}
	return claims
}

// claimsHeld is a claim for every line the ledger holds, except those of skip.
func claimsHeld(l *hostClaims, skip string) []hostClaim {
	var claims []hostClaim
	for _, k := range sortedClaimKeys(l.entries) {
		if k.ns != skip {
			claims = append(claims, claimOf(k.ns, k.host, t0))
		}
	}
	return claims
}

// nearCap is a ledger of established lines that a flood of new tiny claims has
// trimmed, with the victim's and x1's line for shop. pad is chosen so that the
// body is left within absentSuffixMax of maxHostClaimsBytes, counted as the cap
// counts.
// born has nanoseconds, so that a first-seen time is as long as one can be.
var born = t0.Add(123456789)

func nearCap(t *testing.T, pad int) *hostClaims {
	t.Helper()
	l, _ := parseHostClaims("", false)
	var claims []hostClaim
	for _, ns := range []string{"x2", "x3", "x4"} {
		claims = append(claims, manyClaims(ns, 1024, born, pad)...)
	}
	claims = append(claims, claimOf("victim", "shop.example.com", born), claimOf("x1", "shop.example.com", born.Add(time.Hour)))
	claims = append(claims, tinyClaims("t", 1000, born.Add(2*time.Hour))...)
	l.record(claims, t0.Add(3*time.Hour))
	l.record(claimsHeld(l, ""), t0.Add(4*time.Hour))
	for _, ns := range []string{"victim", "x1"} {
		if _, ok := l.entries[claimKey{ns, "shop.example.com"}]; !ok {
			t.Fatalf("setup: the line of %s for shop is gone; the scenario needs retuning", ns)
		}
	}
	return l
}

// countedSlack is how many bytes the cap has left, counting every line as long
// as it can get.
func countedSlack(l *hostClaims) int {
	size := len(hostClaimsHeader) + 1
	for k, e := range l.entries {
		size += reservedLen(k, e)
	}
	return maxHostClaimsBytes - size
}

// tightPads are the pads for which nearCap leaves less room than one line
// going absent grows by, so that counting only the real width would overflow.
func tightPads(t *testing.T) []int {
	t.Helper()
	var pads []int
	for pad := 190; pad <= 230; pad++ {
		if s := countedSlack(nearCap(t, pad)); s >= 0 && s < absentSuffixMax {
			pads = append(pads, pad)
		}
	}
	if len(pads) < 2 {
		t.Fatalf("setup: only pads %v leave the ledger within %d bytes of the cap; the scenario needs retuning", pads, absentSuffixMax)
	}
	return pads
}

func TestHostClaims_AClaimGoingAbsentNeverCostsAnotherNamespaceItsLine(t *testing.T) {
	for _, pad := range tightPads(t) {
		l := nearCap(t, pad)
		held := len(l.entries)
		changed, dropped := l.record(claimsHeld(l, "victim"), t0.Add(5*time.Hour+123456789))
		if !changed || dropped != 0 || len(l.entries) != held {
			t.Errorf("pad %d: the victim going absent dropped %d lines (%d of %d left)", pad, dropped, len(l.entries), held)
		}
		if e, ok := l.entries[claimKey{"victim", "shop.example.com"}]; !ok || e.absentSince.IsZero() {
			t.Fatalf("pad %d: the victim's line is not held as absent", pad)
		}
		l.record(append(claimsHeld(l, "victim"), claimOf("victim", "shop.example.com", born)), t0.Add(5*time.Hour+10*time.Minute))
		l.record(append(claimsHeld(l, "x1"), claimOf("x1", "shop.example.com", born.Add(time.Hour))), t0.Add(6*time.Hour))
		if got := firstSeenOf(t, l, "victim", "shop.example.com"); !got.Equal(born) {
			t.Errorf("pad %d: the victim's shop is aged %v", pad, got)
		}
	}
}

func TestHostClaims_TheBodyNeverExceedsTheCapWhateverGoesAbsent(t *testing.T) {
	for _, pad := range tightPads(t) {
		l := nearCap(t, pad)
		for i, skip := range []string{"victim", "t", "x2", "x3", "x4", "x1"} {
			l.record(claimsHeld(l, skip), t0.Add(time.Duration(5+i)*time.Minute+5*time.Hour+123456789))
			if n := bodySize(l); n > maxHostClaimsBytes {
				t.Errorf("pad %d: after %s went absent the body is %d bytes", pad, skip, n)
			}
		}
		l.record(nil, t0.Add(6*time.Hour+123456789))
		if n := bodySize(l); n > maxHostClaimsBytes {
			t.Errorf("pad %d: after every claim went absent the body is %d bytes", pad, n)
		}
	}
}
