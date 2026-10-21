package controllers

import (
	"sort"
	"strings"
	"time"
	"unicode"
)

// The claim ledger says when the proxy first saw each (namespace, host)
// claim. A claim is aged by that, not by the creationTimestamp of the object
// that makes it: an object can be edited to name a host long after it was
// created, and its creationTimestamp does not move.
//
// The ledger supplies ages, never claims. A line with no claim behind it
// holds nothing.

const (
	// hostClaimGrace is how long the line of a claim that is no longer made
	// is kept, so that an object deleted and recreated, or an edit in two
	// steps, does not start its age again.
	hostClaimGrace = time.Hour
	// maxHostClaimsBytes keeps the ledger well below the 1 MiB a ConfigMap
	// can hold.
	maxHostClaimsBytes = 768 << 10
	// maxHostClaimsPerNamespace is how many lines one namespace may hold. A
	// namespace that can name any number of hosts (one Ingress may carry
	// thousands in spec.tls) could otherwise fill the ledger, and every claim
	// made after that would be aged at the time of each render, so that two
	// namespaces that contest a host tie and the lower name wins. The limit
	// leaves room for hundreds of namespaces under maxHostClaimsBytes.
	maxHostClaimsPerNamespace = 1024

	hostClaimsHeader = "# Written by synapse-operator. Do not edit. <namespace> <host> <first seen> [<absent since>]"
)

// expired says a line has been kept long enough: its claim was seen absent,
// and the grace since is over. A claim made again after that is a new one,
// whether or not a render saw the gap.
func (e claimEntry) expired(now time.Time) bool {
	return !e.absentSince.IsZero() && !e.absentSince.Add(hostClaimGrace).After(now)
}

type claimKey struct{ ns, host string }

type claimEntry struct {
	firstSeen   time.Time
	absentSince time.Time
}

// hostClaims is the ledger of one proxy. exists says the ConfigMap it was
// read from was there: a ledger that does not exist yet is seeded from the
// creation times of the objects, and one that does exist is not.
type hostClaims struct {
	entries map[claimKey]claimEntry
	exists  bool
	// refused counts the claims record left out for a name that cannot be
	// written as a line.
	refused int
	// quotaDropped counts, by namespace, the lines record dropped for
	// maxHostClaimsPerNamespace.
	quotaDropped map[string]int
}

// parseHostClaims reads a ledger body and reports how many lines it could
// not read. Those are skipped.
func parseHostClaims(body string, exists bool) (*hostClaims, int) {
	l := &hostClaims{entries: map[claimKey]claimEntry{}, exists: exists}
	malformed := 0
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 && len(f) != 4 {
			malformed++
			continue
		}
		first, err := time.Parse(time.RFC3339Nano, f[2])
		if err != nil {
			malformed++
			continue
		}
		e := claimEntry{firstSeen: first.UTC()}
		if len(f) == 4 {
			absent, err := time.Parse(time.RFC3339Nano, f[3])
			if err != nil {
				malformed++
				continue
			}
			e.absentSince = absent.UTC()
		}
		k := claimKey{ns: f[0], host: f[1]}
		if old, dup := l.entries[k]; dup {
			// Two lines for one claim: the oldest stands, so that a line
			// added to the text cannot make a claim younger.
			malformed++
			if old.firstSeen.Before(e.firstSeen) {
				e = old
			}
		}
		l.entries[k] = e
	}
	return l, malformed
}

func claimLine(k claimKey, e claimEntry) string {
	s := k.ns + " " + k.host + " " + e.firstSeen.UTC().Format(time.RFC3339Nano)
	if !e.absentSince.IsZero() {
		s += " " + e.absentSince.UTC().Format(time.RFC3339Nano)
	}
	return s
}

func sortedClaimKeys(entries map[claimKey]claimEntry) []claimKey {
	keys := make([]claimKey, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ns != keys[j].ns {
			return keys[i].ns < keys[j].ns
		}
		return keys[i].host < keys[j].host
	})
	return keys
}

// body is the text of the ledger, sorted so that the same entries give the
// same bytes.
func (l *hostClaims) body() string {
	var b strings.Builder
	b.WriteString(hostClaimsHeader)
	b.WriteByte('\n')
	for _, k := range sortedClaimKeys(l.entries) {
		b.WriteString(claimLine(k, l.entries[k]))
		b.WriteByte('\n')
	}
	return b.String()
}

// now is the time a claim new to the ledger is aged at: the clock, but always
// after the newest line, so that a clock that steps back can neither make a
// new claim older than one the ledger already holds nor tie with it.
func (l *hostClaims) now(clock time.Time) time.Time {
	for _, e := range l.entries {
		if next := e.firstSeen.Add(time.Nanosecond); next.After(clock) {
			clock = next
		}
	}
	return clock
}

// record brings the ledger up to date with the claims made now, and reports
// whether its body changed and how many lines were dropped to keep it under
// maxHostClaimsBytes.
func (l *hostClaims) record(claims []hostClaim, now time.Time) (changed bool, dropped int) {
	before := l.body()
	l.quotaDropped = nil
	present := map[claimKey]bool{}
	for _, c := range claims {
		host := hostKey(c.host)
		if host == "" {
			continue
		}
		if strings.ContainsFunc(c.ns, unicode.IsSpace) || strings.ContainsFunc(host, unicode.IsSpace) {
			l.refused++
			continue
		}
		k := claimKey{ns: c.ns, host: host}
		present[k] = true
		e, ok := l.entries[k]
		if ok && e.expired(now) {
			ok = false
		}
		if !ok {
			e.firstSeen = now
			if !l.exists && !c.created.IsZero() {
				e.firstSeen = c.created.UTC()
			}
		}
		e.absentSince = time.Time{}
		l.entries[k] = e
	}
	for k, e := range l.entries {
		if present[k] {
			continue
		}
		switch {
		case e.absentSince.IsZero():
			e.absentSince = now
			l.entries[k] = e
		case e.expired(now):
			delete(l.entries, k)
		}
	}
	l.applyQuota()
	dropped = l.capSize()
	return l.body() != before, dropped
}

// dropOrder lists keys, the newest line first and, among equals, the last in
// key order first: the lines with the least to lose, since a claim that is
// dropped is aged at the time it is next seen.
func (l *hostClaims) dropOrder(keys []claimKey) []claimKey {
	sort.Slice(keys, func(i, j int) bool {
		a, b := l.entries[keys[i]], l.entries[keys[j]]
		if !a.firstSeen.Equal(b.firstSeen) {
			return a.firstSeen.After(b.firstSeen)
		}
		return keys[i].host > keys[j].host
	})
	return keys
}

// byNamespace groups the keys by namespace, each group in dropOrder.
func (l *hostClaims) byNamespace() map[string][]claimKey {
	groups := map[string][]claimKey{}
	for k := range l.entries {
		groups[k.ns] = append(groups[k.ns], k)
	}
	for ns := range groups {
		l.dropOrder(groups[ns])
	}
	return groups
}

// applyQuota drops the lines of a namespace past maxHostClaimsPerNamespace,
// its newest first. A line already in the ledger is older than a claim new
// to it, so a namespace at its quota cannot push out its own older lines.
func (l *hostClaims) applyQuota() {
	counts := map[string]int{}
	for k := range l.entries {
		counts[k.ns]++
	}
	var over bool
	for _, n := range counts {
		if n > maxHostClaimsPerNamespace {
			over = true
			break
		}
	}
	if !over {
		return
	}
	for ns, keys := range l.byNamespace() {
		for _, k := range keys[:max(0, len(keys)-maxHostClaimsPerNamespace)] {
			delete(l.entries, k)
			if l.quotaDropped == nil {
				l.quotaDropped = map[string]int{}
			}
			l.quotaDropped[ns]++
		}
	}
}

// capSize drops lines until the body fits, each time the newest line of the
// namespace that holds the most: a namespace that floods the ledger loses its
// own lines before a small one loses any. Among namespaces with as many, the
// last by name goes first.
func (l *hostClaims) capSize() int {
	size := len(hostClaimsHeader) + 1
	for k, e := range l.entries {
		size += len(claimLine(k, e)) + 1
	}
	if size <= maxHostClaimsBytes {
		return 0
	}
	groups := l.byNamespace()
	dropped := 0
	for size > maxHostClaimsBytes {
		var pick string
		for ns, keys := range groups {
			if len(keys) == 0 {
				continue
			}
			if pick == "" || len(keys) > len(groups[pick]) || (len(keys) == len(groups[pick]) && ns > pick) {
				pick = ns
			}
		}
		if pick == "" {
			break
		}
		k := groups[pick][0]
		groups[pick] = groups[pick][1:]
		size -= len(claimLine(k, l.entries[k])) + 1
		delete(l.entries, k)
		dropped++
	}
	return dropped
}

// age is claims with created replaced by when the ledger first saw each; a
// claim the ledger does not hold is as new as now.
func (l *hostClaims) age(claims []hostClaim, now time.Time) []hostClaim {
	out := make([]hostClaim, len(claims))
	for i, c := range claims {
		out[i] = c
		if e, ok := l.entries[claimKey{ns: c.ns, host: hostKey(c.host)}]; ok {
			out[i].created = e.firstSeen
		} else {
			out[i].created = now
		}
	}
	return out
}
