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

	hostClaimsHeader = "# Written by synapse-operator. Do not edit. <namespace> <host> <first seen> [<absent since>]"
)

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

// now is the time a claim new to the ledger is aged at: the clock, but never
// before the newest line, so that a clock that steps back cannot make a new
// claim older than one the ledger already holds.
func (l *hostClaims) now(clock time.Time) time.Time {
	for _, e := range l.entries {
		if e.firstSeen.After(clock) {
			clock = e.firstSeen
		}
	}
	return clock
}

// record brings the ledger up to date with the claims made now, and reports
// whether its body changed and how many lines were dropped to keep it under
// maxHostClaimsBytes.
func (l *hostClaims) record(claims []hostClaim, now time.Time) (changed bool, dropped int) {
	before := l.body()
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
		case e.absentSince.Add(hostClaimGrace).Before(now):
			delete(l.entries, k)
		}
	}
	dropped = l.capSize()
	return l.body() != before, dropped
}

// capSize drops lines, the newest first and, among equals, the last in key
// order, until the body fits. The newest are the ones with the least to lose:
// a claim that is dropped is aged at the time it is next seen.
func (l *hostClaims) capSize() int {
	size := len(hostClaimsHeader) + 1
	for k, e := range l.entries {
		size += len(claimLine(k, e)) + 1
	}
	if size <= maxHostClaimsBytes {
		return 0
	}
	keys := sortedClaimKeys(l.entries)
	sort.SliceStable(keys, func(i, j int) bool {
		return l.entries[keys[i]].firstSeen.After(l.entries[keys[j]].firstSeen)
	})
	// Stable on key order, newest first: ties keep the earlier key first, and
	// would be dropped first. Walk the ties from the end instead.
	dropped := 0
	for i := 0; i < len(keys) && size > maxHostClaimsBytes; {
		j := i
		for j < len(keys) && l.entries[keys[j]].firstSeen.Equal(l.entries[keys[i]].firstSeen) {
			j++
		}
		for n := j - 1; n >= i && size > maxHostClaimsBytes; n-- {
			size -= len(claimLine(keys[n], l.entries[keys[n]])) + 1
			delete(l.entries, keys[n])
			dropped++
		}
		i = j
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
