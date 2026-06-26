# Staged east-west enforcement

The ruleset in `east-west-rules.json` is authored as `smart_firewall_rules` rows
and flows config-generator → config-api (wirefilter bucket) → agent. Every
expression is verified-parseable against the amygdala scheme (the
`scheme_parses_basic_expressions` test), so the rows can't drift from the fields
the agent actually exposes.

## The signals each rule keys on

| Rule | Needs | Signal |
|---|---|---|
| `ew-policy-violation` | identity + policy-edges feeds | `edge.policy_violation` (NetworkPolicy deny) |
| `ew-undeclared-to-apiserver` | identity + policy-edges feeds | `id.dst_workload` + `edge.declared` |
| `ew-internal-port-scan` | nothing (Phase-1 only) | `flow.unique_dst_ports` |
| `ew-internal-beacon` | nothing (Phase-1 only) | `flow.flows_per_min` + `flow.dst_port_entropy` |
| `ew-policy-violation-scanning` | identity + policy-edges feeds | fusion (edge + flow) |

The two behavioural rules fire on already-shipped Phase-1 fields and work the
moment east-west traffic is visible (Phase 0). The three microseg rules need the
identity MMDB + policy-edges feeds (the operator producers).

## Two ways to run a rule without enforcing

1. **Per-rule `action: "log"`** (recommended for testing) — set a single rule's
   action to `log`. It matches and records a `block_events_raw` row with
   `action = notice`, but installs **no** kernel block, regardless of
   `enforce_block`. This lets you dry-run *one* rule while others stay enforcing
   — the precise lever for staging a new rule. (`block` and `allow` are the
   other two valid smart_firewall actions.)
2. **Global `ids.enforce_block: false`** — flips the *whole* engine to
   alert-only: every rule evaluates, increments `block_count`, and records
   `block_events_raw`, but nothing drops. Use for a fleet-wide observe phase.

The `action` field in `east-west-rules.json` defaults each rule to `block`;
change any to `"log"` to dry-run just that one.

## Stage 1 — alert-first (no drops)

Author the new rules with `action: "log"` (or deploy with
`ids.enforce_block: false`). The rules still evaluate per flow and record
`block_events_raw` — but install **no** kernel drop. This is the observe phase.

```sql
-- Watch what matched, by rule source + action, over the last hour.
-- action='notice' = a log-only rule matched (dry-run); 'block' = enforced.
select source, action, src_ip, count(*)
from block_events_raw
where ts > now() - interval '1 hour'
  and layer = 'smart_firewall'
  and source in ('microseg','behavioral')
group by 1,2,3 order by 4 desc;
```

Tune until the volume is sane and the hits are real (no benign internal jobs
tripping the scan/beacon thresholds; no legitimate edges flagged as violations —
those usually mean a missing/stale NetworkPolicy or an identity-feed gap, not an
attack).

## Stage 2 — enforce the safe ones first

Flip `ids.enforce_block: true`, but **lead with the fused rule**
(`ew-policy-violation-scanning`) — a policy violation that is also fanning out is
the lowest-false-positive case. Keep the single-signal rules alert-only a while
longer, then promote them once Stage 1 shows them clean.

The `is_protected_ip` never-ban guard already wraps every smart-firewall block,
so shared infra / LB VIPs / node IPs can never be blackholed even if a rule
over-matches.

## Stage 3 — microsegmentation (allow-list)

Once `ew-policy-violation` runs clean in enforce mode, the cluster is effectively
microsegmented at the wire: any internal edge not declared by a NetworkPolicy and
governed (ingress-restricted dst or egress-restricted src) is dropped. Extend
coverage by adding NetworkPolicies — the operator EdgeProducer compiles them into
the allow-list automatically; no rule edits needed.

## Why these are safe to author now

- Expressions are scheme-verified (won't fail to compile on the agent).
- `source` attribution (`microseg` / `behavioral`) is wired through
  `BlockSource` → `block_events_raw`, so Stage-1 observation is queryable.
- Alert-first is the default posture; nothing drops until `enforce_block` flips.
