# East-west microsegmentation e2e test (Rung D)

Push-button validation that the agent flags a NetworkPolicy violation on real
east-west traffic. Assumes Rung C is done: the patched synapse image runs on a
6.11 canary node, the operator runs with `--identity-producer --edge-producer`,
and download-api has `S3_IDENTITY_PREFIX` / `S3_POLICY_EDGES_PREFIX` set with
the agent key holding `identity:read` and the operator key `identity:write`.

## Topology

| Workload | Label | Role |
|---|---|---|
| `api` | `app=api` | protected dst, pinned to the canary node |
| `frontend` | `app=frontend` | **allowed** source (NetworkPolicy permits it) |
| `attacker` | `app=attacker` | **violating** source (no rule permits it) |

NetworkPolicy `api-allow-frontend`: `api` accepts ingress only from `frontend`
on 8080. The operator compiles this to:

```
E ew-microseg-test/frontend > ew-microseg-test/api : 8080
G ew-microseg-test/api
```

Expected agent `edge.*` verdicts:

| Flow | declared | policy_violation |
|---|---|---|
| frontend → api:8080 | 1 | 0 |
| attacker → api:8080 | 0 | **1** |

## Prerequisites worth checking

- **Overlay visibility.** The agent must see cross-node pod traffic — i.e. Phase 0
  overlay decap + `cilium_vxlan` attach is active on the canary node. The
  anti-affinity in the manifest forces the generators off the api node so traffic
  is VXLAN-encapped across nodes (what the agent captures + decaps).
- **CNI enforcement caveat.** If Cilium *enforces* the NetworkPolicy, `attacker → api`
  is dropped at the source veth and the connection never establishes — but the
  agent still observes the SYN and flags on the first packet, so the violation is
  visible. For an unambiguous established-flow signal, run in a cluster/namespace
  where CNI NetworkPolicy enforcement is off; the agent is then the enforcing layer
  (the whole point of wire-level microseg).

## Run

```bash
# 1. Apply the topology, pinning api to your canary node.
CANARY_NODE=prod-arxignis-cbwqv-xxxxx
sed "s/CANARY_NODE/$CANARY_NODE/" manifests.yaml | \
  kubectl --kubeconfig ~/.kube/arxignis-kubeconfig.yaml apply -f -

# 2. Wait for the operator to compile + upload the edge-set, then confirm the
#    artifact contains the expected E/G lines.
curl -s -H "Authorization: Bearer $AGENT_KEY" https://<download-api>/v1/policy-edges/version
curl -s -H "Authorization: Bearer $AGENT_KEY" https://<download-api>/v1/policy-edges/download | \
  grep -E 'ew-microseg-test'
# expect:  E ew-microseg-test/frontend > ew-microseg-test/api : 8080
#          G ew-microseg-test/api
```

## Verify the agent verdicts (debug-flip)

```bash
# 3. Flip the canary agent to debug, restart its pod, tail logs.
#    (the proven debug-flip: patch the agent ConfigMap logging.level -> debug)
kubectl -n synapse-os logs -f <canary-agent-pod> | \
  grep -E 'edge\.(declared|policy_violation)|id\.(src|dst)_workload'

# Expect, for the attacker source IP, a line where:
#   id.src_workload=attacker  id.dst_workload=api  edge.policy_violation=1
# and for frontend:
#   id.src_workload=frontend  id.dst_workload=api  edge.declared=1 (no violation)
```

## Stage to enforce + confirm the block

```bash
# 4. Author an alert-first -> drop smart_firewall rule (DB row / config):
#       edge.policy_violation == 1   ->  Drop (ip /32)
#    Then confirm the kernel block + the telemetry row.
psql "$AXPROD" -c "
  select layer, source, src_ip, count(*)
  from block_events_raw
  where ts > now() - interval '5 min' and layer = 'smart_firewall'
  group by 1,2,3 order by 4 desc;"
# expect a row:  smart_firewall | microseg | <attacker pod IP> | N
```

`source = microseg` is the `BlockSource::Microseg` attribution wired for
`edge.*` / `id.*` rules.

## Teardown

```bash
kubectl --kubeconfig ~/.kube/arxignis-kubeconfig.yaml delete namespace ew-microseg-test
# revert the agent ConfigMap logging.level -> info, and remove the drop rule.
```
