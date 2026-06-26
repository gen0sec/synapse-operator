#!/usr/bin/env bash
# Rung A — validate the operator's identity + edge PRODUCERS against a real
# cluster, OUT-OF-CLUSTER, with no images and no deploy. Runs the operator
# binary off your kubeconfig pointed at a local PUT catcher, then shows exactly
# what it would upload (identity.mmdb + policy-edges.txt) built from live cluster
# state. Zero blast radius — the catcher just saves the bodies to disk.
#
# Usage:
#   ./rung-a-producer-check.sh [KUBECONFIG] [RUN_SECONDS]
# Defaults: KUBECONFIG=$HOME/.kube/arxignis-kubeconfig.yaml, RUN_SECONDS=35
set -euo pipefail

KUBECONFIG_PATH="${1:-$HOME/.kube/arxignis-kubeconfig.yaml}"
RUN_SECONDS="${2:-35}"
PORT=9000
OUT="$(mktemp -d)"
OP_DIR="$(cd "$(dirname "$0")/../.." && pwd)" # synapse-operator module root

echo "== Rung A producer check =="
echo "  kubeconfig : $KUBECONFIG_PATH"
echo "  operator   : $OP_DIR"
echo "  output dir : $OUT"
echo "  run for    : ${RUN_SECONDS}s"
[ -f "$KUBECONFIG_PATH" ] || { echo "!! kubeconfig not found: $KUBECONFIG_PATH"; exit 1; }

# 1. PUT catcher: saves /v1/<kind>/upload bodies to $OUT/<kind>.{mmdb,txt}.
python3 - "$PORT" "$OUT" <<'PY' &
import sys, os
from http.server import BaseHTTPRequestHandler, HTTPServer
port, out = int(sys.argv[1]), sys.argv[2]
class H(BaseHTTPRequestHandler):
    def do_PUT(self):
        n = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(n)
        kind = self.path.split('/')[2] if len(self.path.split('/')) > 2 else 'unknown'
        ext = 'mmdb' if 'identity' in kind else 'txt'
        with open(os.path.join(out, f'{kind}.{ext}'), 'wb') as f:
            f.write(body)
        print(f'[catcher] PUT {self.path} -> {kind}.{ext} ({len(body)} bytes)', flush=True)
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"success":true}')
    def log_message(self, *a): pass
HTTPServer(('127.0.0.1', port), H).serve_forever()
PY
CATCHER_PID=$!
trap 'kill $CATCHER_PID 2>/dev/null || true; kill $OP_PID 2>/dev/null || true' EXIT
sleep 1

# 2. Run the operator out-of-cluster with both producers enabled.
echo "== starting operator (out-of-cluster) =="
(
  cd "$OP_DIR"
  KUBECONFIG="$KUBECONFIG_PATH" SYNAPSE_API_KEY="dummy-rung-a" GOFLAGS=-mod=mod GOPROXY=off \
    go run . \
      --identity-producer --edge-producer \
      --download-api-url "http://127.0.0.1:$PORT/v1" \
      --identity-producer-interval 15s --edge-producer-interval 15s 2>&1 \
    | sed 's/^/[operator] /'
) &
OP_PID=$!

sleep "$RUN_SECONDS"
kill "$OP_PID" 2>/dev/null || true
kill "$CATCHER_PID" 2>/dev/null || true
sleep 1

# 3. Inspect what the producers built from the live cluster.
echo
echo "== captured artifacts in $OUT =="
ls -la "$OUT" || true

if [ -f "$OUT/policy-edges.txt" ]; then
  echo
  echo "== policy-edges.txt (first 40 lines) =="
  head -40 "$OUT/policy-edges.txt"
  echo "   ($(grep -c '^E ' "$OUT/policy-edges.txt" 2>/dev/null || echo 0) edges, \
$(grep -c '^G ' "$OUT/policy-edges.txt" 2>/dev/null || echo 0) ingress-governed, \
$(grep -c '^S ' "$OUT/policy-edges.txt" 2>/dev/null || echo 0) egress-governed)"
else
  echo "!! no policy-edges.txt captured — no NetworkPolicies in the cluster, or the producer didn't run. Check [operator] logs above."
fi

if [ -f "$OUT/identity.mmdb" ]; then
  echo
  echo "== identity.mmdb =="
  echo "   $(wc -c < "$OUT/identity.mmdb") bytes"
  if command -v mmdbinspect >/dev/null 2>&1; then
    echo "   (use: mmdbinspect -db $OUT/identity.mmdb <pod-ip>)"
  fi
else
  echo "!! no identity.mmdb captured — no non-host-network pods, or the producer didn't run."
fi

echo
echo "== done. artifacts kept at $OUT =="
trap - EXIT
