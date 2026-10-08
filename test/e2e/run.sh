#!/usr/bin/env bash
# Runs the end-to-end test: starts a k3s cluster in a container, installs the
# operator built from this tree into it, and runs test/e2e against it.
#
#   E2E_SYNAPSE_IMAGE   the Synapse image the proxies run, 0.8.5 or newer.
#                       Required. Taken from the local Docker, and pulled
#                       into it first if it is not there.
#   E2E_KEEP=1          leave the cluster running afterwards
#   E2E_K3S_IMAGE       the k3s image; cluster.sh has the default
#
# Needs docker, kubectl and go. The cluster has a kubeconfig of its own, in a
# temporary directory.
set -euo pipefail

: "${E2E_SYNAPSE_IMAGE:?set E2E_SYNAPSE_IMAGE to the Synapse image the proxies run, 0.8.5 or newer}"

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
work="$(mktemp -d)"
export E2E_KUBECONFIG="$work/kubeconfig"
export E2E_BACKEND_IMAGE="synapse-operator-e2e-backend:local"
# The image config/manager.yaml runs.
operator_image="synapse-operator:local"

k() {
  kubectl --kubeconfig "$E2E_KUBECONFIG" "$@"
}

diagnostics() {
  echo "==> the cluster when the test failed" >&2
  {
    k get synapseproxies,ingressclasses,ingresses,deployments,pods,services --all-namespaces --output wide
    k get events --all-namespaces --sort-by=.lastTimestamp | tail -n 60
    echo "--- operator"
    k --namespace synapse-os logs deployment/synapse-operator --tail=200
    echo "--- proxy"
    k logs --all-namespaces --selector synapse.gen0sec.com/proxy --tail=200 --prefix
  } >&2 2>&1 || true
}

finish() {
  status=$?
  if [ "$status" -ne 0 ]; then
    diagnostics
  fi
  if [ -n "${E2E_KEEP:-}" ]; then
    echo "==> the cluster is left running: kubectl --kubeconfig $E2E_KUBECONFIG; $here/cluster.sh down removes it" >&2
  else
    "$here/cluster.sh" down
    rmdir "$work" 2>/dev/null || true
  fi
  exit "$status"
}
trap finish EXIT

echo "==> cluster"
"$here/cluster.sh" up

echo "==> images"
docker build --quiet --tag "$operator_image" "$root" >/dev/null
docker build --quiet --tag "$E2E_BACKEND_IMAGE" "$here/backend" >/dev/null
docker image inspect "$E2E_SYNAPSE_IMAGE" >/dev/null 2>&1 || docker pull --quiet "$E2E_SYNAPSE_IMAGE"
"$here/cluster.sh" load "$operator_image" "$E2E_BACKEND_IMAGE" "$E2E_SYNAPSE_IMAGE"

echo "==> operator"
k apply --kustomize "$here/operator"
k --namespace synapse-os rollout status deployment/synapse-operator --timeout=180s

echo "==> test"
E2E_HTTP_ADDR="$("$here/cluster.sh" port 80)"
E2E_HTTPS_ADDR="$("$here/cluster.sh" port 443)"
export E2E_HTTP_ADDR E2E_HTTPS_ADDR
cd "$root"
go test -tags e2e -count=1 -v -timeout 30m ./test/e2e/...
