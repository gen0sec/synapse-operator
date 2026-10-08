#!/usr/bin/env bash
# Runs the end-to-end test: starts a k3s cluster in a container, installs the
# operator built from this tree into it, and runs test/e2e against it.
#
#   E2E_SYNAPSE_IMAGE   the Synapse image the proxies run, 0.8.5 or newer,
#                       with a tag other than latest. Required. It is copied
#                       into the cluster from the local Docker, and pulled
#                       into that first if it is not there.
#   E2E_KEEP=1          leave the cluster running afterwards
#   E2E_K3S_IMAGE       the k3s image; cluster.sh has the default
#   E2E_CLUSTER         the name of the cluster's container
#
# Needs docker, kubectl and go. The cluster has a kubeconfig of its own, in a
# temporary directory.
set -euo pipefail

: "${E2E_SYNAPSE_IMAGE:?set E2E_SYNAPSE_IMAGE to the Synapse image the proxies run, 0.8.5 or newer}"

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
export E2E_BACKEND_IMAGE="synapse-operator-e2e-backend:local"
# The image config/manager.yaml runs.
operator_image="synapse-operator:local"

# The cluster has no credentials for a registry, so it can only run an image
# by the name it was copied in under. Kubernetes pulls one tagged latest, or
# not tagged, every time; and a digest is not a name an image is kept under.
case "${E2E_SYNAPSE_IMAGE##*/}" in
*@* | *:latest)
  unusable=yes
  ;;
*:*)
  unusable=""
  ;;
*)
  unusable=yes
  ;;
esac
if [ -n "$unusable" ]; then
  echo "E2E_SYNAPSE_IMAGE is $E2E_SYNAPSE_IMAGE: it has to name a tag, and not latest" >&2
  exit 1
fi

# What is likeliest to fail, before anything is started.
echo "==> images"
docker image inspect "$E2E_SYNAPSE_IMAGE" >/dev/null 2>&1 || docker pull --quiet "$E2E_SYNAPSE_IMAGE"
docker build --quiet --tag "$operator_image" "$root" >/dev/null
docker build --quiet --tag "$E2E_BACKEND_IMAGE" "$here/backend" >/dev/null

k() {
  kubectl --kubeconfig "$E2E_KUBECONFIG" "$@"
}

diagnostics() {
  echo "==> the cluster when the test failed" >&2
  {
    k get synapseproxies --all-namespaces --output wide
    k get ingressclasses,ingresses,deployments,pods,services --all-namespaces --output wide
    k get events --all-namespaces --sort-by=.lastTimestamp | tail -n 60
    echo "--- operator"
    k --namespace synapse-os logs deployment/synapse-operator --tail=200
    k get pods --all-namespaces --selector synapse.gen0sec.com/proxy \
      --output 'jsonpath={range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' |
      while read -r namespace pod; do
        echo "--- proxy $namespace/$pod"
        k --namespace "$namespace" logs "$pod" --tail=200
      done
  } >&2 2>&1 || true
}

work="$(mktemp -d)"
export E2E_KUBECONFIG="$work/kubeconfig"

# Until the cluster is up there is none of this run's to remove: one that was
# already there under the same name is somebody else's.
trap 'rmdir "$work" 2>/dev/null || true' EXIT
echo "==> cluster"
"$here/cluster.sh" up "$E2E_KUBECONFIG"

finish() {
  status=$?
  if [ "$status" -ne 0 ]; then
    diagnostics
  fi
  if [ -n "${E2E_KEEP:-}" ]; then
    echo "==> the cluster is left running. kubectl --kubeconfig $E2E_KUBECONFIG reaches it, and $here/cluster.sh down removes it" >&2
  else
    "$here/cluster.sh" down
    rm -f "$E2E_KUBECONFIG"
    rmdir "$work" 2>/dev/null || true
  fi
  exit "$status"
}
trap finish EXIT

"$here/cluster.sh" load "$operator_image" "$E2E_BACKEND_IMAGE" "$E2E_SYNAPSE_IMAGE"

echo "==> operator"
k apply --kustomize "$here/operator"
k --namespace synapse-os rollout status deployment/synapse-operator --timeout=180s
# The cluster pulls its own images when it first starts. The test begins
# once its DNS is up, so that a slow pull is not counted against a proxy.
k --namespace kube-system rollout status deployment/coredns --timeout=300s

echo "==> test"
E2E_HTTP_ADDR="$("$here/cluster.sh" port 80)"
E2E_HTTPS_ADDR="$("$here/cluster.sh" port 443)"
export E2E_HTTP_ADDR E2E_HTTPS_ADDR
cd "$root"
# Well under the time a CI job is given, so that a run that hangs ends here,
# with a stack and the cluster's state, and not when the job is cancelled.
go test -tags e2e -count=1 -v -timeout 20m ./test/e2e/...
