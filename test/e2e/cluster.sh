#!/usr/bin/env bash
# The cluster the end-to-end test runs on: one k3s server in a container.
#
#   cluster.sh up FILE         start it and write its kubeconfig to FILE,
#                              which must not exist yet
#   cluster.sh load IMAGE...   copy images from the local Docker into it
#   cluster.sh port PORT       print the host address PORT is published at
#   cluster.sh down            remove it, and everything in it
#
# E2E_CLUSTER names the container; synapse-operator-e2e unless it is set.
# The kubeconfig kubectl uses by default is neither read nor written.
set -euo pipefail

name="${E2E_CLUSTER:-synapse-operator-e2e}"
k3s_image="${E2E_K3S_IMAGE:-rancher/k3s:v1.37.1-k3s1}"

# port prints the host address a port of the cluster is published at.
port() {
  docker port "$name" "$1/tcp" | head -n 1
}

# Set once this run has started the container. A start that then fails
# removes it again; a container that was already there is never touched.
started=""
abandon() {
  status=$?
  if [ "$status" -ne 0 ] && [ -n "$started" ]; then
    docker logs --tail 40 "$name" >&2 || true
    docker rm --force --volumes "$name" >/dev/null 2>&1 || true
  fi
  exit "$status"
}

up() {
  local kubeconfig="${1:?cluster.sh up FILE: name the file to write the kubeconfig to}"
  if [ -e "$kubeconfig" ]; then
    echo "$kubeconfig exists already, and would be overwritten" >&2
    exit 1
  fi
  if docker inspect "$name" >/dev/null 2>&1; then
    echo "a container named $name exists already; '$0 down' removes it" >&2
    exit 1
  fi
  trap abandon EXIT
  # Published on loopback only, on ports Docker picks: 6443 is the API
  # server, 80 and 443 are where k3s puts a LoadBalancer Service.
  docker run --detach --name "$name" --hostname "$name" --privileged \
    --tmpfs /run --tmpfs /var/run \
    --publish 127.0.0.1::6443 --publish 127.0.0.1::80 --publish 127.0.0.1::443 \
    "$k3s_image" server \
    --disable=traefik --disable=metrics-server --tls-san=127.0.0.1 >/dev/null
  started=yes

  # SECONDS, not a count of tries: a try can itself take seconds.
  local deadline=$((SECONDS + 120))
  until docker exec "$name" test -s /etc/rancher/k3s/k3s.yaml 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "k3s did not write its kubeconfig within two minutes:" >&2
      exit 1
    fi
    sleep 1
  done
  mkdir -p "$(dirname "$kubeconfig")"
  docker exec "$name" cat /etc/rancher/k3s/k3s.yaml |
    sed "s#https://127.0.0.1:6443#https://$(port 6443)#" >"$kubeconfig"
  chmod 600 "$kubeconfig"

  # The node registers a moment after the API server answers, and pods can
  # only be created once the namespace has its default ServiceAccount.
  deadline=$((SECONDS + 180))
  until kubectl --kubeconfig "$kubeconfig" get serviceaccount default >/dev/null 2>&1 &&
    kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready node --all --timeout=5s >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "the k3s node did not become ready within three minutes:" >&2
      rm -f "$kubeconfig"
      exit 1
    fi
    sleep 1
  done
}

case "${1:-}" in
up)
  up "${2:-}"
  ;;
load)
  shift
  docker save "$@" | docker exec --interactive "$name" ctr --namespace k8s.io images import - >/dev/null
  ;;
port)
  port "${2:?cluster.sh port PORT}"
  ;;
down)
  docker rm --force --volumes "$name" >/dev/null 2>&1 || true
  ;;
*)
  sed -n '2,11p' "$0" >&2
  exit 2
  ;;
esac
