![Gen0Sec logo](./images/logo.svg)

<p align="center">
  <a href="https://github.com/gen0sec/synapse-operator/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache 2-green" alt="License - Apache 2"></a> &nbsp;
  <a href="https://github.com/gen0sec/synapse-operator/actions?query=branch%3Amain"><img src="https://github.com/gen0sec/synapse-operator/actions/workflows/release.yaml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://github.com/gen0sec/synapse-operator/releases"><img src="https://img.shields.io/github/release/gen0sec/synapse-operator.svg?label=Release" alt="Release"></a> &nbsp;
  <img alt="GitHub Downloads (all assets, all releases)" src="https://img.shields.io/github/downloads/gen0sec/synapse-operator/total"> &nbsp;
  <a href="https://docs.gen0sec.com/"><img alt="Static Badge" src="https://img.shields.io/badge/gen0sec-documentation-page?style=flat&link=https%3A%2F%2Fdocs.gen0sec.com%2F"></a> &nbsp;
  <a href="https://discord.gg/jzsW5Q6s9q"><img src="https://img.shields.io/discord/1377189913849757726?label=Discord" alt="Discord"></a> &nbsp;
  <a href="https://x.com/gen0sec"><img src="https://img.shields.io/twitter/follow/gen0sec?style=flat" alt="X (formerly Twitter) Follow" /> </a>
</p>

# Community
[![Join us on Discord](https://img.shields.io/badge/Join%20Us%20on-Discord-5865F2?logo=discord&logoColor=white)](https://discord.gg/jzsW5Q6s9q)
[![Substack](https://img.shields.io/badge/Substack-FF6719?logo=substack&logoColor=fff)](https://gen0sec.substack.com/)


## Synapse Operator (Go)

This Go operator watches Synapse configuration ConfigMaps and Secrets and keeps the running pods in sync by forcing a rollout any time config content changes. It relies on matching labels (default `app.kubernetes.io/name=synapse`) so it naturally plugs into Helm releases of Synapse.

### How It Works
- Reconciles ConfigMaps and Secrets that match the configured label selector.
- Hashes the combined data across all matching config sources in the namespace, with optional per-key ignores (for example, hot-reloadable `upstreams.yaml`).
- Patches Synapse workloads (Deployments, DaemonSets, StatefulSets) with the hash stored under `synapse.gen0sec.com/config-hash` by default.
- Updating the annotation bumps the workload template hash, causing Kubernetes to roll the pods and pick up the new configuration.

### Project Layout
- `main.go` bootstraps a controller-runtime manager with health probes and optional namespace scoping.
- `controllers/configmap_controller.go` contains the reconciliation logic and hashing helper.
- `config/` holds a kustomize deployment (service account, RBAC, manager deployment). Replace `ghcr.io/example/synapse-operator:latest` with your published image.

### Building
```bash
GOOS=linux GOARCH=amd64 go build -o bin/synapse-operator
```
Adjust the target architecture if you are building for another platform.

To containerize:
```bash
docker build -t ghcr.io/<org>/synapse-operator:latest .
docker push ghcr.io/<org>/synapse-operator:latest
```
Update `config/manager.yaml` with the pushed image reference.

### Deploying with Kustomize
```bash
kubectl apply -k config
```
This creates the `synapse-system` namespace, service account, RBAC, and a single replica of the operator.

### Testing From WSL
1. **Prepare tools** - ensure WSL has `docker`, `kubectl`, `kind`, and `helm` installed and on `$PATH`.
2. **Build & load the image** - inside WSL build the Linux image and use `kind load docker-image ghcr.io/<org>/synapse-operator:latest` (or push to a registry reachable by your cluster).
3. **Create a test cluster** - `kind create cluster --name synapse`.
4. **Deploy Synapse via Helm** - use the public chart repo:
   ```bash
   helm repo add gen0sec https://helm.gen0sec.com
   helm repo update
   export ARX_KEY="REPLACE_ME"
   helm upgrade --install synapse-stack gen0sec/synapse-stack \
     -n synapse --create-namespace \
     --set global.namespaces.synapse="synapse" \
     --set global.namespaces.operator="synapse-system" \
     --set synapse.image.repository="ghcr.io/gen0sec/synapse" \
     --set synapse.image.tag="latest" \
     --set synapse.synapse.server.upstream="http://example.com" \
     --set synapse.synapse.network.disableXdp=true \
     --set synapse.synapse.arxignis.apiKey="$ARX_KEY" \
     --set operator.enabled=true \
     --set operator.image.repository="ghcr.io/<org>/synapse-operator" \
     --set operator.image.tag="latest"
   ```
5. **Apply/verify operator** - if the chart already deployed the operator, check it with `kubectl -n synapse-system rollout status deployment/synapse-operator`.
6. **Trigger a config change** - edit the Synapse ConfigMap (`kubectl edit configmap synapse-stack -n synapse`) or use `kubectl patch`.
7. **Verify restart** - watch the rollout: `kubectl rollout status deployment/synapse-stack -n synapse` and ensure pod annotation `synapse.gen0sec.com/config-hash` updates.

### Helm Integration Notes
The Helm chart already labels both the ConfigMap and workloads with `app.kubernetes.io/name=synapse`. The operator leans on that selector to discover which objects belong together. When Helm updates config sources (e.g., via `helm upgrade`), the operator sees the new data, recalculates the hash, and patches the workloads so the change propagates without any manual restarts.

### Configuration Flags
- `--label-selector` - Label selector for config sources and workloads (default `app.kubernetes.io/name=synapse`).
- `--config-hash-annotation` - Annotation key used for the hash (default `synapse.gen0sec.com/config-hash`).
- `--ignore-configmap-keys` - Comma-separated ConfigMap keys to ignore when hashing (default `upstreams.yaml`).
- `--ignore-secret-keys` - Comma-separated Secret keys to ignore when hashing (default empty).
