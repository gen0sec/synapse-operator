![Arxignis logo](./images/logo.png)

<p align="center">
  <a href="https://github.com/arxignis/synapse-operator/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache 2-green" alt="License - Apache 2"></a> &nbsp;
  <a href="https://github.com/arxignis/synapse-operator/actions?query=branch%3Amain"><img src="https://github.com/arxignis/synapse-operator/actions/workflows/release.yaml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://github.com/arxignis/synapse-operator/releases"><img src="https://img.shields.io/github/release/arxignis/synapse-operator.svg?label=Release" alt="Release"></a> &nbsp;
  <img alt="GitHub Downloads (all assets, all releases)" src="https://img.shields.io/github/downloads/arxignis/synapse-operator/total"> &nbsp;
  <a href="https://docs.arxignis.com/"><img alt="Static Badge" src="https://img.shields.io/badge/arxignis-documentation-page?style=flat&link=https%3A%2F%2Fdocs.arxignis.com%2F"></a> &nbsp;
  <a href="https://discord.gg/jzsW5Q6s9q"><img src="https://img.shields.io/discord/1377189913849757726?label=Discord" alt="Discord"></a> &nbsp;
  <a href="https://x.com/arxignis"><img src="https://img.shields.io/twitter/follow/arxignis?style=flat" alt="X (formerly Twitter) Follow" /> </a>
</p>

# Community
[![Join us on Discord](https://img.shields.io/badge/Join%20Us%20on-Discord-5865F2?logo=discord&logoColor=white)](https://discord.gg/jzsW5Q6s9q)
[![Substack](https://img.shields.io/badge/Substack-FF6719?logo=substack&logoColor=fff)](https://arxignis.substack.com/)


## Synapse Operator (Go)

This Go operator watches Synapse configuration ConfigMaps and keeps the running pods in sync by forcing a rollout any time the config content changes. It relies on the same Helm labels as the upstream `synapse-main/helm` chart (`app.kubernetes.io/name=synapse`) so it naturally plugs into Helm releases of Synapse.

### How It Works
- Reconciles only `ConfigMap` objects labelled with `app.kubernetes.io/name=synapse`.
- Hashes the combined `data` and `binaryData` payload of the ConfigMap.
- Patches every Synapse `Deployment` in the same namespace (matching the same label) with the hash stored under `synapse.gen0sec.com/config-hash` in the pod template annotations.
- Updating the annotation bumps the ReplicaSet template hash, causing Kubernetes to roll the pods and pick up the new configuration.

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

### Testing From WSL (no commands executed yet)
1. **Prepare tools** – ensure WSL has `docker`, `kubectl`, and `kind` (or `minikube`) installed and on `$PATH`.
2. **Build & load the image** – inside WSL build the Linux image and use `kind load docker-image ghcr.io/<org>/synapse-operator:latest` (or push to a registry reachable by your cluster).
3. **Create a test cluster** – `kind create cluster --name synapse`.
4. **Deploy Synapse via Helm** – from `synapse-main/helm`, run `helm install synapse ./helm --namespace synapse --create-namespace`. This produces the ConfigMap and Deployment with the expected labels.
5. **Apply the operator manifests** – `kubectl apply -k ../synapse-operator/config`.
6. **Trigger a config change** – edit the Synapse ConfigMap (`kubectl edit configmap synapse -n synapse`) or use `kubectl patch`.
7. **Verify restart** – watch the Deployment rollout: `kubectl rollout status deployment/synapse -n synapse` and ensure pod annotation `synapse.gen0sec.com/config-hash` updates.

### Helm Integration Notes
The Helm chart already labels both the ConfigMap and Deployment with `app.kubernetes.io/name=synapse`. The operator leans on those labels to discover which objects belong together. When Helm updates the ConfigMap (e.g., via `helm upgrade`), the operator sees the new data, recalculates the hash, and patches the Deployment so the change propagates without any manual restarts.

