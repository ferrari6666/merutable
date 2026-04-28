#!/usr/bin/env bash
# Deploy the 3-node merutable cluster to Docker Desktop Kubernetes.
#
# Prereqs:
#   - Docker Desktop with Kubernetes enabled (current context:
#     docker-desktop)
#   - deploy/scripts/build.sh has produced the image locally
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

K8S_DIR="${REPO_ROOT}/deploy/k8s"
PROTO_FILE="${REPO_ROOT}/crates/merutable-cluster/proto/cluster.proto"
NAMESPACE="merutable"

CURRENT_CTX="$(kubectl config current-context)"
if [[ "${CURRENT_CTX}" != "docker-desktop" ]]; then
  echo "warning: kubectl context is '${CURRENT_CTX}', not 'docker-desktop'." >&2
  echo "         Press Ctrl-C to abort, or Enter to continue." >&2
  read -r _
fi

echo "==> applying namespace"
kubectl apply -f "${K8S_DIR}/namespace.yaml"

echo "==> creating proto ConfigMap (used by smoke Job)"
kubectl -n "${NAMESPACE}" create configmap merutable-proto \
  --from-file=cluster.proto="${PROTO_FILE}" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

echo "==> applying services"
kubectl apply -f "${K8S_DIR}/headless-svc.yaml"
kubectl apply -f "${K8S_DIR}/nodeport-svc.yaml"

echo "==> applying StatefulSet"
kubectl apply -f "${K8S_DIR}/statefulset.yaml"

echo "==> waiting for 3 pods to become Ready (timeout 180s)"
kubectl -n "${NAMESPACE}" rollout status statefulset/merutable --timeout=180s

echo
echo "==> pods"
kubectl -n "${NAMESPACE}" get pods -o wide

echo
echo "Next steps:"
echo "  tail logs:    kubectl -n ${NAMESPACE} logs -f merutable-0"
echo "  smoke test:   ${SCRIPT_DIR}/smoke.sh"
echo "  external:     grpcurl -plaintext -import-path ${REPO_ROOT}/crates/merutable-cluster/proto \\"
echo "                  -proto cluster.proto localhost:30091 merutable.cluster.MeruCluster/GetStatus"
echo "  teardown:     ${SCRIPT_DIR}/teardown.sh"
