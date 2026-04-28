#!/usr/bin/env bash
# Run the in-cluster smoke-test Job and stream its output.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

K8S_DIR="${REPO_ROOT}/deploy/k8s"
NAMESPACE="merutable"

# Drop any previous Job so we can re-run idempotently.
kubectl -n "${NAMESPACE}" delete job merutable-smoke --ignore-not-found --wait=true >/dev/null

echo "==> applying smoke Job"
kubectl apply -f "${K8S_DIR}/smoke-job.yaml"

echo "==> waiting for pod to start"
for _ in $(seq 1 30); do
  POD="$(kubectl -n "${NAMESPACE}" get pods \
    -l app.kubernetes.io/component=smoke \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${POD}" ]]; then
    break
  fi
  sleep 1
done

if [[ -z "${POD:-}" ]]; then
  echo "error: smoke pod did not appear" >&2
  exit 1
fi

echo "==> streaming logs from ${POD}"
kubectl -n "${NAMESPACE}" logs -f "${POD}" || true

echo
echo "==> final job status"
kubectl -n "${NAMESPACE}" get job merutable-smoke
