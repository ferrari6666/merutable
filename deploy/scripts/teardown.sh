#!/usr/bin/env bash
# Tear down the lab cluster. Deletes the whole namespace, which
# removes the StatefulSet, Services, Jobs, and ConfigMap.
set -euo pipefail

NAMESPACE="merutable"

echo "==> deleting namespace ${NAMESPACE}"
kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=true

echo "==> done"
