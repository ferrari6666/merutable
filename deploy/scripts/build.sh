#!/usr/bin/env bash
# Build the merutable-node container image for linux/amd64.
#
# Run from any directory; the script cd's to the repo root.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

IMAGE="${IMAGE:-merutable-node:local}"
PLATFORM="${PLATFORM:-linux/amd64}"

cd "${REPO_ROOT}"

echo "==> building ${IMAGE} for ${PLATFORM}"
echo "    repo root: ${REPO_ROOT}"

# --load makes the image available to the local Docker daemon (which
# Docker Desktop Kubernetes shares), so the cluster can pull it as
# imagePullPolicy: IfNotPresent without a registry push.
DOCKER_BUILDKIT=1 docker buildx build \
  --platform "${PLATFORM}" \
  --file "deploy/docker/Dockerfile" \
  --tag "${IMAGE}" \
  --provenance=false \
  --sbom=false \
  --output "type=docker" \
  .

echo "==> built ${IMAGE}"
docker image inspect "${IMAGE}" --format '    size: {{.Size}} bytes, arch: {{.Architecture}}/{{.Os}}'
