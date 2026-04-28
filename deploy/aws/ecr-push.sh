#!/usr/bin/env bash
# Push the local merutable-node image to ECR.
#
# Required env:
#   AWS_REGION     — e.g. us-west-2
#   AWS_ACCOUNT_ID — 12-digit account id (auto-detected via STS if unset)
#
# Optional env:
#   ECR_REPO       — repo name (default: merutable-node)
#   IMAGE_TAG      — tag to push (default: latest)
#   LOCAL_IMAGE    — source image (default: merutable-node:local)
#
# Usage:
#   AWS_REGION=us-west-2 ./deploy/aws/ecr-push.sh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

: "${AWS_REGION:?AWS_REGION must be set}"
ECR_REPO="${ECR_REPO:-merutable-node}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
LOCAL_IMAGE="${LOCAL_IMAGE:-merutable-node:local}"

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
fi

REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
REMOTE_IMAGE="${REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

echo "==> account:  ${AWS_ACCOUNT_ID}"
echo "==> region:   ${AWS_REGION}"
echo "==> source:   ${LOCAL_IMAGE}"
echo "==> remote:   ${REMOTE_IMAGE}"

# 1. Make sure the local image exists (hand off to build.sh if not).
if ! docker image inspect "${LOCAL_IMAGE}" >/dev/null 2>&1; then
  echo "==> local image not found, running build.sh"
  "${REPO_ROOT}/deploy/scripts/build.sh"
fi

# 2. Confirm linux/amd64 (the Dockerfile pins this, but a stale multi-arch
#    manifest can sneak in if someone rebuilt without --platform).
ARCH="$(docker image inspect "${LOCAL_IMAGE}" --format '{{.Architecture}}/{{.Os}}')"
if [[ "${ARCH}" != "amd64/linux" ]]; then
  echo "error: local image is ${ARCH}, expected amd64/linux" >&2
  echo "       rerun: PLATFORM=linux/amd64 ./deploy/scripts/build.sh" >&2
  exit 1
fi

# 3. Create the repo if it doesn't exist (idempotent).
if ! aws ecr describe-repositories \
      --region "${AWS_REGION}" \
      --repository-names "${ECR_REPO}" \
      >/dev/null 2>&1; then
  echo "==> creating ECR repo ${ECR_REPO}"
  aws ecr create-repository \
    --region "${AWS_REGION}" \
    --repository-name "${ECR_REPO}" \
    --image-scanning-configuration scanOnPush=true \
    --image-tag-mutability MUTABLE \
    >/dev/null
else
  echo "==> ECR repo ${ECR_REPO} already exists"
fi

# 4. Docker login to ECR.
echo "==> docker login"
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

# 5. Tag + push.
echo "==> tag + push"
docker tag "${LOCAL_IMAGE}" "${REMOTE_IMAGE}"
docker push "${REMOTE_IMAGE}"

echo
echo "==> done"
echo "    image URI: ${REMOTE_IMAGE}"
echo
echo "export the URI for the user-data renderer:"
echo "  export ECR_IMAGE=${REMOTE_IMAGE}"
