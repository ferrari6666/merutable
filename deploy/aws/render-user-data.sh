#!/usr/bin/env bash
# Render per-node user-data scripts for the 3 ASGs.
#
# Required env:
#   ECR_IMAGE   — full ECR URI + tag (printed by ecr-push.sh)
#   AWS_REGION  — e.g. us-west-2
#   NODE1_IP    — static private IP you will assign to node-1 (AZ-1)
#   NODE2_IP    — static private IP you will assign to node-2 (AZ-2)
#   NODE3_IP    — static private IP you will assign to node-3 (AZ-3)
#
# Output:
#   deploy/aws/user-data/node-1.user-data.sh
#   deploy/aws/user-data/node-2.user-data.sh
#   deploy/aws/user-data/node-3.user-data.sh
#
# Each rendered file is what you paste into the matching Launch
# Template's "User data" field (base64 encoding is handled by EC2).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TPL="${SCRIPT_DIR}/user-data/user-data.sh.tpl"
OUT_DIR="${SCRIPT_DIR}/user-data"

: "${ECR_IMAGE:?ECR_IMAGE must be set (run ecr-push.sh first)}"
: "${AWS_REGION:?AWS_REGION must be set}"
: "${NODE1_IP:?NODE1_IP must be set (static private IP for AZ-1 node)}"
: "${NODE2_IP:?NODE2_IP must be set (static private IP for AZ-2 node)}"
: "${NODE3_IP:?NODE3_IP must be set (static private IP for AZ-3 node)}"

render() {
  local node_id="$1" az="$2" seeds="$3" out="$4"
  # Use '|' as sed delimiter since paths/URIs contain '/'.
  sed \
    -e "s|__NODE_ID__|${node_id}|g" \
    -e "s|__AZ__|${az}|g" \
    -e "s|__SEEDS__|${seeds}|g" \
    -e "s|__ECR_IMAGE__|${ECR_IMAGE}|g" \
    -e "s|__AWS_REGION__|${AWS_REGION}|g" \
    "${TPL}" > "${out}"
  chmod 0644 "${out}"
  echo "  wrote ${out}"
}

echo "==> rendering user-data"
render 1 AZ-1 "2:AZ-2:${NODE2_IP}:9100 3:AZ-3:${NODE3_IP}:9100" "${OUT_DIR}/node-1.user-data.sh"
render 2 AZ-2 "1:AZ-1:${NODE1_IP}:9100 3:AZ-3:${NODE3_IP}:9100" "${OUT_DIR}/node-2.user-data.sh"
render 3 AZ-3 "1:AZ-1:${NODE1_IP}:9100 2:AZ-2:${NODE2_IP}:9100" "${OUT_DIR}/node-3.user-data.sh"

echo
echo "==> done"
echo "    paste each file into its Launch Template's User data field,"
echo "    or supply via --user-data file://deploy/aws/user-data/node-N.user-data.sh"
