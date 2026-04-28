#!/bin/bash
# merutable-node EC2 user-data (Amazon Linux 2023, x86_64).
#
# Placeholders (replaced by render-user-data.sh):
#   __NODE_ID__     — numeric node id (1, 2, 3)
#   __AZ__          — ring AZ label (AZ-1, AZ-2, AZ-3)
#   __SEEDS__       — space-separated "<id>:<az>:<ip>:<port>" pairs
#   __ECR_IMAGE__   — full ECR image URI including tag
#   __AWS_REGION__  — e.g. us-west-2
#
# What this does:
#   1. Installs Docker + awscli.
#   2. Logs into ECR via the instance profile.
#   3. Pulls the merutable-node image.
#   4. Writes /etc/merutable-node.env with the per-node settings.
#   5. Writes a systemd unit that runs the container, restarts on
#      failure, and re-pulls + re-logs-in on every start (so an image
#      update just needs `systemctl restart merutable-node`).
set -euxo pipefail

NODE_ID="__NODE_ID__"
AZ="__AZ__"
SEEDS="__SEEDS__"
ECR_IMAGE="__ECR_IMAGE__"
AWS_REGION="__AWS_REGION__"

# ------------------------------------------------------------------
# 1. Packages
# ------------------------------------------------------------------
dnf -y update
dnf -y install docker awscli
systemctl enable --now docker

# ------------------------------------------------------------------
# 2. Data dir on instance storage (root EBS is fine for the lab)
# ------------------------------------------------------------------
install -d -m 0755 /var/lib/merutable

# ------------------------------------------------------------------
# 3. Env file consumed by the systemd unit
# ------------------------------------------------------------------
cat >/etc/merutable-node.env <<EOF
NODE_ID=${NODE_ID}
AZ=${AZ}
SEEDS=${SEEDS}
ECR_IMAGE=${ECR_IMAGE}
AWS_REGION=${AWS_REGION}
RUST_LOG=info,merutable_cluster=debug
EOF
chmod 0644 /etc/merutable-node.env

# ------------------------------------------------------------------
# 4. Wrapper that (re)logs into ECR and pulls before exec
# ------------------------------------------------------------------
cat >/usr/local/bin/merutable-node-run.sh <<'EOF'
#!/bin/bash
set -euo pipefail
# shellcheck disable=SC1091
source /etc/merutable-node.env

REGISTRY="${ECR_IMAGE%%/*}"

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

docker pull "${ECR_IMAGE}"

# Remove any stale container so --name is free.
docker rm -f merutable-node >/dev/null 2>&1 || true

exec docker run \
  --rm \
  --name merutable-node \
  --network host \
  -v /var/lib/merutable:/data \
  -e RUST_LOG="${RUST_LOG}" \
  "${ECR_IMAGE}" \
  "${NODE_ID}" \
  "${AZ}" \
  /data \
  9100 \
  ${SEEDS}
EOF
chmod 0755 /usr/local/bin/merutable-node-run.sh

# ------------------------------------------------------------------
# 5. systemd unit
# ------------------------------------------------------------------
cat >/etc/systemd/system/merutable-node.service <<'EOF'
[Unit]
Description=merutable cluster node
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
Restart=on-failure
RestartSec=5
EnvironmentFile=/etc/merutable-node.env
ExecStart=/usr/local/bin/merutable-node-run.sh
ExecStop=/usr/bin/docker stop --time=10 merutable-node

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now merutable-node.service

echo "merutable-node user-data complete: node_id=${NODE_ID} az=${AZ}"
