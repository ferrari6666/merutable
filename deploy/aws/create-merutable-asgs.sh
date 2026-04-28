#!/usr/bin/env bash
#
# create-merutable-asgs.sh
#
# Creates:
#   1. Launch template  `merutable-cluster-lt` (derived from Alluxio worker LT
#      cdp-dev1-1-trino1-wg2025011310292263650000000c)
#   2. Three AutoScaling Groups, one per us-west-2 AZ:
#        merutable-cluster-uswest2a   (us-west-2a)
#        merutable-cluster-uswest2b   (us-west-2b)
#        merutable-cluster-uswest2c   (us-west-2c)
#      each with desired=1, min=0, max=3 of i4i.2xlarge
#
# Same account / EKS cluster as Alluxio:
#   account        240852588578
#   region         us-west-2
#   EKS cluster    cdp-dev1-1-trino1
#
# Differences vs Alluxio worker:
#   - NVMe mounts at /data/merutable/{i} instead of /alluxiostorevol{i}
#   - kubelet labels:  nodepool=merutable, dedicated=merutable,
#                      recycle-group=merutable, SFSG=Processing
#   - taint:           dedicated=merutable:NoSchedule
#
# Usage:
#   eval "$(./deploy/aws/falcon-creds.sh)"
#   ./deploy/aws/create-merutable-asgs.sh
#
# Re-runnable: will skip LT creation if it already exists; will skip each ASG
# that already exists.

set -euo pipefail

REGION=us-west-2
CLUSTER=cdp-dev1-1-trino1
LT_NAME=merutable-cluster-lt
USER_DATA_FILE="$(dirname "$0")/merutable-userdata.sh"

AMI=ami-0ac3cd222baa9f348
INSTANCE_TYPE=i4i.2xlarge
IAM_PROFILE_ARN="arn:aws:iam::240852588578:instance-profile/cdp-dev1-1-trino120250113102921621300000004"
SG_IDS=(sg-022c5e371cb8a2e69 sg-08eaa04290d339c00 sg-0c3a520d7afc52dcf sg-04a0fe0faa3de6565)

# Per-AZ subnet lists (mirror Alluxio worker ASGs verbatim).
SUBNETS_2A="subnet-090371ec4cfb8f177,subnet-00d6e0702d974d639,subnet-0aa9846a0bcd089c8,subnet-087e732ba9e8e15da,subnet-03225901e8a3a0840,subnet-03cb8876fdf1f68e3,subnet-06bbf284ce86e84f1,subnet-05ba3ca77966c06dd,subnet-00223ab0d0930d9a6,subnet-086ca50b84d6c95f1,subnet-09e78d83dedd93916"
SUBNETS_2B="subnet-0889e2900c39ccf0b,subnet-0d8d3c8f8f38036ec,subnet-0593766dab1cce76e,subnet-0062bb78bc5edb32d,subnet-04838c7d37f64a2ea,subnet-043e611dbf665c609,subnet-08bb8b75f26681a24,subnet-0a307ba9a05e2ce84,subnet-0a560ebb75bc91b27,subnet-0fd3a4d51a6e3d098,subnet-054ce3161ccdf0ece"
SUBNETS_2C="subnet-0a07769017ed8c72c,subnet-0c5ef3b64b0bb5e55,subnet-0406537c66445cd9b,subnet-031b123e47a1cd9e2,subnet-031e832212db25c95,subnet-0ad169d278da1e2a6,subnet-092bb8d943854fa9d,subnet-06b2adf53ab5dae16,subnet-09c8fbd96304f0ba9,subnet-070147bf5fd26ae63,subnet-0b33d082a4fceea27"

log() { printf '[create-asgs] %s\n' "$*" >&2; }

# ---- 1. launch template -------------------------------------------------
existing_lt=$(aws ec2 describe-launch-templates \
  --region "$REGION" \
  --launch-template-names "$LT_NAME" \
  --query 'LaunchTemplates[0].LaunchTemplateId' \
  --output text 2>/dev/null || echo "None")

if [[ "$existing_lt" != "None" && -n "$existing_lt" ]]; then
  log "launch template $LT_NAME already exists: $existing_lt"
  LT_ID="$existing_lt"
else
  log "creating launch template $LT_NAME..."
  USER_DATA_B64=$(base64 < "$USER_DATA_FILE" | tr -d '\n')

  SG_JSON=$(printf '"%s",' "${SG_IDS[@]}")
  SG_JSON="[${SG_JSON%,}]"

  cat > /tmp/merutable-lt-data.json <<JSON
{
  "ImageId": "$AMI",
  "InstanceType": "$INSTANCE_TYPE",
  "IamInstanceProfile": { "Arn": "$IAM_PROFILE_ARN" },
  "NetworkInterfaces": [{
    "AssociatePublicIpAddress": false,
    "DeviceIndex": 0,
    "Groups": $SG_JSON,
    "NetworkCardIndex": 0
  }],
  "MetadataOptions": {
    "HttpTokens": "optional",
    "HttpPutResponseHopLimit": 2,
    "HttpProtocolIpv6": "disabled",
    "InstanceMetadataTags": "disabled"
  },
  "Monitoring": { "Enabled": true },
  "BlockDeviceMappings": [{
    "DeviceName": "/dev/xvda",
    "Ebs": {
      "Encrypted": true,
      "DeleteOnTermination": true,
      "VolumeSize": 100,
      "VolumeType": "gp2"
    }
  }],
  "UserData": "$USER_DATA_B64",
  "TagSpecifications": [
    {
      "ResourceType": "instance",
      "Tags": [
        {"Key": "nodepool", "Value": "merutable"},
        {"Key": "p_servicename", "Value": "cdp-data-processing-controller"},
        {"Key": "p_service_instance", "Value": "cdp-data-processing-controller-trino1"},
        {"Key": "p_serviceid", "Value": "dbe9ef6c-6e15-11ea-bc55-0242ac130003"},
        {"Key": "p_servicegroup", "Value": "cdp"},
        {"Key": "p_environment", "Value": "dev1"},
        {"Key": "p_environment_type", "Value": "dev"},
        {"Key": "p_region", "Value": "us-west-2"},
        {"Key": "p_falcon_instance", "Value": "dev1-uswest2"},
        {"Key": "p_functionaldomain", "Value": "cdp001"},
        {"Key": "p_confidentiality", "Value": "Mission Critical"},
        {"Key": "module", "Value": "merutable-cluster"},
        {"Key": "environment", "Value": "iac"}
      ]
    },
    {
      "ResourceType": "volume",
      "Tags": [
        {"Key": "nodepool", "Value": "merutable"},
        {"Key": "p_servicename", "Value": "cdp-data-processing-controller"},
        {"Key": "p_service_instance", "Value": "cdp-data-processing-controller-trino1"},
        {"Key": "p_environment", "Value": "dev1"},
        {"Key": "p_environment_type", "Value": "dev"},
        {"Key": "p_region", "Value": "us-west-2"}
      ]
    }
  ]
}
JSON

  LT_ID=$(aws ec2 create-launch-template \
    --region "$REGION" \
    --launch-template-name "$LT_NAME" \
    --launch-template-data file:///tmp/merutable-lt-data.json \
    --query 'LaunchTemplate.LaunchTemplateId' \
    --output text)
  log "launch template created: $LT_ID"
fi

# ---- 2. per-AZ ASGs ------------------------------------------------------
create_asg() {
  local name=$1 az=$2 subnets=$3
  local existing
  existing=$(aws autoscaling describe-auto-scaling-groups \
    --region "$REGION" \
    --auto-scaling-group-names "$name" \
    --query 'AutoScalingGroups[0].AutoScalingGroupName' \
    --output text 2>/dev/null || echo "None")
  if [[ "$existing" != "None" && -n "$existing" ]]; then
    log "ASG $name already exists; skipping"
    return
  fi

  log "creating ASG $name in $az..."
  aws autoscaling create-auto-scaling-group \
    --region "$REGION" \
    --auto-scaling-group-name "$name" \
    --launch-template "LaunchTemplateId=$LT_ID,Version=\$Latest" \
    --min-size 0 \
    --max-size 3 \
    --desired-capacity 1 \
    --vpc-zone-identifier "$subnets" \
    --health-check-type EC2 \
    --health-check-grace-period 300 \
    --capacity-rebalance \
    --tags \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=Name,Value=$name,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=nodepool,Value=merutable,PropagateAtLaunch=true" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/enabled,Value=true,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/$CLUSTER,Value=owned,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/node-template/label/nodepool,Value=merutable,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/node-template/label/dedicated,Value=merutable,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/node-template/taint/dedicated,Value=merutable:NoSchedule,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/node-template/label/topology.kubernetes.io/zone,Value=$az,PropagateAtLaunch=false" \
      "ResourceId=$name,ResourceType=auto-scaling-group,Key=kubernetes.io/cluster/$CLUSTER,Value=owned,PropagateAtLaunch=true"

  log "ASG $name created"
}

create_asg merutable-cluster-uswest2a us-west-2a "$SUBNETS_2A"
create_asg merutable-cluster-uswest2b us-west-2b "$SUBNETS_2B"
create_asg merutable-cluster-uswest2c us-west-2c "$SUBNETS_2C"

log "done. verify with:"
log "  aws autoscaling describe-auto-scaling-groups --region $REGION \\"
log "    --auto-scaling-group-names merutable-cluster-uswest2a merutable-cluster-uswest2b merutable-cluster-uswest2c"
