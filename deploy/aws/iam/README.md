# IAM setup for the merutable EC2 ASGs

The EC2 instances need one IAM role with:
- `ec2.amazonaws.com` assume-role trust
- Permission to `docker login` to ECR and pull the image

This role is attached to the Launch Template via an **instance profile**.

## One-time create (bash + awscli)

```bash
# 1. Role with EC2 trust policy
aws iam create-role \
  --role-name merutable-node-ec2 \
  --assume-role-policy-document file://deploy/aws/iam/trust-policy.json

# 2. Attach the ECR-read inline policy
aws iam put-role-policy \
  --role-name merutable-node-ec2 \
  --policy-name ecr-read \
  --policy-document file://deploy/aws/iam/ecr-read-policy.json

# 3. Also attach SSM-core so you can `aws ssm start-session` into the
#    instances without opening SSH (handy for debugging user-data).
aws iam attach-role-policy \
  --role-name merutable-node-ec2 \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

# 4. Instance profile wrapping the role (EC2 can only consume profiles,
#    not roles directly)
aws iam create-instance-profile \
  --instance-profile-name merutable-node-ec2

aws iam add-role-to-instance-profile \
  --instance-profile-name merutable-node-ec2 \
  --role-name merutable-node-ec2
```

Use `merutable-node-ec2` as the **IAM instance profile** on each Launch
Template.

## Teardown

```bash
aws iam remove-role-from-instance-profile \
  --instance-profile-name merutable-node-ec2 \
  --role-name merutable-node-ec2
aws iam delete-instance-profile --instance-profile-name merutable-node-ec2
aws iam delete-role-policy --role-name merutable-node-ec2 --policy-name ecr-read
aws iam detach-role-policy --role-name merutable-node-ec2 \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam delete-role --role-name merutable-node-ec2
```
