MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="//"

--//
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash
#!/bin/bash -xe

exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "execute commands for nvme setup to support merutable raft/wal"

yum install nvme-cli -y
nvme_drives=$(nvme list | grep "Amazon EC2 NVMe Instance Storage" | cut -d " " -f 1 || true)
readarray -t nvme_drives < <(printf "%s" "$nvme_drives")
num_drives=${#nvme_drives[@]}

if [ "$num_drives" -gt 0 ]; then

  INSTANCE_TYPE=$(curl -s http://169.254.169.254/latest/meta-data/instance-type)
  INSTANCE_FAMILY=$(echo "$INSTANCE_TYPE" | cut -d'.' -f1)

  if [[ "$INSTANCE_FAMILY" == "i4i" ]]; then
    echo "Instance belongs to merutable"
    mkdir -p /data/merutable
    for ((i=0; i<${#nvme_drives[@]}; i++)); do
        echo "NVMe Drive $i: ${nvme_drives[$i]}"
        nd="data/merutable/${i}"
        if blkid ${nvme_drives[$i]} | grep -q 'TYPE="xfs"'; then
          echo "Device ${nvme_drives[$i]} is already formatted as XFS. Skipping mkfs.xfs."
        else
          mkfs.xfs ${nvme_drives[$i]}
        fi
        mkdir -p /${nd}
        mount ${nvme_drives[$i]} /${nd}
        ls -l ${nvme_drives[$i]}
        echo "${nvme_drives[$i]} /${nd} xfs defaults,nofail 0 2" >> /etc/fstab
    done
  else
    echo "Instance is not i4i; falling back to LVM/ext4 kubelet mount"
    pvcreate "${nvme_drives[@]}"
    vgcreate my_vg "${nvme_drives[@]}"
    lvcreate -n my_lv -l 100%FREE my_vg
    mkfs.ext4 -L KUBELET /dev/my_vg/my_lv
    mount_location="/nvme"
    mkdir -vp "$mount_location"
    mkdir -p /var/lib/kubelet/
    ln -s /nvme/ /var/lib/kubelet/pods
    mount /dev/my_vg/my_lv "$mount_location"
    echo /dev/my_vg/my_lv $mount_location ext4 defaults,nofail,nodiratime,x-systemd.before=kubelet.service 0 2 >> /etc/fstab
  fi

fi

echo "nvme setup completed"


cat << EOF > /tmp/nodeadm-config.yaml
"apiVersion": "node.eks.aws/v1alpha1"
"kind": "NodeConfig"
"spec":
  "cluster":
    "apiServerEndpoint": "https://E8703885791A980EF0A05CA930F2DAE7.gr7.us-west-2.eks.amazonaws.com"
    "certificateAuthority": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EWXlOREV5TVRrMU1Gb1hEVE16TURZeU1URXlNVGsxTUZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBSzh6CkNoenJnSkllYzBzNXdHeVNnNk9OUjFlWnFZc0c4WldhNFIwWWhzYWRLOEZGVStTSTBmVm5JTVQ1cDZIdDlBcnEKckRrbmxBMVhVaGJYT1B6alphZktDVDdzYjRVTmw2MHVFRmdJL2QvMklZdlpqQlBkVjVoNXBvaUsraDUwNDRaaApFdEJ4SWdtYmZ0UVpMbUlVZHVCbkFyTzZ2dFZRdSt1SmptMlFtUnp4VUZDZElnVm1VQWdNZFBTVEorZytRUnZQClZXbG9VL210OCtjazdBZEovc0lqMUo0Wlk5QjYyc2N0NzdocUhnbFd2S25vWlBiYlE2Z0lFcTRybHlwaHRLclEKSDQ5MVd5VWJXQ0JyMkFkMkU5MkgvamErRzVKVEFjU3VFOHpKb3JGbmdCS3N2QUozdlFmUFI4ak1rM0lJRUlyQQpDRlJ1dnpWYzlYcWNnVkZYREc4Q0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZKNkZGMlFBYXRBYnlXZUYva0ZIK1JaaVZxeXdNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBRUdiczl4RkVrQ2xIZzlGVTNxQgozcVpLejQxSGVBY3plZjlEbUtKRlVhNEo5RE1VK1ZzU0dPQUN6YjQvT3Q3RFVIZEZuVU0va2tlRUhhMUNKQnRTCno0ZXplbXh3WGxZaXdZc082ZDkycWJrdUlObnJaTzJ3T0p4bk1kY2ZOM0VWa01CNGo0ZXhhUi9NUkY5VEdmTjYKSi9LQVRpMEdCc0J5Q09aL3dyU0JpeC91L3pkUThNMzJkblREaS8yelc0MHpUa2thK0NuU1pGY1FZcjFkUEJoego4a0tyVEkzQ0J4YzJyRXFsdklFem05eXFjcVBkWUE5Y2ZZQ0J1WS90c3pveEVRb0I1YnJCRFYrWXplRVhJL3h2ClhCeVVFM1ZKZWowL0dFbFMwNnhqMWR2YWNocDM1ZzJ3Snl3cTZ1allqcGpPbEh1bXBHY1ZmOWp2b2liQmJWTk0KSnlRPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="
    "cidr": "172.20.0.0/16"
    "name": "cdp-dev1-1-trino1"
  "kubelet":
    "flags":
    - "--node-labels 'ami_name=sfdc-al2023-eks-node-1.33.8-Apr-26-v0-1777212301,nodepool=merutable,dedicated=merutable,recycle-group=merutable,SFSG=Processing'"
    - "--register-with-taints 'dedicated=merutable:NoSchedule'"

EOF

nodeadm config check -c file:///tmp/nodeadm-config.yaml

--//
Content-Type: application/node.eks.aws

"apiVersion": "node.eks.aws/v1alpha1"
"kind": "NodeConfig"
"spec":
  "cluster":
    "apiServerEndpoint": "https://E8703885791A980EF0A05CA930F2DAE7.gr7.us-west-2.eks.amazonaws.com"
    "certificateAuthority": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EWXlOREV5TVRrMU1Gb1hEVE16TURZeU1URXlNVGsxTUZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBSzh6CkNoenJnSkllYzBzNXdHeVNnNk9OUjFlWnFZc0c4WldhNFIwWWhzYWRLOEZGVStTSTBmVm5JTVQ1cDZIdDlBcnEKckRrbmxBMVhVaGJYT1B6alphZktDVDdzYjRVTmw2MHVFRmdJL2QvMklZdlpqQlBkVjVoNXBvaUsraDUwNDRaaApFdEJ4SWdtYmZ0UVpMbUlVZHVCbkFyTzZ2dFZRdSt1SmptMlFtUnp4VUZDZElnVm1VQWdNZFBTVEorZytRUnZQClZXbG9VL210OCtjazdBZEovc0lqMUo0Wlk5QjYyc2N0NzdocUhnbFd2S25vWlBiYlE2Z0lFcTRybHlwaHRLclEKSDQ5MVd5VWJXQ0JyMkFkMkU5MkgvamErRzVKVEFjU3VFOHpKb3JGbmdCS3N2QUozdlFmUFI4ak1rM0lJRUlyQQpDRlJ1dnpWYzlYcWNnVkZYREc4Q0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZKNkZGMlFBYXRBYnlXZUYva0ZIK1JaaVZxeXdNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBRUdiczl4RkVrQ2xIZzlGVTNxQgozcVpLejQxSGVBY3plZjlEbUtKRlVhNEo5RE1VK1ZzU0dPQUN6YjQvT3Q3RFVIZEZuVU0va2tlRUhhMUNKQnRTCno0ZXplbXh3WGxZaXdZc082ZDkycWJrdUlObnJaTzJ3T0p4bk1kY2ZOM0VWa01CNGo0ZXhhUi9NUkY5VEdmTjYKSi9LQVRpMEdCc0J5Q09aL3dyU0JpeC91L3pkUThNMzJkblREaS8yelc0MHpUa2thK0NuU1pGY1FZcjFkUEJoego4a0tyVEkzQ0J4YzJyRXFsdklFem05eXFjcVBkWUE5Y2ZZQ0J1WS90c3pveEVRb0I1YnJCRFYrWXplRVhJL3h2ClhCeVVFM1ZKZWowL0dFbFMwNnhqMWR2YWNocDM1ZzJ3Snl3cTZ1allqcGpPbEh1bXBHY1ZmOWp2b2liQmJWTk0KSnlRPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="
    "cidr": "172.20.0.0/16"
    "name": "cdp-dev1-1-trino1"
  "kubelet":
    "flags":
    - "--node-labels 'ami_name=sfdc-al2023-eks-node-1.33.8-Apr-26-v0-1777212301,nodepool=merutable,dedicated=merutable,recycle-group=merutable,SFSG=Processing'"
    - "--register-with-taints 'dedicated=merutable:NoSchedule'"


--//
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash


--//--
