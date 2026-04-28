# Local 3-node merutable cluster on Docker Desktop Kubernetes

Spins up three `merutable-node` pods (one per simulated AZ) behind a
headless Service, plus a NodePort for external gRPC access and a
smoke-test Job that exercises the gRPC surface end-to-end.

## Layout

```
deploy/
├── docker/
│   ├── Dockerfile          # multi-stage linux/amd64 build
│   └── .dockerignore
├── k8s/
│   ├── namespace.yaml
│   ├── headless-svc.yaml   # stable pod DNS: merutable-<N>.merutable-hl...
│   ├── nodeport-svc.yaml   # host access on localhost:30091
│   ├── statefulset.yaml    # 3 pods, ordinal→AZ, emptyDir /data
│   └── smoke-job.yaml      # grpcurl CreateTable + Put + Get
└── scripts/
    ├── build.sh            # docker buildx --platform linux/amd64
    ├── deploy.sh           # kubectl apply + rollout wait
    ├── smoke.sh            # run smoke Job, stream logs
    └── teardown.sh         # delete namespace
```

## Prerequisites

- Docker Desktop with Kubernetes enabled (context: `docker-desktop`)
- `kubectl` on `PATH`
- `docker buildx` (ships with Docker Desktop)
- Optional, for host-side probing: `grpcurl`

## Quick start

```bash
./deploy/scripts/build.sh      # ~first run ≈ 5–10 min, cached rebuilds seconds
./deploy/scripts/deploy.sh
./deploy/scripts/smoke.sh
```

Inspect:

```bash
kubectl -n merutable get pods -o wide
kubectl -n merutable logs -f merutable-0
```

Reach the cluster from the host:

```bash
grpcurl -plaintext \
  -import-path crates/merutable-cluster/proto \
  -proto cluster.proto \
  localhost:30091 \
  merutable.cluster.MeruCluster/GetStatus
```

Tear down:

```bash
./deploy/scripts/teardown.sh
```

## How the StatefulSet wires the nodes

The pods get stable DNS from the headless Service. The container
`command` block derives identity from the pod ordinal:

| Ordinal | node_id | AZ   | seeds                                                              |
|--------:|--------:|------|--------------------------------------------------------------------|
| 0       |       1 | AZ-1 | `2:AZ-2:merutable-1.<domain>:9100 3:AZ-3:merutable-2.<domain>:9100`|
| 1       |       2 | AZ-2 | `1:AZ-1:merutable-0.<domain>:9100 3:AZ-3:merutable-2.<domain>:9100`|
| 2       |       3 | AZ-3 | `1:AZ-1:merutable-0.<domain>:9100 2:AZ-2:merutable-1.<domain>:9100`|

This sidesteps **GAP-10** (no peer-discovery handshake yet) by passing
each peer's real `node_id` / `az` / `address` at startup.

`podManagementPolicy: OrderedReady` forces serial startup so seed
DNS is resolvable by the time later pods try to connect.

## Known limitations

The docs in `docs/PHASE1_RAFT_CLUSTER.md` list several open gaps that
affect what this lab can exercise today:

- **Multi-node Raft replication is not wired yet** (GAP-1, GAP-11,
  GAP-19). Three pods will start cleanly and each responds to its own
  gRPC, but cross-pod `Put`/`Delete` replication through a shared Raft
  group isn't in the production code path. Writes proxied from a
  non-preferred-leader pod will currently return `NotLeader` unless
  the leader happens to be the addressed pod.
- **"3 AZs" is a label convention only** — all three pods run on one
  Docker Desktop node. No real failure isolation.
- **`emptyDir` /data is ephemeral.** Pod restart / reschedule wipes
  state. Swap the `volumes:` entry in `statefulset.yaml` for a
  `volumeClaimTemplates` block if persistence matters.

The infra is still useful right now for smoke-testing the binary, the
gRPC surface, and as the harness that the remaining multi-node GAPs
will be validated against.

## Customizing

- **Image name / tag**: `IMAGE=merutable-node:dev ./deploy/scripts/build.sh`
- **Platform**: the binary is pinned to `linux/amd64`; change
  `PLATFORM` in `build.sh` to target something else, but you'll also
  need to drop `--platform=linux/amd64` from the Dockerfile `FROM`
  lines.
- **Log verbosity**: edit the `RUST_LOG` env var in
  `statefulset.yaml` (default `info,merutable_cluster=debug`).
