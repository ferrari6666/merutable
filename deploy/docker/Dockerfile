# syntax=docker/dockerfile:1.6
#
# Build a linux/amd64 image for the merutable-node binary.
#
# Build from the repo root:
#   docker build --platform linux/amd64 \
#     -f deploy/docker/Dockerfile \
#     -t merutable-node:local .

ARG RUST_VERSION=1.89
ARG DEBIAN_VERSION=bookworm

# -------------------------------------------------------------------
# Stage 1: builder
#
# We let buildx pick the arch from --platform (driven by
# deploy/scripts/build.sh). On Apple Silicon, building an amd64 image
# for use on Docker Desktop / kind nodes causes the resulting binary
# to run under QEMU user-mode emulation, which does NOT implement
# io_uring_setup — the node would abort with ENOSYS at RaftLog::open.
# Passing the arch through keeps the builder + final binary in lockstep.
# -------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      protobuf-compiler \
      pkg-config \
      libssl-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy workspace manifests + crate sources. We intentionally copy the
# whole workspace (rather than a cargo-chef recipe) to keep this
# Dockerfile self-contained and easy to reason about. BuildKit cache
# mounts below handle incremental rebuild speed.
COPY Cargo.toml Cargo.lock ./
COPY crates/merutable ./crates/merutable
COPY crates/merutable-cluster ./crates/merutable-cluster
COPY crates/merutable-python ./crates/merutable-python

# merutable-python needs PyO3 dev headers and is excluded from the
# workspace default-members; we build only the cluster binary here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    RUSTFLAGS="-C force-frame-pointers=yes" \
    cargo build --release \
      --package merutable-cluster \
      --bin merutable-node \
      --bin bench \
 && cp target/release/merutable-node /usr/local/bin/merutable-node \
 && cp target/release/bench /usr/local/bin/merutable-bench

# -------------------------------------------------------------------
# Stage 2: runtime
# -------------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /data merutable \
 && mkdir -p /data \
 && chown -R merutable:merutable /data

COPY --from=builder /usr/local/bin/merutable-node /usr/local/bin/merutable-node
COPY --from=builder /usr/local/bin/merutable-bench /usr/local/bin/merutable-bench

USER merutable
WORKDIR /data

EXPOSE 9100

ENTRYPOINT ["/usr/local/bin/merutable-node"]
