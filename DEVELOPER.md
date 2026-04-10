# Developer guide

## Prerequisites

- **Rust stable** (1.80+): install via [rustup](https://rustup.rs/)
  ```
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Git**

## Building

```bash
# Debug build (fast compile, slow runtime)
cargo build --workspace

# Release build (LTO enabled, optimized)
cargo build --workspace --release
```

## Running tests

```bash
# All tests, debug mode
cargo test --workspace

# All tests, release mode (catches release-only UB)
cargo test --workspace --release
```

## Linting

```bash
# Format check
cargo fmt --check --all

# Fix formatting
cargo fmt --all

# Clippy with deny warnings (matches CI)
cargo clippy --workspace --all-targets -- -D warnings
```

## Benchmarks

```bash
cargo bench --workspace
```

Benchmarks cover bloom filter probes, memtable insert throughput, and compaction iterator merge rate.

## CI

CI runs on every push and PR to `main` via GitHub Actions (`.github/workflows/ci.yml`):

1. `cargo fmt --check --all`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`
4. `cargo test --workspace --release`

Benchmarks run on PRs only.

## Workspace layout

```
merutable/
├── Cargo.toml                    # Workspace root, shared dependency versions
├── crates/
│   ├── merutable-types/          # Core types (key encoding, schema, errors)
│   ├── merutable-wal/            # Write-ahead log
│   ├── merutable-memtable/       # Skip-list memtable + arena
│   ├── merutable-parquet/        # Parquet SSTable + bloom filter
│   ├── merutable-iceberg/        # Iceberg catalog, manifest, deletion vectors
│   ├── merutable-store/          # Object store abstraction
│   ├── merutable-engine/         # Engine orchestration (flush, compaction, read/write)
│   └── merutable/                # Public API
└── .github/workflows/ci.yml     # CI pipeline
```

Dependencies flow downward: `merutable` -> `merutable-engine` -> `{iceberg, parquet, memtable, wal, store}` -> `merutable-types`.

## Adding a dependency

All dependency versions are pinned in the workspace root `Cargo.toml` under `[workspace.dependencies]`. Individual crates reference them with `{ workspace = true }`. Never add version specs in crate-level `Cargo.toml` files.
