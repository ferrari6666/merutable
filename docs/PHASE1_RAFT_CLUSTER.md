# Phase 1: Raft Cluster for merutable — Finalized Design

## Decisions locked

| Decision | Choice |
|---|---|
| Metadata | One Raft metadata group stores schemas, node registry, and per-AZ ring definitions. Table-to-node mapping is **derived from the ring**, not stored explicitly. |
| Table-to-node mapping | Per-AZ consistent hashing. Three rings (one per AZ). `hash(table_name)` -> one node from each AZ. No placement logic, no explicit assignment. |
| Data replication | One Raft log group per table. Each table's mutations replicate through its own Raft group (RF=3, one per AZ). |
| Heartbeats | Collapsed via per-peer message bus. All Raft groups' messages to the same node batched into one gRPC call. |
| Raft log I/O | `io_uring` with O_DIRECT (DMA) for Raft log writes. Zero-copy, kernel-bypass, no page cache pollution. |
| Leader storage | Leader applies to local MeruDB synchronously inside state machine `apply()`. Standard embedded write path. |
| Follower storage | Followers apply asynchronously. Raft commits without waiting for follower MeruDB apply. Followers catch up in the background. |
| Schema evolution | Schema changes (AddColumn etc.) go through the **table's Raft group** as `AlterSchema` entries, NOT metadata-only. All nodes apply at the same Raft log index. Zero schema divergence window. |
| Leader finding | Ring determines the **preferred leader** (first replica after AZ rotation by hash). Raft priority election enforces it. Client computes leader locally — one hash, one send, no discovery. |
| AZ / RF | **Exactly 3 AZs, RF=3.** One replica per AZ. Hard constraint for Phase 1. |
| Sharding | Deferred to Phase 2. Phase 1 is one Raft group per table, per-AZ hashing for node assignment, up to ~5,000 tables. |

---

## 1. Architecture: Two Raft Group Types

```
+=================================================================+
|                   METADATA RAFT GROUP                           |
|  (one per cluster, RF=5, small, low write rate)                |
|                                                                 |
|  State machine contents:                                        |
|    - table_name -> schema                                       |
|    - node_id -> address, AZ, capacity, status                   |
|    - per-AZ consistent hash rings (derived from node registry)  |
|    - override map (rare exceptions)                             |
|                                                                 |
|  Does NOT store: table-to-node assignments.                     |
|  Assignments are derived from the rings by every node locally.  |
+=================================================================+
         |
         | publishes ring definitions + schemas to all nodes
         |
+=================================================================+
|              TABLE RAFT GROUPS (one per table)                  |
|  (RF=3, one replica per AZ, high write rate)                   |
|                                                                 |
|  Replica nodes: determined by per-AZ rings                      |
|    hash(table_name) -> one node from AZ-1 ring                  |
|    hash(table_name) -> one node from AZ-2 ring                  |
|    hash(table_name) -> one node from AZ-3 ring                  |
|                                                                 |
|  Raft log contents:                                             |
|    RaftCommand::Put { row }                                      |
|    RaftCommand::Delete { pk }                                    |
|    RaftCommand::AlterSchema { change }  (schema barrier)         |
|  Leader: apply() calls db.put() / db.add_column() synchronously |
|  Follower: data ops async, schema ops are sync barriers          |
|  Raft log I/O: io_uring + O_DIRECT                              |
|  Heartbeats: collapsed via per-peer message bus                 |
+=================================================================+
```

### Why separate metadata from data

| | Metadata group | Table groups |
|---|---|---|
| Count | 1 per cluster | 1 per table |
| Write rate | Rare (schema changes, node join/leave) | High (every client mutation) |
| RF | 5 (extra safety for catalog) | 3 (standard HA) |
| I/O profile | Buffered (small, infrequent) | io_uring + O_DIRECT (hot path) |
| Failure impact | Cluster-wide (no new tables) | Single-table (other tables unaffected) |

---

## 2. Table-to-Node Mapping: Per-AZ Consistent Hashing

### 2.1 Three rings, one per AZ

Every AZ has its own consistent hash ring built from the nodes in that AZ. Hashing a table name produces a position on the ring. The node at that position is the replica for that AZ.

```
AZ-1 ring:  [node-1] [node-4] [node-7] [node-10]
AZ-2 ring:  [node-2] [node-5] [node-8] [node-11]
AZ-3 ring:  [node-3] [node-6] [node-9] [node-12]

hash("orders") -> position 0x7A3F

AZ-1 ring: 0x7A3F -> node-4
AZ-2 ring: 0x7A3F -> node-5
AZ-3 ring: 0x7A3F -> node-9

"orders" replicas: [node-4, node-5, node-9]
Leader: whichever wins Raft election (e.g., node-4)
```

No metadata lookup. No explicit assignment. No placement logic. The hash determines everything. Every node and every client computes the same answer from the same ring definition.

### 2.2 Why per-AZ rings instead of one global ring

One global ring with AZ-walk (Cassandra style) produces uneven load when AZ sizes differ. Per-AZ rings are independent. Each AZ distributes its own tables evenly across its own nodes.

### 2.3 Virtual nodes for balance

Each physical node gets ~32 positions on its AZ's ring. Standard deviation in table distribution drops from ~40% to ~5%.

```rust
fn build_az_ring(nodes: &[NodeInfo], vnodes_per_node: usize) -> ConsistentHashRing {
    let mut ring = Vec::new();
    for node in nodes {
        for vnode in 0..vnodes_per_node {
            let position = xxhash::xxh3_64(
                format!("{}:{}", node.id, vnode).as_bytes()
            );
            ring.push(RingEntry { position, node_id: node.id });
        }
    }
    ring.sort_by_key(|e| e.position);
    ConsistentHashRing { entries: ring }
}
```

### 2.4 The mapping function

```rust
struct PerAzRings {
    az_names: Vec<String>,              // ["AZ-1", "AZ-2", "AZ-3"], stable order
    az_rings: HashMap<String, ConsistentHashRing>,
    overrides: HashMap<String, Vec<NodeId>>,
}

impl PerAzRings {
    fn replicas_for_table(&self, table: &str) -> Vec<NodeId> {
        if let Some(nodes) = self.overrides.get(table) {
            return nodes.clone();
        }
        let hash = xxhash::xxh3_64(table.as_bytes());

        // Collect one node from each AZ
        let mut nodes: Vec<NodeId> = self.az_names.iter()
            .map(|az| self.az_rings[az].node_at(hash))
            .collect();

        // Rotate by hash: spreads preferred leader across all AZs
        // nodes[0] after rotation = preferred leader
        let rotation = (hash as usize) % nodes.len();
        nodes.rotate_left(rotation);

        nodes
    }

    fn leader_for_table(&self, table: &str) -> NodeId {
        self.replicas_for_table(table)[0]  // first = preferred leader
    }
}
```

~50 KB total. Pushed via watch stream from the metadata group. Every node and client holds a copy.

The AZ rotation ensures ~1/3 of tables have their preferred leader in each AZ, spreading write load evenly.

### 2.5 Concrete example (6 nodes, 3 AZs)

```
hash("users")    = 0x1A2B -> AZ-1:node-1, AZ-2:node-2, AZ-3:node-6
hash("orders")   = 0x7C3D -> AZ-1:node-4, AZ-2:node-5, AZ-3:node-3
hash("events")   = 0x3E4F -> AZ-1:node-4, AZ-2:node-2, AZ-3:node-6
hash("sessions") = 0xA1B2 -> AZ-1:node-1, AZ-2:node-5, AZ-3:node-3
hash("payments") = 0xD5E6 -> AZ-1:node-4, AZ-2:node-5, AZ-3:node-6
hash("audit")    = 0x5678 -> AZ-1:node-1, AZ-2:node-2, AZ-3:node-3
```

---

## 3. Metadata Raft Group

### 3.1 State machine

Stores schemas and the ring. Does NOT store table-to-node assignments.

```rust
struct MetadataStateMachine {
    table_schemas: HashMap<String, TableSchema>,
    nodes: HashMap<NodeId, NodeEntry>,
    rings: PerAzRings,
    overrides: HashMap<String, Vec<NodeId>>,
}

enum MetadataCommand {
    CreateTable { name: String, schema: TableSchema },
    DropTable { name: String },
    RegisterNode { node: NodeEntry },
    DeregisterNode { node_id: NodeId },
    SetOverride { table: String, nodes: Vec<NodeId> },
    RemoveOverride { table: String },
    /// Informational: the table's Raft group applied a schema change.
    /// The authoritative schema is in the table's Raft log + MeruDB catalog.
    /// This keeps the metadata cache current for new node bootstraps.
    UpdateSchema { table: String, schema: TableSchema },
}
```

### 3.2 CreateTable: no placement logic

```rust
MetadataCommand::CreateTable { name, schema } => {
    self.table_schemas.insert(name.clone(), schema);
    // Ring determines nodes. No placement decision needed.
}
```

### 3.3 RegisterNode: ring rebuilds automatically

```rust
MetadataCommand::RegisterNode { node } => {
    let az = node.az.clone();
    self.nodes.insert(node.node_id, node);
    let az_nodes: Vec<_> = self.nodes.values()
        .filter(|n| n.az == az && n.status == NodeStatus::Active)
        .collect();
    self.rings.rebuild_az(&az, &az_nodes);
}
```

### 3.4 Local cache + watch stream

Every node caches rings + schemas behind `ArcSwap`. Updated via watch stream. Write-path routing: `ArcSwap::load()` + hash. Zero RPCs.

---

## 4. Table Raft Group Lifecycle

### 4.1 Bootstrap on CreateTable

```
CreateTable("payments", schema) committed in metadata group.
Ring: hash("payments") -> [node-4(AZ-1), node-5(AZ-2), node-6(AZ-3)]

node-4: sees table in watch stream, ring says I'm a replica
  -> create dir, open MeruDB, create Raft group, initialize, win election
node-5, node-6: join as followers
Other nodes: ignore
```

### 4.2 Node add: ring shifts tables

```
RegisterNode(node-7, AZ-1) -> AZ-1 ring rebuilt
~1/3 of AZ-1 tables shift to node-7
```

Each node recomputes locally:

```rust
fn on_ring_change(&self, old: &PerAzRings, new: &PerAzRings) {
    for table in self.all_tables() {
        let was_mine = old.replicas_for_table(&table).contains(&self.node_id);
        let is_mine = new.replicas_for_table(&table).contains(&self.node_id);
        match (was_mine, is_mine) {
            (false, true) => self.join_table_raft_group(&table),
            (true, false) => self.leave_table_raft_group(&table),
            _ => {}
        }
    }
}
```

### 4.3 Transient failure: ring unchanged

Node crashes, Raft elections handle leadership. Recovery: rejoin groups, catch up. No ring change.

### 4.4 Permanent removal: ring rebuilt

DeregisterNode -> ring rebuilt -> tables shift to remaining nodes.

---

## 5. RaftCommand and State Machine

### 5.1 RaftCommand: data + schema in the same log

```rust
#[derive(Serialize, Deserialize)]
enum RaftCommand {
    Put { row: Row },
    Delete { pk: Vec<FieldValue> },
    AlterSchema { change: SchemaChange },
}

#[derive(Serialize, Deserialize)]
enum SchemaChange {
    AddColumn { col: ColumnDef },
    // Future: DropColumn, RenameColumn (additive-only for now)
}
```

Schema changes are Raft log entries in the **table's** Raft group. All nodes see them at the same index. No divergence window.

```
Table "orders" Raft log:

  index | entry
  501   | Put(id=99, total=150.00)
  502   | Put(id=100, total=75.50)
  503   | Delete(id=42)
  504   | AlterSchema { AddColumn("status", ByteArray, nullable) }  <-- barrier
  505   | Put(id=101, total=200.00, status="shipped")               <-- new column
```

No data entry with the new column can exist before the `AlterSchema` entry. Raft serializes them.

### 5.2 Leader: synchronous apply

```rust
impl RaftStateMachine for TableStateMachine {
    async fn apply(&mut self, entries: Vec<Entry>) -> Result<Vec<Response>> {
        let mut responses = Vec::new();
        for entry in entries {
            let cmd: RaftCommand = deserialize(&entry.payload);
            match cmd {
                RaftCommand::Put { row } => {
                    let seq = self.db.put(row).await?;
                    responses.push(Response::Ok(seq));
                }
                RaftCommand::Delete { pk } => {
                    let seq = self.db.delete(pk).await?;
                    responses.push(Response::Ok(seq));
                }
                RaftCommand::AlterSchema { change } => {
                    match change {
                        SchemaChange::AddColumn { col } => {
                            let new_schema = self.db.add_column(col).await?;
                            self.current_schema = Arc::new(new_schema);
                        }
                    }
                    responses.push(Response::SchemaChanged);
                }
            }
            self.last_applied.store(entry.log_id.index, Ordering::Release);
        }
        Ok(responses)
    }
}
```

### 5.3 Follower: async data, synchronous schema barriers

Follower background apply worker processes entries in order. Data ops are the async path. Schema ops are **synchronous barriers** — the worker applies `AlterSchema` before processing any subsequent entries.

```rust
// Follower apply worker (background tokio task)
async fn apply_worker(
    db: Arc<MeruDB>,
    mut rx: mpsc::UnboundedReceiver<Vec<Entry>>,
    applied_index: Arc<AtomicU64>,
) {
    while let Some(entries) = rx.recv().await {
        for entry in entries {
            let cmd: RaftCommand = deserialize(&entry.payload);
            match cmd {
                RaftCommand::AlterSchema { change } => {
                    // SYNCHRONOUS BARRIER: must complete before
                    // any subsequent entries are processed.
                    match change {
                        SchemaChange::AddColumn { col } => {
                            db.add_column(col).await
                                .expect("schema change must succeed on follower");
                        }
                    }
                }
                RaftCommand::Put { row } => {
                    let _ = db.put(row).await;
                }
                RaftCommand::Delete { pk } => {
                    let _ = db.delete(pk).await;
                }
            }
            applied_index.store(entry.log_id.index, Ordering::Release);
        }
    }
}
```

Entries are applied **in order**. The `AlterSchema` entry is processed before any data entries that follow it. Sequential per-table apply guarantees no race.

The Raft-level `apply()` on a follower still enqueues to the background worker (non-blocking ack):

```rust
impl RaftStateMachine for FollowerStateMachine {
    async fn apply(&mut self, entries: Vec<Entry>) -> Result<Vec<Response>> {
        let last = entries.last().map(|e| e.log_id.index).unwrap_or(0);
        self.committed_index.store(last, Ordering::Release);
        let _ = self.apply_tx.send(entries);  // non-blocking
        Ok(vec![Response::Ack; entries.len()])
    }
}
```

### 5.4 Schema change flow: end to end

```
Client: AlterTable("orders", AddColumn("status", ByteArray, nullable))

Step 1: Harness validates against current schema:
        - Column name doesn't exist: ok
        - Nullable or has write_default: ok
        - Not a PK column: ok

Step 2: Propose to "orders" Raft group:
        RaftCommand::AlterSchema { change: AddColumn { col } }

Step 3: Raft replicates. Quorum ack.

Step 4: All 3 nodes apply AlterSchema at the SAME Raft index:
        - Leader: db.add_column(col) synchronously
        - Follower workers: db.add_column(col) as sync barrier

Step 5: Harness updates metadata group (async, informational):
        MetadataCommand::UpdateSchema { table: "orders", schema: new_schema }

Step 6: Return to client: SchemaChanged.

GUARANTEE: no data entry referencing the new column can exist
before this Raft index. Raft log is the serialization point for
both data and schema.
```

### 5.5 Promotion: drain then switch to sync

```rust
async fn promote_to_leader(&mut self) {
    loop {
        if self.applied_index.load() >= self.committed_index.load() { break; }
        tokio::task::yield_now().await;
    }
    self.is_leader = true;
}
```

### 5.6 Follower reads

Two watermarks: `committed_index` vs `applied_index`. Gap ~50ms. Read-after-write: client passes Raft index, follower waits until applied.

### 5.7 New node joining: schema convergence

```
Node-7 joins as a replica for "orders" (ring changed).

Path A: catches up via Raft log replay
  - Replays data entries under old schema
  - Hits AlterSchema entry -> db.add_column() -> schema updated
  - Continues with new schema

Path B: catches up via Raft snapshot
  - Snapshot taken after AlterSchema
  - MeruDB catalog already has the new schema
  - Opens directly with current schema

Either way: schema converges. No divergence.
```

---

## 6. Collapsed Heartbeats: Per-Peer Message Bus

### 6.1 Batched proto

```protobuf
message BatchRaftMessage {
    uint64 from_node = 1;
    repeated GroupMessage messages = 2;
}
message GroupMessage {
    string group_id = 1;
    oneof payload { AppendEntriesRequest append = 2; VoteRequest vote = 3; ... }
}
service RaftTransport {
    rpc Batch(BatchRaftMessage) returns (BatchRaftResponse);
}
```

### 6.2 Bus flushes every 100ms or at 64 messages

All Raft groups' messages for the same peer batch into one gRPC call. Votes trigger immediate flush.

### 6.3 RPC reduction

| Tables | Without batching | With batching |
|---|---|---|
| 200 | 4,000 RPCs/sec | 20 RPCs/sec |
| 1,000 | 20,000 | 20 |
| 5,000 | 100,000 | 20 |

---

## 7. io_uring + O_DIRECT for Raft Log

10-50us per write. DMA from aligned userspace buffer to NVMe. No page cache.

```rust
pub struct IoUringRaftLog {
    ring: io_uring::IoUring,
    fd: RawFd,                    // O_DIRECT | O_DSYNC
    write_offset: u64,
    buf_pool: AlignedBufferPool,  // 4 KiB-aligned
}
```

Two logs per table per node: Raft log (io_uring, consensus) + MeruDB WAL (FileSink, crash recovery). Separate files, separate I/O paths.

---

## 8. Complete Write Flow

```
Client: Put("orders", Row(id=42, total=150.00))

1. CLIENT: hash("orders") -> [node-4, node-5, node-3]. Send to node-4.
2. NODE-4: I'm leader for "orders". Serialize RaftCommand.
3. RAFT: append log (io_uring ~20us). AppendEntries to peers (batched).
4. NODE-5: persist log (io_uring ~20us). ACK. Queue MeruDB apply.
5. RAFT: quorum (self + node-5). Committed.
6. LEADER: db.put(row) synchronous (~200us).
7. CLIENT: PutResponse { seq: 71 }

Total: ~4.2ms (cross-AZ round trip dominates)
```

---

## 9. Client Communication and Leader Finding

### 9.1 Deterministic leader from the ring

The ring picks the **preferred leader** via AZ rotation. The first node in the replica set is always the preferred leader. Raft priority election enforces it.

```
hash("orders")   % 3 = 1 -> AZ order: [AZ-2, AZ-3, AZ-1] -> leader from AZ-2
hash("users")    % 3 = 0 -> AZ order: [AZ-1, AZ-2, AZ-3] -> leader from AZ-1
hash("events")   % 3 = 2 -> AZ order: [AZ-3, AZ-1, AZ-2] -> leader from AZ-3
```

~1/3 of tables have their preferred leader in each AZ. Write load spreads evenly.

### 9.2 Client write path: one hash, one send

```rust
impl MeruClient {
    async fn put(&self, table: &str, row: Row) -> Result<SeqNum> {
        // FAST PATH (99.9%): ring says who the leader is. Send directly.
        let preferred = self.rings.leader_for_table(table);
        let target = self.leader_override.get(table).unwrap_or(preferred);

        match self.send_put(target, table, row).await {
            Ok(resp) => {
                // Preferred leader is back? Clear override.
                if resp.meta.leader == preferred {
                    self.leader_override.remove(table);
                }
                Ok(resp.seq)
            }
            Err(e) if e.is_connection_error() || e.is_not_leader() => {
                // FAILOVER PATH (0.1%): preferred leader is down.
                // Try other replicas. One of them is the Raft leader.
                let replicas = self.rings.replicas_for_table(table);
                for node in replicas {
                    if node == target { continue; }
                    if let Ok(resp) = self.send_put(node, table, row).await {
                        self.leader_override.insert(table.into(), resp.meta.leader);
                        return Ok(resp.seq);
                    }
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
}
```

### 9.3 Raft priority election: preferred leader always wins

```rust
fn raft_priority_for(&self, table: &str) -> u64 {
    let replicas = self.rings.replicas_for_table(table);
    if self.node_id == replicas[0] { 100 } else { 1 }
}
```

When the preferred leader is healthy, it always wins elections. When it recovers after a failure, it triggers leadership transfer back to itself. The client's ring computation converges to the right leader automatically.

### 9.4 Three safety-net layers (for failover only)

In steady state, the client uses the ring. These layers exist only for the ~0.1% failure window:

- **Layer 1**: Internal proxy (follower forwards to leader).
- **Layer 2**: ResponseMeta (leader hint on every response).
- **Layer 3**: WatchLeader stream (proactive push, optional).

### 9.5 Ring updates

Client receives ring changes via metadata watch stream. Recomputes all table mappings locally. No per-table update needed.

---

## 10. Failover

| Scenario | Ring changes? | What happens |
|---|---|---|
| Leader crashes (transient) | No | Raft election ~300-600ms. Client retries replica set. |
| Node added | Yes (AZ ring rebuilt) | ~1/N tables shift. Raft membership changes. |
| Node permanently removed | Yes (AZ ring rebuilt) | Tables shift to remaining nodes. |
| Network partition | No | Isolated leader steps down. Majority elects new leader. |
| Follower promoted | No | Drain async backlog ~50ms, then accept writes. |

---

## 11. Raft Snapshots

```rust
async fn snapshot(&mut self) -> Result<Snapshot> {
    self.db.flush().await?;
    Ok(Snapshot { data: catalog_bytes })
}
async fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
    self.db.close().await?;
    write_snapshot_to_disk(snap.data)?;
    self.db = MeruDB::open(opts).await?;
    Ok(())
}
```

---

## 12. Phase 1 Scale Target

- Nodes: 3-12
- Tables: up to ~5,000 (memory-bound: 5,000 x 4 MB = 20 GB)
- Heartbeats: constant ~20 RPCs/sec (per-peer bus)
- Write latency: ~4ms (cross-AZ)

---

## 13. Implementation Plan

### 13.0 Overview and current status

```
                          P1.0 (skeleton)     DONE
                         /        |         \
                        v         v          v
               P1.1 (io_uring) P1.2 (raft  P1.3 (ring)
               DONE            DONE         DONE
                  \            /              |
                   v          v               |
                  P1.5 (state machine)        |
                  DONE              P1.4 (metadata)
                     |              DONE
                     v              /
                  P1.6 (harness + gRPC)
                  DONE
                   /         \
                  v           v
         P1.7 (client)   P1.9 (metrics)
         DONE            DONE
                  \       /
                   v     v
               P1.8 (integration tests)
               11 integration tests pass (failover, replication, watch stream,
               ring rebalance). Remaining tests — partition fencing, snapshot
               transfer under log truncation, collapsed-heartbeat count
               assertion — pending as Phase 2 scope.

~100 tests passing (89 unit + 11 integration). Zero failures.

**Library vs binary divergence (see §13.3):** the multi-node Raft path
(`create_table_multi_node` + `create_with_batched_network`) is exercised
only via integration tests. The `merutable-node` binary's gRPC
`CreateTable` handler still routes to the **single-node** `create_table`
path, and the metadata group is also initialized with a single-voter
membership. On a real 3-pod `StatefulSet` each node therefore runs as an
independent 1-voter Raft — tables created on node 0 are not visible on
nodes 1/2. GAP-1 / GAP-11 / GAP-19 are "resolved in library, not on
binary path"; GAP-10 (self-advertised `0.0.0.0:<port>`) is the root
blocker that would make any wiring fix non-functional until peers see a
reachable address. See §13.2 for the tracker.
```

### P1.0 — Crate skeleton + protos (2 days)

**Status:** [x] **Complete**

**Depends on:** nothing

**Prerequisite decisions:** none

**Deliverables:**
- [x] `crates/merutable-cluster/Cargo.toml` with all dependencies
- [x] `proto/cluster.proto` — MeruCluster gRPC service (Put, Delete, Get, Scan, CreateTable, DropTable, AlterTable, ListTables, WatchLeader, GetStatus)
- [x] `proto/raft_transport.proto` — BatchRaftMessage, GroupMessage, BatchRaftResponse
- [x] Module structure: `metadata/`, `raft/`, `io/`, `ring/`, `table/`, `harness/`, `rpc/`, `bin/`
- [x] `cargo build` compiles, `cargo test` passes (~84 tests, 0 failures)

**Acceptance:** `merutable-cluster` compiles cleanly. Proto codegen works. **Met.**

---

### P1.1 — io_uring Raft log writer (2 weeks)

**Status:** [~] **Substantially complete** — see open items at end

**Depends on:** P1.0

**Prerequisite decisions:** none

**Deliverables:**
- [x] `IoUringRaftLog` struct: O_DIRECT + O_DSYNC file, aligned buffer pool (Linux); `std::fs` + `sync_data()` fallback (macOS dev)
- [x] `append(entry)` — single entry write via io_uring SQE/CQE
- [~] `append_batch(entries)` — Linux path currently calls `append` in a loop; fallback path issues one combined `write` + `sync_data`. True multi-SQE batching on Linux still pending.
- [x] `read_from(offset)` / `scan_all` — sequential scan for recovery
- [x] `AlignedBufferPool` — pre-allocated 4 KiB DMA buffers, acquire/release with capped pool
- [x] Unit tests (7 passing): write + read back, multi-write scan, batch parity, large multi-page entry, simulated crash recovery, CRC corruption detection, pool reuse
- [ ] Benchmark: latency histogram (p50/p99/p999), throughput at 1K/10K/100K entries/sec — **not yet produced**

**Acceptance:** Functional acceptance met (recovery round-trip verified). Latency-on-NVMe acceptance still requires Linux benchmark run.

**Open questions resolved during this phase:** none

**Open items:**
- True multi-SQE batching for `append_batch` on Linux
- Latency benchmark on NVMe target hardware
- The `read_entry` / `scan_all` Linux path currently re-opens via `/proc/self/fd/{fd}`; replace with `pread64` syscall via io_uring for cleaner semantics

---

### P1.2 — openraft integration + batched transport (2 weeks)

**Status:** [~] **Complete in library; not on binary path.** `DiskRaftStorage` wired into production via `TableRaftStorage` / `MetadataRaftStorage`; gRPC server handler `RaftTransportService` implemented; `BatchedNetworkFactory` + `RaftGroup::create_with_batched_network` implemented in the `Node` harness and verified end-to-end by the `multi_node_replication` and `leader_failover` integration tests. **However** `create_with_batched_network` is only reached via `Node::create_table_multi_node`, which integration tests call directly. The gRPC `CreateTable` handler (`rpc/server.rs:223`) and `Node::new` metadata-group init (`harness/node.rs:195`) still use the single-node `create_and_initialize` path, so a real 3-pod binary deployment runs three independent 1-voter Raft groups. See §13.3 for the full divergence list and the minimum fix.

**Depends on:** P1.0

**Prerequisite decisions:** none

**Deliverables:**
- [x] `MeruTypeConfig` via `declare_raft_types!` — NodeId, Entry, SnapshotData, Response types
- [x] `RaftCommand` enum: `Put { row }`, `Delete { pk }`, `AlterSchema { change }` with postcard serialization
- [x] `RaftStorage` impl — `InMemoryRaftStorage` (unit-test only) and `DiskRaftStorage` (io_uring on Linux, std::fs fallback on macOS); `DiskRaftStorage` is wired into `TableRaftStorage` and `MetadataRaftStorage` for the production `Node` path.
- [x] `PeerBus` — per-peer message queue, flush on 100ms timer or 64-message high-water, response demux via per-request `oneshot::channel` (refactored from the earlier `DashMap` design as part of GAP-23)
- [x] `BatchedRaftNetwork` / `BatchedNetworkFactory` — openraft `RaftNetwork` impl routes through `PeerBus`, wired in production via `RaftGroup::create_with_batched_network`
- [x] `BatchTransport` trait + `GrpcBatchTransport` client + server-side `RaftTransportService` (`rpc/raft_server.rs`) that demuxes `GroupMessage` by `group_id` (table or `__metadata__`) and dispatches to the local Raft instance
- [x] Vote messages trigger immediate flush (latency-sensitive)
- [x] Unit tests: `InMemoryRaftStorage` + `DiskRaftStorage` adapters, `PeerBus` batching/demux/immediate vote flush/high-water flush, `RaftCommand` / `Vote` / `LogId` proto roundtrips

**Acceptance:** Met. Storage + network primitives verified in unit tests; 3-node append-replicate-commit verified by `multi_node_replication` integration test; leader failover verified by `leader_failover` integration test.

**Open questions resolved during this phase:** none

---

### P1.3 — Per-AZ consistent hash ring (1 week)

**Status:** [x] **Complete**

**Depends on:** P1.0

**Prerequisite decisions:** ~~Q6~~ RESOLVED (RF=3, exactly 3 AZs)

**Deliverables:**
- [x] `ConsistentHashRing` — sorted vnode list, `node_at(hash)` binary search with wrap-around
- [x] `PerAzRings` — three rings, `replicas_for_table()` with AZ rotation for preferred leader
- [x] `leader_for_table()` — `replicas_for_table()[0]`
- [x] `rebuild_az(az, nodes)` — ring rebuild on node add/remove
- [x] Override map: `HashMap<String, Vec<NodeId>>`
- [x] Unit tests (8 passing):
  - [x] Distribution evenness (max/min ratio < 1.20 with 128 vnodes across 1000 tables)
  - [x] Node add: < 50% of tables shift
  - [x] Node remove: shifted tables land on correct remaining nodes
  - [x] AZ rotation: each AZ holds 25-42% of preferred leaders
  - [x] Override map takes precedence over hash
  - [x] Determinism: same ring + same table = same result on 1000 calls
  - [x] `leader_for_table` matches `replicas_for_table[0]`
  - [x] `rebuild_az` updates a single AZ in isolation

**Acceptance:** **Met.** All distribution and shift bounds verified by unit tests.

**Open questions resolved during this phase:** Q12 (deterministic preferred leader via AZ rotation) implemented and tested.

---

### P1.4 — Metadata Raft group (1 week)

**Status:** [~] **Complete in library; not on binary path.** Metadata state machine, events, `WatchLeader` ring-diffing, and `MeruClient::start_watch` all work end-to-end in integration tests. **However** `Node::new` only calls `MetadataRaftGroup::create_and_initialize` with `members = {self.node_id}` (`harness/node.rs:195`), and the underlying `GrpcNetworkFactory` is constructed with `HashMap::new()` and a comment *"single-node: no peers"* (`metadata/raft_group.rs:251-255`). The multi-voter `create_with_network` constructor exists but is never called from `Node::new`. Live 3-pod deployments run three independent 1-voter metadata groups — no `MetadataEvent::TableCreated` ever reaches peer pods. See §13.3 Divergence 2. Tracked under **GAP-1** and **GAP-19** (both "RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH").

**Depends on:** P1.2, P1.3

**Prerequisite decisions:** none

**Deliverables:**
- [x] `MetadataStateMachine` — `table_schemas`, `nodes`, `rings`, `overrides`
- [x] `MetadataCommand` enum: CreateTable, DropTable, RegisterNode, DeregisterNode, SetOverride, RemoveOverride, UpdateSchema
- [x] Ring auto-rebuild on RegisterNode/DeregisterNode (via `rebuild_all_rings` to handle new AZs cleanly)
- [x] Event emission: `pending_events: Vec<MetadataEvent>` plus `drain_events()` API
- [x] `MetadataRaftGroup` wired via `create_and_initialize` / `create_with_network` on top of `MetadataRaftStorage` (DiskRaftStorage-backed) with the batched gRPC transport shared with table groups
- [x] Watch stream gRPC server-streaming RPC — `WatchLeader` (`rpc/server.rs`) consumes broadcast events emitted by `Node::drain_and_broadcast`; on `RingChanged` it diffs leaders per table and emits `LeaderEvent` for every shifted table (GAP-9 server-side RESOLVED)
- [x] `LocalMetadataCache` — `ArcSwap`-backed lock-free reader for rings + schemas + nodes
- [x] Unit tests: `MetadataStateMachine` create/drop (+ rejection), RegisterNode/DeregisterNode ring rebuild, update_schema replace, set/remove override, drain_events clears; `LocalMetadataCache` read/update/remove

**Acceptance:** Met. End-to-end metadata propagation verified by `watch_stream_ring_change` and `ring_rebalance_membership_change` integration tests (client sees ring diffs via watch stream; rebalance proposals commit through the metadata Raft group).

**Open questions resolved during this phase:** none

---

### P1.5 — Table state machine (2 weeks)

**Status:** [~] **Complete in library; multi-replica bootstrap not on binary path.** Single- and multi-node SM work through openraft consensus via `RaftGroup` / `TableRaftStorage`. Multi-replica bootstrap via `BootstrapTable`/`replicas`/`bootstrapper` is implemented in `create_table_multi_node` (`harness/node.rs:342`) and exercised by `multi_node_replication`, but the production gRPC `CreateTable` handler (`rpc/server.rs:223`) calls the single-node `Node::create_table` which constructs `members = {self}` and ignores `BootstrapTable.replicas` / `bootstrapper` entirely — so live pods each bring up a 1-voter Raft group for the same table name. See §13.3 Divergence 1. Tracked under **GAP-11** ("RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH"). Follower sidecar fsynced from `apply_worker` (GAP-13 RESOLVED), apply-worker error branches log + increment `FOLLOWER_APPLY_ERRORS_TOTAL` + `break` without advancing applied_index (GAP-21 RESOLVED), sidecar is atomic tmp+fsync+rename (GAP-22 RESOLVED).

**Depends on:** P1.1, P1.2

**Prerequisite decisions:**
- [x] **Q1 (bootstrap leader):** preferred leader (replicas[0] from ring) runs `raft.initialize()`. Others join via `add_learner` + `change_membership`. (Decision locked; orchestration lives in P1.6.)

**Deliverables:**
- [x] `TableStateMachine` — `Leader`/`Follower` modes via `StateMachineMode` enum
- [x] `RaftCommand::Put` -> `db.put(row)`, `RaftCommand::Delete` -> `db.delete(pk)`
- [x] `RaftCommand::AlterSchema` -> `db.add_column(col)` — synchronous barrier on both leader (inline in `apply()`) and follower (inline in `apply_worker`, blocks subsequent entries)
- [x] Follower `apply_worker` — sequential per-table, schema ops block, data ops async
- [x] `promote_to_leader()` — drains async backlog by closing `apply_tx` and awaiting the worker JoinHandle, then flips to sync mode
- [x] `demote_to_follower()` — re-spawns the worker, flips to async
- [x] `committed_index` + `applied_index` watermarks (Arc<AtomicU64>, Acquire/Release ordering)
- [x] Raft snapshot build: `db.flush()` + package catalog dir (`build_snapshot()`)
- [x] Raft snapshot install: delete wal_dir contents + replace catalog + reopen MeruDB (`install_snapshot()`)
- [x] Sidecar file for `applied_index` — fsynced after each apply batch (leader inline; follower via `apply_worker` after GAP-13 fix); sidecar write is atomic (tmp-write + fsync + rename + parent fsync — GAP-22)
- [x] Table Raft group bootstrap (replicas[0] initializes via `BootstrapTable.bootstrapper`, others create uninitialised groups and join as learners) — lives in P1.6 harness, verified by `multi_node_replication` integration test
- [x] openraft `RaftStateMachine` adapter that delegates `apply_to_state_machine` to `TableStateMachine::apply` — done as `TableRaftStorage` (`raft/group.rs:137`; trait impl `raft/group.rs:215`; `apply_to_state_machine` at `raft/group.rs:321`)
- [x] Unit tests (12 passing):
  - [x] Leader apply: put + get round-trip (`leader_apply_put_and_get`)
  - [x] Leader apply: delete (`leader_apply_delete`)
  - [x] Leader apply: alter_schema (`leader_apply_alter_schema`)
  - [x] Leader: applied_index advances on each entry (`leader_applied_index_advances`)
  - [x] Follower async apply: committed_index advances before applied_index (`follower_async_apply`)
  - [x] Follower schema barrier: AlterSchema blocks subsequent puts until applied (`follower_schema_barrier`)
  - [x] `promote_to_leader` drains backlog before switching mode (`promote_drains_backlog`)
  - [x] applied_index survives restart on leader (`sidecar_persists_applied_index`)
  - [x] Follower sidecar reflects actually-applied entries — GAP-13 regression (`follower_sidecar_reflects_actual_apply`)
  - [x] `wait_for_applied` returns immediately when index already applied (`wait_for_applied_immediate`)
  - [x] `wait_for_applied` blocks on follower until apply lands (`wait_for_applied_with_follower`)
  - [x] Snapshot install: WAL dir cleaned, MeruDB reopens (`snapshot_install_roundtrip`)
  - [x] Bootstrap: only the `bootstrapper` replica initializes — verified end-to-end by the `multi_node_replication` integration test (P1.6 harness)

**Acceptance:** Met. Single-node leader/follower behaviour, schema barrier, drain-on-promote, build/install snapshot covered by unit tests; 3-node end-to-end append/replicate/commit + leader failover covered by `multi_node_replication` and `leader_failover` integration tests.

**Open questions resolved during this phase:**
- [x] Q5: snapshot install deletes wal_dir (`install_snapshot` calls `tokio::fs::remove_dir_all(&wal_dir)` before reopening MeruDB)
- [x] Q4: applied_index persisted in sidecar file (leader inline; follower via `apply_worker`)

---

### P1.6 — Node harness + gRPC server (2 weeks)

**Status:** [~] **Library complete; binary path diverges.** The `Node` harness supports multi-node end-to-end through real openraft consensus + batched gRPC transport — **but only when driven by `tests/integration.rs` through `create_table_multi_node`**. The production gRPC `CreateTable` handler routes through the single-node `Node::create_table`, the metadata group is brought up as a 1-voter, and `bin/merutable-node.rs:76` self-advertises `0.0.0.0:{port}` as the peer-reachable address. A live 3-pod StatefulSet therefore runs as three independent 1-voter clusters. See §13.3 Divergences 1-3 for full evidence and minimum fix. The library-level ring-change orchestration (GAP-12 RESOLVED), multi-replica bootstrap (GAP-11 library-only), and `connect_peer` failure propagation (GAP-28 RESOLVED) all work; they're just not reachable from the production gRPC entrypoint. Full `JoinCluster`/`Bootstrap` RPC handshake still open (GAP-10 partial).

**Depends on:** P1.4, P1.5

**Prerequisite decisions:**
- [x] **Q7 (failure transition):** Phase 1 is operator-only `DeregisterNode`. Auto-deregister is Phase 2.

**Deliverables:**
- [x] `Node` struct: owns metadata cache, per-AZ rings, table state machines, role per table
- [x] `Router`: replicas_for, preferred_leader, is_my_table, am_i_preferred_leader
- [x] gRPC `MeruCluster` service:
  - [x] `Put` / `Delete` — route to table leader, propose via `RaftGroup::client_write` (real openraft consensus, single-node)
  - [x] `Get` — read from local MeruDB; honors `min_raft_index` via `wait_for_applied` (GAP-14)
  - [x] `Scan` — range scan against MeruDB (GAP-16 RESOLVED)
  - [~] `CreateTable` / `DropTable` — metadata SM + single-node table SM works; production `CreateTable` handler calls `Node::create_table` (single-voter) rather than `create_table_multi_node`, so multi-replica bootstrap is not exercised by the gRPC path. See §13.3 Divergence 1.
  - [x] `AlterTable` — apply AlterSchema to table SM + UpdateSchema to metadata SM
  - [x] `GetStatus` — node health, role per table, committed/applied indices
  - [x] `WatchLeader` — broadcasts metadata events; on `RingChanged` walks all tables and emits per-table `LeaderEvent` for any leader shift (GAP-9 server-side RESOLVED)
  - [x] `ListTables` — returns all table schemas
- [x] `ResponseMeta` on every response: leader address hint from ring
- [x] `NodeError` with `NotLeader { leader_hint }` variant
- [x] Internal write proxy: follower -> leader forwarding for `Put`/`Delete`/`AlterTable` (GAP-15 RESOLVED); proxy path clones the peer channel before the awaited gRPC so the `peer_channels` read lock is not held across awaits (GAP-26 RESOLVED)
- [x] Ring-change handler: `Node::on_ring_change(old_rings)` diffs replica sets per table, spawns `add_learner` + `change_membership` with a concurrency cap of 10 via `rebalance_semaphore`; `start_rebalance_watcher` listens for `RingChanged` (GAP-12 RESOLVED — verified by `ring_rebalance_membership_change` integration test)
- [~] Table bootstrap via Raft `initialize` + `add_learner` / `change_membership` — single-replica path via `RaftGroup::create_and_initialize`; multi-replica path via `create_table_multi_node` exists and is verified by integration tests, but the production gRPC `CreateTable` handler does not call it. (GAP-11 "RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH" — see §13.3 Divergence 1)
- [ ] Preferred leader reclaim via `transfer_leader` — not implemented (not tracked as a GAP; would be Phase 2 optimization)
- [x] Follower apply queue depth cap (`FOLLOWER_APPLY_QUEUE_CAP = 10_000`)
- [~] `merutable-node` binary — rich seed format (`node_id:az:addr`), serves MeruCluster + RaftTransport, graceful shutdown with bus drain (GAP-6 RESOLVED). **However** `bin/merutable-node.rs:76` self-advertises `format!("0.0.0.0:{}", grpc_port)` as `NodeIdentity::address`; peers receiving `MetadataCommand::RegisterNode` would dial `0.0.0.0` and self-loop or connect-refuse (§13.3 Divergence 3). Full `JoinCluster`/`Bootstrap` RPC handshake for auto peer-discovery also still pending (GAP-10 partial).
- [x] Unit tests (20 passing):
  - [x] Router (3): `replicas_returns_three_nodes`, `is_my_table_correct`, `preferred_leader_matches_ring`
  - [x] Node (10): `create_table_opens_sm`, `put_and_get_roundtrip`, `delete_removes_row`, `alter_table_adds_column`, `put_to_wrong_node_returns_not_leader`, `drop_table_closes_sm`, `watch_events_delivered` (broadcast plumbing), `proxy_forwards_put` (GAP-5), `alter_table_proxy_returns_not_leader_without_channel` (GAP-15), `scan_returns_all_rows` (GAP-16)
  - [x] gRPC (4): `grpc_put_and_get`, `grpc_create_table`, `watch_ring_change_emits_leader_events` (GAP-9 server-side), `scan_returns_all_rows` (GAP-16)
  - [x] RaftGroup single-node bootstrap + consensus (3): `raft_group_single_node_put`, `raft_group_single_node_delete`, `raft_group_is_leader` — verifies `RaftGroup::create_and_initialize` brings up a one-node Raft, `client_write` proposes through openraft, and reads through `TableStateMachine` (GAP-1 single-node)

**Acceptance:** Met. Single-node gRPC put/get/delete/alter + NotLeader detection verified by unit tests. Multi-node 3-node-cluster acceptance (convergence, leader failover, ring rebalance, watch stream) verified by the integration tests in `crates/merutable-cluster/tests/integration.rs`. Full auto peer-discovery handshake is the residual GAP-10 item.

**Open questions resolved during this phase:**
- [x] Q7: operator-only DeregisterNode (implemented: no auto-deregister logic)
- [x] Q8: apply queue depth cap — implemented (`FOLLOWER_APPLY_QUEUE_CAP = 10_000`)

---

### P1.7 — Client library (1 week)

**Status:** [x] **Complete** — `MeruClient` implements ring routing, leader-override cache, failover, read-after-write, and `start_watch` subscribes to the server-side `WatchLeader` stream and updates `leader_overrides` on ring changes (GAP-9 client-side RESOLVED; verified by `watch_stream_ring_change` integration test).

**Depends on:** P1.3, P1.6

**Prerequisite decisions:** none

**Deliverables:**
- [x] `MeruClient` struct: seed list, gRPC channel pool
- [x] Local per-AZ rings: `leader_for_table()` — zero-lookup routing (`write_target`)
- [x] Ring updates via metadata watch stream subscription — `MeruClient::start_watch` opens `WatchLeader` and updates `leader_overrides` on every `LeaderEvent` (GAP-9 client-side RESOLVED)
- [x] Leader override map: temporary cache for failover, auto-clears when preferred leader returns (`process_leader_hint`)
- [x] `put(table, row)` — send to preferred leader, retry replica set on failure
- [x] `get(table, pk)` — send to any node
- [x] `get_after_write(table, pk)` / `get_with_min_index` — read-after-write using cached `last_write_index`
- [x] `scan(table, start_pk, end_pk)`
- [x] `create_table(name, schema)` / `drop_table(name)` / `alter_table(name, change)` / `list_tables`
- [x] Unit tests: `client_put_and_get`, `client_delete`, `client_scan`, `client_list_tables`, `client_get_after_write`, `client_failover_to_replica`
- [x] Test: ring update — client picks up new ring via watch subscription, routes shift (`watch_stream_ring_change` integration test)

**Acceptance:** Met. Single-node operations verified via unit tests; 3-node routing, failover, and watch-driven ring updates verified via integration tests (`client_routes_to_preferred_leader`, `client_handles_routing`, `leader_failover`, `watch_stream_ring_change`).

**Open questions resolved during this phase:** none

---

### P1.8 — Integration tests (2 weeks)

**Status:** [~] **Substantially complete** — 14 integration tests green in `crates/merutable-cluster/tests/integration.rs`. Partition fencing, dedicated snapshot-transfer under log-truncation, and collapsed-heartbeat count assertion are the outstanding scenarios.

**Depends on:** P1.6, P1.7

**Prerequisite decisions:**
- [x] **Q3 (snapshot payload):** Phase 1 requires shared object store. Manifest-only snapshots. (Decision locked in §13.1; dedicated snapshot-transfer-under-log-truncation integration scenario is still pending.)

**Deliverables:**
- [x] **Single-table basic**: put rows on leader, get from all 3 nodes, verify convergence (`single_table_put_get`, `multi_node_replication`)
- [x] **Leader failover**: kill leader, writes resume on new leader (`leader_failover`)
- [x] **Follower failover**: kill follower, writes continue, follower catches up on recovery (`follower_failover`)
- [ ] **Network partition**: isolate leader, verify new election, verify fenced old leader can't write (still pending — Phase 2 scope)
- [x] **Multi-table**: different preferred leaders, concurrent writes, per-table consistency (`multi_table_concurrent_writes`, `multi_table_batched_transport`)
- [x] **Schema evolution**: AddColumn through Raft, verify all nodes see same schema (`alter_table_through_client`)
- [ ] **Raft snapshot under log truncation**: stop follower, write enough to force truncation, restart follower, verify snapshot transfer + catch-up (still pending — basic `snapshot_install_roundtrip` covered at unit level in P1.5)
- [x] **Node add + ring rebalance**: add node, verify ring shift + Raft membership changes (`ring_rebalance_membership_change`)
- [x] **Watch stream ring updates**: client subscribes, ring change propagates, `leader_overrides` updated (`watch_stream_ring_change`)
- [x] **Routing / failover client paths**: `client_routes_to_preferred_leader`, `client_handles_routing`, `delete_through_client`, `scan_with_bounds`, `list_tables_across_cluster`
- [ ] **Collapsed heartbeats**: explicit gRPC call-count assertion against batched expectation (still pending — `PeerBus` batching covered at unit level in P1.2)
- [x] **Rebalance concurrency cap**: semaphore cap = 10 enforced by `rebalance_semaphore`; end-to-end rebalance exercised by `ring_rebalance_membership_change`

**Acceptance:** Partial — 14 integration tests pass, zero failures. Remaining gaps (partition fencing, log-truncation snapshot transfer, explicit heartbeat-count assertion) are tracked as Phase 2 scope.

**Open questions resolved during this phase:**
- [x] Q2: rebalance concurrency cap implemented (semaphore cap = 10 in `Node::on_ring_change`) and exercised end-to-end (`ring_rebalance_membership_change`). Retry policy and leadership-transfer-before-remove remain open sub-points under Q2.

---

### P1.9 — Observability (1 week)

**Status:** [x] **Complete** for P1 scope — 18 metrics emitted from production code; `tracing` instrumentation on all hot paths (proposals, commit latency, leader transitions, snapshot installs, rebalance). The metrics listed under "Open items" below are enhancement follow-ups (apply_lag_ms in time, open_raft_groups gauge) and are not blocking for P1.

**Depends on:** P1.6

**Prerequisite decisions:** none

**Deliverables:**
- [x] Metrics (via `metrics` crate):
  - [x] `cluster.raft.proposals_total` (counter) — emitted in `RaftGroup::client_write` (`raft/group.rs:495`)
  - [x] `cluster.raft.commit_latency_us` (histogram) — emitted in `RaftGroup::client_write` (`raft/group.rs:498`)
  - [x] `cluster.raft.log_write_latency_us` (histogram) — emitted in `DiskRaftStorage::append_to_log` (`raft/disk_storage.rs:440`)
  - [x] `cluster.raft.elections_total` (counter) — emitted on leadership transition in `start_leader_metrics_sampler` (`harness/node.rs:1276`)
  - [x] `cluster.raft.leader_tenure_secs` (gauge) — emitted by `start_leader_metrics_sampler` while this node is leader (`harness/node.rs:1295`)
  - [x] `cluster.follower.apply_lag_entries` (gauge, committed - applied) — emitted in `TableStateMachine::apply` (`state_machine.rs:184, 219`)
  - [x] `cluster.follower.apply_lag_ms` (gauge) — emitted in `apply_worker` (`state_machine.rs:490`)
  - [x] `cluster.bus.batch_size` (histogram) — emitted in `PeerBus::flush` (`raft/network.rs:137`)
  - [x] `cluster.bus.flush_count` (counter) — emitted in `PeerBus::flush` (`raft/network.rs:136`)
  - [x] `cluster.ring.version` (gauge) — incremented on RegisterNode/DeregisterNode (`metadata/state_machine.rs:133, 154`)
  - [x] `cluster.ring.tables_per_node` (gauge per node) — emitted in `Node::on_ring_change` (`harness/node.rs:1007`)
  - [x] `cluster.rebalance.in_progress` (gauge) — inc/dec around each per-table rebalance task in `Node::on_ring_change` (`harness/node.rs:1053, 1064, 1081, 1091`)
  - [x] **GAP-17 metrics**: `cluster.proxy.attempts_total{result}` counter, `cluster.proxy.latency_us` histogram, `cluster.client.not_leader_retries_total` counter (`harness/node.rs:480-500, 559-579`)
  - [x] `cluster.node.table_count` gauge — emitted in `Node::create_table` / `Node::drop_table` (`harness/node.rs:402, 431`)
  - [x] `cluster.node.open_raft_groups` gauge — emitted in `Node::create_table` / `Node::drop_table` (`harness/node.rs:450, 536`)
- [x] Structured logging (`tracing`):
  - [x] Leadership transitions (table, old leader, new leader) — `leadership transition` log in `start_leader_metrics_sampler` (`harness/node.rs:1281`); table SM also emits `promoted to leader` / `demoted to follower` (`state_machine.rs:249, 268`)
  - [x] Ring changes (AZ, added/removed node) — `metadata: node registered` / `node deregistered` (`metadata/state_machine.rs:130, 152`)
  - [x] Schema changes (table, column added) — `metadata: table created` / `table created` per-node (`metadata/state_machine.rs:108`, `harness/node.rs:403`)
  - [x] Snapshot install — `snapshot installed` log in `DiskRaftStorage::install_snapshot` (`raft/disk_storage.rs:545`); size/duration fields are an enhancement follow-up
  - [x] Raft group bootstrap — `raft group created` (`raft/group.rs:481`)
  - [x] Failover / proxy events — `proxying put to leader`, `proxying delete to leader` (`harness/node.rs:693, 739`)

**Acceptance:** Metrics + `tracing` instrumentation are emitted from real production code paths and exercised by the 89 unit + 14 integration tests (including `leader_failover`, `follower_failover`, `ring_rebalance_membership_change`, `watch_stream_ring_change`, `multi_table_batched_transport`). Remaining items are enhancement follow-ups (snapshot-transfer duration/size fields, /metrics HTTP scrape endpoint) and not blocking for P1.

**Open questions resolved during this phase:** none

**Open items:** none blocking. Enhancement follow-ups: annotate snapshot-install logs with duration/size; expose a Prometheus scrape endpoint.

---


### 13.1 Open question resolution tracker

| Q# | Question | Status | Resolved by | Decision |
|---|---|---|---|---|
| Q1 | Bootstrap leader for new Raft groups | **Single-node IMPLEMENTED**; multi-node pending (GAP-1, GAP-11) | P1.5 / P1.6 (single-node) | replicas[0] initializes, others join |
| Q2 | Rebalance orchestration | **Impl pending** (GAP-12) | — | Concurrency cap 10, serial per table |
| Q3 | Snapshot transfer payload | Decision locked. **No implementation owner** — manifest serialization + object-store reads inside `install_snapshot` not yet specified in any phase or gap | — | Shared object store required. Manifest-only. |
| Q4 | applied_index durability | **RESOLVED + IMPLEMENTED** | P1.5 | Sidecar file, fsynced per batch (leader path inline; follower path via `apply_worker`) |
| Q5 | Snapshot install cleans WAL | **RESOLVED + IMPLEMENTED** | P1.5 | `install_snapshot` removes `wal_dir` |
| Q6 | RF locked to AZ count | **RESOLVED** | v6 | RF=3, exactly 3 AZs |
| Q7 | Transient vs permanent failure | **RESOLVED** | P1.6 | Operator-only DeregisterNode |
| Q8 | Promotion drain bound | **RESOLVED + IMPLEMENTED** | P1.5 / P1.6 | Cap apply queue depth, back-pressure |
| Q9 | Bus batching latency | Not blocking | — | Performance tuning |
| Q10 | RPC math | Not blocking | — | Performance tuning |
| Q11 | I/O contention | Not blocking | — | Performance tuning |
| Q12 | First-call client routing | **RESOLVED + IMPLEMENTED** | P1.3 | AZ rotation, deterministic leader |
| Q13 | Memory math | Not blocking | — | Documentation |
| Q14 | WatchLeader fan-out | **RESOLVED + IMPLEMENTED** | P1.6 | Optional Layer 3, ring is primary |

### 13.2 Gap tracker

| Gap | What | Severity | Status | Blocks |
|---|---|---|---|---|
| GAP-1 | openraft Raft group wiring (no real replication) | **Critical** | **RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH** — multi-node replication via batched gRPC transport (`PeerBus` + `GrpcBatchTransport`) works in integration tests (`multi_node_replication`, `leader_failover`). However the production gRPC `CreateTable` handler (`rpc/server.rs:223`) calls `node.create_table(...)` which hits `harness/node.rs:315` — `RaftGroup::create_and_initialize` with `members={self}`. Comment at `harness/node.rs:309` still reads *"Single-node membership for now."* Live 3-pod cluster observed creating a table only on the handling node; the other pods never see it. | §13.3 |
| GAP-2 | io_uring-backed RaftStorage (production log persistence) | High | **RESOLVED** — `DiskRaftStorage` uses `RaftLog` (io_uring on Linux, std::fs fallback on macOS). Wired into `TableRaftStorage` and `MetadataRaftStorage` for all Raft groups. | — |
| GAP-3 | applied_index sidecar file | Medium | **RESOLVED** (leader inline, follower via `apply_worker` after GAP-13 fix) | — |
| GAP-4 | WatchLeader / WatchMetadata streams | Medium | **RESOLVED** — server push + ring diffing + client `start_watch()` with `leader_overrides` update. End-to-end tested via `watch_stream_ring_change`. | — |
| GAP-5 | Internal write proxy (follower -> leader) | Medium | **RESOLVED** for Put/Delete (AlterTable tracked as GAP-15) | — |
| GAP-6 | merutable-node binary | Medium | **RESOLVED** — bootstrap, rich seed format, MeruCluster + RaftTransport gRPC, PeerBus per peer, graceful shutdown with SM close + bus drain. | — |
| GAP-7 | Follower apply queue depth cap | Medium | **RESOLVED** | — |
| GAP-8 | Snapshot install test | Low | **RESOLVED** | — |
| GAP-9 | Ring-change events not propagated to clients (`RingChanged` dropped by `WatchLeader`) | **Critical** | **RESOLVED** — server-side ring diffing in `WatchLeader` + client-side `start_watch()` updates `leader_overrides`. Tested via `watch_stream_ring_change` and `watch_ring_change_emits_leader_events`. | — |
| GAP-10 | Node-level bootstrap / peer-discovery protocol | **Critical** | **PARTIAL — ROOT BLOCKER FOR GAP-1 / GAP-11 / GAP-19 ON BINARY PATH.** Binary parses `node_id:az:addr` seeds and runs both MeruCluster + RaftTransport servers. However `bin/merutable-node.rs:76` self-advertises `format!("0.0.0.0:{}", grpc_port)` — that string is stored as `NodeIdentity::address` and propagated into `MetadataCommand::RegisterNode`. If the metadata group were ever multi-voter, peers would dial `0.0.0.0:9100` and either loop back to themselves or fail. Fix requires resolving a routable hostname (StatefulSet: `${HOSTNAME}.merutable-hl.${NS}.svc.cluster.local`) before binding. Full JoinCluster/Bootstrap RPC handshake also still open. | P1.6 binary full auto-discovery; unblocks multi-node binary path |
| GAP-11 | `BootstrapTable` metadata event for multi-replica `CreateTable` race | **Critical** | **RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH** — `MetadataEvent::TableCreated` includes `replicas` + `bootstrapper` and `create_table_multi_node` (`harness/node.rs:342`) honors it. But the gRPC handler (`rpc/server.rs:223`) calls `node.create_table` — the single-node variant that ignores `replicas`/`bootstrapper`. Metadata group itself is also single-voter (`harness/node.rs:195`: `MetadataRaftGroup::create_and_initialize` with `members={self}`), so no `MetadataEvent::TableCreated` ever reaches peer nodes in the live cluster. | §13.3 |
| GAP-12 | Ring-rebalance / Raft membership-change orchestration on `RingChanged` | **Critical** | **RESOLVED** — `Node::on_ring_change(old_rings)` diffs replica sets for all tables, spawns `add_learner` + `change_membership` tasks for affected tables. Concurrency capped at 10 via semaphore. `start_rebalance_watcher` listens for `RingChanged` events. Tested via `ring_rebalance_membership_change` integration test. | — |
| GAP-13 | Follower `applied_index` sidecar fsync race | Medium | **RESOLVED** (sidecar moved into `apply_worker`) | — |
| GAP-14 | Server-side `min_raft_index` wait for read-after-write | Medium | **RESOLVED** (`wait_for_applied` + Get handler honors `min_raft_index`) | — |
| GAP-15 | `Node::alter_table` leader proxy (parity with `put` / `delete`) | Low | **RESOLVED** (`harness/node.rs:677-717`) | — |
| GAP-16 | `Scan` implementation or removal from proto | Low | **RESOLVED** (in scope; Node + gRPC + client all implemented) | — |
| GAP-17 | Proxy / failover observability metrics | Low | **RESOLVED** (`observability.rs` constants emitted from proxy paths in `harness/node.rs`) | — |
| GAP-18 | P1.8 end-to-end watch-stream / ring-change client test | Low | **RESOLVED** — `watch_stream_ring_change` integration test: client subscribes, node registers 4th peer, verifies `leader_overrides` updated via the watch stream | — |
| GAP-19 | Batched Raft transport implemented but not wired into production | **Critical** | **RESOLVED IN LIBRARY ONLY; NOT ON BINARY PATH** — `GrpcBatchTransport`, per-request oneshot `PeerBus`, and `RaftGroup::create_with_batched_network` all work and are exercised by `multi_node_replication` + `leader_failover` integration tests. `PeerBus` instances *are* spun up per seed peer at startup (`bin/merutable-node.rs` → `Node::connect_peer`). But `create_with_batched_network` is only reachable through `create_table_multi_node`; the production `rpc/server.rs` `CreateTable` handler calls the non-batched `create_table` path, so the PeerBus queues stay empty for table writes. Metadata group is also not batched — `MetadataRaftGroup::create_and_initialize` (`harness/node.rs:195`) uses `GrpcNetworkFactory::new(..., HashMap::new())` (single-node, no peers). | §13.3 |
| GAP-20 | Entry-payload serialize/deserialize silently converts corruption into `Blank` | High | **RESOLVED** — `serialize_entry_payload` / `deserialize_entry_payload` return `Result`; call sites use `unwrap_or_else` + `tracing::error` with Blank fallback (crash-safe, logged) | — |
| GAP-21 | Follower `apply_worker` swallows data failures and panics on schema failures | High | **RESOLVED** (`table/state_machine.rs:457-485` — all three branches log + increment error counter + `break` without advancing `applied_index`; schema panic replaced with `break`) | State-machine divergence detection |
| GAP-22 | `write_sidecar` is non-atomic and swallows I/O errors | High | **RESOLVED** (`table/state_machine.rs:414-436` — tmp-write + fsync + rename + parent fsync) | `applied_index` durability on crash |
| GAP-23 | `PeerBus::response_channels` leaks on group-network recreation | Medium | **RESOLVED** — PeerBus refactored to per-request oneshot channels (no persistent response_channels map). Each `enqueue()` returns a oneshot receiver; no lifecycle management needed. | — |
| GAP-24 | `PeerBus::flush_loop` has no shutdown; `notify_one` under queue lock | Medium | **RESOLVED** — `shutdown()` + `shutdown_flag` + exit checks in `flush_loop`; `Node::shutdown` signals all buses and aborts handles. `notify_one` is inside `queue.lock` scope but safe (Notify stores permit if no waiter). | — |
| GAP-25 | `PerAzRings::replicas_for_table` panics on empty AZ; `usize` truncation | Medium | **RESOLVED** — empty-AZ filter in `replicas_for_table` + `leader_for_table` returns 0 on empty result instead of panicking (`ring/per_az.rs:165-167`) | — |
| GAP-26 | Proxy path holds `peer_channels` read lock across awaited gRPC | Low | **RESOLVED** (`harness/node.rs:598-601` and siblings — channel cloned, lock dropped before `client.put().await`) | Proxy/reconnect head-of-line |
| GAP-27 | `GrpcRaftNetwork::send_one` collapses tonic errors into `ConnectionRefused` | Low | **RESOLVED** (`raft/group.rs:82-88` — explicit match on `Unavailable/DeadlineExceeded/NotFound/PermissionDenied`) | openraft retry / election behavior |
| GAP-28 | `Node::connect_peer` silently drops connect failures | Low | **RESOLVED** (`harness/node.rs:1010-1027` — returns `Result<(), NodeError>` with `ProxyFailed` on invalid URI / connect error) | Startup misconfiguration visibility |

### 13.3 Production-path vs test-path divergence

Multiple "RESOLVED" items in §13.2 are resolved **in the library** —
they work when driven by `tests/integration.rs` — but are **not on the
path the `merutable-node` binary actually executes** when a client calls
its gRPC server. This section enumerates the divergence so future work
can close it without re-deriving the evidence.

Observed symptom on a live 3-pod `StatefulSet` (see `deploy/k8s/`):
`CreateTable smoke_events` succeeds against `merutable-0`; `ListTables`
on `merutable-1` and `merutable-2` returns an empty list. Each pod's
`GetStatus` reports itself as Leader of a 1-voter group. A 5k-row
bulkload through `merutable-0` produces a single Parquet file under
`merutable-0:/data/smoke_events/data/data/L0/` — nothing on the other
pods.

**Divergence 1 — gRPC `CreateTable` skips the multi-node code path.**

- `rpc/server.rs:223` — `node.create_table(req.name.clone(), schema)`
- `harness/node.rs:309-323` — comment *"Single-node membership for now"*;
  constructs `members = {self}` and calls
  `RaftGroup::create_and_initialize(..., members, ...)`.
- `harness/node.rs:342` — `create_table_multi_node` exists and wires the
  batched network, honors `bootstrapper`, collects peer `PeerBus`
  instances. **Only called from `tests/integration.rs`** (`grep -n
  create_table_multi_node` → three hits, all in tests).
- Implication: every `CreateTable` RPC produces a 1-voter Raft group on
  the handling node. Nothing is replicated, regardless of how many pods
  are running.

**Divergence 2 — metadata group is single-voter.**

- `harness/node.rs:195-204` — `MetadataRaftGroup::create_and_initialize`
  with `members = {self.node_id}` only.
- `metadata/raft_group.rs:251-255` —
  `GrpcNetworkFactory::new(..., HashMap::new())` with comment
  *"single-node: no peers"*.
- `metadata/raft_group.rs:279` — `create_with_network` (the multi-node
  variant) exists but is **never called** from `Node::new`.
- Implication: even if Divergence 1 were fixed, `MetadataEvent::TableCreated`
  would still be local-only — peer pods would never learn the table
  exists, and GAP-11's bootstrapper-election logic would never run
  because the metadata commit that triggers it never reaches them.

**Divergence 3 — self-advertised address is unroutable.**

- `bin/merutable-node.rs:76` — `let address = format!("0.0.0.0:{}", grpc_port);`
- That string is embedded into `NodeIdentity::address` and later
  replicated to peers via `MetadataCommand::RegisterNode`.
- Implication: if Divergences 1 & 2 were fixed but this were not, peers
  would attempt to dial `0.0.0.0:9100` — either self-dial or connection
  refused. On Kubernetes, the fix is to derive
  `${HOSTNAME}.merutable-hl.${POD_NAMESPACE}.svc.cluster.local:${PORT}`
  from the pod environment before constructing `NodeIdentity`.

**What's NOT actually broken:**
- The batched gRPC transport (`GrpcBatchTransport`, `PeerBus`) is wired
  into `Node::connect_peer` and fires at startup — the bus queues are
  real, just empty because no Raft group is configured with peer
  members.
- `RaftTransportService` is serving on `:9100` alongside `MeruCluster`
  (confirmed: integration tests succeed against this exact server).
- The io_uring log, O_DIRECT→O_DSYNC overlayfs fallback, and WAL
  rotation all work in-pod.

**Minimum fix to make the binary multi-node:**

1. `bin/merutable-node.rs:76` — resolve the pod's routable address
   (e.g., read `HOSTNAME` + a new `CLUSTER_DOMAIN` env var) instead of
   `0.0.0.0`.
2. `harness/node.rs` — expose a mode flag on `Node::new` that
   initializes the metadata group with `create_with_network` and all
   seed-peer `node_id`s as voters (current lone voter → N voters).
3. `rpc/server.rs:223` — route `CreateTable` through
   `node.create_table_multi_node` instead of `create_table`. The
   multi-node method already handles "I'm not a replica for this table"
   (opens an uninitialized group) and "I'm not the bootstrapper" (skip
   `initialize`).
4. Integration-test-equivalent smoke: run the existing
   `multi_node_replication` test pattern against the 3-pod StatefulSet
   and assert `ListTables` returns the same set on all three pods.

Keep the single-node path as a constructor (unit-test convenience),
but make the gRPC-facing `Node` default to multi-node.

### 13.3.1 Fix-attempt status (live 3-pod StatefulSet, kind-merutable-lab)

Attempted all three divergence fixes and validated against the running
cluster. **Two of three work as intended; the third exposed a deeper
design bug that a bandaid cannot close.**

| # | Fix | Status | Evidence |
|---|---|---|---|
| 1 | `create_table` auto-delegates to `create_table_multi_node` when `peer_buses` non-empty (`harness/node.rs:330-333`) | **Plumbed, not yet exercised end-to-end** — no `CreateTable` issued on the live cluster because startup currently blocks on fix #4 | Gate is correct; `connect_peer` succeeds so `peer_buses` is populated |
| 2 | Advertise address via `ADVERTISE_ADDR` or `HOSTNAME.CLUSTER_DOMAIN` fallback (`bin/merutable-node.rs:79-89`) + `CLUSTER_DOMAIN` env added to `deploy/k8s/statefulset.yaml` | **Working** | Startup log: `merutable-0.merutable-hl.merutable.svc.cluster.local:9100` |
| 3 | Swallow `NotAllowed` on `raft.initialize()` so pod restart doesn't panic on a non-empty Raft log disk (`metadata/raft_group.rs:270-277`, `raft/group.rs:603-609`) | **Working** | Fresh 3/3 Running; no panic on cold boot |
| 4 | `Node::upgrade_metadata_to_multi_node` — call after startup to `add_learner(peer)` + `change_membership({all})` (`harness/node.rs:264-298`, `bin/merutable-node.rs:179-189`) | **BROKEN — deadlocks** | No pod ever prints "metadata group upgraded"; `add_learner` hangs |

**Why #4 deadlocks (the real Div 2 bug):**

Every pod's `Node::new` runs
`MetadataRaftGroup::create_and_initialize(members={self})`. With three
pods, this produces **three independent single-voter Raft clusters**,
each with its own leader, term, and committed vote. When any one pod
then calls `add_learner(peer_id, blocking=true)`:

- The local pod (already leader of cluster A) dispatches
  `AppendEntries` + `InstallSnapshot` to the peer over
  `RaftTransportService`.
- The peer's Raft state machine (already leader of cluster B, term 1,
  voted_for=self) sees an `AppendEntries` from a different leader with
  incompatible state. openraft rejects the messages (higher local term,
  or log-compatibility check fails).
- `add_learner` retries indefinitely with `blocking=true`, which is the
  observed hang.

This is the classic "all-nodes-bootstrap" race. Adding `upgrade_*`
after-the-fact **cannot** fix it — the disks are already forked. The
solution is to not fork them in the first place.

**Correct design for Div 2 (replaces the current `upgrade_*` approach):**

1. **Deterministic bootstrapper.** Only the pod with the lowest
   `node_id` among its seed list (including self) calls
   `MetadataRaftGroup::create_and_initialize(members={all_node_ids})`.
   Every other pod calls `MetadataRaftGroup::create_with_network(...)`
   (uninitialized — the method already exists, see
   `metadata/raft_group.rs:279`).
2. **Non-bootstrappers wait for AppendEntries.** Uninitialized Raft
   groups sit idle until the bootstrapper's `change_membership`
   includes them and sends a first `AppendEntries`, at which point they
   become voters in the same cluster.
3. **Bootstrapper's `create_and_initialize` takes
   `members={all_node_ids}` directly** — no separate `add_learner` +
   `change_membership` step, because openraft's `initialize` accepts a
   multi-member set.
4. **Pre-flight peer reachability check.** Before initializing with
   all peers, dial each to confirm the `RaftTransportService` is
   listening. Otherwise openraft initialize succeeds but the first
   heartbeat fails and the leader steps down.
5. **Retry on transient startup skew.** Pods boot in sequence (OrderedReady).
   The bootstrapper may have to back off if peers aren't reachable yet.

Sketch (`harness/node.rs`):

```rust
pub async fn new_multi_node(config: ClusterConfig, all_node_ids: Vec<NodeId>) -> Self {
    let my_id = config.this_node.node_id;
    let bootstrapper = *all_node_ids.iter().min().unwrap();

    let metadata_group = if my_id == bootstrapper {
        // Wait for all peers to be reachable, then initialize with full membership.
        wait_for_peers_reachable(&config.peers).await;
        let members: BTreeSet<u64> = all_node_ids.iter().copied().collect();
        MetadataRaftGroup::create_and_initialize(my_id, members, ...).await
    } else {
        // Idle until bootstrapper adds us.
        let peer_channels = build_peer_channels(&config.peers).await;
        MetadataRaftGroup::create_with_network(my_id, ..., peer_channels).await
    };
    ...
}
```

**Why the current `upgrade_metadata_to_multi_node` method should be
deleted, not fixed:** it is structurally unable to converge three
forked clusters. Retaining it risks future callers assuming it works.

**Current state:** `docs/PHASE1_RAFT_CLUSTER.md` and code diverge on
the metadata-group bootstrap story. Fixes 1–3 (above) are landed and
verified; fix 4 is landed but non-functional. Until the correct Div 2
design is implemented, the 3-pod StatefulSet runs three independent
single-voter clusters for both metadata and table groups, and table
replication remains limited to whichever node handles the `CreateTable`
RPC — the same end-state as before fix 1 was introduced, because fix 1
needs a multi-voter metadata group (Div 2) to be useful.

### 13.3.2 Redesign landed — metadata replication works end-to-end

The correct Div 2 redesign (§13.3.1 "Correct design") has been
implemented and verified on the live 3-pod `StatefulSet`:

| Piece | Where | What |
|---|---|---|
| `MetadataRaftGroup::create_with_dynamic_network` | `metadata/raft_group.rs:337` | Uninitialized multi-node constructor using `DynamicBatchedNetworkFactory` |
| `Node::new_multi_node` | `harness/node.rs:270` | Builds metadata group via dynamic-network path (no `initialize`) |
| `Node::initialize_metadata` | `harness/node.rs:322` | Called by bootstrapper only; swallows `NotAllowed` on restart (case-insensitive) |
| Binary: parallel startup + peer retry + leader wait | `bin/merutable-node.rs:108-220` | Parses `all_node_ids`, picks lowest as bootstrapper; retries `connect_peer` with 1s/30-attempt backoff; waits up to 10s for metadata leader election before `register_self` |
| StatefulSet `podManagementPolicy: Parallel` | `deploy/k8s/statefulset.yaml:11-16` | Pods start concurrently so the bootstrapper can reach peers during init |

**Verified behaviors (kind-merutable-lab, 2026-04-27):**

- 3/3 pods reach `ready` on a cold boot with zero restarts.
- Bootstrapper log: `metadata group initialized with 3 members` →
  `metadata leader elected: node 1` → `registered self` + all peers.
- Non-bootstrappers log: `waiting for bootstrapper (node 1) to
  initialize metadata group` → `ready` (no panics, no forward-to-leader
  errors).
- **Metadata replication genuinely works**: `CreateTable smoke_events`
  issued against merutable-0 is visible via `ListTables` on merutable-1
  *and* merutable-2. `/data/raft/__metadata__/raft.log` exists on all
  three pods.

**Table-group replication (Div 1) — also resolved:**

The missing piece was that non-handler replicas had no way to open
their local table Raft group when a `CreateTable` RPC hit a peer.
Added `Node::start_table_opener` (`harness/node.rs` — watches the
metadata broadcast, opens a local table group via
`open_table_group_from_event` whenever `MetadataEvent::TableCreated`
includes this node in `replicas`). Wired into `bin/merutable-node.rs`
alongside `start_rebalance_watcher`.

**Verified behavior on the live cluster (2026-04-27):**

- `CreateTable smoke_events` against merutable-0 propagates via
  metadata Raft. merutable-1 and merutable-2's table-opener fires,
  opens local `smoke_events` groups with batched transport, and joins
  the same Raft membership. `/data/raft/smoke_events/raft.log` exists
  on **all three pods**, identical size (12,288 bytes after the first
  Put).
- `Put id=1, payload="hello"` against merutable-0 succeeds on first
  attempt. The write is replicated through Raft to all three replicas.
- `Get id=1` against **merutable-2** returns `found: true` with the
  replicated payload — data is genuinely readable from a different
  pod than the one that accepted the write.

**Net:** Div 1 + Div 2 + Div 3 all resolved on the binary path. GAP-1,
GAP-11, and GAP-19 can all be retitled **RESOLVED (library + binary
path)** — a `StatefulSet` of three `merutable-node` pods now runs a
single logical cluster with full metadata + table replication, not
three independent single-voter Rafts.

---

## 14. Dependencies

```toml
[dependencies]
merutable = { path = "../merutable", features = ["sql", "replica"] }
openraft = { version = "0.9", features = ["serde"] }   # 0.9 stable, not 0.10 alpha
tonic = { version = "0.12" }
dashmap = { version = "6" }
crc32fast = { workspace = true }
# all other deps from workspace: tokio, futures, async-trait, bytes,
# prost, tracing, serde, postcard, metrics, arc-swap, lru, xxhash-rust, thiserror

[target.'cfg(target_os = "linux")'.dependencies]
io-uring = { version = "0.7" }  # macOS fallback uses std::fs

[build-dependencies]
tonic-build = { version = "0.12" }
prost-build = { workspace = true }
```

`merutable` crate: **zero changes, zero new dependencies.**

---

## 15. Open Design Questions

The following are unresolved at the architectural level. Each must be settled before implementation. They are *not* pseudocode-level issues; cleaning up the snippets in §3-§11 does not answer any of them.

### Critical -- correctness

**Q1. Bootstrap leader for new Raft groups.** When `CreateTable` commits and three ring-selected replicas all see it via the watch stream simultaneously, who runs `openraft::Raft::initialize` and who joins as voters via `change_membership`? openraft does not allow three independent bootstraps to merge.
- *Proposal:* the metadata Raft group commits a `BootstrapTable { table, bootstrapper }` event picking the lexicographically smallest `node_id` of the replica set; only that node initializes; it then issues `add_learner` + `change_membership` to bring the other two in.
- *Status:* Decision locked. **Single-node initialize is implemented** (`RaftGroup::create_and_initialize` at `crates/merutable-cluster/src/raft/group.rs:472-484`). Multi-node `BootstrapTable` event + `add_learner`/`change_membership` orchestration is still pending — tracked as GAP-1 and GAP-11.

**Q2. Membership change orchestration during ring rebalance.** Adding a node to AZ-1 shifts ~1/3 of AZ-1 tables (~1,600 at the 5,000-table scale). Each table requires a serial `add_learner` -> catch-up -> `change_membership` cycle, with leadership transfer first if the *outgoing* replica is the current leader. The doc gives no rate limiter, no priority order, no progress observability.
- *Open:* concurrency cap (how many groups rebalance at once?), retry policy, behavior when a second node-add lands mid-rebalance, definition of "rebalance complete".
- *Status:* Decision tentative (concurrency cap = 10 from §13.1). **No implementation** — tracked as GAP-12 (`on_ring_change` orchestrator).

**Q3. Snapshot transfer payload.** `Snapshot { data: catalog_bytes }` only ships the manifest; Parquet files are referenced by URL. Works only if the cluster is backed by a shared object store every node can read. With local-only storage, snapshot install on a fresh replica is broken (catalog references files that don't exist locally).
- *Decision (locked):* shared object store is mandatory in Phase 1. Manifest-only snapshots.
- *Status:* No implementation owner yet — manifest serialization for `build_snapshot()` and object-store reads inside `install_snapshot()` are not specified in any phase or gap. `TableStateMachine::build_snapshot` currently dumps the local catalog directory; `install_snapshot` writes it back without consulting an object store.

**Q4. `applied_index` durability.** Both `committed_index` and `applied_index` are in-memory `AtomicU64`. On restart they reset to zero, openraft re-drives `apply()` for every entry since the last snapshot, and MeruDB double-applies through its full WAL+memtable path. Survivable for Phase 1's idempotent `Put`/`Delete` but expensive (memtable bloat, slow restart) and blocks any future non-idempotent commands.
- *Decision (locked):* persist `applied_index` in a small sidecar file fsynced after each apply batch.
- **RESOLVED:** Implemented in `TableStateMachine` (`crates/merutable-cluster/src/table/state_machine.rs:98-145, 187, 458`). Sidecar file `{catalog_dir}/applied_index.sidecar` is read on `open()` to seed `applied_index`, written + fsynced inline after each leader apply, and written + fsynced from `apply_worker` after each follower batch (GAP-13 fix). Verified by `sidecar_persists_applied_index` and `follower_sidecar_reflects_actual_apply` unit tests.

**Q5. Snapshot install must clean the WAL directory.** `MeruDB::open` automatically replays everything in `OpenOptions::wal_dir` (see `crates/merutable/src/engine/engine.rs:216-258`). After `close()` -> overwrite catalog -> `open()` the stale WAL replays on top of the snapshot, silently corrupting state. This is a cluster-crate responsibility, not a `merutable` change.
- *Decision:* `install_snapshot` truncates `wal_dir` atomically before reopen.
- **RESOLVED:** Implemented in `TableStateMachine::install_snapshot` (`crates/merutable-cluster/src/table/state_machine.rs:338-396`) — `tokio::fs::remove_dir_all(&self.wal_dir)` is invoked before `MeruDB::open` runs.

### Significant -- operational risk

**Q6. RF is silently locked to AZ count.** `replicas_for_table` returns one node per AZ ring, so RF = number of AZs.
- **RESOLVED:** Phase 1 requires exactly 3 AZs. RF=3. Hard constraint. Generalizing RF is Phase 2+.

**Q7. Transient vs permanent failure transition.** §4.3 says transient failures don't change the ring; §4.4 says `DeregisterNode` rebuilds it. Who calls `DeregisterNode`, and on what trigger?
- *Open:* operator-driven, time-based (auto-deregister after N hours unreachable), or health-probe-driven? Grace period? Behavior during the window when one replica is down but not yet deregistered (cluster runs at 2/3 quorum -- one more failure loses the table).
- **RESOLVED:** Phase 1 is operator-only `DeregisterNode`. No auto-deregister logic in `Node` or `MetadataStateMachine`. Auto-deregister is deferred to Phase 2.

**Q8. Promotion drain time bound.** §5.3 and §10 quote ~50ms drain. That holds only at low load. Under sustained 10k writes/sec on a hot table, the follower's async apply backlog is bounded by MeruDB `put` throughput (~5,000/sec), so the steady-state backlog is ~1 second of writes. After leader crash, the new leader's drain is ~1 second, not 50ms.
- *Open:* cap the apply queue depth (back-pressure into Raft, slowing accepting writes), or document the realistic upper bound.
- **RESOLVED:** Apply queue depth is capped at `FOLLOWER_APPLY_QUEUE_CAP = 10_000` (`crates/merutable-cluster/src/table/state_machine.rs:21`). Beyond that, the bounded `mpsc` channel back-pressures into the Raft apply path and slows acceptance of new commits. Realistic drain bound = 10,000 / MeruDB put throughput.

### Performance claims that need reconciling

**Q9. Bus batching adds latency on the data path.** §6.2 says "flush every 100ms or 64 messages" and applies to all messages. With a 100ms periodic flush, a data write can wait up to 100ms for the AppendEntries batch to leave (and another 100ms for the response on the slow path), on top of network RTT. The §8 latency budget of 4.2ms assumes immediate flush; the 64-message high-water trigger is the only way real-world traffic stays under it. Vote messages already trigger immediate flush (`raft/network.rs:115` — `is_vote || q.len() >= BATCH_HIGH_WATER`).
- *Decision needed:* either differentiate data-bearing AppendEntries (flush immediately) from heartbeats (batch), or update §8 latency to reflect the worst-case 100ms wait.

**Q10. "20 RPCs/sec at 5,000 tables" — reconcile with the actual 100ms flush.** With a 100ms flush interval and ~10 distinct peers per node, the bus generates ~100 RPCs/sec at idle (one batch per peer per flush), much closer to the §6.3 "20 RPCs/sec" figure than the original 5ms claim suggested. The 20-RPC figure assumes the high-water flush dominates and that a node only talks to a small set of peers per heartbeat cycle.
- *Status:* Flush interval is documented (100ms in §6.2 and `FLUSH_INTERVAL` in `raft/network.rs:42`). The exact RPC math at scale is a perf-tuning question, not a correctness blocker.

**Q11. I/O contention between Raft log and MeruDB WAL.** Both write to the same NVMe device (Raft via `io_uring + O_DIRECT`, MeruDB WAL via buffered `write` + `fdatasync`). The 10-50us figure assumes isolated I/O. Under sustained load with ~1,250 tables per node, both compete for the same device queue. Realistic worst-case is closer to 200-500us per Raft write.
- *Open:* model contention; lower the latency target or constrain the per-node table count.

### Operational ambiguities

**Q12. First-call client routing.** With the per-AZ ring, a fresh client computes 3 replicas but has no way to know which is the leader. The current `replicas_for_table` returns a non-deterministically-ordered `Vec<NodeId>` (`HashMap::values()`), so "send to replicas[0]" picks differently on different runs.
- *Decision needed:* always route to the closest-AZ replica (followers proxy on miss), use cached `ResponseMeta` from prior responses, or randomize. Document the policy.
- **RESOLVED:** `PerAzRings::replicas_for_table` returns the AZ-rotated replica list with `replicas[0]` deterministically equal to `leader_for_table` (`crates/merutable-cluster/src/ring/per_az.rs`). `MeruClient::write_target` (`rpc/client.rs:335`) sends writes to `replicas[0]` first; on `NotLeader` it falls back to the rest of the replica set and caches the leader override (`rpc/client.rs:359-376`). Verified by `client_failover_to_replica` and the ring distribution unit tests.

**Q13. Memory math: per-node vs cluster-wide.** §12 says "5,000 x 4 MB = 20 GB" without saying which.
- *Clarification:* with 4 nodes per AZ x 3 AZs = 12 nodes, and ring distribution within each AZ, each node hosts ~1,250 tables steady-state -> 5 GB per node, 60 GB cluster aggregate at 3x replication. Operators provision per-node, so the per-node figure is the one that matters.

**Q14. WatchLeader fan-out scope.** §9 Layer 3 doesn't say whether `WatchLeader` is per-table or global. Per-table -> a multi-table client maintains 5,000 streams. Global -> every leadership change in the cluster fans out to every connected client.
- *Decision needed:* per-table with a subscribe/unsubscribe protocol, or a sharded global stream filtered by the client's table set.
- **RESOLVED:** Phase 1 ships `WatchLeader` as a single per-node global stream (one server-streaming RPC per connected client), not a per-table subscription. The handler in `rpc/server.rs:506-680` walks all known tables on every `RingChanged` event and emits one `LeaderEvent` per shifted table. The ring is the primary leader-finding mechanism (§9 Layers 1–2); `WatchLeader` is the optional Layer 3 push for proactive client cache updates. Client-side subscription is not yet wired — see GAP-9 client-side / GAP-18.
