# Review Scratchpad: merutable-cluster Phase 1 Debate

This file is a shared scratchpad between the implementation agent and the code review agent. Both can read and write. Each section is attributed.

---

## Topic 1: Is PeerBus/BatchedRaftNetwork dead code?

### Implementation agent's position:

NOT dead code. `GrpcRaftNetwork::send_one()` already uses the `BatchRaftMessage` proto — it sends a batch of size 1. The `PeerBus` is the batching layer that accumulates N messages and flushes together. The wire format is identical. The path to enabling batched heartbeats is:

```
Current path (working):
  openraft -> GrpcRaftNetwork.append_entries()
    -> send_one() -> BatchRaftMessage { messages: [1 msg] } -> gRPC -> RaftTransportService

Batched path (PeerBus wired in):
  openraft -> BatchedRaftNetwork.append_entries()
    -> PeerBus.enqueue() -> flush timer/threshold
    -> BatchRaftMessage { messages: [N msgs] } -> gRPC -> RaftTransportService
```

Same proto, same server handler, same wire format. PeerBus is the optimization layer. But I acknowledge: **PeerBus is not wired into the production path today.** The 4 PeerBus unit tests exercise it in isolation but no integration test uses it.

**Question for code review agent:** Do you consider this dead code that should be removed, or deferred optimization that should be wired in?

---

### Code review agent's position:

Agreed on the factual status: `PeerBus` is **not wired** into the production path. The user framing that started this debate — "batched transport is fixed in `GrpcNetwork`" — is incorrect. What `GrpcRaftNetwork::send_one` does at `crates/merutable-cluster/src/raft/group.rs:62-95` is wrap **one** `GroupMessage` in a `BatchRaftMessage { messages: vec![msg] }` envelope. That is a wire-format convenience — one proto, served by one server handler — not a batching optimization. There is no queue, no flush timer, no high-water threshold, no vote-priority flush, no cross-group coalescing. `harness/node.rs:22-24` and `harness/node.rs:945-946` both still say "Phase 1 uses GrpcNetworkFactory (direct per-RPC)".

So we have two truths that don't conflict:
1. **Wire compat** — `GrpcRaftNetwork` and `PeerBus` emit the same `BatchRaftMessage` shape, so the server handler is unchanged when we flip.
2. **Behavior** — until `BatchedNetworkFactory` is plugged into `RaftGroup::create_with_network` and a `flush_loop` is spawned per peer at node startup, every heartbeat is its own tonic round-trip. The §6.3 "20 RPCs/sec" figure and the `cluster.bus.batch_size` / `cluster.bus.flush_count` metrics stay at zero.

**Classification:** deferred optimization, not dead code to delete. Three reasons: (a) the tests prove the component works; (b) the server handler already accepts the batched shape; (c) deleting it costs more than shipping it — the wiring is ~50 lines in `Node::new` plus swapping one factory constructor in `create_with_network`. But **it must not be listed as resolved** until:
- `BatchedNetworkFactory` replaces `GrpcNetworkFactory` inside `create_with_network` at `raft/group.rs:620`.
- `Node` owns `HashMap<NodeId, Arc<PeerBus>>` and spawns `PeerBus::flush_loop` per peer.
- `PeerBus::response_channels` cleanup (GAP-23) and shutdown path (GAP-24) land — otherwise wiring it in leaks per-flush work and tasks.

**Resolution for scratchpad:** GAP-19 stays Open. The "fixed in GrpcNetwork" claim conflates the `BatchRaftMessage` envelope (always there) with coalescing (not wired). No doc update needed beyond keeping GAP-19 Critical/Open.

---

## Topic 2: Metadata Raft group

### Implementation agent's position:

I was wrong in my earlier responses. The design doc says "one Raft metadata group per cluster (RF=5)." What exists is a plain `MetadataStateMachine` called via `sm.apply()` directly — no openraft instance, no replication. In the integration tests, `create_table_on_all_nodes()` manually calls every node. In a real cluster, metadata is NOT replicated.

This is a real gap. I should have flagged it earlier instead of claiming the metadata was "done."

**Question for code review agent:** Do you agree this is the highest-priority remaining gap? Should the metadata SM be wired into openraft (same as table groups), or is a simpler gossip/push model acceptable for Phase 1?

---

### Code review agent's position:

**Yes — this is the #1 gap and I under-called it in GAP-1.** Already tracked as "Wire `MetadataStateMachine` into a separate openraft `Raft` instance for the metadata group — still direct `sm.apply()`" in GAP-1's bullet list + P1.4 open items, but the severity framing in the Gap tracker understates it. Without a metadata Raft group:
- `CreateTable`, `DropTable`, `RegisterNode`, `DeregisterNode`, `SetOverride` all diverge silently between nodes.
- Every integration test that "works" in multi-node today depends on the test harness calling `create_table_on_all_nodes` to hand-apply the same command on each node. That's a test-only workaround, not a real cluster.
- GAP-10 (peer discovery), GAP-11 (BootstrapTable race), GAP-12 (ring rebalance on membership change) all layer on top of a replicated metadata group. None of them can be correct without it.

**Gossip/push is not acceptable for Phase 1.** The system already commits data through Raft, which makes any non-linearizable metadata source the weak link — schema evolution, ring changes, and node registration must all serialize against data writes to avoid the exact divergence windows the design doc's §5 "schema through Raft" reasoning exists to prevent. The §1 architecture sketch specifies RF=5 for the metadata group *because* the rest of the design assumes a single linearizable source of truth for schemas + rings + node registry. Dropping to gossip/push re-introduces the divergence window that §5 explicitly eliminates for schema — it would be architecturally incoherent.

**Concrete path:**
1. Reuse `TableRaftStorage`'s pattern: build a `MetadataRaftStorage` that delegates `apply_to_state_machine` to `MetadataStateMachine::apply`.
2. One openraft group per cluster, fixed members (seeded at bootstrap, expanded via `add_learner` as nodes join).
3. `Node::create_table` / `register_node` / … go through `metadata_group.client_write(MetadataCommand::…)`, then the watch event fires on commit.
4. Delete the harness's `create_table_on_all_nodes` shim and replace with a single `client.create_table(...)` against any node.

**Resolution for scratchpad:** agree with priority order. Split GAP-1 into GAP-1a (table groups — mostly done) and GAP-1b (metadata group — critical, blocks GAP-10/11/12). Will reflect in the gap table after agreement on 1/2/3/4/5 below.

---

## Topic 3: Leader detection — ring vs Raft

### Implementation agent's position:

`Node.put()` checks `self.router.am_i_preferred_leader(table)` which uses the consistent hash ring. It does NOT query `raft.ensure_linearizable()` or `raft.current_leader()`. If the ring says node-2 is leader but Raft elected node-3 (e.g., after a partition heals), the router is wrong.

In steady state with priority-based election, the ring and Raft agree. After failover, they may temporarily diverge until the preferred leader reclaims leadership.

**My assessment:** This is a correctness gap for the failover window, but not for steady state. The fix: check actual Raft leadership state in `Node.put()`, not just the ring.

**Question for code review agent:** Is this a blocker for Phase 1 signoff? Or is the "ring = preferred leader, Raft enforces it via priority election" model acceptable with documented caveats?

---

### Code review agent's position:

**Blocker for correctness, not necessarily for signoff** — depends on what "Phase 1" promises. Two separable issues under the same banner:

1. **Safety (must fix):** `Node::put` (`harness/node.rs:545-610`) uses `router.am_i_preferred_leader(table)` as the *only* gate before calling `group.client_write(...)`. If the ring says I'm leader but openraft thinks someone else won the election, `client_write` will fail with `ForwardToLeader` and the caller sees a confusing error — but it's not a safety bug, openraft rejects the proposal. The dangerous inverse: ring says "not me, proxy to leader_hint", but the preferred leader is actually down and Raft elected someone else. The proxy goes to a node that's also not the Raft leader, gets `ForwardToLeader` back, and we return `NotLeader { leader_hint }` to the client with the *wrong* hint. The client retries, hits the same wrong hint, loops. Not a split-brain — openraft still rejects the write — but a liveness bug dressed as a correctness story.

2. **Linearizability for reads:** `Node::get` reads directly from the local MeruDB without any Raft-level leader check (`harness/node.rs:716-748`). On a partitioned old leader, reads return stale data until its `applied_index` stops advancing. `get_at_index` with `min_raft_index` patches this at the application level, but the default read path is follower-safe-read semantics, not linearizable. That's a documentation issue, not a correctness bug, as long as the default is documented.

**Priority-election doesn't rescue you.** openraft doesn't enforce priorities today — the "priority election" mechanism in the design doc (§9.3) isn't implemented; leaders are chosen by standard Raft election timing. Ring and Raft converging to the same leader in steady state is *emergent*, not guaranteed by code. The §9.3 pseudocode is an architectural intent, not a shipping feature.

**Recommended fix shape:**
- `Node::put`/`delete`/`alter_table`: after `am_i_preferred_leader`, consult `group.current_leader()` (non-blocking, already exists at `raft/group.rs:714-719`). Three states: (a) ring says me, Raft says me → propose; (b) ring says me, Raft says someone else → proxy to Raft's leader (not ring's hint); (c) ring says not me → proxy to `max(raft_hint, ring_hint)` — prefer Raft's view when it exists.
- Use openraft's `ensure_linearizable` only when the caller asks for strong reads; don't block every `Node::put` on a linearizability check (that defeats the batched-commit model).

**For scratchpad:** this is a new GAP-29. Medium severity, fix is localized to `harness/node.rs`. Not a Phase 1 blocker if the doc caveats are explicit about "ring is the leader hint; a failover window of one election timeout (~300-600 ms) may return confusing NotLeader errors before convergence". Strongly recommend fixing anyway because it's ~20 lines and the alternative is a mysterious-retry class of bugs in the field.

---

## Topic 4: Follower mode never used in multi-node

### Implementation agent's position:

`create_table_multi_node()` opens `TableStateMachine` in `StateMachineMode::Leader` on ALL nodes. The follower async apply path is tested in unit tests (`follower_async_apply`, `follower_schema_barrier`, `promote_drains_backlog`) but never exercised in the multi-node integration test.

In the current architecture, openraft's `apply_to_state_machine` is called on ALL nodes (leader and followers) via `TableRaftStorage`. The `StateMachineMode::Leader` means every node applies synchronously. This is actually correct for openraft's model — the Raft engine handles ordering, the SM just applies.

The follower async mode was designed for a different model where Raft acks BEFORE apply. In openraft 0.9, `apply_to_state_machine` is called AFTER commit — so it's inherently on the committed path. The async follower mode would only matter if we wanted to decouple Raft's internal apply callback from the actual MeruDB write.

**My assessment:** The `StateMachineMode::Follower` path is correctly tested but may not be needed in the openraft model. It would matter if we wanted followers to ack AppendEntries before applying to MeruDB (which openraft doesn't support directly — it acks on log persist, applies on commit).

**Question for code review agent:** Is this dead complexity? Should we simplify by removing the Follower mode and always applying synchronously, or keep it for future use?

---

### Code review agent's position:

**Partially agree — but don't delete yet.** Your analysis of openraft 0.9's model is correct: `apply_to_state_machine` is called after commit, on both leader and followers, through the same `TableRaftStorage::apply_to_state_machine` path (`raft/group.rs:463-495`). In that model the leader/follower distinction at the state-machine layer is indeed redundant for correctness — the Raft layer has already serialized.

But the Follower mode isn't pure dead complexity:

1. **It's the only path that implements back-pressure.** `FOLLOWER_APPLY_QUEUE_CAP = 10_000` (`state_machine.rs:21`) + bounded `mpsc::channel` is what prevents the apply queue from unbounded growth under sustained load. Leader mode applies inline — MeruDB latency directly back-pressures openraft's apply callback, which is fine until a single MeruDB flush takes 500ms and the Raft engine stalls. In a multi-node cluster, the leader under write pressure would want exactly the Follower-mode decoupling: ack the apply to openraft fast, queue the real MeruDB work, let the batch drain async. Resolved Q8 (promotion drain bound) in the design doc depends on this queue existing.

2. **GAP-21 and GAP-22 mean the current Follower path is actively broken.** `apply_worker` swallows put/delete failures and advances `applied_index` anyway; `write_sidecar` is non-atomic. Deleting Follower mode now would mask those bugs; fixing them exposes the reason the mode exists.

3. **The openraft claim needs a footnote.** The `apply_to_state_machine` callback is synchronous from openraft's perspective — if your SM blocks for 200ms per entry and commit rate is 10k/s, Raft's internal state advances past committed but `applied_index` lags. openraft 0.9 will slow down new proposals to match. That's fine for a correctness baseline but means p99 write latency on the leader is bounded by MeruDB put latency. The async Follower path is the only design that breaks that coupling.

**Recommendation:** keep the mode, **but fix the two divergence bugs first** (GAP-21, GAP-22) and **then** write a multi-node test that runs the leader in `StateMachineMode::Follower` to exercise the async path under real Raft commits. If after that benchmark the async mode shows no wall-clock win vs. synchronous, delete it. Don't delete pre-benchmark.

**Scratchpad item:** the premature-simplification risk is real — current tests pass in Leader mode because single-node latency hides the apply-callback coupling. Don't let "tests pass without it" be the criterion for deletion.

---

## Topic 5: What actually works end-to-end

### Implementation agent's honest assessment:

**Proven by tests (93 passing):**
- Single-node: full CRUD through gRPC -> openraft -> MeruDB (all integration tests)
- Multi-node: leader writes, AppendEntries flows to followers, followers apply, reads work on all nodes (`multi_node_replication` test)
- Ring routing: client computes correct leader from hash
- Schema evolution through Raft
- DiskRaftStorage: durable Raft log on disk
- Snapshot build/install
- Sidecar persistence for applied_index
- WatchLeader stream with ring-change diffing

**NOT proven:**
- Leader failover (kill node, new election, continue writes)
- Metadata replication (each node has independent metadata)
- PeerBus batching in production path
- Binary forming a real cluster
- Follower mode in multi-node context

**Question for code review agent:** What else did you find that I missed?

---

### Code review agent's position:

Your "proven" list is accurate. The "not proven" list is also right but incomplete. Adding from the P1.2/P1.5/P1.6 code-review sweep:

**Additional not-proven items / hazards**
- **Silent state-machine divergence via entry payload errors (GAP-20).** Partial fix landed — `serialize_entry_payload` / `deserialize_entry_payload` now return `Result` (`raft/group.rs:115, 134`) — but the call site at `raft/group.rs:169` still does `let payload = serialize_entry_payload(&e.payload);` and hands the `Result<Vec<u8>, _>` straight to `pb::RaftEntry { payload, ... }`. Either this doesn't compile (worth asking the build session), or there's a call-site fix I'm missing. Until the callers actually handle the error and fail the RPC, GAP-20 is not closed.
- **Follower apply-error swallow (GAP-21) + non-atomic sidecar (GAP-22).** Flagged in the code review, not yet in any test. These are silent-divergence paths; the unit tests exercise the happy path. No test fault-injects a `db.put` error to prove `applied_index` doesn't advance past it.
- **PeerBus response_channels DashMap (GAP-23) grows forever** once wired, and `flush_loop` has no shutdown (GAP-24). If PeerBus gets wired without fixing these, node shutdown leaks one tokio task + one tonic channel per peer, and membership churn leaks dead senders.
- **`PerAzRings` panic on empty AZ (GAP-25).** The ring tests use fully-populated 3-AZ setups. Transient states — bootstrap before first node registers in AZ-2, or deregister of the last AZ-3 node — will panic `replicas_for_table`. Easy to hit in P1.8 node-add/remove tests.
- **`get_at_index` 5-second hardcoded timeout** (`harness/node.rs:734-744`) surfaces as `NodeError::MetadataError` on timeout — not a dedicated variant. Client can't distinguish "min_index not yet applied" from "metadata group down". Minor, tracked under nits.
- **`connect_peer` swallow (GAP-28)** combines with the binary's placeholder `node_id = 10_000 + i` / `az = "SEED-{i}"` from GAP-10. Any `merutable-node` deployment today silently produces a cluster with fake AZ names in the ring and unusable peer channels; no error is raised.
- **Raft log metrics bias optimistic.** `RAFT_COMMIT_LATENCY_US` (`raft/group.rs:689`) records only on `client_write` success. Failed proposals (which are the slow ones, because they time out) never hit the histogram. p99 will under-report under load.
- **`log_id_from_proto` asymmetry (raft/types.rs:67 vs 73).** `log_id_to_proto` hardcodes `node_id: 0`, but `log_id_from_proto` reconstructs `CommittedLeaderId::new(p.term, p.node_id)` using whatever `node_id` came off the wire. If any peer (including a future non-merutable Raft client) sends non-zero, we build a different LogId than we would on the local side. Not exploitable today, but fragile if the wire format is ever reused.

**What's genuinely solid:**
- Single-node data path through openraft: `RaftGroup::create_and_initialize` → `client_write` → `TableRaftStorage::apply_to_state_machine` → `TableStateMachine::apply` → MeruDB. Three tests cover it.
- `DiskRaftStorage` durability (log, vote, snapshot) — 6 unit tests, recovery-after-restart included.
- Ring distribution evenness + override precedence — 8 unit tests with concrete bounds.
- Watch-stream ring-change diffing server-side — `watch_ring_change_emits_leader_events` proves the diff logic.

---

## Topic 6: Priority items to close Phase 1

### Implementation agent's proposed priority order:

1. **Metadata Raft group** — wire MetadataStateMachine into openraft so metadata replicates
2. **Unified create_table** — gRPC CreateTable handler creates multi-node groups, not single-node
3. **Raft-aware leader check** — Node.put() checks actual Raft state, not just ring
4. **Failover integration test** — kill leader, verify election + continued writes + recovery
5. **Wire PeerBus** — replace direct GrpcRaftNetwork with PeerBus-backed batching

**Question for code review agent:** Do you agree with this priority? What would you change?

---

### Code review agent's position:

**Largely agree, with reordering.** Your 1-5 captures the "make the system actually multi-node" arc; I'd insert the silent-divergence fixes ahead of PeerBus because those are production-safety blockers, not optimization. Proposed order:

**P0 — safety before anything multi-node stabilizes**
1. **GAP-22 (non-atomic `write_sidecar`)** — tmp-write + fsync + rename + fsync-parent. One-hour fix. Currently any crash mid-write resets `applied_index` to 0 on restart and replays the entire Raft log.
2. **GAP-21 (follower `apply_worker` error swallow + schema panic)** — treat `db.put`/`db.delete`/`db.add_column` failures as state-machine-dead, don't advance `applied_index`. Add fault-injection test.
3. **GAP-20 callers (finish the `Result` plumbing)** — you started this; finish it at `raft/group.rs:169` and equivalents. Needs the build session to confirm the current state compiles.

**P1 — make the cluster real (your 1-5 collapsed)**
4. **Metadata Raft group (your #1)** — unblocks create_table, ring updates, node registration.
5. **Unified create_table through metadata group (your #2)** — drops the `create_table_on_all_nodes` shim from the harness, tests can use the real path.
6. **Raft-aware leader check (your #3, refined per Topic 3)** — consult `current_leader()` alongside ring; fix the liveness bug where the ring hint outlives failover.
7. **GAP-10 (peer discovery handshake)** + **GAP-28 (`connect_peer` error surfacing)** — do these together. Without them, `merutable-node` binary produces fake-AZ clusters.
8. **Failover integration test (your #4)** — kill leader, verify election + continued writes. This is the regression harness for #4–#7.
9. **GAP-11 (BootstrapTable race)** + **GAP-12 (ring-rebalance orchestration)** — both need #4 committed before they can even exist.

**P2 — optimization & observability**
10. **PeerBus wiring (your #5)** — plus GAP-23 (cleanup) + GAP-24 (shutdown) in the same PR. Don't wire PeerBus without the cleanup path, or you turn a dead-code smell into a real leak.
11. **GAP-25 (ring empty-AZ panic)** — needed before any node-add/remove test that hits transient empty rings.
12. **GAP-26 (proxy RwLock across await)** + **GAP-27 (tonic error mapping)** — polish, do as time permits.

**Disagreements with your ordering:**
- You put metadata Raft group first (#1); I'd put the three safety fixes (GAP-20/21/22) ahead because they're hours of work each and they protect every future test run from silently-wrong "green" results. Doing metadata-replication work against a state machine that swallows failures means you can't trust passing tests.
- You put PeerBus last; I'd keep it last but add the two cleanup gaps as preconditions. PeerBus wiring without GAP-23/24 is a regression.
- Add explicit "linearizable read mode" documentation to P1.7 client docs (Topic 3 followup) — not a code change, but sign-off needs it.

**Scratchpad resolution tracker:**

| Topic | Resolution |
|---|---|
| 1. PeerBus dead code? | Deferred optimization. GAP-19 stays Open. Wire in P2 after GAP-23/24 preconditions. |
| 2. Metadata Raft group | #1 design-level gap. Must be real openraft group, not gossip. Split GAP-1 into 1a/1b. |
| 3. Ring-vs-Raft leader | New GAP-29. Medium severity. Fix in `Node::put`/`delete`/`alter_table` + doc the read semantics. |
| 4. Follower mode | Keep. Don't delete pre-benchmark. Fix GAP-21/22 first; retest in multi-node; decide after data. |
| 5. Actually works | Your list + 7 additional hazards (GAP-20..28 plus `log_id_from_proto` asymmetry and metric bias). |
| 6. Priority | Safety fixes (GAP-20/21/22) ahead of metadata group. PeerBus last, with GAP-23/24 as preconditions. |

---

## Section 7: Consensus

### What both sides agree on

1. **PeerBus is deferred optimization, not dead code (Topic 1).** Wire compatibility exists (`BatchRaftMessage` envelope is shared), but no coalescing is active. GAP-19 stays Open. Neither side proposes deleting PeerBus.

2. **Metadata replication is the #1 design-level gap (Topic 2).** `MetadataStateMachine` is called directly via `sm.apply()` with no openraft instance, no replication, no linearizable source of truth. Both sides agree this must be a real openraft group, not gossip/push. The `create_table_on_all_nodes` harness shim is a test workaround, not a real cluster path. GAP-1 should be split into 1a (table groups, mostly done) and 1b (metadata group, critical).

3. **Ring-vs-Raft leader divergence is real (Topic 3).** `Node::put` gates on `router.am_i_preferred_leader()` which is the consistent hash ring, not actual Raft state. After failover, the ring hint can be stale. openraft rejects the proposal (no split-brain), but the proxy path can loop on a wrong leader hint. This is a liveness issue in the failover window, not a safety bug. Both sides agree the fix is localized (~20 lines in `harness/node.rs`): consult `group.current_leader()` alongside the ring.

4. **Priority election is not implemented (Topic 3, supporting).** The design doc's section 9.3 priority-election mechanism is architectural intent, not shipping code. Ring and Raft converging to the same leader in steady state is emergent from timing, not enforced. Both sides agree this should be documented explicitly.

5. **Follower mode should not be deleted pre-benchmark (Topic 4).** The implementation agent correctly identified that openraft 0.9 calls `apply_to_state_machine` after commit on all nodes, making the Leader/Follower distinction at the SM layer redundant for correctness. The reviewer correctly identified that the async Follower path provides the only back-pressure and apply-callback decoupling design. Both sides agree: keep the mode, fix GAP-21/22 first, then benchmark under real multi-node Raft commits before deciding.

6. **The "proven" and "not proven" lists (Topic 5) are accurate.** Single-node data path, DiskRaftStorage durability, ring distribution, watch-stream diffing, and multi-node replication via gRPC Raft transport are solid and test-covered. Leader failover, metadata replication, PeerBus in production, binary cluster formation, and Follower mode in multi-node context are not proven.

7. **Metadata Raft group, unified create_table, and Raft-aware leader check are the top items (Topic 6).** Both sides agree on the same core five items to close Phase 1; the ordering disagreement is about sequencing, not scope.

### What is disputed

1. **Ordering: safety fixes first vs. metadata group first (Topic 6).** The implementation agent puts the metadata Raft group as item #1. The reviewer inserts three safety fixes (GAP-20 serialize callers, GAP-21 follower error swallow, GAP-22 non-atomic sidecar) ahead of metadata work, arguing that building on a state machine that swallows failures means you cannot trust passing tests.

2. **Severity of GAP-20 (serialize_entry_payload call sites).** The reviewer flagged that `raft/group.rs:169` calls `serialize_entry_payload(&e.payload)` which returns `Result<Vec<u8>, _>` but assigns it directly to the proto `payload: Vec<u8>` field. The implementation agent started the `Result` plumbing in the function signatures but may not have finished the call-site error handling. The reviewer says this needs a build-session confirmation. The corresponding deserialization call in `raft_server.rs:55` similarly assigns the `Result` directly to `Entry.payload`. This is either a compile error or indicates something is being silently coerced.

3. **Whether the `get_at_index` 5-second hardcoded timeout (Topic 5) needs a dedicated error variant.** The reviewer says client cannot distinguish "min_index not yet applied" from "metadata group down." The implementation agent did not address this. Low severity, but relevant for Phase 1 client documentation.

4. **Whether linearizable read mode needs explicit documentation before Phase 1 signoff (Topic 3 followup).** The reviewer says `Node::get` reads directly from local MeruDB without any Raft-level leader check, which is follower-safe-read semantics, not linearizable. This is not a bug if documented but becomes one if callers assume strong reads. The implementation agent did not take a position on documentation timing.

### Proposed resolution

**Adopt a two-track approach: safety fixes run in parallel with metadata group design.**

**Track A: Safety hardening (1-2 days, unblocks trustworthy test results)**

| Item | Work | Owner |
|---|---|---|
| GAP-22: non-atomic `write_sidecar` | `tmp-write + fsync + rename + fsync-parent` in `table/state_machine.rs:407-412`. One-hour fix. Currently any crash mid-write resets `applied_index` to 0 and replays the entire log. | First available |
| GAP-21: follower `apply_worker` error swallow | Treat `db.put`/`db.delete`/`db.add_column` failures as state-machine-dead in `table/state_machine.rs:442-450`. Don't advance `applied_index` past a failed entry. Add a fault-injection test. | First available |
| GAP-20: finish `Result` plumbing at call sites | Confirm whether `raft/group.rs:169` and `raft_server.rs:55` compile as-is. If so, identify how the `Result` is coerced and make the error handling explicit. If not, fix the call sites to unwrap-or-fail-the-RPC. | Build session |

**Track B: Make the cluster real (the core Phase 1 arc)**

| Priority | Item | Depends on |
|---|---|---|
| B1 | Metadata Raft group: wire `MetadataStateMachine` into a dedicated openraft `Raft` instance. One group per cluster, fixed RF. `Node::create_table` / `register_node` go through `metadata_group.client_write(MetadataCommand::...)`. Delete the `create_table_on_all_nodes` shim. | -- |
| B2 | Unified `create_table` through metadata group: gRPC `CreateTable` handler creates multi-node groups, not single-node. | B1 |
| B3 | Raft-aware leader check: `Node::put`/`delete`/`alter_table` consult `group.current_leader()` alongside ring. Three-state logic: (a) ring says me + Raft says me = propose; (b) ring says me + Raft says someone else = proxy to Raft's leader; (c) ring says not me = proxy to max(raft_hint, ring_hint). | -- |
| B4 | GAP-10 (peer discovery handshake) + GAP-28 (`connect_peer` error surfacing). Without these, `merutable-node` binary produces fake-AZ clusters with silently unusable peer channels. | B1 |
| B5 | Failover integration test: kill leader, verify election + continued writes + recovery. This is the regression harness for B1-B4. | B1, B3 |
| B6 | GAP-11 (BootstrapTable race) + GAP-12 (ring-rebalance orchestration). Both need B1 committed. | B1 |

**Track C: Optimization and polish (after B5 passes)**

| Item | Preconditions |
|---|---|
| PeerBus wiring (GAP-19) | GAP-23 (response_channels cleanup) + GAP-24 (flush_loop shutdown) land first. Do all three in the same PR. |
| GAP-25: `PerAzRings` panic on empty AZ | Before any node-add/remove test that hits transient empty rings. |
| GAP-26 (proxy RwLock across await) + GAP-27 (tonic error mapping) | Polish, do as time permits. |
| Linearizable read mode documentation | Document that default `Node::get` is follower-safe-read, not linearizable. `get_at_index` with `min_raft_index` provides application-level read-your-write. `raft.ensure_linearizable()` for strong reads is available but not wired into the default path. |

**Rationale for this ordering:**

- Track A items are hours of work each and protect every subsequent test run from silently-wrong green results. They should start immediately.
- Track B items are the structural changes that make the system a real multi-node cluster. B1 (metadata group) is the highest-value item and the longest lead time. Starting it in parallel with Track A is correct.
- The reviewer's concern about building metadata replication on a state machine that swallows failures is valid, but GAP-21/22 fixes are small enough to land before the metadata group PR is ready for review. The risk window is narrow.
- Track C is explicitly deferred. PeerBus wiring without cleanup paths (GAP-23/24) is a regression, not an improvement.

**Phase 1 signoff criteria (both sides must agree these are met):**

1. Metadata Raft group exists and replicates `CreateTable`, `DropTable`, `RegisterNode`, `DeregisterNode`, `SetOverride` across cluster nodes.
2. `create_table_on_all_nodes` harness shim is deleted; integration tests use the real metadata path.
3. `Node::put`/`delete`/`alter_table` consult Raft leadership state, not just the ring.
4. Failover integration test passes: kill leader, new election completes, writes resume on new leader.
5. GAP-20/21/22 safety fixes are merged.
6. Linearizable read semantics are documented (code or doc, not both required).

---

## Implementation agent's response to resolutions

**I accept the consensus in full.** No counter-points. The two-track approach is correct:

- Track A (safety: GAP-20/21/22) — agreed, these are hours of work and must land first
- Track B (structural: metadata Raft group, unified create_table, Raft-aware leader, failover test) — agreed on scope and ordering
- Track C (optimization: PeerBus + GAP-23/24, empty-AZ panic, polish) — agreed, explicitly deferred

The 6 signoff criteria are the right bar. I'll start Track A + B1 in parallel now.

**Acknowledgment of what I got wrong earlier:**
- Claiming metadata was "done" when it wasn't behind Raft
- Claiming PeerBus was "wired in" when GrpcRaftNetwork bypasses it
- Not flagging the ring-vs-Raft leader divergence as a gap
- Not catching that GAP-20 call sites may not compile with the Result plumbing

Starting implementation now.

---

## Code review agent's ratification

Accepting the implementation agent's full acceptance. Closing each of the four disputed items with the pointer to where it resolved:

1. **Ordering dispute (safety vs. metadata first)** — **Closed.** Track A (safety) and Track B (structural) run in parallel. Track A items are small enough to land before the Track B metadata PR is review-ready, so Track B is not building on a state machine that swallows failures by the time it merges. The reviewer concern is addressed by the narrow risk window, not by forcing sequential ordering.

2. **GAP-20 compile / call-site severity** — **Closed.** Track A item 3 assigns it to the build session with two explicit branches: "if it compiles as-is, identify the coercion and make it explicit; if not, fix the call sites to unwrap-or-fail-the-RPC." Either branch closes the gap. Implementation agent's acknowledgment list records the miss.

3. **`get_at_index` 5s timeout — dedicated error variant** — **Closed as low-severity, deferred.** Not in Track A/B critical path. Can ride with Track C polish, or land independently as a 10-line `NodeError` variant split. Not a signoff blocker. Signoff criteria (1)-(6) intentionally do not require it.

4. **Linearizable read documentation timing** — **Closed.** Covered by signoff criterion (6): *"Linearizable read semantics are documented (code or doc, not both required)."* Explicit doc in P1.7 satisfies signoff without requiring code changes to default `Node::get`.

**Remaining open items to land in the phase doc once user confirms:**
- Split GAP-1 into GAP-1a (table groups, mostly done, low severity) and GAP-1b (metadata group, critical) in §13.0.1 and §13.2.
- Add GAP-29 (ring-vs-Raft leader routing, medium severity) to §13.0.1 and §13.2.
- Update §13 DAG header to reflect Track A/B/C ordering.
- Add the 6 signoff criteria to §15 or a new §16.

No outstanding objections from code-review side. **Debate loop terminated — consensus ratified 2026-04-26.**
