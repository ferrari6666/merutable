// GAP-1: RaftGroup — wires a real openraft Raft<MeruTypeConfig> instance
// so that writes go through Raft consensus instead of calling the state
// machine directly.
//
// TableRaftStorage wraps DiskRaftStorage and delegates
// apply_to_state_machine to TableStateMachine, closing the loop between
// openraft's commit path and the application's MeruDB engine.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use openraft::storage::{Adaptor, LogState, Snapshot};
use openraft::{
    AnyError, BasicNode, Config, Entry, EntryPayload, LogId, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote, ErrorSubject, ErrorVerb,
};
use openraft::error::{
    NetworkError, RPCError, RaftError,
    InstallSnapshotError,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};

use super::disk_storage::DiskRaftStorage;
use super::network::{BatchedNetworkFactory, PeerBus};
use super::types::{MeruTypeConfig, RaftCommand, RaftResponse};
use crate::observability::{RAFT_COMMIT_LATENCY_US, RAFT_PROPOSALS_TOTAL};
use crate::proto::raft_transport as pb;
use crate::proto::raft_transport::group_message::Payload as PbPayload;
use crate::table::state_machine::{ApplyEntry, TableStateMachine};

// ---------------------------------------------------------------------------
// GrpcRaftNetwork — sends Raft RPCs directly to a peer via gRPC
// ---------------------------------------------------------------------------

/// Sends Raft RPCs to a single peer over gRPC via the RaftTransport::Batch
/// handler. Each RPC is sent as a single-message batch (no coalescing).
///
/// For the single-node fallback, methods return `NotConnected` errors.
pub struct GrpcRaftNetwork {
    /// Group (table) identifier.
    group_id: String,
    /// The gRPC client for this peer. `None` for single-node (no peers).
    client: Option<
        pb::raft_transport_client::RaftTransportClient<tonic::transport::Channel>,
    >,
    /// Our node ID, included in the batch envelope.
    this_node_id: u64,
}

impl GrpcRaftNetwork {
    /// Send a single GroupMessage to the peer and return the response.
    async fn send_one(
        &mut self,
        msg: pb::GroupMessage,
    ) -> Result<pb::GroupMessage, RPCError<u64, BasicNode, RaftError<u64>>> {
        let client = self.client.as_mut().ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "single-node: no peers",
            )))
        })?;

        let batch = pb::BatchRaftMessage {
            from_node: self.this_node_id,
            messages: vec![msg],
        };

        let resp = client.batch(batch).await.map_err(|e| {
            // Preserve the actual tonic error kind in the io::ErrorKind so
            // callers (and openraft) can distinguish transient vs. permanent
            // failures instead of treating everything as ConnectionRefused.
            let io_kind = match e.code() {
                tonic::Code::Unavailable => std::io::ErrorKind::ConnectionRefused,
                tonic::Code::DeadlineExceeded => std::io::ErrorKind::TimedOut,
                tonic::Code::NotFound => std::io::ErrorKind::NotFound,
                tonic::Code::PermissionDenied => std::io::ErrorKind::PermissionDenied,
                _ => std::io::ErrorKind::Other,
            };
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                io_kind,
                format!("gRPC batch failed ({}): {}", e.code(), e.message()),
            )))
        })?;

        resp.into_inner()
            .responses
            .into_iter()
            .next()
            .ok_or_else(|| {
                RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "empty batch response from peer",
                )))
            })
    }
}

// ---------------------------------------------------------------------------
// Entry payload serialization for cross-node Raft replication
// ---------------------------------------------------------------------------

/// Tag bytes for distinguishing entry payload types in the proto `bytes` field.
const PAYLOAD_TAG_BLANK: u8 = 0x00;
const PAYLOAD_TAG_NORMAL: u8 = 0x01;
const PAYLOAD_TAG_MEMBERSHIP: u8 = 0x02;

/// Serialize an openraft `EntryPayload` to bytes for the proto wire format.
///
/// Format: 1-byte tag + payload data
/// - Blank:      [0x00]
/// - Normal:     [0x01] + postcard-serialized RaftCommand
/// - Membership: [0x02] + JSON-serialized Membership
///
/// Returns an error if serialization fails (e.g. postcard or JSON encoding error).
pub fn serialize_entry_payload(payload: &EntryPayload<MeruTypeConfig>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match payload {
        EntryPayload::Blank => Ok(vec![PAYLOAD_TAG_BLANK]),
        EntryPayload::Normal(cmd) => {
            let mut buf = vec![PAYLOAD_TAG_NORMAL];
            buf.extend_from_slice(&postcard::to_stdvec(cmd)?);
            Ok(buf)
        }
        EntryPayload::Membership(membership) => {
            let mut buf = vec![PAYLOAD_TAG_MEMBERSHIP];
            buf.extend_from_slice(&serde_json::to_vec(membership)?);
            Ok(buf)
        }
    }
}

/// Deserialize bytes from the proto wire format back to an openraft `EntryPayload`.
///
/// Returns an error if the data is corrupt or cannot be deserialized.
pub fn deserialize_entry_payload(data: &[u8]) -> Result<EntryPayload<MeruTypeConfig>, Box<dyn std::error::Error>> {
    if data.is_empty() {
        return Ok(EntryPayload::Blank);
    }
    match data[0] {
        PAYLOAD_TAG_BLANK => Ok(EntryPayload::Blank),
        PAYLOAD_TAG_NORMAL => {
            let cmd = postcard::from_bytes(&data[1..])
                .map_err(|e| format!("corrupt Normal entry payload: {e}"))?;
            Ok(EntryPayload::Normal(cmd))
        }
        PAYLOAD_TAG_MEMBERSHIP => {
            let membership = serde_json::from_slice(&data[1..])
                .map_err(|e| format!("corrupt Membership entry payload: {e}"))?;
            Ok(EntryPayload::Membership(membership))
        }
        tag => Err(format!("unknown entry payload tag: 0x{:02x}", tag).into()),
    }
}

impl RaftNetwork<MeruTypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MeruTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64>>,
    > {
        use super::types::{log_id_to_proto, vote_to_proto};

        let entries: Vec<pb::RaftEntry> = rpc
            .entries
            .iter()
            .map(|e| {
                let payload = serialize_entry_payload(&e.payload)
                    .unwrap_or_else(|err| {
                        tracing::error!("failed to serialize entry at index {}: {err}", e.log_id.index);
                        vec![PAYLOAD_TAG_BLANK]
                    });
                pb::RaftEntry {
                    log_id: Some(log_id_to_proto(&e.log_id)),
                    payload,
                }
            })
            .collect();

        let msg = pb::GroupMessage {
            group_id: self.group_id.clone(),
            payload: Some(PbPayload::AppendRequest(pb::AppendEntriesRequest {
                vote: Some(vote_to_proto(&rpc.vote)),
                prev_log_id: rpc.prev_log_id.as_ref().map(log_id_to_proto),
                entries,
                leader_commit: rpc.leader_commit.as_ref().map(log_id_to_proto),
            })),
        };

        let resp = self.send_one(msg).await?;

        match resp.payload {
            Some(PbPayload::AppendResponse(ar)) => {
                use super::types::vote_from_proto;
                if ar.success {
                    Ok(AppendEntriesResponse::Success)
                } else if ar.conflict.is_some() {
                    Ok(AppendEntriesResponse::Conflict)
                } else {
                    let vote = ar.vote.as_ref().map(vote_from_proto).unwrap_or_default();
                    Ok(AppendEntriesResponse::HigherVote(vote))
                }
            }
            _ => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response payload for append_entries",
            )))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MeruTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        use super::types::{log_id_to_proto, vote_from_proto, vote_to_proto};

        let msg = pb::GroupMessage {
            group_id: self.group_id.clone(),
            payload: Some(PbPayload::InstallSnapshotRequest(
                pb::InstallSnapshotRequest {
                    vote: Some(vote_to_proto(&rpc.vote)),
                    last_included: rpc.meta.last_log_id.as_ref().map(log_id_to_proto),
                    offset: rpc.offset,
                    data: rpc.data.clone(),
                    done: rpc.done,
                },
            )),
        };

        // send_one returns RPCError<u64, BasicNode, RaftError<u64>>
        // but we need RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>
        let resp = self.send_one(msg).await.map_err(|e| match e {
            RPCError::Network(ne) => RPCError::Network(ne),
            RPCError::RemoteError(re) => RPCError::Network(NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::Other, format!("{}", re)),
            )),
            RPCError::Unreachable(ue) => RPCError::Unreachable(ue),
            RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
            RPCError::Timeout(t) => RPCError::Timeout(t),
        })?;

        match resp.payload {
            Some(PbPayload::InstallSnapshotResponse(sr)) => {
                Ok(InstallSnapshotResponse {
                    vote: sr.vote.as_ref().map(vote_from_proto).unwrap_or_default(),
                })
            }
            _ => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response payload for install_snapshot",
            )))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64>>,
    > {
        use super::types::{log_id_from_proto, log_id_to_proto, vote_from_proto, vote_to_proto};

        let msg = pb::GroupMessage {
            group_id: self.group_id.clone(),
            payload: Some(PbPayload::VoteRequest(pb::VoteRequest {
                vote: Some(vote_to_proto(&rpc.vote)),
                last_log_id: rpc.last_log_id.as_ref().map(log_id_to_proto),
            })),
        };

        let resp = self.send_one(msg).await?;

        match resp.payload {
            Some(PbPayload::VoteResponse(vr)) => Ok(VoteResponse {
                vote: vr.vote.as_ref().map(vote_from_proto).unwrap_or_default(),
                vote_granted: vr.vote_granted,
                last_log_id: vr.last_log_id.as_ref().map(log_id_from_proto),
            }),
            _ => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response payload for vote",
            )))),
        }
    }
}

// ---------------------------------------------------------------------------
// GrpcNetworkFactory
// ---------------------------------------------------------------------------

/// Factory that creates `GrpcRaftNetwork` instances for each target peer.
///
/// Owns a map of gRPC channels to peer nodes. When openraft needs to
/// communicate with a peer, it calls `new_client()` which creates a
/// `GrpcRaftNetwork` backed by the gRPC channel for that peer.
///
/// For single-node mode (empty channels map), `new_client()` returns a
/// network that returns NotConnected errors.
pub struct GrpcNetworkFactory {
    group_id: String,
    this_node_id: u64,
    /// Peer node ID -> gRPC channel to that peer.
    channels: HashMap<u64, tonic::transport::Channel>,
}

impl GrpcNetworkFactory {
    pub fn new(
        group_id: String,
        this_node_id: u64,
        channels: HashMap<u64, tonic::transport::Channel>,
    ) -> Self {
        Self {
            group_id,
            this_node_id,
            channels,
        }
    }
}

impl RaftNetworkFactory<MeruTypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(
        &mut self,
        target: u64,
        _node: &BasicNode,
    ) -> Self::Network {
        let client = self.channels.get(&target).map(|ch| {
            pb::raft_transport_client::RaftTransportClient::new(ch.clone())
        });
        GrpcRaftNetwork {
            group_id: self.group_id.clone(),
            client,
            this_node_id: self.this_node_id,
        }
    }
}

// ---------------------------------------------------------------------------
// TableRaftStorage — DiskRaftStorage + TableStateMachine delegation
// ---------------------------------------------------------------------------

/// A `RaftStorage` implementation that wraps `DiskRaftStorage` for durable
/// log/vote/snapshot persistence, and overrides `apply_to_state_machine` to
/// delegate to `TableStateMachine::apply()` — connecting Raft's commit path
/// to the real MeruDB engine.
#[derive(Clone)]
pub struct TableRaftStorage {
    disk: DiskRaftStorage,
    sm: Arc<TableStateMachine>,
}

impl TableRaftStorage {
    /// Create a new `TableRaftStorage` backed by `DiskRaftStorage` and the
    /// given `TableStateMachine`.
    pub fn new(disk: DiskRaftStorage, sm: Arc<TableStateMachine>) -> Self {
        Self { disk, sm }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader — delegate to DiskRaftStorage
// ---------------------------------------------------------------------------

impl RaftLogReader<MeruTypeConfig> for TableRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MeruTypeConfig>>, StorageError<u64>> {
        self.disk.try_get_log_entries(range).await
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder — delegate to DiskRaftStorage
// ---------------------------------------------------------------------------

impl RaftSnapshotBuilder<MeruTypeConfig> for TableRaftStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MeruTypeConfig>, StorageError<u64>> {
        self.disk.build_snapshot().await
    }
}

// ---------------------------------------------------------------------------
// RaftStorage — delegate everything to DiskRaftStorage except
//               apply_to_state_machine (which goes to TableStateMachine)
// ---------------------------------------------------------------------------

impl RaftStorage<MeruTypeConfig> for TableRaftStorage {
    type LogReader = TableRaftStorage;
    type SnapshotBuilder = TableRaftStorage;

    // --- Vote (delegated) ---

    async fn save_vote(
        &mut self,
        vote: &Vote<u64>,
    ) -> Result<(), StorageError<u64>> {
        self.disk.save_vote(vote).await
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.disk.read_vote().await
    }

    // --- Log (delegated) ---

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<MeruTypeConfig>, StorageError<u64>> {
        self.disk.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MeruTypeConfig>> + Send,
    {
        self.disk.append_to_log(entries).await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        self.disk.delete_conflict_logs_since(log_id).await
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        self.disk.purge_logs_upto(log_id).await
    }

    // --- State machine (the key override) ---

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, BasicNode>,
        ),
        StorageError<u64>,
    > {
        self.disk.last_applied_state().await
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<MeruTypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<u64>> {
        // 1. Let DiskRaftStorage update its last_applied / last_membership
        //    tracking. Its apply_to_state_machine is a no-op for Normal
        //    entries (just tracks metadata), so this is safe.
        let responses = self.disk.apply_to_state_machine(entries).await?;

        // 2. Build ApplyEntry list for Normal entries and delegate to
        //    TableStateMachine.
        let mut apply_entries = Vec::new();
        for entry in entries {
            if let EntryPayload::Normal(cmd) = &entry.payload {
                apply_entries.push(ApplyEntry {
                    index: entry.log_id.index,
                    command: cmd.clone(),
                });
            }
        }

        if !apply_entries.is_empty() {
            self.sm.apply(apply_entries).await.map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    AnyError::error(format!("TableStateMachine::apply failed: {e}")),
                )
            })?;
        }

        Ok(responses)
    }

    // --- Snapshot (delegated) ---

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        self.disk.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.disk.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MeruTypeConfig>>, StorageError<u64>> {
        self.disk.get_current_snapshot().await
    }
}

// ---------------------------------------------------------------------------
// RaftGroup
// ---------------------------------------------------------------------------

/// A wrapper around one openraft `Raft<MeruTypeConfig>` instance + its
/// storage. One per table on each node.
///
/// Writes go through `client_write`, which proposes to Raft, blocks until
/// committed + applied (via `TableRaftStorage::apply_to_state_machine`
/// -> `TableStateMachine::apply()`), and returns the response.
///
/// Reads go directly to the `TableStateMachine` via `sm()`.
pub struct RaftGroup {
    raft: openraft::Raft<MeruTypeConfig>,
    sm: Arc<TableStateMachine>,
}

impl RaftGroup {
    /// Create and initialize a single-node Raft group for a table.
    ///
    /// - `table`: table name (used for the openraft cluster name)
    /// - `node_id`: this node's ID
    /// - `members`: initial membership set (for single-node, a set with just `node_id`)
    /// - `sm`: the `TableStateMachine` that will handle applied entries
    /// - `raft_dir`: directory for the Raft log and hard state files
    pub async fn create_and_initialize(
        table: &str,
        node_id: u64,
        members: BTreeSet<u64>,
        sm: Arc<TableStateMachine>,
        raft_dir: PathBuf,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: format!("meru-{}", table),
            // For single-node, elections happen instantly. Keep timeouts
            // reasonable for tests.
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        };

        let network_factory = GrpcNetworkFactory::new(
            table.to_string(),
            node_id,
            HashMap::new(), // No peers for single-node.
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = TableRaftStorage::new(disk, sm.clone());
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        // Initialize the membership. For a single-node cluster, this makes
        // the node immediately elect itself as leader and start accepting
        // proposals. On pod restart the log already has entries; openraft
        // returns NotAllowed, which means "already initialized" — safe.
        match raft.initialize(members).await {
            Ok(()) => tracing::info!(table = %table, "raft group created"),
            Err(e) if e.to_string().contains("NotAllowed") => {
                tracing::info!(table = %table, "raft group already initialized on disk — skipping");
            }
            Err(e) => return Err(e.into()),
        }

        Ok(Self { raft, sm })
    }

    /// Create a Raft group with real gRPC network for multi-node replication.
    /// The group is created but NOT initialized — call `initialize()` on the
    /// returned group only from the bootstrapper node.
    ///
    /// - `table`: table name (used for the openraft cluster name)
    /// - `node_id`: this node's ID
    /// - `sm`: the `TableStateMachine` that will handle applied entries
    /// - `raft_dir`: directory for the Raft log and hard state files
    /// - `peer_channels`: gRPC channels to peer nodes (peer_id -> channel)
    pub async fn create_with_network(
        table: &str,
        node_id: u64,
        sm: Arc<TableStateMachine>,
        raft_dir: PathBuf,
        peer_channels: HashMap<u64, tonic::transport::Channel>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: format!("meru-{}", table),
            // Multi-node: use longer timeouts to account for gRPC latency.
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 200,
            // Pack more log entries per AppendEntries RPC. Default is 300;
            // raising to 1024 lets a burst of writes amortize per-RPC
            // framing cost at high concurrency. Has no effect at conc=1
            // (only 1 entry to pack), so latency at low load is unchanged.
            max_payload_entries: 1024,
            ..Default::default()
        };

        let network_factory = GrpcNetworkFactory::new(
            table.to_string(),
            node_id,
            peer_channels,
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = TableRaftStorage::new(disk, sm.clone());
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        tracing::info!(table = %table, node_id = node_id, "raft group created (multi-node, uninit)");

        Ok(Self { raft, sm })
    }

    /// Create a Raft group that routes messages through shared `PeerBus`
    /// instances for batched transport. All Raft groups targeting the same
    /// peer share a single `PeerBus`, collapsing heartbeats into one gRPC
    /// call per flush interval.
    ///
    /// The group is created but NOT initialized — call `initialize()` on
    /// the returned group after all replicas have created their groups.
    pub async fn create_with_batched_network(
        table: &str,
        node_id: u64,
        sm: Arc<TableStateMachine>,
        raft_dir: PathBuf,
        peer_buses: Arc<HashMap<u64, Arc<PeerBus>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: format!("meru-{}", table),
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 200,
            max_payload_entries: 1024,
            ..Default::default()
        };

        let network_factory = BatchedNetworkFactory::new(
            table.to_string(),
            peer_buses,
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = TableRaftStorage::new(disk, sm.clone());
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        tracing::info!(table = %table, node_id = node_id, "raft group created (batched transport, uninit)");

        Ok(Self { raft, sm })
    }

    /// Initialize this group's membership. Only the bootstrapper node should
    /// call this. After initialization, the node starts an election.
    pub async fn initialize(
        &self,
        members: BTreeSet<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.initialize(members).await?;
        Ok(())
    }

    /// Add a learner to this Raft group. Only the leader should call this.
    /// If `blocking` is true, waits until the learner is caught up.
    pub async fn add_learner(
        &self,
        id: u64,
        blocking: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft
            .add_learner(id, BasicNode::default(), blocking)
            .await?;
        Ok(())
    }

    /// Change the membership of this Raft group. Only the leader should call this.
    /// `members` is the new set of voter node IDs.
    pub async fn change_membership(
        &self,
        members: BTreeSet<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.change_membership(members, false).await?;
        Ok(())
    }

    /// Propose a write through Raft consensus. Blocks until the entry is
    /// committed and applied to the state machine.
    ///
    /// In a single-node cluster this completes immediately (no replication
    /// needed). In a multi-node cluster it waits for a quorum.
    pub async fn client_write(
        &self,
        cmd: RaftCommand,
    ) -> Result<RaftResponse, Box<dyn std::error::Error>> {
        metrics::counter!(RAFT_PROPOSALS_TOTAL).increment(1);
        let start = Instant::now();
        let resp = self.raft.client_write(cmd).await?;
        metrics::histogram!(RAFT_COMMIT_LATENCY_US).record(start.elapsed().as_micros() as f64);
        tracing::debug!(index = %resp.log_id.index, "raft entry committed");
        Ok(resp.data)
    }

    /// Check if this node is currently the Raft leader for this group.
    pub async fn is_leader(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    /// Access the underlying `TableStateMachine` for reads.
    pub fn sm(&self) -> &Arc<TableStateMachine> {
        &self.sm
    }

    /// Access the underlying openraft `Raft` instance.
    ///
    /// Used by the `RaftTransportService` to deliver incoming
    /// AppendEntries, Vote, and InstallSnapshot messages from peers.
    pub fn raft(&self) -> &openraft::Raft<MeruTypeConfig> {
        &self.raft
    }

    /// Non-blocking check: who does this node think is the current leader?
    /// Returns `Some(node_id)` if a leader is known, `None` otherwise.
    pub fn current_leader(&self) -> Option<u64> {
        let rx = self.raft.metrics();
        let leader = rx.borrow().current_leader;
        leader
    }

    /// Shutdown the Raft node.
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.shutdown().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::state_machine::StateMachineMode;
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};
    use merutable::value::{FieldValue, Row};
    use merutable::OpenOptions;

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: false,
                    ..Default::default()
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: ColumnType::ByteArray,
                    nullable: true,
                    ..Default::default()
                },
            ],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn test_options(tmp: &tempfile::TempDir) -> OpenOptions {
        OpenOptions::new(test_schema())
            .wal_dir(tmp.path().join("wal"))
            .catalog_uri(tmp.path().join("data").to_string_lossy().to_string())
    }

    fn make_row(id: i64, val: &str) -> Row {
        Row::new(vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Bytes(bytes::Bytes::from(val.to_string()))),
        ])
    }

    #[tokio::test]
    async fn raft_group_single_node_put() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(
            TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
                .await
                .unwrap(),
        );

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("raft_log");
        let group = RaftGroup::create_and_initialize("test_table", 1, members, sm.clone(), raft_dir)
            .await
            .unwrap();

        // Write through Raft consensus.
        let _resp = group
            .client_write(RaftCommand::Put {
                row: make_row(42, "hello"),
            })
            .await
            .unwrap();

        // Verify the SM has the data.
        let row = group.sm().get(&[FieldValue::Int64(42)]).unwrap();
        assert!(row.is_some(), "row should be present after Raft write");
        let row = row.unwrap();
        assert_eq!(
            *row.get(1).unwrap(),
            FieldValue::Bytes(bytes::Bytes::from("hello"))
        );

        // Verify applied_index advanced.
        assert!(
            group.sm().applied_index() > 0,
            "applied_index should advance after writes"
        );
    }

    #[tokio::test]
    async fn raft_group_single_node_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(
            TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
                .await
                .unwrap(),
        );

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("raft_log");
        let group = RaftGroup::create_and_initialize("test_table", 1, members, sm.clone(), raft_dir)
            .await
            .unwrap();

        // Put a row.
        group
            .client_write(RaftCommand::Put {
                row: make_row(7, "to_delete"),
            })
            .await
            .unwrap();
        assert!(group.sm().get(&[FieldValue::Int64(7)]).unwrap().is_some());

        // Delete it through Raft.
        group
            .client_write(RaftCommand::Delete {
                pk: vec![FieldValue::Int64(7)],
            })
            .await
            .unwrap();

        assert!(
            group.sm().get(&[FieldValue::Int64(7)]).unwrap().is_none(),
            "row should be gone after Raft delete"
        );
    }

    #[tokio::test]
    async fn raft_group_is_leader() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(
            TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
                .await
                .unwrap(),
        );

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("raft_log");
        let group = RaftGroup::create_and_initialize("test_table", 1, members, sm, raft_dir)
            .await
            .unwrap();

        // Single-node cluster: this node should be leader.
        assert!(
            group.is_leader().await,
            "single-node should be leader after initialization"
        );
    }
}
