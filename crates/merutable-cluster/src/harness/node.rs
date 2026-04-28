// P1.6: Node struct — owns Raft groups, metadata cache, ring, role transitions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use merutable::schema::TableSchema;
use merutable::value::{FieldValue, Row};
use merutable::OpenOptions;

use crate::config::{ClusterConfig, NodeId, NodeIdentity};
use crate::harness::routing::Router;
use crate::observability::{
    NODE_OPEN_RAFT_GROUPS, NODE_TABLE_COUNT, NOT_LEADER_RETRIES_TOTAL,
    PROXY_ATTEMPTS_TOTAL, PROXY_LATENCY_US, REBALANCE_IN_PROGRESS,
    RAFT_ELECTIONS_TOTAL, RAFT_LEADER_TENURE_SECS,
};
use crate::metadata::cache::LocalMetadataCache;
use crate::metadata::raft_group::{MetadataRaftGroup, METADATA_GROUP_ID};
use crate::metadata::state_machine::{
    MetadataCommand, MetadataEvent, MetadataStateMachine, NodeEntry,
    NodeStatus,
};
use crate::proto_convert::{
    column_def_to_proto, field_value_to_proto, proto_schema_to_schema, row_to_proto_row,
    ProtoConvertError,
};
use crate::raft::group::RaftGroup;
use crate::raft::network::{GrpcBatchTransport, PeerBus};
use crate::raft::types::{RaftCommand, SchemaChange};
use crate::ring::per_az::PerAzRings;
use crate::table::state_machine::{StateMachineMode, TableStateMachine};

// ---------------------------------------------------------------------------
// Public result/info types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PutResult {
    pub seq: u64,
    pub raft_index: u64,
}

#[derive(Debug)]
pub struct DeleteResult {
    pub seq: u64,
    pub raft_index: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableRole {
    Leader,
    Follower,
    NotMine,
}

pub struct NodeStatusInfo {
    pub node_id: NodeId,
    pub address: String,
    pub az: String,
    pub table_count: usize,
    pub tables: HashMap<String, TableRoleInfo>,
}

pub struct TableRoleInfo {
    pub role: TableRole,
    pub committed_index: u64,
    pub applied_index: u64,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum NodeError {
    /// This node is not the leader for the requested table.
    NotLeader {
        leader_hint: NodeId,
    },
    /// The table does not exist.
    TableNotFound(String),
    /// The table already exists.
    TableAlreadyExists(String),
    /// A metadata state machine error.
    MetadataError(String),
    /// An underlying MeruDB error.
    StorageError(merutable::error::Error),
    /// The requested schema change is invalid.
    InvalidChange(String),
    /// A proxy attempt to the leader failed.
    ProxyFailed(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::NotLeader { leader_hint } => {
                write!(f, "not leader; preferred leader is node {}", leader_hint)
            }
            NodeError::TableNotFound(name) => write!(f, "table not found: {}", name),
            NodeError::TableAlreadyExists(name) => {
                write!(f, "table already exists: {}", name)
            }
            NodeError::MetadataError(msg) => write!(f, "metadata error: {}", msg),
            NodeError::StorageError(e) => write!(f, "storage error: {}", e),
            NodeError::InvalidChange(msg) => write!(f, "invalid change: {}", msg),
            NodeError::ProxyFailed(msg) => write!(f, "proxy failed: {}", msg),
        }
    }
}

impl std::error::Error for NodeError {}

impl From<merutable::error::Error> for NodeError {
    fn from(e: merutable::error::Error) -> Self {
        NodeError::StorageError(e)
    }
}

impl From<ProtoConvertError> for NodeError {
    fn from(e: ProtoConvertError) -> Self {
        NodeError::ProxyFailed(e.0)
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// The central orchestrator for a single cluster node.
///
/// Owns the metadata cache, Raft groups (one per table), and routing.
/// Writes go through `RaftGroup::client_write()` which proposes to the
/// openraft Raft instance, ensuring real consensus even for single-node
/// clusters. Reads go directly to the `TableStateMachine` via the group.
pub struct Node {
    config: ClusterConfig,
    router: Arc<Router>,
    metadata_cache: Arc<LocalMetadataCache>,
    /// The metadata Raft group that replicates all metadata mutations
    /// (CreateTable, RegisterNode, etc.) through openraft consensus.
    /// The `MetadataStateMachine` is owned by the group's storage layer;
    /// reads use the `LocalMetadataCache` instead.
    metadata_group: Arc<MetadataRaftGroup>,
    /// One RaftGroup per table this node is a replica for. Each group
    /// wraps an openraft `Raft<MeruTypeConfig>` instance + a
    /// `TableStateMachine`.
    ///
    /// Wrapped in `Arc` so the `RaftTransportService` can share access
    /// for dispatching incoming Raft messages from peers.
    table_groups: Arc<tokio::sync::RwLock<HashMap<String, Arc<RaftGroup>>>>,
    /// Broadcast channel for metadata events (GAP-4: WatchLeader).
    watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
    /// Cached gRPC channels to peer nodes for write proxying (GAP-5).
    peer_channels:
        tokio::sync::RwLock<HashMap<NodeId, tonic::transport::Channel>>,
    /// Per-peer PeerBus instances for batched Raft transport (GAP-19).
    /// Each PeerBus collects messages from all Raft groups destined for
    /// that peer and flushes them as a single batched gRPC call.
    peer_buses: Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<PeerBus>>>>,
    /// Handles for the PeerBus flush_loop tasks, so we can abort on shutdown.
    bus_handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Concurrency cap for ring-rebalance membership changes (GAP-12).
    /// At most 10 tables can be rebalancing simultaneously on this node.
    rebalance_semaphore: Arc<tokio::sync::Semaphore>,
}

impl Node {
    /// Create a new Node with empty state.
    ///
    /// Initializes a single-node metadata Raft group that replicates all
    /// metadata mutations through openraft consensus.
    pub async fn new(config: ClusterConfig) -> Self {
        let vnodes_per_node = 128;
        let metadata_sm = Arc::new(tokio::sync::RwLock::new(
            MetadataStateMachine::new(vnodes_per_node),
        ));

        // Start with an empty cache.
        let cache = Arc::new(LocalMetadataCache::new(
            PerAzRings::new(&[], vnodes_per_node),
            HashMap::new(),
            HashMap::new(),
        ));

        let router = Arc::new(Router::new(config.this_node.node_id, cache.clone()));

        let (watch_tx, _) = tokio::sync::broadcast::channel(256);

        // Create and initialize the metadata Raft group.
        let mut members = std::collections::BTreeSet::new();
        members.insert(config.this_node.node_id);
        let raft_dir = config.raft_dir.join(METADATA_GROUP_ID);
        let metadata_group = Arc::new(MetadataRaftGroup::create_and_initialize(
            config.this_node.node_id,
            members,
            metadata_sm.clone(),
            cache.clone(),
            raft_dir,
            watch_tx.clone(),
        )
        .await
        .expect("metadata raft group initialization must succeed"));

        Self {
            config,
            router,
            metadata_cache: cache,
            metadata_group,
            table_groups: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            watch_tx,
            peer_channels: tokio::sync::RwLock::new(HashMap::new()),
            peer_buses: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            bus_handles: tokio::sync::Mutex::new(Vec::new()),
            rebalance_semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
        }
    }

    /// Register this node in the metadata state machine and update the cache.
    ///
    /// The cache is updated atomically inside `apply_to_state_machine` before
    /// the event is broadcast.
    pub async fn register_self(&self) {
        let entry = NodeEntry {
            node_id: self.config.this_node.node_id,
            address: self.config.this_node.address.clone(),
            az: self.config.this_node.az.clone(),
            table_count: 0,
            status: NodeStatus::Active,
        };
        // ForwardToLeader can arise during startup when leadership
        // has moved since we sampled current_leader(). We swallow that
        // specific error: whoever is actually the leader will register
        // this node via register_node(). Any other error still panics.
        match self
            .metadata_group
            .client_write(MetadataCommand::RegisterNode { node: entry })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{:?}", e);
                if msg.contains("ForwardToLeader") {
                    tracing::warn!("register_self: not leader, skipping: {}", msg);
                } else {
                    panic!("register_self: metadata raft write failed: {:?}", e);
                }
            }
        }
    }

    /// Bootstrap a cluster: register self and all known peers.
    /// Phase 1 simplified — all nodes are known upfront.
    pub async fn bootstrap_cluster(&self, all_nodes: &[NodeIdentity]) {
        self.register_self().await;
        for node in all_nodes {
            if node.node_id != self.config.this_node.node_id {
                self.register_node(NodeEntry {
                    node_id: node.node_id,
                    address: node.address.clone(),
                    az: node.az.clone(),
                    table_count: 0,
                    status: NodeStatus::Active,
                })
                .await;
            }
        }
    }

    /// Create a Node for multi-node deployment.
    ///
    /// §13.3 Div 2 fix: the metadata Raft group is created with
    /// `DynamicBatchedNetworkFactory` pointing at the shared `peer_buses`
    /// map, so it can reach peers once `connect_peer` populates the map.
    ///
    /// **Bootstrapper** (min node_id among `all_node_ids`): calls
    /// `create_with_dynamic_network` then `initialize(members={all})`.
    /// **Non-bootstrapper**: calls `create_with_dynamic_network` only —
    /// sits idle until the bootstrapper's `initialize` includes it.
    ///
    /// The gRPC server MUST be started and `connect_peer` called for all
    /// peers BEFORE the bootstrapper calls `initialize_metadata`, so that
    /// openraft's heartbeats can reach every member.
    pub async fn new_multi_node(config: ClusterConfig, _all_node_ids: &[NodeId]) -> Self {
        let vnodes_per_node = 128;
        let metadata_sm = Arc::new(tokio::sync::RwLock::new(
            MetadataStateMachine::new(vnodes_per_node),
        ));

        let cache = Arc::new(LocalMetadataCache::new(
            PerAzRings::new(&[], vnodes_per_node),
            HashMap::new(),
            HashMap::new(),
        ));

        let router = Arc::new(Router::new(config.this_node.node_id, cache.clone()));
        let (watch_tx, _) = tokio::sync::broadcast::channel(256);

        // Shared peer_buses map — initially empty. connect_peer populates it.
        // The metadata group's DynamicBatchedNetworkFactory reads from this
        // at RPC time, so peers become reachable as soon as connect_peer runs.
        let peer_buses = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        let raft_dir = config.raft_dir.join(METADATA_GROUP_ID);
        let metadata_group = Arc::new(
            MetadataRaftGroup::create_with_dynamic_network(
                config.this_node.node_id,
                metadata_sm.clone(),
                cache.clone(),
                raft_dir,
                watch_tx.clone(),
                Arc::clone(&peer_buses),
            )
            .await
            .expect("metadata raft group creation must succeed"),
        );

        Self {
            config,
            router,
            metadata_cache: cache,
            metadata_group,
            table_groups: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            watch_tx,
            peer_channels: tokio::sync::RwLock::new(HashMap::new()),
            peer_buses,
            bus_handles: tokio::sync::Mutex::new(Vec::new()),
            rebalance_semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
        }
    }

    /// Initialize the metadata Raft group with all members.
    ///
    /// Only the bootstrapper (min node_id) should call this, after the
    /// gRPC server is running and all peers are connected via `connect_peer`.
    pub async fn initialize_metadata(
        &self,
        all_node_ids: &[NodeId],
    ) -> Result<(), NodeError> {
        let members: std::collections::BTreeSet<u64> =
            all_node_ids.iter().copied().collect();
        match self.metadata_group.initialize(members).await {
            Ok(()) => {
                tracing::info!("metadata raft group initialized with {} members", all_node_ids.len());
                Ok(())
            }
            Err(e) if {
                let s = e.to_string().to_lowercase();
                s.contains("notallowed") || s.contains("not allowed")
            } => {
                // Already initialized (pod restart with existing disk).
                tracing::info!("metadata raft group already initialized on disk");
                Ok(())
            }
            Err(e) => Err(NodeError::MetadataError(format!(
                "metadata initialize failed: {}", e
            ))),
        }
    }

    /// Register a peer node (used in tests and bootstrap).
    ///
    /// The cache is updated atomically inside `apply_to_state_machine` before
    /// the event is broadcast.
    pub async fn register_node(&self, entry: NodeEntry) {
        let id = entry.node_id;
        let az = entry.az.clone();
        self.metadata_group
            .client_write(MetadataCommand::RegisterNode { node: entry })
            .await
            .expect("register_node: metadata raft write must succeed");
        tracing::info!(node_id = %id, az = %az, "node registered");
    }

    /// Create a new table. Applies CreateTable through the metadata Raft
    /// group, then if this node is a replica per the ring, opens a
    /// TableStateMachine.
    /// Create a new table. Applies CreateTable through the metadata Raft
    /// group, then if this node is a replica per the ring, opens a
    /// TableStateMachine + RaftGroup.
    ///
    /// Automatically selects multi-node or single-node Raft group based on
    /// whether peer buses are connected (§13.3 Divergence 1 fix).
    pub async fn create_table(
        &self,
        name: String,
        schema: TableSchema,
    ) -> Result<(), NodeError> {
        // If peers are connected, delegate to the multi-node path which
        // uses batched transport and all-replica membership.
        let has_peers = !self.peer_buses.read().await.is_empty();
        if has_peers {
            return self.create_table_multi_node(name, schema).await;
        }

        // Single-node fallback (no peers connected — unit tests, dev mode).
        self.metadata_group
            .client_write(MetadataCommand::CreateTable {
                name: name.clone(),
                schema: schema.clone(),
            })
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("already exists") {
                    NodeError::TableAlreadyExists(msg)
                } else {
                    NodeError::MetadataError(msg)
                }
            })?;

        if self.router.is_my_table(&name) {
            let mode = if self.router.am_i_preferred_leader(&name) {
                StateMachineMode::Leader
            } else {
                StateMachineMode::Follower
            };
            let table_dir = self.config.data_dir.join(&name);
            let opts = OpenOptions::new(schema)
                .wal_dir(table_dir.join("wal"))
                .catalog_uri(table_dir.join("data").to_string_lossy().to_string());
            let tsm = Arc::new(TableStateMachine::open(opts, mode).await?);

            let mut members = std::collections::BTreeSet::new();
            members.insert(self.config.this_node.node_id);

            let raft_dir = self.config.raft_dir.join(&name);
            let group = RaftGroup::create_and_initialize(
                &name,
                self.config.this_node.node_id,
                members,
                tsm,
                raft_dir,
            )
            .await
            .map_err(|e| NodeError::MetadataError(format!("RaftGroup init failed: {e}")))?;

            let mut groups = self.table_groups.write().await;
            groups.insert(name.clone(), Arc::new(group));
            metrics::gauge!(NODE_TABLE_COUNT).set(groups.len() as f64);
            metrics::gauge!(NODE_OPEN_RAFT_GROUPS).set(groups.len() as f64);
            tracing::info!(table = %name, role = ?mode, "table created (single-node)");
        }

        Ok(())
    }

    /// Create a table's local RaftGroup backed by a `BatchedNetworkFactory`
    /// for multi-node replication. All Raft groups targeting the same peer
    /// share a `PeerBus`, collapsing heartbeats into one gRPC call per
    /// flush interval (GAP-19).
    ///
    /// Call `initialize_raft_group` separately on the bootstrapper node
    /// after all replicas have created their groups.
    pub async fn create_table_multi_node(
        &self,
        name: String,
        schema: merutable::schema::TableSchema,
    ) -> Result<(), NodeError> {
        // 1. Apply to metadata SM through Raft consensus.
        self.metadata_group
            .client_write(MetadataCommand::CreateTable {
                name: name.clone(),
                schema: schema.clone(),
            })
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("already exists") {
                    NodeError::TableAlreadyExists(msg)
                } else {
                    NodeError::MetadataError(msg)
                }
            })?;
        // Cache is updated atomically inside apply_to_state_machine.

        // 2. If this node is a replica, open a RaftGroup with batched network.
        if self.router.is_my_table(&name) {
            let mode = if self.router.am_i_preferred_leader(&name) {
                StateMachineMode::Leader
            } else {
                StateMachineMode::Follower
            };
            let table_dir = self.config.data_dir.join(&name);
            let opts = merutable::OpenOptions::new(schema)
                .wal_dir(table_dir.join("wal"))
                .catalog_uri(table_dir.join("data").to_string_lossy().to_string());
            let tsm = Arc::new(TableStateMachine::open(opts, mode).await?);

            // Collect the shared PeerBus instances for peer replicas.
            let replicas = self.router.replicas_for(&name);
            let my_id = self.config.this_node.node_id;
            let peer_bus_map = {
                let buses = self.peer_buses.read().await;
                let mut map = std::collections::HashMap::new();
                for &replica_id in &replicas {
                    if replica_id == my_id {
                        continue;
                    }
                    if let Some(bus) = buses.get(&replica_id) {
                        map.insert(replica_id, Arc::clone(bus));
                    }
                }
                Arc::new(map)
            };

            let raft_dir = self.config.raft_dir.join(&name);
            let group = RaftGroup::create_with_batched_network(
                &name,
                self.config.this_node.node_id,
                tsm,
                raft_dir,
                peer_bus_map,
            )
            .await
            .map_err(|e| NodeError::MetadataError(format!("RaftGroup init failed: {e}")))?;

            let group = Arc::new(group);

            // Insert into the table_groups map BEFORE membership orchestration
            // so that the RaftTransportService can find this group when it
            // receives incoming Raft messages from peers during add_learner.
            {
                let mut groups = self.table_groups.write().await;
                groups.insert(name.clone(), group.clone());
                metrics::gauge!(NODE_TABLE_COUNT).set(groups.len() as f64);
            metrics::gauge!(NODE_OPEN_RAFT_GROUPS).set(groups.len() as f64);
            }

            // GAP-11: All replicas call initialize with the same membership.
            // openraft treats this as idempotent: "More than one node
            // performing initialize() with the same config is safe."
            // This ensures every node is ready to participate in Raft
            // immediately — no ordering dependency on a single bootstrapper.
            // The deterministic bootstrapper (min node_id) is recorded in
            // MetadataEvent::TableCreated for observability, but all nodes
            // initialize to avoid the race where the bootstrapper's
            // AppendEntries arrive before non-bootstrapper nodes are ready.
            let all_members: std::collections::BTreeSet<u64> =
                replicas.iter().copied().collect();
            group
                .initialize(all_members)
                .await
                .map_err(|e| NodeError::MetadataError(format!("raft initialize failed: {e}")))?;
            tracing::info!(table = %name, role = ?mode, "table created (multi-node, batched transport)");
        }

        Ok(())
    }

    /// Drop a table. Applies DropTable through the metadata Raft group,
    /// closes and removes the local TableStateMachine if present.
    pub async fn drop_table(&self, name: String) -> Result<(), NodeError> {
        // 1. Apply to metadata SM through Raft consensus.
        self.metadata_group
            .client_write(MetadataCommand::DropTable { name: name.clone() })
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("not found") {
                    NodeError::TableNotFound(msg)
                } else {
                    NodeError::MetadataError(msg)
                }
            })?;
        // Cache is updated atomically inside apply_to_state_machine.

        // 2. Close and remove local RaftGroup if we have one.
        let mut groups = self.table_groups.write().await;
        if let Some(group) = groups.remove(&name) {
            let _ = group.sm().close().await;
        }
        metrics::gauge!(NODE_TABLE_COUNT).set(groups.len() as f64);

        tracing::info!(table = %name, "table dropped");

        Ok(())
    }

    /// Determine if this node is the Raft leader for the given table,
    /// consulting both the consistent hash ring and the actual Raft state.
    ///
    /// Returns `Ok(group)` if we should propose locally (we are the Raft
    /// leader), or `Err(leader_hint)` with the best-known leader node ID
    /// to proxy to.
    ///
    /// Logic:
    /// - If we have the RaftGroup, check `current_leader()` (non-blocking)
    /// - If Raft says we are the leader → propose locally
    /// - If Raft says someone else → proxy to Raft's leader
    /// - If Raft has no leader info → fall back to ring's preferred leader
    async fn resolve_leader(
        &self,
        table: &str,
    ) -> Result<Arc<RaftGroup>, NodeId> {
        let my_id = self.config.this_node.node_id;

        // First check: does the ring say we should be involved at all?
        // If the ring says we're not the preferred leader, defer to the
        // ring (or Raft) for the proxy target.
        if !self.router.am_i_preferred_leader(table) {
            // Check if Raft knows a different leader in a multi-node group
            // (e.g. after failover where we won the election).
            let groups = self.table_groups.read().await;
            if let Some(group) = groups.get(table) {
                if let Some(raft_leader) = group.current_leader() {
                    if raft_leader != my_id {
                        // Raft knows a different leader — prefer Raft's view.
                        return Err(raft_leader);
                    }
                    // Raft says we are the leader. Only trust this over the
                    // ring if the Raft group has multiple members (real
                    // multi-node election). A single-node group always
                    // self-elects, which doesn't override the ring.
                    let membership = group.raft().metrics().borrow().membership_config.clone();
                    let voter_count = membership
                        .membership()
                        .get_joint_config()
                        .iter()
                        .flat_map(|s| s.iter())
                        .collect::<std::collections::HashSet<_>>()
                        .len();
                    if voter_count > 1 {
                        // Real multi-node election — we won. Propose locally.
                        return Ok(group.clone());
                    }
                    // Single-node group self-elected — ring is authoritative.
                }
            }
            // No Raft info or single-node — use ring's preferred leader.
            return Err(self.router.preferred_leader(table));
        }

        // Ring says we are the preferred leader.
        let groups = self.table_groups.read().await;
        let group = match groups.get(table) {
            Some(g) => g.clone(),
            None => {
                // Ring says we're the leader but we don't have the group.
                return Err(self.router.preferred_leader(table));
            }
        };
        drop(groups);

        // Consult actual Raft state (non-blocking metrics check).
        if let Some(raft_leader) = group.current_leader() {
            if raft_leader == my_id {
                return Ok(group);
            }
            // Raft says someone else won — proxy to them.
            return Err(raft_leader);
        }

        // Raft has no leader info yet but ring says us — try proposing.
        // Raft will reject with ForwardToLeader if we're wrong.
        Ok(group)
    }

    /// Proxy a put request to the given leader node.
    async fn proxy_put(
        &self,
        table: &str,
        leader_id: NodeId,
        row: &Row,
    ) -> Result<PutResult, NodeError> {
        let channel = {
            let peers = self.peer_channels.read().await;
            peers.get(&leader_id).cloned()
        };
        if let Some(channel) = channel {
            use crate::proto::cluster as pb;
            use crate::proto::cluster::meru_cluster_client::MeruClusterClient;
            tracing::info!(table = %table, leader = %leader_id, "proxying put to leader");
            let proxy_start = Instant::now();
            let mut client = MeruClusterClient::new(channel);
            let proto_row = row_to_proto_row(row);
            let result = client
                .put(pb::PutRequest {
                    table: table.to_string(),
                    row: Some(proto_row),
                })
                .await;
            metrics::histogram!(PROXY_LATENCY_US)
                .record(proxy_start.elapsed().as_micros() as f64);
            match result {
                Ok(resp) => {
                    metrics::counter!(PROXY_ATTEMPTS_TOTAL, "result" => "success").increment(1);
                    let inner = resp.into_inner();
                    return Ok(PutResult {
                        seq: inner.seq,
                        raft_index: inner.raft_index,
                    });
                }
                Err(_) => {
                    metrics::counter!(PROXY_ATTEMPTS_TOTAL, "result" => "fail").increment(1);
                }
            }
        }
        metrics::counter!(NOT_LEADER_RETRIES_TOTAL).increment(1);
        Err(NodeError::NotLeader {
            leader_hint: leader_id,
        })
    }

    /// Proxy a delete request to the given leader node.
    async fn proxy_delete(
        &self,
        table: &str,
        leader_id: NodeId,
        pk: &[FieldValue],
    ) -> Result<DeleteResult, NodeError> {
        let channel = {
            let peers = self.peer_channels.read().await;
            peers.get(&leader_id).cloned()
        };
        if let Some(channel) = channel {
            use crate::proto::cluster as pb;
            use crate::proto::cluster::meru_cluster_client::MeruClusterClient;
            tracing::info!(table = %table, leader = %leader_id, "proxying delete to leader");
            let proxy_start = Instant::now();
            let mut client = MeruClusterClient::new(channel);
            let proto_pk = pk.iter().map(|fv| field_value_to_proto(fv)).collect();
            let result = client
                .delete(pb::DeleteRequest {
                    table: table.to_string(),
                    pk_values: proto_pk,
                })
                .await;
            metrics::histogram!(PROXY_LATENCY_US)
                .record(proxy_start.elapsed().as_micros() as f64);
            match result {
                Ok(resp) => {
                    metrics::counter!(PROXY_ATTEMPTS_TOTAL, "result" => "success").increment(1);
                    let inner = resp.into_inner();
                    return Ok(DeleteResult {
                        seq: inner.seq,
                        raft_index: inner.raft_index,
                    });
                }
                Err(_) => {
                    metrics::counter!(PROXY_ATTEMPTS_TOTAL, "result" => "fail").increment(1);
                }
            }
        }
        metrics::counter!(NOT_LEADER_RETRIES_TOTAL).increment(1);
        Err(NodeError::NotLeader {
            leader_hint: leader_id,
        })
    }

    /// Put a row into a table. Consults both ring and Raft state to
    /// determine the leader. Proxies to the actual Raft leader if this
    /// node is not the leader.
    pub async fn put(
        &self,
        table: &str,
        row: Row,
    ) -> Result<PutResult, NodeError> {
        match self.resolve_leader(table).await {
            Ok(group) => {
                // We are the Raft leader — propose locally.
                group.client_write(RaftCommand::Put { row }).await.map_err(|e| {
                    NodeError::MetadataError(format!("raft client_write failed: {e}"))
                })?;
                Ok(PutResult {
                    seq: group.sm().read_seq(),
                    raft_index: group.sm().applied_index(),
                })
            }
            Err(leader_id) => {
                self.proxy_put(table, leader_id, &row).await
            }
        }
    }

    /// Delete a row by primary key. Consults both ring and Raft state
    /// to determine the leader. Proxies to the actual Raft leader if
    /// this node is not the leader.
    pub async fn delete(
        &self,
        table: &str,
        pk: Vec<FieldValue>,
    ) -> Result<DeleteResult, NodeError> {
        match self.resolve_leader(table).await {
            Ok(group) => {
                group.client_write(RaftCommand::Delete { pk }).await.map_err(|e| {
                    NodeError::MetadataError(format!("raft client_write failed: {e}"))
                })?;
                Ok(DeleteResult {
                    seq: group.sm().read_seq(),
                    raft_index: group.sm().applied_index(),
                })
            }
            Err(leader_id) => {
                self.proxy_delete(table, leader_id, &pk).await
            }
        }
    }

    /// Point lookup by primary key. Works on any replica that has the SM.
    ///
    /// **Read semantics:** This is a *follower-safe read*, NOT a linearizable
    /// read. It reads directly from the local MeruDB without any Raft-level
    /// leader check. On a partitioned old leader, reads may return stale data
    /// until its `applied_index` stops advancing.
    ///
    /// For read-after-write consistency, use `get_at_index` with the
    /// `raft_index` returned by the preceding write, or use
    /// `MeruClient::get_after_write` which does this automatically.
    pub async fn get(
        &self,
        table: &str,
        pk: &[FieldValue],
    ) -> Result<Option<Row>, NodeError> {
        let groups = self.table_groups.read().await;
        let group = groups
            .get(table)
            .ok_or_else(|| NodeError::TableNotFound(table.to_string()))?
            .clone();
        drop(groups);

        Ok(group.sm().get(pk)?)
    }

    /// Point lookup that waits until `applied_index >= min_index` before reading.
    ///
    /// Returns the row (or None) after the wait succeeds, or `NodeError` if
    /// the table is not found or the wait times out.
    pub async fn get_at_index(
        &self,
        table: &str,
        pk: &[FieldValue],
        min_index: u64,
    ) -> Result<Option<Row>, NodeError> {
        let groups = self.table_groups.read().await;
        let group = groups
            .get(table)
            .ok_or_else(|| NodeError::TableNotFound(table.to_string()))?
            .clone();
        drop(groups);

        if min_index > 0 && group.sm().applied_index() < min_index {
            let reached = group
                .sm()
                .wait_for_applied(min_index, std::time::Duration::from_secs(5))
                .await;
            if !reached {
                return Err(NodeError::MetadataError(format!(
                    "timed out waiting for applied_index to reach {}",
                    min_index,
                )));
            }
        }

        Ok(group.sm().get(pk)?)
    }

    /// Range scan. Returns rows in PK order. Works on any replica
    /// that has the SM.
    pub async fn scan(
        &self,
        table: &str,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> Result<Vec<Row>, NodeError> {
        let groups = self.table_groups.read().await;
        let group = groups
            .get(table)
            .ok_or_else(|| NodeError::TableNotFound(table.to_string()))?
            .clone();
        drop(groups);

        Ok(group.sm().scan(start_pk, end_pk)?)
    }

    /// Alter a table's schema (e.g. add column). Applies the change to
    /// the table's SM, then updates the metadata SM.
    pub async fn alter_table(
        &self,
        table: &str,
        change: SchemaChange,
    ) -> Result<TableSchema, NodeError> {
        let group = match self.resolve_leader(table).await {
            Ok(g) => g,
            Err(leader_id) => {
                // Proxy alter_table to the leader.
                let channel = {
                    let peers = self.peer_channels.read().await;
                    peers.get(&leader_id).cloned()
                };
                if let Some(channel) = channel {
                    use crate::proto::cluster as pb;
                    use crate::proto::cluster::meru_cluster_client::MeruClusterClient;
                    let mut client = MeruClusterClient::new(channel);
                    let proto_change = match &change {
                        SchemaChange::AddColumn { col } => {
                            pb::alter_table_request::Change::AddColumn(column_def_to_proto(col))
                        }
                    };
                    let result = client
                        .alter_table(pb::AlterTableRequest {
                            table: table.to_string(),
                            change: Some(proto_change),
                        })
                        .await;
                    match result {
                        Ok(resp) => {
                            let inner = resp.into_inner();
                            if let Some(proto_schema) = inner.new_schema {
                                return proto_schema_to_schema(&proto_schema).map_err(Into::into);
                            }
                            return Err(NodeError::ProxyFailed(
                                "alter_table response missing new_schema".to_string(),
                            ));
                        }
                        Err(_) => {
                            return Err(NodeError::NotLeader {
                                leader_hint: leader_id,
                            });
                        }
                    }
                }
                return Err(NodeError::NotLeader {
                    leader_hint: leader_id,
                });
            }
        };

        // Capture the current schema before proposing the change, so we can
        // compute the evolved schema after the Raft write succeeds.
        let current_schema = group.sm().schema().clone();

        // Propose the schema change through Raft consensus.
        group
            .client_write(RaftCommand::AlterSchema {
                change: change.clone(),
            })
            .await
            .map_err(|e| {
                NodeError::MetadataError(format!("raft client_write failed: {e}"))
            })?;

        // Compute the evolved schema from the change that was applied.
        // We know it succeeded (Raft applied it without error), so we can
        // safely reconstruct the new schema.
        let new_schema = match change {
            SchemaChange::AddColumn { col } => {
                let mut evolved = current_schema;
                evolved.columns.push(col);
                evolved
            }
        };

        // Update metadata SM through Raft consensus.
        self.metadata_group
            .client_write(MetadataCommand::UpdateSchema {
                table: table.to_string(),
                schema: new_schema.clone(),
            })
            .await
            .map_err(|e| {
                NodeError::MetadataError(format!("metadata raft write failed: {e}"))
            })?;
        // Cache is updated atomically inside apply_to_state_machine.
        Ok(new_schema)
    }

    /// Handle a ring change: diff old vs new replica sets for all tables
    /// and orchestrate Raft membership changes for affected tables.
    ///
    /// Only the current Raft leader of each table drives the change.
    /// Concurrency is capped at 10 in-flight membership changes.
    ///
    /// GAP-12: Ring-rebalance orchestration.
    pub async fn on_ring_change(&self, old_rings: &PerAzRings) {
        use crate::observability::RING_TABLES_PER_NODE;

        let my_id = self.config.this_node.node_id;
        let all_tables = self.metadata_cache.all_tables();
        let new_rings = self.metadata_cache.rings();

        // Emit tables-per-node metric for this node after the ring change.
        let my_table_count = all_tables
            .iter()
            .filter(|t| new_rings.replicas_for_table(t).contains(&my_id))
            .count();
        metrics::gauge!(RING_TABLES_PER_NODE).set(my_table_count as f64);

        for table in &all_tables {
            let old_replicas = old_rings.replicas_for_table(table);
            let new_replicas = new_rings.replicas_for_table(table);

            if old_replicas == new_replicas {
                continue;
            }

            // Only the current Raft leader of this table orchestrates.
            let group = {
                let groups = self.table_groups.read().await;
                match groups.get(table) {
                    Some(g) => g.clone(),
                    None => continue,
                }
            };

            let raft_leader = group.current_leader();
            if raft_leader != Some(my_id) {
                continue;
            }

            let old_set: std::collections::BTreeSet<u64> =
                old_replicas.iter().copied().collect();
            let new_set: std::collections::BTreeSet<u64> =
                new_replicas.iter().copied().collect();

            let added: Vec<u64> = new_set.difference(&old_set).copied().collect();
            let removed: Vec<u64> = old_set.difference(&new_set).copied().collect();

            if added.is_empty() && removed.is_empty() {
                continue;
            }

            let sem = self.rebalance_semaphore.clone();
            let group = group.clone();
            let table_name = table.clone();

            tokio::spawn(async move {
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore closed
                };

                metrics::gauge!(REBALANCE_IN_PROGRESS).increment(1.0);

                // Step 1: Add new replicas as learners.
                for &node_id in &added {
                    if let Err(e) = group.add_learner(node_id, true).await {
                        tracing::warn!(
                            table = %table_name,
                            node_id = node_id,
                            error = %e,
                            "rebalance: add_learner failed"
                        );
                        metrics::gauge!(REBALANCE_IN_PROGRESS).decrement(1.0);
                        return;
                    }
                    tracing::info!(
                        table = %table_name,
                        node_id = node_id,
                        "rebalance: learner added and caught up"
                    );
                }

                // Step 2: Change membership to the new set.
                if let Err(e) = group.change_membership(new_set).await {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "rebalance: change_membership failed"
                    );
                    metrics::gauge!(REBALANCE_IN_PROGRESS).decrement(1.0);
                    return;
                }

                tracing::info!(
                    table = %table_name,
                    added = ?added,
                    removed = ?removed,
                    "rebalance: membership change complete"
                );
                metrics::gauge!(REBALANCE_IN_PROGRESS).decrement(1.0);
            });
        }
    }

    /// Query the role this node plays for a given table.
    pub fn table_role(&self, table: &str) -> TableRole {
        if !self.router.is_my_table(table) {
            return TableRole::NotMine;
        }
        if self.router.am_i_preferred_leader(table) {
            TableRole::Leader
        } else {
            TableRole::Follower
        }
    }

    /// Return a health summary for this node.
    pub async fn status(&self) -> NodeStatusInfo {
        let groups = self.table_groups.read().await;
        let mut tables = HashMap::new();
        for (name, group) in groups.iter() {
            let role = self.table_role(name);
            tables.insert(
                name.clone(),
                TableRoleInfo {
                    role,
                    committed_index: group.sm().committed_index(),
                    applied_index: group.sm().applied_index(),
                },
            );
        }
        NodeStatusInfo {
            node_id: self.config.this_node.node_id,
            address: self.config.this_node.address.clone(),
            az: self.config.this_node.az.clone(),
            table_count: groups.len(),
            tables,
        }
    }

    /// Access the router (for gRPC server to build response metadata).
    pub fn router(&self) -> &Arc<Router> {
        &self.router
    }

    /// Access the metadata cache.
    pub fn metadata_cache(&self) -> &Arc<LocalMetadataCache> {
        &self.metadata_cache
    }

    /// Gracefully shutdown the node: close all table state machines
    /// (flushing memtable data) and clear the table groups map.
    pub async fn shutdown(&self) {
        let mut groups = self.table_groups.write().await;
        for (name, group) in groups.iter() {
            if let Err(e) = group.sm().close().await {
                tracing::warn!(table = %name, error = %e, "error closing table SM during shutdown");
            }
        }
        groups.clear();

        // Signal all PeerBus flush loops to exit (GAP-24) and abort handles.
        {
            let buses = self.peer_buses.read().await;
            for bus in buses.values() {
                bus.shutdown();
            }
        }
        {
            let mut handles = self.bus_handles.lock().await;
            for handle in handles.drain(..) {
                handle.abort();
            }
        }

        tracing::info!("node shutdown: all table state machines and peer buses closed");
    }

    /// Access the config.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Check if we hold a local RaftGroup for the given table.
    pub async fn has_table_group(&self, table: &str) -> bool {
        self.table_groups.read().await.contains_key(table)
    }

    /// Get a specific RaftGroup by table name.
    pub async fn get_raft_group(&self, table: &str) -> Option<Arc<RaftGroup>> {
        self.table_groups.read().await.get(table).cloned()
    }

    /// Return the shared `Arc<RwLock<HashMap<...>>>` of table groups.
    ///
    /// Used by `RaftTransportService` to dispatch incoming Raft messages
    /// from peers to the correct local RaftGroup.
    pub fn shared_table_groups(
        &self,
    ) -> Arc<tokio::sync::RwLock<HashMap<String, Arc<RaftGroup>>>> {
        self.table_groups.clone()
    }

    // PeerBus + GrpcBatchTransport are now wired into the production path
    // via connect_peer + create_table_multi_node (GAP-19 resolved).

    /// Read-lock the table groups map. Used by the gRPC server to access
    /// individual RaftGroup state machines (e.g. for min_raft_index waits).
    pub async fn table_groups_ref(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<String, Arc<RaftGroup>>> {
        self.table_groups.read().await
    }

    /// Subscribe to the metadata event broadcast stream.
    pub fn subscribe_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<MetadataEvent> {
        self.watch_tx.subscribe()
    }

    /// Start a background task that watches for `RingChanged` events and
    /// orchestrates Raft membership changes for affected tables (GAP-12).
    ///
    /// Call this once after bootstrap. The task runs until the node shuts down.
    /// Start a background task that subscribes to `MetadataEvent::TableCreated`
    /// events and opens a local table Raft group whenever this node is in
    /// the replica set for the new table. This is how non-handler replicas
    /// (the nodes that didn't receive the `CreateTable` gRPC) catch up —
    /// they learn about the table through metadata Raft replication and
    /// then locally open their table group.
    pub fn start_table_opener(self: &Arc<Self>) {
        let node = Arc::clone(self);
        let mut rx = self.watch_tx.subscribe();
        let my_id = self.config.this_node.node_id;

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(MetadataEvent::TableCreated {
                        name,
                        schema,
                        replicas,
                        ..
                    }) => {
                        if !replicas.contains(&my_id) {
                            continue;
                        }
                        {
                            let groups = node.table_groups.read().await;
                            if groups.contains_key(&name) {
                                continue;
                            }
                        }
                        tracing::info!(
                            table = %name,
                            ?replicas,
                            "opening table group (triggered by metadata event)"
                        );
                        if let Err(e) = node
                            .open_table_group_from_event(name.clone(), schema, replicas)
                            .await
                        {
                            tracing::error!(
                                table = %name,
                                error = %e,
                                "failed to open table group from metadata event"
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    /// Open a local table Raft group using the replica set from a
    /// `TableCreated` event. Mirrors the group-opening half of
    /// `create_table_multi_node` but skips the metadata write (that already
    /// happened on the handler node).
    async fn open_table_group_from_event(
        self: &Arc<Self>,
        name: String,
        schema: merutable::schema::TableSchema,
        replicas: Vec<NodeId>,
    ) -> Result<(), NodeError> {
        let my_id = self.config.this_node.node_id;
        let mode = if self.router.am_i_preferred_leader(&name) {
            StateMachineMode::Leader
        } else {
            StateMachineMode::Follower
        };
        let table_dir = self.config.data_dir.join(&name);
        let opts = merutable::OpenOptions::new(schema)
            .wal_dir(table_dir.join("wal"))
            .catalog_uri(table_dir.join("data").to_string_lossy().to_string());
        let tsm = Arc::new(TableStateMachine::open(opts, mode).await?);

        let peer_bus_map = {
            let buses = self.peer_buses.read().await;
            let mut map = std::collections::HashMap::new();
            for &replica_id in &replicas {
                if replica_id == my_id {
                    continue;
                }
                if let Some(bus) = buses.get(&replica_id) {
                    map.insert(replica_id, Arc::clone(bus));
                }
            }
            Arc::new(map)
        };

        let raft_dir = self.config.raft_dir.join(&name);
        let group = RaftGroup::create_with_batched_network(
            &name,
            my_id,
            tsm,
            raft_dir,
            peer_bus_map,
        )
        .await
        .map_err(|e| NodeError::MetadataError(format!("RaftGroup init failed: {e}")))?;

        let group = Arc::new(group);
        {
            let mut groups = self.table_groups.write().await;
            groups.insert(name.clone(), group.clone());
            metrics::gauge!(NODE_TABLE_COUNT).set(groups.len() as f64);
            metrics::gauge!(NODE_OPEN_RAFT_GROUPS).set(groups.len() as f64);
        }

        let all_members: std::collections::BTreeSet<u64> =
            replicas.iter().copied().collect();
        group
            .initialize(all_members)
            .await
            .map_err(|e| NodeError::MetadataError(format!("raft initialize failed: {e}")))?;

        tracing::info!(table = %name, role = ?mode, "table group opened from metadata event");
        Ok(())
    }

    pub fn start_rebalance_watcher(self: &Arc<Self>) {
        let node = Arc::clone(self);
        let mut rx = self.watch_tx.subscribe();

        tokio::spawn(async move {
            // Snapshot the current rings so we can diff on change.
            let mut prev_rings = (*node.metadata_cache.rings()).clone();

            loop {
                match rx.recv().await {
                    Ok(MetadataEvent::RingChanged { .. }) => {
                        // The cache already has the new rings (updated in
                        // apply_to_state_machine before the broadcast).
                        node.on_ring_change(&prev_rings).await;

                        // Snapshot the new rings for the next diff.
                        prev_rings = (*node.metadata_cache.rings()).clone();
                    }
                    Ok(_) => {
                        // Other events — ignore.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Dropped events — re-snapshot and continue.
                        prev_rings = (*node.metadata_cache.rings()).clone();
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });
    }

    /// Start a background task that polls Raft metrics for each table group
    /// every second, detecting leadership transitions and emitting:
    /// - `RAFT_ELECTIONS_TOTAL` — incremented on each leader change
    /// - `RAFT_LEADER_TENURE_SECS` — seconds since this node became leader
    pub fn start_leader_metrics_sampler(self: &Arc<Self>) {
        let node = Arc::clone(self);
        let my_id = self.config.this_node.node_id;

        tokio::spawn(async move {
            // table -> (last_known_leader, became_leader_at)
            let mut leader_state: HashMap<String, (Option<u64>, Option<Instant>)> =
                HashMap::new();

            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                let groups = node.table_groups.read().await;
                for (table, group) in groups.iter() {
                    let current_leader = group.current_leader();
                    let entry = leader_state
                        .entry(table.clone())
                        .or_insert((None, None));

                    if current_leader != entry.0 {
                        // Leader changed.
                        if current_leader.is_some() {
                            metrics::counter!(RAFT_ELECTIONS_TOTAL).increment(1);
                            tracing::info!(
                                table = %table,
                                old_leader = ?entry.0,
                                new_leader = ?current_leader,
                                "leadership transition"
                            );
                        }
                        entry.0 = current_leader;
                        // Track when we became leader.
                        if current_leader == Some(my_id) {
                            entry.1 = Some(Instant::now());
                        } else {
                            entry.1 = None;
                        }
                    }

                    // Emit tenure if we are leader.
                    if let Some(since) = entry.1 {
                        metrics::gauge!(RAFT_LEADER_TENURE_SECS)
                            .set(since.elapsed().as_secs_f64());
                    }
                }

                // Clean up tables that were dropped.
                leader_state.retain(|t, _| groups.contains_key(t));
            }
        });
    }

    /// Connect to a peer node for write proxying.
    ///
    /// Returns `Ok(())` on success or `NodeError::ProxyFailed` if the
    /// connection could not be established, so the caller can log/handle it.
    pub async fn connect_peer(&self, node_id: NodeId, address: &str) -> Result<(), NodeError> {
        let endpoint = tonic::transport::Channel::from_shared(format!(
            "http://{}",
            address
        ))
        .map_err(|e| {
            NodeError::ProxyFailed(format!(
                "invalid peer address '{}': {}", address, e
            ))
        })?
        // TCP_NODELAY disables Nagle's algorithm. Without this, the kernel
        // holds small frames up to ~40ms waiting to coalesce with more
        // data. Raft AppendEntries are small (~350 bytes) and latency-
        // critical, so Nagle hurts both idle heartbeat tail and burst
        // throughput. Neutral at low concurrency, measurable win at high.
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        // Larger flow-control windows let openraft's replication loop keep
        // more AppendEntries in flight before HTTP/2 backpressure stalls
        // the stream. 4 MB covers ~1000 concurrent 4 KB entries.
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024));
        let channel = endpoint.connect().await.map_err(|e| {
            NodeError::ProxyFailed(format!(
                "failed to connect to peer {} at '{}': {}", node_id, address, e
            ))
        })?;
        self.peer_channels.write().await.insert(node_id, channel.clone());

        // Create a PeerBus for batched Raft transport to this peer and
        // spawn its flush_loop so messages are coalesced and flushed on
        // the 100ms timer or immediately for votes / high-water.
        let transport = GrpcBatchTransport::new(channel);
        let bus = Arc::new(PeerBus::new(
            node_id,
            self.config.this_node.node_id,
            Box::new(transport),
        ));
        let bus_clone = Arc::clone(&bus);
        let handle = tokio::spawn(async move {
            bus_clone.flush_loop().await;
        });
        self.peer_buses.write().await.insert(node_id, bus);
        self.bus_handles.lock().await.push(handle);

        Ok(())
    }

    /// Access the metadata Raft group.
    ///
    /// Used by `RaftTransportService` to dispatch incoming Raft messages
    /// from peers for the metadata group.
    pub fn metadata_raft_group(&self) -> &Arc<MetadataRaftGroup> {
        &self.metadata_group
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, NodeIdentity};
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};
    use merutable::value::{FieldValue, Row};

    fn test_schema(name: &str) -> TableSchema {
        TableSchema {
            table_name: name.to_string(),
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

    fn make_row(id: i64, val: &str) -> Row {
        Row::new(vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Bytes(bytes::Bytes::from(val.to_string()))),
        ])
    }

    /// Build a ClusterConfig whose `this_node` is `node_id` with the given AZ.
    fn make_config(
        node_id: NodeId,
        az: &str,
        tmp: &tempfile::TempDir,
    ) -> ClusterConfig {
        ClusterConfig {
            this_node: NodeIdentity {
                node_id,
                address: format!("127.0.0.1:{}", 9000 + node_id),
                az: az.to_string(),
            },
            seed_nodes: vec![],
            data_dir: tmp.path().join("data"),
            raft_dir: tmp.path().join("raft"),
            grpc_port: (9000 + node_id) as u16,
        }
    }

    /// Register 6 nodes (2 per AZ) and return the set of peer entries
    /// (excluding `this_node_id`).
    fn peer_entries(this_node_id: NodeId) -> Vec<NodeEntry> {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut entries = Vec::new();
        for (az_idx, az) in azs.iter().enumerate() {
            for i in 0..2 {
                let nid = (az_idx * 100 + i + 1) as NodeId;
                if nid != this_node_id {
                    entries.push(NodeEntry {
                        node_id: nid,
                        address: format!("10.0.{}.{}:9000", az_idx + 1, i + 1),
                        az: az.to_string(),
                        table_count: 0,
                        status: NodeStatus::Active,
                    });
                }
            }
        }
        entries
    }

    /// Build a Node and register all 6 nodes. Returns the node and a
    /// table name that this node IS the preferred leader for.
    async fn setup_node_with_leader_table(
        tmp: &tempfile::TempDir,
    ) -> (Node, String) {
        // We need to find a (node_id, table_name) pair where the node
        // IS the preferred leader. Try node 1 in AZ-1 first.
        let node_id: NodeId = 1;
        let az = "AZ-1";
        let config = make_config(node_id, az, tmp);
        let node = Node::new(config).await;
        node.register_self().await;
        for entry in peer_entries(node_id) {
            node.register_node(entry).await;
        }

        // Find a table name where node 1 is the preferred leader.
        let mut table_name = String::new();
        for i in 0..500 {
            let candidate = format!("test_table_{}", i);
            if node.router.am_i_preferred_leader(&candidate) {
                table_name = candidate;
                break;
            }
        }
        assert!(
            !table_name.is_empty(),
            "could not find a table where node 1 is preferred leader"
        );
        (node, table_name)
    }

    /// Build a Node and register all 6 nodes. Returns the node and a
    /// table name that this node is NOT the preferred leader for.
    async fn setup_node_with_follower_table(
        tmp: &tempfile::TempDir,
    ) -> (Node, String) {
        let node_id: NodeId = 1;
        let az = "AZ-1";
        let config = make_config(node_id, az, tmp);
        let node = Node::new(config).await;
        node.register_self().await;
        for entry in peer_entries(node_id) {
            node.register_node(entry).await;
        }

        // Find a table name where node 1 is a replica but NOT the leader.
        let mut table_name = String::new();
        for i in 0..500 {
            let candidate = format!("test_table_{}", i);
            if node.router.is_my_table(&candidate)
                && !node.router.am_i_preferred_leader(&candidate)
            {
                table_name = candidate;
                break;
            }
        }
        assert!(
            !table_name.is_empty(),
            "could not find a table where node 1 is follower"
        );
        (node, table_name)
    }

    #[tokio::test]
    async fn create_table_opens_sm() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        assert!(
            node.has_table_group(&table_name).await,
            "node should have a RaftGroup for the table"
        );
    }

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        let row = make_row(42, "hello");
        let put_result = node.put(&table_name, row).await.unwrap();
        assert!(put_result.seq > 0);

        let got = node.get(&table_name, &[FieldValue::Int64(42)]).await.unwrap();
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(
            *got.get(1).unwrap(),
            FieldValue::Bytes(bytes::Bytes::from("hello"))
        );
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        let row = make_row(7, "to_delete");
        node.put(&table_name, row).await.unwrap();

        // Verify it's there.
        assert!(
            node.get(&table_name, &[FieldValue::Int64(7)])
                .await
                .unwrap()
                .is_some()
        );

        // Delete.
        let del = node
            .delete(&table_name, vec![FieldValue::Int64(7)])
            .await
            .unwrap();
        assert!(del.seq > 0);

        // Verify it's gone.
        assert!(
            node.get(&table_name, &[FieldValue::Int64(7)])
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn alter_table_adds_column() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        let new_col = ColumnDef {
            name: "extra".into(),
            col_type: ColumnType::Double,
            nullable: true,
            ..Default::default()
        };
        let new_schema = node
            .alter_table(
                &table_name,
                SchemaChange::AddColumn { col: new_col },
            )
            .await
            .unwrap();

        assert_eq!(new_schema.columns.len(), 3);
        assert_eq!(new_schema.columns[2].name, "extra");

        // Verify the metadata cache was updated.
        let cached = node.metadata_cache().schema_for(&table_name);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().columns.len(), 3);
    }

    #[tokio::test]
    async fn put_to_wrong_node_returns_not_leader() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_follower_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        let row = make_row(1, "should_fail");
        let result = node.put(&table_name, row).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            NodeError::NotLeader { leader_hint } => {
                assert_ne!(
                    leader_hint,
                    node.config().this_node.node_id,
                    "leader hint should point to a different node"
                );
            }
            other => panic!("expected NotLeader, got: {}", other),
        }
    }

    #[tokio::test]
    async fn drop_table_closes_sm() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();
        assert!(node.has_table_group(&table_name).await);

        node.drop_table(table_name.clone()).await.unwrap();
        assert!(
            !node.has_table_group(&table_name).await,
            "RaftGroup should be removed after drop_table"
        );
    }

    /// GAP-4 test: subscribe to metadata events, create a table, and
    /// verify that a TableCreated event is received via the broadcast.
    #[tokio::test]
    async fn watch_events_delivered() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        // Subscribe BEFORE creating the table.
        let mut rx = node.subscribe_events();

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema)
            .await
            .unwrap();

        // We should receive the TableCreated event.
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx.recv(),
        )
        .await
        .expect("timed out waiting for event")
        .expect("broadcast channel closed");

        match event {
            MetadataEvent::TableCreated { name, .. } => {
                assert_eq!(name, table_name);
            }
            other => panic!(
                "expected TableCreated for '{}', got {:?}",
                table_name, other
            ),
        }
    }

    /// GAP-5 test: a follower node without a peer channel for the
    /// leader returns NotLeader cleanly (the proxy path is exercised
    /// but falls through because no channel exists).
    #[tokio::test]
    async fn proxy_forwards_put() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_follower_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema)
            .await
            .unwrap();

        // The node has no peer_channels configured, so the proxy
        // attempt should fall through to NotLeader.
        let row = make_row(1, "proxy_test");
        let result = node.put(&table_name, row).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            NodeError::NotLeader { leader_hint } => {
                // The leader_hint should point to a different node.
                assert_ne!(
                    leader_hint,
                    node.config().this_node.node_id,
                    "leader hint should not be self"
                );
            }
            other => panic!("expected NotLeader, got: {}", other),
        }

        // Also verify delete follows the same path.
        let del_result =
            node.delete(&table_name, vec![FieldValue::Int64(1)]).await;
        assert!(del_result.is_err());
        match del_result.unwrap_err() {
            NodeError::NotLeader { leader_hint } => {
                assert_ne!(leader_hint, node.config().this_node.node_id);
            }
            other => panic!("expected NotLeader, got: {}", other),
        }
    }

    /// GAP-15 test: follower node without peer channel returns NotLeader
    /// for alter_table (existing behavior, verify it doesn't panic).
    #[tokio::test]
    async fn alter_table_proxy_returns_not_leader_without_channel() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_follower_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema)
            .await
            .unwrap();

        // Try to alter_table on a follower with no peer channels.
        let new_col = merutable::schema::ColumnDef {
            name: "extra".into(),
            col_type: merutable::schema::ColumnType::Double,
            nullable: true,
            ..Default::default()
        };
        let result = node
            .alter_table(
                &table_name,
                SchemaChange::AddColumn { col: new_col },
            )
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            NodeError::NotLeader { leader_hint } => {
                assert_ne!(
                    leader_hint,
                    node.config().this_node.node_id,
                    "leader hint should not be self"
                );
            }
            other => panic!("expected NotLeader, got: {}", other),
        }
    }

    /// GAP-16 test: create table, put 5 rows, scan with no bounds,
    /// verify 5 rows returned at the Node level.
    #[tokio::test]
    async fn scan_returns_all_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, table_name) = setup_node_with_leader_table(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema).await.unwrap();

        // Put 5 rows.
        for i in 1..=5i64 {
            let row = make_row(i, &format!("scan_val_{}", i));
            node.put(&table_name, row).await.unwrap();
        }

        // Scan with no bounds.
        let rows = node.scan(&table_name, None, None).await.unwrap();
        assert_eq!(
            rows.len(),
            5,
            "scan should return 5 rows, got {}",
            rows.len()
        );
    }
}
