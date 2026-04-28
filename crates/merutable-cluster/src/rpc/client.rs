// P1.7: MeruClient — ring-based routing, leader cache, failover.

use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tonic::transport::Channel;

use crate::config::{NodeId, NodeIdentity};
use crate::proto::cluster as pb;
use crate::proto::cluster::meru_cluster_client::MeruClusterClient;
use crate::proto_convert::{
    column_def_to_proto, field_value_to_proto, proto_row_to_row, proto_schema_to_schema,
    row_to_proto_row, schema_to_proto, ProtoConvertError,
};
use crate::raft::types::SchemaChange;
use crate::ring::per_az::PerAzRings;

use merutable::schema::TableSchema;
use merutable::value::{FieldValue, Row};

impl From<ProtoConvertError> for ClientError {
    fn from(e: ProtoConvertError) -> Self {
        ClientError::Proto(e.0)
    }
}

// ---------------------------------------------------------------------------
// ClientError
// ---------------------------------------------------------------------------

/// Errors returned by `MeruClient` operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("all replicas failed for table: {0}")]
    AllReplicasFailed(String),

    #[error("not leader: preferred leader is node {0}")]
    NotLeader(NodeId),

    #[error("proto conversion error: {0}")]
    Proto(String),
}

impl ClientError {
    /// Returns `true` if the error indicates a not-leader condition.
    pub fn is_not_leader(&self) -> bool {
        match self {
            ClientError::NotLeader(_) => true,
            ClientError::Grpc(s)
                if s.code() == tonic::Code::FailedPrecondition =>
            {
                true
            }
            _ => false,
        }
    }

    /// Returns `true` if the error indicates the node is unavailable.
    pub fn is_unavailable(&self) -> bool {
        match self {
            ClientError::Transport(_) => true,
            ClientError::Grpc(s) if s.code() == tonic::Code::Unavailable => {
                true
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// PutResult / DeleteResult (client-side view)
// ---------------------------------------------------------------------------

/// Result of a successful `put` operation.
#[derive(Debug)]
pub struct PutResult {
    pub seq: u64,
    pub raft_index: u64,
}

/// Result of a successful `delete` operation.
#[derive(Debug)]
pub struct DeleteResult {
    pub seq: u64,
    pub raft_index: u64,
}

// ---------------------------------------------------------------------------
// MeruClient
// ---------------------------------------------------------------------------

/// Client for a merutable cluster.
///
/// Uses ring-based routing to send requests directly to the preferred leader.
/// Maintains a leader-override cache for failover: if the preferred leader is
/// down, the client tries other replicas and caches the override until the
/// preferred leader returns.
pub struct MeruClient {
    /// Per-AZ rings for zero-lookup routing.
    rings: Arc<ArcSwap<PerAzRings>>,
    /// gRPC channels to all known nodes: node_id -> Channel.
    channels: Arc<DashMap<NodeId, Channel>>,
    /// Address -> node_id mapping (for resolving leader hints).
    addr_to_node: Arc<DashMap<String, NodeId>>,
    /// Temporary leader overrides: table -> actual leader node_id.
    /// Set on failover, cleared when the preferred leader returns.
    leader_overrides: Arc<DashMap<String, NodeId>>,
    /// The last raft_index seen per table (for read-after-write).
    last_write_index: Arc<DashMap<String, u64>>,
    /// Handle for the background watch_leader task (if started).
    watch_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MeruClient {
    /// Connect to a cluster given an explicit list of node identities.
    ///
    /// Builds per-AZ rings and establishes gRPC channels to every node.
    /// For Phase 1 there is no dynamic discovery — the caller must supply
    /// the full node list.
    pub async fn connect(
        nodes: Vec<NodeIdentity>,
    ) -> Result<Self, ClientError> {
        let rings = PerAzRings::new(&nodes, 128);
        let channels = DashMap::new();
        let addr_to_node = DashMap::new();

        for node in &nodes {
            let endpoint =
                tonic::transport::Endpoint::from_shared(format!(
                    "http://{}",
                    node.address
                ))
                .map_err(|e| {
                    ClientError::Transport(
                        tonic::transport::Error::from(e),
                    )
                })?
                .tcp_nodelay(true)
                .http2_adaptive_window(true)
                .initial_stream_window_size(Some(4 * 1024 * 1024))
                .initial_connection_window_size(Some(8 * 1024 * 1024));
            let channel = endpoint.connect().await?;
            channels.insert(node.node_id, channel);
            addr_to_node.insert(node.address.clone(), node.node_id);
        }

        Ok(Self {
            rings: Arc::new(ArcSwap::from_pointee(rings)),
            channels: Arc::new(channels),
            addr_to_node: Arc::new(addr_to_node),
            leader_overrides: Arc::new(DashMap::new()),
            last_write_index: Arc::new(DashMap::new()),
            watch_handle: std::sync::Mutex::new(None),
        })
    }

    // -----------------------------------------------------------------------
    // Routing helpers
    // -----------------------------------------------------------------------

    /// Get the target node for a write to this table.
    ///
    /// Checks the leader-override cache first (set during failover),
    /// then falls back to the ring-computed preferred leader.
    fn write_target(&self, table: &str) -> NodeId {
        if let Some(override_ref) = self.leader_overrides.get(table) {
            return *override_ref;
        }
        self.rings.load().leader_for_table(table)
    }

    /// Get a gRPC client for a specific node.
    fn client_for(
        &self,
        node_id: NodeId,
    ) -> Result<MeruClusterClient<Channel>, ClientError> {
        let channel = self
            .channels
            .get(&node_id)
            .ok_or(ClientError::NodeNotFound(node_id))?
            .clone();
        Ok(MeruClusterClient::new(channel))
    }

    /// Process a leader hint from a `ResponseMeta`.
    ///
    /// If the response's leader address matches the preferred leader, clear
    /// any override. Otherwise cache the override for future requests.
    fn process_leader_hint(
        &self,
        table: &str,
        meta: &Option<pb::ResponseMeta>,
    ) {
        if let Some(meta) = meta {
            let preferred = self.rings.load().leader_for_table(table);
            if let Some(node_ref) = self.addr_to_node.get(&meta.leader_addr) {
                let leader_node_id = *node_ref;
                if leader_node_id == preferred {
                    self.leader_overrides.remove(table);
                } else {
                    self.leader_overrides
                        .insert(table.to_string(), leader_node_id);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Leader watch
    // -----------------------------------------------------------------------

    /// Start a background task that subscribes to the `WatchLeader` gRPC
    /// stream on any connected node and updates `leader_overrides` as
    /// `LeaderEvent` messages arrive.
    ///
    /// This is best-effort: if the stream drops, the task logs a warning
    /// and exits (no retry in Phase 1). The client continues to work
    /// without the watch -- ring routing handles steady state.
    pub async fn start_watch(&self) -> Result<(), ClientError> {
        // Pick any connected node to stream from.
        let node_id = self
            .channels
            .iter()
            .next()
            .map(|entry| *entry.key())
            .ok_or(ClientError::Proto(
                "no channels available for watch".to_string(),
            ))?;

        let mut client = self.client_for(node_id)?;
        let stream = client
            .watch_leader(pb::WatchLeaderRequest { tables: vec![] })
            .await?
            .into_inner();

        let rings = self.rings.clone();
        let leader_overrides = self.leader_overrides.clone();
        let addr_to_node = self.addr_to_node.clone();

        let handle = tokio::spawn(async move {
            use tonic::codegen::tokio_stream::StreamExt;
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(event) => {
                        let table = &event.table;
                        let leader_node_id = event.leader_node_id;
                        let preferred = rings.load().leader_for_table(table);

                        // Also try to resolve by address for consistency.
                        let resolved_id = addr_to_node
                            .get(&event.leader_addr)
                            .map(|r| *r)
                            .unwrap_or(leader_node_id);

                        if resolved_id == preferred {
                            leader_overrides.remove(table);
                        } else {
                            leader_overrides
                                .insert(table.to_string(), resolved_id);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "watch_leader stream ended; leader overrides may become stale"
                        );
                        break;
                    }
                }
            }
        });

        // Store the handle so it can be dropped with the client.
        if let Ok(mut guard) = self.watch_handle.lock() {
            *guard = Some(handle);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Data operations
    // -----------------------------------------------------------------------

    /// Internal: attempt a single Put RPC to a specific node.
    async fn try_put(
        &self,
        node_id: NodeId,
        table: &str,
        row: &Row,
    ) -> Result<pb::PutResponse, ClientError> {
        let mut client = self.client_for(node_id)?;
        let resp = client
            .put(pb::PutRequest {
                table: table.to_string(),
                row: Some(row_to_proto_row(row)),
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Put a row into a table.
    ///
    /// Sends the write to the preferred leader (or cached override). On
    /// failure, tries other replicas in the table's replica set.
    pub async fn put(
        &self,
        table: &str,
        row: Row,
    ) -> Result<PutResult, ClientError> {
        let target = self.write_target(table);

        match self.try_put(target, table, &row).await {
            Ok(resp) => {
                self.process_leader_hint(table, &resp.meta);
                self.last_write_index
                    .insert(table.to_string(), resp.raft_index);
                Ok(PutResult {
                    seq: resp.seq,
                    raft_index: resp.raft_index,
                })
            }
            Err(e) if e.is_not_leader() || e.is_unavailable() => {
                // Failover: try other replicas.
                let replicas = self.rings.load().replicas_for_table(table);
                for &node_id in &replicas {
                    if node_id == target {
                        continue;
                    }
                    match self.try_put(node_id, table, &row).await {
                        Ok(resp) => {
                            // Cache the responding node as the override.
                            if let Some(meta) = &resp.meta {
                                if let Some(node_ref) =
                                    self.addr_to_node.get(&meta.leader_addr)
                                {
                                    self.leader_overrides.insert(
                                        table.to_string(),
                                        *node_ref,
                                    );
                                }
                            }
                            self.last_write_index
                                .insert(table.to_string(), resp.raft_index);
                            return Ok(PutResult {
                                seq: resp.seq,
                                raft_index: resp.raft_index,
                            });
                        }
                        Err(_) => continue,
                    }
                }
                // All replicas failed — return the original error.
                Err(ClientError::AllReplicasFailed(table.to_string()))
            }
            Err(e) => Err(e),
        }
    }

    /// Internal: attempt a single Delete RPC to a specific node.
    async fn try_delete(
        &self,
        node_id: NodeId,
        table: &str,
        pk: &[FieldValue],
    ) -> Result<pb::DeleteResponse, ClientError> {
        let mut client = self.client_for(node_id)?;
        let pk_values = pk.iter().map(field_value_to_proto).collect();
        let resp = client
            .delete(pb::DeleteRequest {
                table: table.to_string(),
                pk_values,
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Delete a row from a table by primary key.
    ///
    /// Uses the same failover logic as `put`.
    pub async fn delete(
        &self,
        table: &str,
        pk: Vec<FieldValue>,
    ) -> Result<DeleteResult, ClientError> {
        let target = self.write_target(table);

        match self.try_delete(target, table, &pk).await {
            Ok(resp) => {
                self.process_leader_hint(table, &resp.meta);
                self.last_write_index
                    .insert(table.to_string(), resp.raft_index);
                Ok(DeleteResult {
                    seq: resp.seq,
                    raft_index: resp.raft_index,
                })
            }
            Err(e) if e.is_not_leader() || e.is_unavailable() => {
                let replicas = self.rings.load().replicas_for_table(table);
                for &node_id in &replicas {
                    if node_id == target {
                        continue;
                    }
                    match self.try_delete(node_id, table, &pk).await {
                        Ok(resp) => {
                            if let Some(meta) = &resp.meta {
                                if let Some(node_ref) =
                                    self.addr_to_node.get(&meta.leader_addr)
                                {
                                    self.leader_overrides.insert(
                                        table.to_string(),
                                        *node_ref,
                                    );
                                }
                            }
                            self.last_write_index
                                .insert(table.to_string(), resp.raft_index);
                            return Ok(DeleteResult {
                                seq: resp.seq,
                                raft_index: resp.raft_index,
                            });
                        }
                        Err(_) => continue,
                    }
                }
                Err(ClientError::AllReplicasFailed(table.to_string()))
            }
            Err(e) => Err(e),
        }
    }

    /// Get a single row by primary key.
    ///
    /// **Read semantics:** This is a *follower-safe read*, NOT a linearizable
    /// read. The serving node reads directly from its local MeruDB without a
    /// Raft-level leader check. On a partitioned old leader, reads may return
    /// stale data.
    ///
    /// For read-after-write consistency, use `get_after_write` which passes
    /// the last observed `raft_index` as `min_raft_index`, causing the server
    /// to wait until the serving node has applied at least that index.
    pub async fn get(
        &self,
        table: &str,
        pk: &[FieldValue],
    ) -> Result<Option<Row>, ClientError> {
        self.get_with_min_index(table, pk, 0).await
    }

    /// Get a row with read-after-write consistency.
    ///
    /// Uses the last observed raft_index for this table as `min_raft_index`
    /// so the serving node waits until it has applied at least that index.
    pub async fn get_after_write(
        &self,
        table: &str,
        pk: &[FieldValue],
    ) -> Result<Option<Row>, ClientError> {
        let min_index = self
            .last_write_index
            .get(table)
            .map(|v| *v)
            .unwrap_or(0);
        self.get_with_min_index(table, pk, min_index).await
    }

    /// Internal: Get with an explicit min_raft_index.
    async fn get_with_min_index(
        &self,
        table: &str,
        pk: &[FieldValue],
        min_raft_index: u64,
    ) -> Result<Option<Row>, ClientError> {
        let target = self.write_target(table);
        let mut client = self.client_for(target)?;
        let pk_values = pk.iter().map(field_value_to_proto).collect();

        let resp = client
            .get(pb::GetRequest {
                table: table.to_string(),
                pk_values,
                min_raft_index,
            })
            .await?;

        let inner = resp.into_inner();
        self.process_leader_hint(table, &inner.meta);

        if inner.found {
            match inner.row {
                Some(r) => Ok(Some(proto_row_to_row(&r)?)),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Scan rows in a table within an optional primary key range.
    pub async fn scan(
        &self,
        table: &str,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> Result<Vec<Row>, ClientError> {
        let target = self.write_target(table);
        let mut client = self.client_for(target)?;

        let start = start_pk
            .map(|pk| pk.iter().map(field_value_to_proto).collect())
            .unwrap_or_default();
        let end = end_pk
            .map(|pk| pk.iter().map(field_value_to_proto).collect())
            .unwrap_or_default();

        let resp = client
            .scan(pb::ScanRequest {
                table: table.to_string(),
                start_pk: start,
                end_pk: end,
            })
            .await?;

        let inner = resp.into_inner();
        self.process_leader_hint(table, &inner.meta);

        inner
            .rows
            .iter()
            .map(|r| proto_row_to_row(r).map_err(Into::into))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Table management
    // -----------------------------------------------------------------------

    /// Create a new table.
    ///
    /// create_table targets the metadata Raft leader, which is not
    /// necessarily the ring-preferred node for this table. On
    /// `FailedPrecondition` (not leader) or unavailable, retry across
    /// every connected node until one accepts the write.
    pub async fn create_table(
        &self,
        name: &str,
        schema: TableSchema,
    ) -> Result<(), ClientError> {
        let schema_proto = schema_to_proto(&schema);
        let first_target = self.write_target(name);
        let mut last_err: Option<ClientError> = None;
        let mut tried: Vec<NodeId> = Vec::new();

        let try_one = |node_id: NodeId| -> Option<
            std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<pb::CreateTableResponse, ClientError>>
                        + Send
                        + '_,
                >,
            >,
        > {
            let client_res = self.client_for(node_id);
            let schema_proto = schema_proto.clone();
            let name = name.to_string();
            Some(Box::pin(async move {
                let mut client = client_res?;
                let resp = client
                    .create_table(pb::CreateTableRequest {
                        name,
                        schema: Some(schema_proto),
                    })
                    .await?;
                Ok(resp.into_inner())
            }))
        };

        // Preferred target first.
        if let Some(fut) = try_one(first_target) {
            match fut.await {
                Ok(inner) => {
                    self.process_leader_hint(name, &inner.meta);
                    return Ok(());
                }
                Err(e) if e.is_not_leader() || e.is_unavailable() => {
                    tried.push(first_target);
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        // Failover to all other known nodes.
        let node_ids: Vec<NodeId> =
            self.channels.iter().map(|entry| *entry.key()).collect();
        for nid in node_ids {
            if tried.contains(&nid) {
                continue;
            }
            if let Some(fut) = try_one(nid) {
                match fut.await {
                    Ok(inner) => {
                        self.process_leader_hint(name, &inner.meta);
                        return Ok(());
                    }
                    Err(e) if e.is_not_leader() || e.is_unavailable() => {
                        last_err = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ClientError::AllReplicasFailed(name.to_string())
        }))
    }

    /// Drop a table.
    pub async fn drop_table(&self, name: &str) -> Result<(), ClientError> {
        let target = self.write_target(name);
        let mut client = self.client_for(target)?;

        let resp = client
            .drop_table(pb::DropTableRequest {
                name: name.to_string(),
            })
            .await?;

        let inner = resp.into_inner();
        self.process_leader_hint(name, &inner.meta);
        Ok(())
    }

    /// Alter a table schema (e.g. add a column).
    pub async fn alter_table(
        &self,
        table: &str,
        change: SchemaChange,
    ) -> Result<TableSchema, ClientError> {
        let target = self.write_target(table);
        let mut client = self.client_for(target)?;

        let change_proto = match change {
            SchemaChange::AddColumn { col } => {
                pb::alter_table_request::Change::AddColumn(
                    column_def_to_proto(&col),
                )
            }
        };

        let resp = client
            .alter_table(pb::AlterTableRequest {
                table: table.to_string(),
                change: Some(change_proto),
            })
            .await?;

        let inner = resp.into_inner();
        self.process_leader_hint(table, &inner.meta);

        let new_schema = inner
            .new_schema
            .ok_or_else(|| {
                ClientError::Proto(
                    "AlterTableResponse missing new_schema".to_string(),
                )
            })?;
        proto_schema_to_schema(&new_schema).map_err(Into::into)
    }

    /// List all tables in the cluster.
    ///
    /// Sends the request to any reachable node (the first channel in the map).
    pub async fn list_tables(&self) -> Result<Vec<TableSchema>, ClientError> {
        // Pick any node to ask — list_tables is not table-specific.
        let node_id = self
            .channels
            .iter()
            .next()
            .map(|entry| *entry.key())
            .ok_or(ClientError::Proto(
                "no channels available".to_string(),
            ))?;

        let mut client = self.client_for(node_id)?;

        let resp = client
            .list_tables(pb::ListTablesRequest {})
            .await?;

        let inner = resp.into_inner();
        inner
            .tables
            .iter()
            .map(|s| proto_schema_to_schema(s).map_err(Into::into))
            .collect()
    }

    /// Return the number of active leader overrides (for testing).
    ///
    /// A nonzero count means the watch stream or failover path has
    /// detected leaders that differ from the ring's preferred leader.
    pub fn leader_override_count(&self) -> usize {
        self.leader_overrides.len()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, NodeId, NodeIdentity};
    use crate::harness::node::Node;
    use crate::metadata::state_machine::{NodeEntry, NodeStatus as MetaNodeStatus};
    use crate::proto::cluster::meru_cluster_server::MeruClusterServer;
    use crate::rpc::server::MeruClusterService;
    use merutable::schema::{ColumnDef, ColumnType};

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
            grpc_port: 0,
        }
    }

    fn peer_entries(this_node_id: NodeId) -> Vec<NodeEntry> {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut entries = Vec::new();
        for (az_idx, az) in azs.iter().enumerate() {
            for i in 0..2 {
                let nid = (az_idx * 100 + i + 1) as NodeId;
                if nid != this_node_id {
                    entries.push(NodeEntry {
                        node_id: nid,
                        address: format!(
                            "10.0.{}.{}:9000",
                            az_idx + 1,
                            i + 1
                        ),
                        az: az.to_string(),
                        table_count: 0,
                        status: MetaNodeStatus::Active,
                    });
                }
            }
        }
        entries
    }

    /// Start a gRPC server on a random port and return:
    /// (server address, table name for which this node is preferred leader, Node)
    async fn start_server(
        tmp: &tempfile::TempDir,
    ) -> (String, String, Arc<Node>) {
        let node_id: NodeId = 1;
        let config = make_config(node_id, "AZ-1", tmp);
        let node = Arc::new(Node::new(config).await);
        node.register_self().await;
        for entry in peer_entries(node_id) {
            node.register_node(entry).await;
        }

        // Find a table name for which this node is the preferred leader.
        let mut table_name = String::new();
        for i in 0..500 {
            let candidate = format!("test_table_{}", i);
            if node.router().am_i_preferred_leader(&candidate) {
                table_name = candidate;
                break;
            }
        }
        assert!(!table_name.is_empty());

        let service = MeruClusterService::new(node.clone());

        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = format!("127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let incoming =
                tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(
                    listener,
                );
            tonic::transport::Server::builder()
                .add_service(MeruClusterServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        (addr_str, table_name, node)
    }

    /// Build a MeruClient connected to a single-node "cluster".
    async fn make_client(addr: &str) -> MeruClient {
        // For a single-node test cluster, we assign all 3 AZs to the same
        // node so that the client's ring routes everything to node 1.
        // However, the server only considers itself leader for tables
        // where am_i_preferred_leader is true under its own ring (which
        // has 6 nodes across 3 AZs). We need the client to use the SAME
        // ring as the server so routing agrees.
        //
        // The simplest approach: connect with a single node identity and
        // let the ring have just one AZ. Then leader_for_table always
        // returns node 1.
        let nodes = vec![NodeIdentity {
            node_id: 1,
            address: addr.to_string(),
            az: "AZ-1".to_string(),
        }];
        MeruClient::connect(nodes).await.unwrap()
    }

    // -----------------------------------------------------------------------
    // client_put_and_get
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_put_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, table_name, node) = start_server(&tmp).await;

        // Create the table directly on the node.
        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        let client = make_client(&addr).await;

        // Put via client.
        let row = Row::new(vec![
            Some(FieldValue::Int64(42)),
            Some(FieldValue::Bytes(bytes::Bytes::from_static(
                b"hello_client",
            ))),
        ]);
        let put_result = client.put(&table_name, row).await.unwrap();
        assert!(put_result.seq > 0);
        assert!(put_result.raft_index > 0);

        // Get via client.
        let got = client
            .get(&table_name, &[FieldValue::Int64(42)])
            .await
            .unwrap();
        assert!(got.is_some());
        let got_row = got.unwrap();
        assert_eq!(got_row.fields.len(), 2);
        match &got_row.fields[1] {
            Some(FieldValue::Bytes(b)) => {
                assert_eq!(b.as_ref(), b"hello_client");
            }
            other => panic!("expected Bytes, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // client_delete
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, table_name, node) = start_server(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        let client = make_client(&addr).await;

        // Put a row.
        let row = Row::new(vec![
            Some(FieldValue::Int64(7)),
            Some(FieldValue::Bytes(bytes::Bytes::from_static(b"to_delete"))),
        ]);
        client.put(&table_name, row).await.unwrap();

        // Verify it exists.
        let got = client
            .get(&table_name, &[FieldValue::Int64(7)])
            .await
            .unwrap();
        assert!(got.is_some());

        // Delete it.
        let del_result = client
            .delete(&table_name, vec![FieldValue::Int64(7)])
            .await
            .unwrap();
        assert!(del_result.seq > 0);

        // Verify it's gone.
        let got_after = client
            .get(&table_name, &[FieldValue::Int64(7)])
            .await
            .unwrap();
        assert!(got_after.is_none());
    }

    // -----------------------------------------------------------------------
    // client_scan
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, table_name, node) = start_server(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        let client = make_client(&addr).await;

        // Put 5 rows.
        for i in 1..=5i64 {
            let row = Row::new(vec![
                Some(FieldValue::Int64(i)),
                Some(FieldValue::Bytes(bytes::Bytes::from(
                    format!("scan_val_{}", i).into_bytes(),
                ))),
            ]);
            client.put(&table_name, row).await.unwrap();
        }

        // Scan with no bounds.
        let rows = client.scan(&table_name, None, None).await.unwrap();
        assert_eq!(
            rows.len(),
            5,
            "scan should return 5 rows, got {}",
            rows.len()
        );
    }

    // -----------------------------------------------------------------------
    // client_list_tables
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_list_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, table_name, node) = start_server(&tmp).await;

        // Create 2 tables directly on the node.
        // The first table is the one the node is leader for.
        let schema1 = test_schema(&table_name);
        node.create_table(table_name.clone(), schema1)
            .await
            .unwrap();

        // Find a second table name the node is also leader for.
        let mut table2_name = String::new();
        for i in 0..500 {
            let candidate = format!("list_test_{}", i);
            if node.router().am_i_preferred_leader(&candidate)
                && candidate != table_name
            {
                table2_name = candidate;
                break;
            }
        }
        assert!(!table2_name.is_empty());

        let schema2 = test_schema(&table2_name);
        node.create_table(table2_name.clone(), schema2)
            .await
            .unwrap();

        let client = make_client(&addr).await;
        let tables = client.list_tables().await.unwrap();

        let names: Vec<&str> =
            tables.iter().map(|t| t.table_name.as_str()).collect();
        assert!(
            names.contains(&table_name.as_str()),
            "list_tables should contain '{}'",
            table_name
        );
        assert!(
            names.contains(&table2_name.as_str()),
            "list_tables should contain '{}'",
            table2_name
        );
    }

    // -----------------------------------------------------------------------
    // client_get_after_write
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_get_after_write() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, table_name, node) = start_server(&tmp).await;

        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        let client = make_client(&addr).await;

        // Put a row — this records the raft_index in last_write_index.
        let row = Row::new(vec![
            Some(FieldValue::Int64(100)),
            Some(FieldValue::Bytes(bytes::Bytes::from_static(b"raw"))),
        ]);
        let put_result = client.put(&table_name, row).await.unwrap();
        assert!(put_result.raft_index > 0);

        // Verify last_write_index was updated.
        let stored_index = client
            .last_write_index
            .get(&table_name)
            .map(|v| *v)
            .unwrap_or(0);
        assert_eq!(stored_index, put_result.raft_index);

        // get_after_write should find the row (single-node, so already applied).
        let got = client
            .get_after_write(&table_name, &[FieldValue::Int64(100)])
            .await
            .unwrap();
        assert!(got.is_some());
        let got_row = got.unwrap();
        match &got_row.fields[1] {
            Some(FieldValue::Bytes(b)) => {
                assert_eq!(b.as_ref(), b"raw");
            }
            other => panic!("expected Bytes, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // client_failover_to_replica
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn client_failover_to_replica() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, _table_name, node) = start_server(&tmp).await;

        // Find a table name for which this node is NOT the preferred leader
        // under the server's ring. The server will return NotLeader.
        let mut non_leader_table = String::new();
        for i in 0..500 {
            let candidate = format!("failover_table_{}", i);
            if !node.router().am_i_preferred_leader(&candidate) {
                non_leader_table = candidate;
                break;
            }
        }
        assert!(
            !non_leader_table.is_empty(),
            "should find a table this node is not leader for"
        );

        // Create the table anyway (the node stores it, but won't accept
        // writes since it's not the leader).
        let schema = test_schema(&non_leader_table);
        node.create_table(non_leader_table.clone(), schema)
            .await
            .unwrap();

        let client = make_client(&addr).await;

        // The put should fail — the node returns NotLeader and there are no
        // other replicas to try (single-node cluster), so AllReplicasFailed.
        let row = Row::new(vec![
            Some(FieldValue::Int64(1)),
            Some(FieldValue::Bytes(bytes::Bytes::from_static(
                b"should_fail",
            ))),
        ]);
        let result = client.put(&non_leader_table, row).await;
        assert!(
            result.is_err(),
            "put to non-leader table should fail"
        );

        let err = result.unwrap_err();
        // Should be either FailedPrecondition (NotLeader) or AllReplicasFailed.
        assert!(
            err.is_not_leader()
                || matches!(err, ClientError::AllReplicasFailed(_)),
            "expected NotLeader or AllReplicasFailed, got: {:?}",
            err,
        );
    }
}
