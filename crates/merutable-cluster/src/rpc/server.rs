// P1.6: gRPC MeruCluster service implementation.

use std::sync::Arc;

use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::harness::node::{Node, NodeError, TableRole};
use crate::proto::cluster as pb;
use crate::proto::cluster::meru_cluster_server::MeruCluster;
use crate::proto_convert::{
    proto_column_def, proto_field_values_to_field_values, proto_row_to_row,
    proto_schema_to_schema, row_to_proto_row, schema_to_proto, ProtoConvertError,
};

impl From<ProtoConvertError> for Status {
    fn from(e: ProtoConvertError) -> Self {
        Status::invalid_argument(e.0)
    }
}

// ---------------------------------------------------------------------------
// Helper: map NodeError to gRPC Status
// ---------------------------------------------------------------------------

/// Returns true if the error message came from an openraft `ForwardToLeader`,
/// meaning the node contacted is not the current leader and the client should
/// retry on a different replica. openraft renders this as "has to forward
/// request to:" (Display) or contains "ForwardToLeader" (Debug).
fn is_forward_to_leader(msg: &str) -> bool {
    msg.contains("ForwardToLeader") || msg.contains("has to forward request to")
}

fn node_error_to_status(e: NodeError) -> Status {
    match e {
        NodeError::NotLeader { leader_hint } => Status::failed_precondition(
            format!("not leader; preferred leader node: {}", leader_hint),
        ),
        NodeError::TableNotFound(name) => {
            Status::not_found(format!("table not found: {}", name))
        }
        NodeError::TableAlreadyExists(name) => {
            Status::already_exists(format!("table already exists: {}", name))
        }
        NodeError::MetadataError(msg) => {
            if is_forward_to_leader(&msg) {
                Status::failed_precondition(format!("not leader (metadata): {}", msg))
            } else {
                Status::internal(format!("metadata error: {}", msg))
            }
        }
        NodeError::StorageError(e) => {
            Status::internal(format!("storage error: {}", e))
        }
        NodeError::InvalidChange(msg) => {
            Status::invalid_argument(format!("invalid change: {}", msg))
        }
        NodeError::ProxyFailed(msg) => {
            Status::unavailable(format!("proxy failed: {}", msg))
        }
    }
}

// ---------------------------------------------------------------------------
// MeruClusterService
// ---------------------------------------------------------------------------

pub struct MeruClusterService {
    node: Arc<Node>,
}

impl MeruClusterService {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    /// Build a ResponseMeta for a given table (leader address hint).
    fn response_meta(&self, table: &str) -> pb::ResponseMeta {
        let leader_id = self.node.router().preferred_leader(table);
        let leader_addr = self
            .node
            .metadata_cache()
            .node(leader_id)
            .map(|n| n.address.clone())
            .unwrap_or_default();
        pb::ResponseMeta {
            leader_addr,
            leader_token: 0,
            raft_term: 0,
        }
    }
}

#[tonic::async_trait]
impl MeruCluster for MeruClusterService {
    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let req = request.into_inner();
        let row = req
            .row
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing row"))?;
        let row = proto_row_to_row(row)?;

        let result = self
            .node
            .put(&req.table, row)
            .await
            .map_err(node_error_to_status)?;

        Ok(Response::new(pb::PutResponse {
            seq: result.seq,
            raft_index: result.raft_index,
            meta: Some(self.response_meta(&req.table)),
        }))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        let req = request.into_inner();
        let pk = proto_field_values_to_field_values(&req.pk_values)?;

        let result = self
            .node
            .delete(&req.table, pk)
            .await
            .map_err(node_error_to_status)?;

        Ok(Response::new(pb::DeleteResponse {
            seq: result.seq,
            raft_index: result.raft_index,
            meta: Some(self.response_meta(&req.table)),
        }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        let req = request.into_inner();
        let pk = proto_field_values_to_field_values(&req.pk_values)?;

        // GAP-14: wait for applied_index to reach min_raft_index if requested.
        if req.min_raft_index > 0 {
            let groups = self.node.table_groups_ref().await;
            if let Some(group) = groups.get(&req.table) {
                let sm = group.sm();
                if sm.applied_index() < req.min_raft_index {
                    let reached = sm
                        .wait_for_applied(
                            req.min_raft_index,
                            std::time::Duration::from_secs(5),
                        )
                        .await;
                    if !reached {
                        return Err(Status::deadline_exceeded(format!(
                            "timed out waiting for applied_index to reach {}",
                            req.min_raft_index,
                        )));
                    }
                }
            }
            drop(groups);
        }

        let result = self
            .node
            .get(&req.table, &pk)
            .await
            .map_err(node_error_to_status)?;

        let (found, row) = match result {
            Some(r) => (true, Some(row_to_proto_row(&r))),
            None => (false, None),
        };

        Ok(Response::new(pb::GetResponse {
            found,
            row,
            applied_index: 0,
            meta: Some(self.response_meta(&req.table)),
        }))
    }

    async fn scan(
        &self,
        request: Request<pb::ScanRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let req = request.into_inner();
        let start_pk = if req.start_pk.is_empty() {
            None
        } else {
            Some(proto_field_values_to_field_values(&req.start_pk)?)
        };
        let end_pk = if req.end_pk.is_empty() {
            None
        } else {
            Some(proto_field_values_to_field_values(&req.end_pk)?)
        };

        let rows = self
            .node
            .scan(
                &req.table,
                start_pk.as_deref(),
                end_pk.as_deref(),
            )
            .await
            .map_err(node_error_to_status)?;

        let proto_rows: Vec<pb::Row> = rows.iter().map(row_to_proto_row).collect();

        Ok(Response::new(pb::ScanResponse {
            rows: proto_rows,
            meta: Some(self.response_meta(&req.table)),
        }))
    }

    async fn create_table(
        &self,
        request: Request<pb::CreateTableRequest>,
    ) -> Result<Response<pb::CreateTableResponse>, Status> {
        let req = request.into_inner();
        let schema = req
            .schema
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing schema"))?;
        let schema = proto_schema_to_schema(schema)?;

        self.node
            .create_table(req.name.clone(), schema)
            .await
            .map_err(node_error_to_status)?;

        Ok(Response::new(pb::CreateTableResponse {
            meta: Some(self.response_meta(&req.name)),
        }))
    }

    async fn drop_table(
        &self,
        request: Request<pb::DropTableRequest>,
    ) -> Result<Response<pb::DropTableResponse>, Status> {
        let req = request.into_inner();

        self.node
            .drop_table(req.name.clone())
            .await
            .map_err(node_error_to_status)?;

        Ok(Response::new(pb::DropTableResponse {
            meta: Some(pb::ResponseMeta::default()),
        }))
    }

    async fn alter_table(
        &self,
        request: Request<pb::AlterTableRequest>,
    ) -> Result<Response<pb::AlterTableResponse>, Status> {
        let req = request.into_inner();
        let change = req
            .change
            .ok_or_else(|| Status::invalid_argument("missing change"))?;

        let schema_change = match change {
            pb::alter_table_request::Change::AddColumn(col_proto) => {
                let col = proto_column_def(&col_proto)?;
                crate::raft::types::SchemaChange::AddColumn { col }
            }
        };

        let new_schema = self
            .node
            .alter_table(&req.table, schema_change)
            .await
            .map_err(node_error_to_status)?;

        Ok(Response::new(pb::AlterTableResponse {
            new_schema: Some(schema_to_proto(&new_schema)),
            meta: Some(self.response_meta(&req.table)),
        }))
    }

    async fn list_tables(
        &self,
        _request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        let cache = self.node.metadata_cache();
        let table_names = cache.all_tables();
        let tables: Vec<pb::TableSchema> = table_names
            .iter()
            .filter_map(|name| cache.schema_for(name).map(|s| schema_to_proto(&s)))
            .collect();

        Ok(Response::new(pb::ListTablesResponse {
            tables,
            meta: Some(pb::ResponseMeta::default()),
        }))
    }

    async fn get_status(
        &self,
        _request: Request<pb::Empty>,
    ) -> Result<Response<pb::NodeStatus>, Status> {
        let info = self.node.status().await;
        let tables: std::collections::HashMap<String, pb::TableRoleInfo> = info
            .tables
            .into_iter()
            .map(|(name, tri)| {
                let role_str = match tri.role {
                    TableRole::Leader => "leader",
                    TableRole::Follower => "follower",
                    TableRole::NotMine => "none",
                };
                (
                    name,
                    pb::TableRoleInfo {
                        role: role_str.to_string(),
                        committed_index: tri.committed_index,
                        applied_index: tri.applied_index,
                        raft_term: 0,
                    },
                )
            })
            .collect();

        Ok(Response::new(pb::NodeStatus {
            node_id: info.node_id,
            address: info.address,
            az: info.az,
            is_healthy: true,
            table_count: info.table_count as u32,
            tables,
        }))
    }

    type WatchLeaderStream = ReceiverStream<Result<pb::LeaderEvent, Status>>;

    async fn watch_leader(
        &self,
        request: Request<pb::WatchLeaderRequest>,
    ) -> Result<Response<Self::WatchLeaderStream>, Status> {
        let req = request.into_inner();
        let filter_tables: std::collections::HashSet<String> =
            req.tables.into_iter().collect();
        let filter_all = filter_tables.is_empty();

        let mut bcast_rx = self.node.subscribe_events();
        let node = self.node.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            use crate::metadata::state_machine::MetadataEvent;
            use std::collections::HashMap;

            // GAP-9: snapshot the current leader for each table so we
            // can detect which leaders changed on RingChanged events.
            let mut prev_leaders: HashMap<String, u64> = HashMap::new();
            for table in node.metadata_cache().all_tables() {
                prev_leaders.insert(
                    table.clone(),
                    node.router().preferred_leader(&table),
                );
            }

            loop {
                match bcast_rx.recv().await {
                    Ok(event) => {
                        // Collect events to send. Most events produce 0 or
                        // 1 entries; RingChanged may produce many.
                        let mut events_to_send: Vec<pb::LeaderEvent> =
                            Vec::new();

                        match &event {
                            MetadataEvent::TableCreated { name, .. }
                            | MetadataEvent::SchemaUpdated {
                                table: name, ..
                            } => {
                                let leader_id =
                                    node.router().preferred_leader(name);
                                // Track newly created tables.
                                prev_leaders.insert(name.clone(), leader_id);
                                if filter_all
                                    || filter_tables.contains(name.as_str())
                                {
                                    let leader_addr = node
                                        .metadata_cache()
                                        .node(leader_id)
                                        .map(|n| n.address.clone())
                                        .unwrap_or_default();
                                    events_to_send.push(pb::LeaderEvent {
                                        table: name.clone(),
                                        leader_addr,
                                        leader_node_id: leader_id,
                                        raft_term: 0,
                                    });
                                }
                            }
                            MetadataEvent::TableDropped { name } => {
                                prev_leaders.remove(name.as_str());
                                if filter_all
                                    || filter_tables.contains(name.as_str())
                                {
                                    events_to_send.push(pb::LeaderEvent {
                                        table: name.clone(),
                                        leader_addr: String::new(),
                                        leader_node_id: 0,
                                        raft_term: 0,
                                    });
                                }
                            }
                            MetadataEvent::RingChanged { .. } => {
                                // GAP-9: iterate ALL tables, compare
                                // current preferred leader against previous.
                                let current_tables =
                                    node.metadata_cache().all_tables();

                                for table in &current_tables {
                                    let new_leader =
                                        node.router().preferred_leader(table);
                                    let old_leader =
                                        prev_leaders.get(table).copied();

                                    let changed = match old_leader {
                                        Some(old) => old != new_leader,
                                        None => true, // new table
                                    };

                                    if changed
                                        && (filter_all
                                            || filter_tables
                                                .contains(table.as_str()))
                                    {
                                        let leader_addr = node
                                            .metadata_cache()
                                            .node(new_leader)
                                            .map(|n| n.address.clone())
                                            .unwrap_or_default();
                                        events_to_send.push(
                                            pb::LeaderEvent {
                                                table: table.clone(),
                                                leader_addr,
                                                leader_node_id: new_leader,
                                                raft_term: 0,
                                            },
                                        );
                                    }

                                    prev_leaders
                                        .insert(table.clone(), new_leader);
                                }

                                // Handle tables that disappeared (in
                                // prev_leaders but not in current cache).
                                let current_set: std::collections::HashSet<
                                    &String,
                                > = current_tables.iter().collect();
                                let vanished: Vec<String> = prev_leaders
                                    .keys()
                                    .filter(|k| !current_set.contains(k))
                                    .cloned()
                                    .collect();
                                for table in vanished {
                                    prev_leaders.remove(&table);
                                }
                            }
                            MetadataEvent::OverrideSet { table, .. }
                            | MetadataEvent::OverrideRemoved { table } => {
                                let leader_id =
                                    node.router().preferred_leader(table);
                                prev_leaders
                                    .insert(table.clone(), leader_id);
                                if filter_all
                                    || filter_tables.contains(table.as_str())
                                {
                                    let leader_addr = node
                                        .metadata_cache()
                                        .node(leader_id)
                                        .map(|n| n.address.clone())
                                        .unwrap_or_default();
                                    events_to_send.push(pb::LeaderEvent {
                                        table: table.clone(),
                                        leader_addr,
                                        leader_node_id: leader_id,
                                        raft_term: 0,
                                    });
                                }
                            }
                        };

                        for evt in events_to_send {
                            if tx.send(Ok(evt)).await.is_err() {
                                return; // client disconnected
                            }
                        }
                    }
                    Err(
                        tokio::sync::broadcast::error::RecvError::Lagged(_),
                    ) => {
                        // Dropped some events; keep going.
                        continue;
                    }
                    Err(
                        tokio::sync::broadcast::error::RecvError::Closed,
                    ) => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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
    use crate::proto::cluster::meru_cluster_client::MeruClusterClient;
    use crate::proto::cluster::meru_cluster_server::MeruClusterServer;
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};

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
            grpc_port: 0, // will use OS-assigned
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
                        address: format!("10.0.{}.{}:9000", az_idx + 1, i + 1),
                        az: az.to_string(),
                        table_count: 0,
                        status: MetaNodeStatus::Active,
                    });
                }
            }
        }
        entries
    }

    /// Start a gRPC server and return the client and the table name for
    /// which this node is the preferred leader.
    async fn start_server(
        tmp: &tempfile::TempDir,
    ) -> (
        MeruClusterClient<tonic::transport::Channel>,
        String,
        Arc<Node>,
    ) {
        let node_id: NodeId = 1;
        let config = make_config(node_id, "AZ-1", tmp);
        let node = Arc::new(Node::new(config).await);
        node.register_self().await;
        for entry in peer_entries(node_id) {
            node.register_node(entry).await;
        }

        // Find a leader table.
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

        // Bind to 127.0.0.1:0 for OS-assigned port.
        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

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

        let client = MeruClusterClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        (client, table_name, node)
    }

    #[tokio::test]
    async fn grpc_put_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut client, table_name, node) = start_server(&tmp).await;

        // Create the table directly on the node (it's our leader table).
        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        // Put via gRPC.
        let put_resp = client
            .put(pb::PutRequest {
                table: table_name.clone(),
                row: Some(pb::Row {
                    fields: vec![
                        pb::OptionalFieldValue {
                            is_null: false,
                            value: Some(pb::FieldValue {
                                value: Some(pb::field_value::Value::Int64(99)),
                            }),
                        },
                        pb::OptionalFieldValue {
                            is_null: false,
                            value: Some(pb::FieldValue {
                                value: Some(pb::field_value::Value::ByteArray(
                                    b"grpc_hello".to_vec(),
                                )),
                            }),
                        },
                    ],
                }),
            })
            .await
            .unwrap();

        let put_inner = put_resp.into_inner();
        assert!(put_inner.seq > 0);

        // Get via gRPC.
        let get_resp = client
            .get(pb::GetRequest {
                table: table_name.clone(),
                pk_values: vec![pb::FieldValue {
                    value: Some(pb::field_value::Value::Int64(99)),
                }],
                min_raft_index: 0,
            })
            .await
            .unwrap();

        let get_inner = get_resp.into_inner();
        assert!(get_inner.found);
        let row = get_inner.row.unwrap();
        assert_eq!(row.fields.len(), 2);
        // Check the second field (val).
        let val_field = &row.fields[1];
        assert!(!val_field.is_null);
        match &val_field.value {
            Some(pb::FieldValue {
                value: Some(pb::field_value::Value::ByteArray(b)),
            }) => {
                assert_eq!(b, b"grpc_hello");
            }
            other => panic!("expected ByteArray, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn grpc_create_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut client, table_name, _node) = start_server(&tmp).await;

        // Create table via gRPC.
        let schema = test_schema(&table_name);
        let proto_schema = schema_to_proto(&schema);
        let resp = client
            .create_table(pb::CreateTableRequest {
                name: table_name.clone(),
                schema: Some(proto_schema),
            })
            .await
            .unwrap();
        assert!(resp.into_inner().meta.is_some());

        // ListTables should return it.
        let list_resp = client
            .list_tables(pb::ListTablesRequest {})
            .await
            .unwrap();

        let tables = list_resp.into_inner().tables;
        let found = tables
            .iter()
            .any(|t| t.table_name == table_name);
        assert!(
            found,
            "created table '{}' should appear in ListTables response",
            table_name
        );
    }

    /// GAP-9 test: register 6 nodes, register 50 tables in the metadata
    /// cache, subscribe to watch, register a 7th node (triggers RingChanged),
    /// verify at least one LeaderEvent is received for a table whose leader
    /// shifted.
    #[tokio::test]
    async fn watch_ring_change_emits_leader_events() {
        let tmp = tempfile::tempdir().unwrap();
        let node_id: NodeId = 1;
        let config = make_config(node_id, "AZ-1", &tmp);
        let node = Arc::new(Node::new(config).await);
        node.register_self().await;
        for entry in peer_entries(node_id) {
            node.register_node(entry).await;
        }

        // Register 50 tables in the metadata cache (we don't need actual
        // RaftGroups — the watch_leader handler only queries the cache
        // and the router).
        for i in 0..50 {
            let name = format!("ring_watch_{}", i);
            let schema = test_schema(&name);
            node.metadata_cache().update_schema(name, schema);
        }

        // Build the MeruClusterService and subscribe via the handler.
        let service = MeruClusterService::new(node.clone());
        let watch_req = tonic::Request::new(pb::WatchLeaderRequest {
            tables: vec![], // all tables
        });
        let mut stream =
            service.watch_leader(watch_req).await.unwrap().into_inner();

        // Give the spawned task a moment to snapshot prev_leaders.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Register a 7th node in AZ-1 — triggers RingChanged.
        // This should shift some AZ-1 vnodes, changing the leader for
        // some of the 50 tables.
        let new_node = NodeEntry {
            node_id: 3,
            address: "10.0.1.3:9000".to_string(),
            az: "AZ-1".to_string(),
            table_count: 0,
            status: MetaNodeStatus::Active,
        };
        node.register_node(new_node).await;

        // Collect LeaderEvents from the stream.
        use tonic::codegen::tokio_stream::StreamExt as _;
        let mut leader_events = Vec::new();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(2);
        loop {
            let remaining = deadline - tokio::time::Instant::now();
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(evt))) => {
                    leader_events.push(evt);
                }
                _ => break,
            }
        }

        // We should have received at least one LeaderEvent for a table
        // whose leader shifted due to the ring change.
        assert!(
            !leader_events.is_empty(),
            "watch_leader should have emitted at least one LeaderEvent after RingChanged"
        );

        // Each event should have a valid table and leader_node_id.
        for evt in &leader_events {
            assert!(
                !evt.table.is_empty(),
                "LeaderEvent table should not be empty"
            );
            assert!(
                evt.leader_node_id > 0,
                "LeaderEvent leader_node_id should be > 0"
            );
        }
    }

    /// GAP-16 test: create table, put 5 rows, scan with no bounds,
    /// verify 5 rows returned.
    #[tokio::test]
    async fn scan_returns_all_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut client, table_name, node) = start_server(&tmp).await;

        // Create the table.
        let schema = test_schema(&table_name);
        node.create_table(table_name.clone(), schema.clone())
            .await
            .unwrap();

        // Put 5 rows.
        for i in 1..=5 {
            client
                .put(pb::PutRequest {
                    table: table_name.clone(),
                    row: Some(pb::Row {
                        fields: vec![
                            pb::OptionalFieldValue {
                                is_null: false,
                                value: Some(pb::FieldValue {
                                    value: Some(
                                        pb::field_value::Value::Int64(i),
                                    ),
                                }),
                            },
                            pb::OptionalFieldValue {
                                is_null: false,
                                value: Some(pb::FieldValue {
                                    value: Some(
                                        pb::field_value::Value::ByteArray(
                                            format!("val_{}", i).into_bytes(),
                                        ),
                                    ),
                                }),
                            },
                        ],
                    }),
                })
                .await
                .unwrap();
        }

        // Scan with no bounds.
        let scan_resp = client
            .scan(pb::ScanRequest {
                table: table_name.clone(),
                start_pk: vec![],
                end_pk: vec![],
            })
            .await
            .unwrap();

        let inner = scan_resp.into_inner();
        assert_eq!(
            inner.rows.len(),
            5,
            "scan should return 5 rows, got {}",
            inner.rows.len()
        );
        assert!(inner.meta.is_some());
    }
}
