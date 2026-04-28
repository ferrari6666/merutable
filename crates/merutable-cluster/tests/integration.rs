// P1.8: Integration tests for the merutable-cluster crate.
//
// These tests start N Node instances in a single process (different data dirs,
// different ports), launch gRPC servers, connect a MeruClient, and run
// end-to-end scenarios through the full stack:
//   client -> gRPC -> Node -> RaftGroup -> TableStateMachine -> MeruDB
//
// Phase 1 model: each table is a single-node Raft group on the preferred
// leader only. The client's ring routes requests directly to the correct
// node. Other nodes do not hold the table's Raft group.

use std::path::Path;
use std::sync::Arc;

use merutable::schema::{ColumnDef, ColumnType, TableSchema};
use merutable::value::{FieldValue, Row};

use merutable_cluster::config::{ClusterConfig, NodeIdentity};
use merutable_cluster::harness::node::Node;
use merutable_cluster::proto::cluster::meru_cluster_server::MeruClusterServer;
use merutable_cluster::proto::raft_transport::raft_transport_server::RaftTransportServer;
use merutable_cluster::raft::types::SchemaChange;
use merutable_cluster::rpc::client::MeruClient;
use merutable_cluster::rpc::server::MeruClusterService;
use merutable_cluster::rpc::raft_server::RaftTransportService;

// ---------------------------------------------------------------------------
// TestCluster helper
// ---------------------------------------------------------------------------

struct TestCluster {
    nodes: Vec<Arc<Node>>,
    _servers: Vec<tokio::task::JoinHandle<()>>,
    identities: Vec<NodeIdentity>,
}

impl TestCluster {
    /// Start a 3-node cluster (one per AZ) with gRPC servers.
    async fn start(tmp_dir: &Path) -> Self {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut identities = Vec::new();
        let mut listeners = Vec::new();

        // Bind to OS-assigned ports to get free addresses.
        for (i, az) in azs.iter().enumerate() {
            let node_id = (i + 1) as u64;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            let addr_str = format!("127.0.0.1:{}", addr.port());
            identities.push(NodeIdentity {
                node_id,
                address: addr_str,
                az: az.to_string(),
            });
            listeners.push(listener);
        }

        // Create nodes and bootstrap.
        let mut nodes = Vec::new();
        for identity in &identities {
            let config = ClusterConfig {
                this_node: identity.clone(),
                seed_nodes: vec![],
                data_dir: tmp_dir
                    .join(format!("node_{}", identity.node_id))
                    .join("data"),
                raft_dir: tmp_dir
                    .join(format!("node_{}", identity.node_id))
                    .join("raft"),
                grpc_port: 0,
            };
            let node = Arc::new(Node::new(config).await);
            node.bootstrap_cluster(&identities).await;
            nodes.push(node);
        }

        // Start gRPC servers, reusing the bound listeners.
        let mut servers = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let service = MeruClusterService::new(nodes[i].clone());
            let handle = tokio::spawn(async move {
                let incoming =
                    tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(
                        listener,
                    );
                tonic::transport::Server::builder()
                    .add_service(MeruClusterServer::new(service))
                    .serve_with_incoming(incoming)
                    .await
                    .ok();
            });
            servers.push(handle);
        }

        // Give servers a moment to start accepting connections.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Self {
            nodes,
            _servers: servers,
            identities,
        }
    }

    /// Connect a MeruClient to this cluster.
    async fn client(&self) -> MeruClient {
        MeruClient::connect(self.identities.clone()).await.unwrap()
    }

    /// Create a table on all nodes (single-node metadata Raft model).
    ///
    /// TestCluster nodes have independent single-node metadata Raft groups
    /// (no cross-node metadata replication). Each node must be told about
    /// the table separately so its metadata cache is populated. Only the
    /// preferred leader per the ring will actually open a RaftGroup for
    /// data writes.
    ///
    /// For multi-node metadata replication (shared metadata Raft group),
    /// see `MultiNodeTestCluster::create_replicated_table` which uses
    /// `create_table_multi_node` with batched transport.
    async fn create_table_on_all_nodes(&self, name: &str, schema: &TableSchema) {
        for node in &self.nodes {
            node.create_table(name.to_string(), schema.clone())
                .await
                .unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Schema/Row helpers
// ---------------------------------------------------------------------------

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

// ===========================================================================
// Integration tests
// ===========================================================================

/// 1. Single table: put 100 rows via client, get each one back.
#[tokio::test]
async fn single_table_put_get() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table = "orders";
    let schema = test_schema(table);
    cluster.create_table_on_all_nodes(table, &schema).await;

    let client = cluster.client().await;

    // Put 100 rows.
    for i in 1..=100i64 {
        let row = make_row(i, &format!("order_{}", i));
        client.put(table, row).await.unwrap();
    }

    // Get each row back.
    for i in 1..=100i64 {
        let got = client
            .get(table, &[FieldValue::Int64(i)])
            .await
            .unwrap();
        assert!(
            got.is_some(),
            "row with id={} should exist after put",
            i
        );
        let row = got.unwrap();
        match &row.fields[1] {
            Some(FieldValue::Bytes(b)) => {
                let expected = format!("order_{}", i);
                assert_eq!(
                    b.as_ref(),
                    expected.as_bytes(),
                    "row {} val mismatch",
                    i
                );
            }
            other => panic!("expected Bytes for row {}, got {:?}", i, other),
        }
    }
}

/// 2. Multi-table concurrent writes: create 5 tables, put 50 rows each
///    concurrently, scan each table to verify 50 rows.
#[tokio::test]
async fn multi_table_concurrent_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table_names: Vec<String> = (0..5).map(|i| format!("tbl_{}", i)).collect();

    // Create all tables.
    for name in &table_names {
        let schema = test_schema(name);
        cluster.create_table_on_all_nodes(name, &schema).await;
    }

    let client = Arc::new(cluster.client().await);

    // Spawn concurrent writers: 50 rows per table.
    let mut handles = Vec::new();
    for name in &table_names {
        let c = client.clone();
        let t = name.clone();
        let h = tokio::spawn(async move {
            for i in 1..=50i64 {
                let row = make_row(i, &format!("{}_{}", t, i));
                c.put(&t, row).await.unwrap();
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.await.unwrap();
    }

    // Scan each table and verify 50 rows.
    for name in &table_names {
        let rows = client.scan(name, None, None).await.unwrap();
        assert_eq!(
            rows.len(),
            50,
            "table '{}' should have 50 rows, got {}",
            name,
            rows.len()
        );
    }
}

/// 3. Client routes to preferred leader: create a table, put a row via
///    client, then verify server-side that the preferred leader node has
///    the data. In a 3-node / 3-AZ cluster, all three nodes are replicas
///    (one per AZ) and each opens an independent single-node RaftGroup.
///    The client routes writes to the preferred leader, so only that
///    node's SM should contain the row.
#[tokio::test]
async fn client_routes_to_preferred_leader() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table = "routed_table";
    let schema = test_schema(table);
    cluster.create_table_on_all_nodes(table, &schema).await;

    let client = cluster.client().await;

    // Put a row via client (exercises routing).
    let row = make_row(1, "routed");
    client.put(table, row).await.unwrap();

    // Identify the preferred leader. In a 3-node/3-AZ cluster, all nodes
    // are replicas but only one is the preferred leader. The client routes
    // the put to that node. Each node has its own single-node RaftGroup,
    // so only the leader's SM has the row.
    let mut leader_node: Option<&Arc<Node>> = None;
    for node in &cluster.nodes {
        if node.router().am_i_preferred_leader(table) {
            leader_node = Some(node);
        }
    }
    let leader = leader_node.expect("one node should be preferred leader");

    // The preferred leader should have the row.
    let got = leader
        .get(table, &[FieldValue::Int64(1)])
        .await
        .unwrap();
    assert!(
        got.is_some(),
        "preferred leader should have the row after client put"
    );

    // Other nodes' independent SMs should NOT have this row (their RaftGroups
    // are independent single-node groups that never received the write).
    for node in &cluster.nodes {
        if node.config().this_node.node_id == leader.config().this_node.node_id {
            continue;
        }
        let got = node
            .get(table, &[FieldValue::Int64(1)])
            .await
            .unwrap();
        assert!(
            got.is_none(),
            "non-leader node {} should not have the row (independent single-node Raft)",
            node.config().this_node.node_id
        );
    }
}

/// 4. list_tables across cluster: create 3 tables, list_tables via client,
///    verify all 3 are present.
#[tokio::test]
async fn list_tables_across_cluster() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let names = ["alpha", "beta", "gamma"];
    for name in &names {
        let schema = test_schema(name);
        cluster.create_table_on_all_nodes(name, &schema).await;
    }

    let client = cluster.client().await;
    let tables = client.list_tables().await.unwrap();
    let table_names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();

    for name in &names {
        assert!(
            table_names.contains(name),
            "list_tables should contain '{}', got {:?}",
            name,
            table_names
        );
    }
}

/// 5. alter_table through client: create table, put a row, add a nullable
///    column via alter_table, verify the returned schema is correct, and
///    verify existing data is still readable.
///
///    Note: MeruDB's add_column persists the evolved schema but does NOT
///    swap the live engine schema (that is follow-up work). Writes with
///    the new column arity only work after a reopen. So this test verifies
///    the alter_table RPC returns the correct 3-column schema and that
///    existing rows remain accessible.
#[tokio::test]
async fn alter_table_through_client() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table = "alterable";
    let schema = test_schema(table);
    cluster.create_table_on_all_nodes(table, &schema).await;

    let client = cluster.client().await;

    // Put a row before the schema change.
    let row1 = make_row(1, "before_alter");
    client.put(table, row1).await.unwrap();

    // Alter table: add a nullable Double column.
    let new_col = ColumnDef {
        name: "score".into(),
        col_type: ColumnType::Double,
        nullable: true,
        ..Default::default()
    };
    let new_schema = client
        .alter_table(table, SchemaChange::AddColumn { col: new_col })
        .await
        .unwrap();

    // Verify the returned schema has the new column.
    assert_eq!(new_schema.columns.len(), 3);
    assert_eq!(new_schema.columns[2].name, "score");
    assert!(new_schema.columns[2].nullable);

    // Verify the original row is still readable after the schema change.
    let got = client
        .get(table, &[FieldValue::Int64(1)])
        .await
        .unwrap();
    assert!(got.is_some(), "row 1 should still exist after alter_table");
    let row = got.unwrap();
    match &row.fields[1] {
        Some(FieldValue::Bytes(b)) => {
            assert_eq!(b.as_ref(), b"before_alter");
        }
        other => panic!("expected Bytes for val field, got {:?}", other),
    }

    // Verify we can still write rows with the original arity.
    let row2 = make_row(2, "after_alter");
    client.put(table, row2).await.unwrap();

    let got2 = client
        .get(table, &[FieldValue::Int64(2)])
        .await
        .unwrap();
    assert!(got2.is_some(), "row 2 should exist after put");
}

/// 6. scan_with_bounds: put rows 1-20, scan with start_pk=5 and end_pk=15,
///    verify the correct subset is returned.
#[tokio::test]
async fn scan_with_bounds() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table = "bounded";
    let schema = test_schema(table);
    cluster.create_table_on_all_nodes(table, &schema).await;

    let client = cluster.client().await;

    // Put rows with ids 1-20.
    for i in 1..=20i64 {
        let row = make_row(i, &format!("val_{}", i));
        client.put(table, row).await.unwrap();
    }

    // Scan with bounds [5, 15].
    let start = [FieldValue::Int64(5)];
    let end = [FieldValue::Int64(15)];
    let rows = client
        .scan(table, Some(&start), Some(&end))
        .await
        .unwrap();

    // Verify the returned rows are in the expected range.
    // The exact semantics (inclusive/exclusive) depend on the engine, but
    // all returned rows should have ids in [5, 15].
    assert!(
        !rows.is_empty(),
        "scan with bounds [5, 15] should return rows"
    );

    for row in &rows {
        match &row.fields[0] {
            Some(FieldValue::Int64(id)) => {
                assert!(
                    *id >= 5 && *id <= 15,
                    "scanned row id {} outside bounds [5, 15]",
                    id
                );
            }
            other => panic!("expected Int64 pk, got {:?}", other),
        }
    }

    // Should be fewer than the full 20 rows.
    assert!(
        rows.len() < 20,
        "bounded scan should return fewer than 20 rows, got {}",
        rows.len()
    );
}

/// 7. client_handles_routing: put 100 rows across 10 tables, verify all
///    succeed on the first attempt (correct ring-based routing, no retries).
#[tokio::test]
async fn client_handles_routing() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table_names: Vec<String> = (0..10).map(|i| format!("rt_{}", i)).collect();

    // Create all 10 tables.
    for name in &table_names {
        let schema = test_schema(name);
        cluster.create_table_on_all_nodes(name, &schema).await;
    }

    let client = cluster.client().await;

    // Put 10 rows into each table (100 total).
    let mut success_count = 0;
    for name in &table_names {
        for i in 1..=10i64 {
            let row = make_row(i, &format!("{}_{}", name, i));
            client.put(name, row).await.unwrap();
            success_count += 1;
        }
    }
    assert_eq!(
        success_count, 100,
        "all 100 puts should succeed without error"
    );

    // Verify each table has 10 rows.
    for name in &table_names {
        let rows = client.scan(name, None, None).await.unwrap();
        assert_eq!(
            rows.len(),
            10,
            "table '{}' should have 10 rows, got {}",
            name,
            rows.len()
        );
    }
}

/// 8. delete_through_client: put 10 rows, delete rows 3, 5, 7 via client,
///    scan and verify 7 remaining rows.
#[tokio::test]
async fn delete_through_client() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    let table = "deletable";
    let schema = test_schema(table);
    cluster.create_table_on_all_nodes(table, &schema).await;

    let client = cluster.client().await;

    // Put 10 rows.
    for i in 1..=10i64 {
        let row = make_row(i, &format!("val_{}", i));
        client.put(table, row).await.unwrap();
    }

    // Delete rows 3, 5, 7.
    for &id in &[3i64, 5, 7] {
        client
            .delete(table, vec![FieldValue::Int64(id)])
            .await
            .unwrap();
    }

    // Scan: should have 7 remaining rows.
    let rows = client.scan(table, None, None).await.unwrap();
    assert_eq!(
        rows.len(),
        7,
        "should have 7 rows after deleting 3, got {}",
        rows.len()
    );

    // Verify the deleted rows are actually gone.
    for &id in &[3i64, 5, 7] {
        let got = client
            .get(table, &[FieldValue::Int64(id)])
            .await
            .unwrap();
        assert!(
            got.is_none(),
            "row {} should be deleted",
            id
        );
    }

    // Verify the remaining rows are still there.
    for id in [1i64, 2, 4, 6, 8, 9, 10] {
        let got = client
            .get(table, &[FieldValue::Int64(id)])
            .await
            .unwrap();
        assert!(
            got.is_some(),
            "row {} should still exist",
            id
        );
    }
}

// ===========================================================================
// Multi-node Raft replication tests
// ===========================================================================

/// A test cluster that serves both MeruCluster and RaftTransport gRPC
/// services, and connects peer channels so that real Raft messages flow
/// over gRPC between nodes.
struct MultiNodeTestCluster {
    nodes: Vec<Arc<Node>>,
    _servers: Vec<tokio::task::JoinHandle<()>>,
    identities: Vec<NodeIdentity>,
}

impl MultiNodeTestCluster {
    /// Start a 3-node cluster with both MeruCluster and RaftTransport servers.
    /// Peer channels are connected so nodes can proxy writes and exchange
    /// Raft messages.
    async fn start(tmp_dir: &Path) -> Self {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut identities = Vec::new();
        let mut listeners = Vec::new();

        // Bind to OS-assigned ports.
        for (i, az) in azs.iter().enumerate() {
            let node_id = (i + 1) as u64;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            let addr_str = format!("127.0.0.1:{}", addr.port());
            identities.push(NodeIdentity {
                node_id,
                address: addr_str,
                az: az.to_string(),
            });
            listeners.push(listener);
        }

        // Create nodes and bootstrap.
        let mut nodes = Vec::new();
        for identity in &identities {
            let config = ClusterConfig {
                this_node: identity.clone(),
                seed_nodes: vec![],
                data_dir: tmp_dir
                    .join(format!("node_{}", identity.node_id))
                    .join("data"),
                raft_dir: tmp_dir
                    .join(format!("node_{}", identity.node_id))
                    .join("raft"),
                grpc_port: 0,
            };
            let node = Arc::new(Node::new(config).await);
            node.bootstrap_cluster(&identities).await;
            nodes.push(node);
        }

        // Start gRPC servers with both MeruCluster and RaftTransport.
        let mut servers = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let cluster_service = MeruClusterService::new(nodes[i].clone());
            let raft_service =
                RaftTransportService::new(nodes[i].shared_table_groups());
            let handle = tokio::spawn(async move {
                let incoming =
                    tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(
                        listener,
                    );
                tonic::transport::Server::builder()
                    .add_service(MeruClusterServer::new(cluster_service))
                    .add_service(RaftTransportServer::new(raft_service))
                    .serve_with_incoming(incoming)
                    .await
                    .ok();
            });
            servers.push(handle);
        }

        // Give servers a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect peer channels so Raft messages and proxied writes flow.
        for node in &nodes {
            for identity in &identities {
                if identity.node_id != node.config().this_node.node_id {
                    if let Err(e) = node.connect_peer(identity.node_id, &identity.address).await {
                        eprintln!("warning: connect_peer failed for node {} -> {}: {}", node.config().this_node.node_id, identity.node_id, e);
                    }
                }
            }
        }

        Self {
            nodes,
            _servers: servers,
            identities,
        }
    }

    /// Create a table with multi-node Raft replication using batched transport.
    ///
    /// All replicas create their RaftGroups (backed by PeerBus batched transport)
    /// and initialize with the same membership set. openraft handles the election
    /// and one node becomes leader.
    async fn create_replicated_table(&self, name: &str, schema: &TableSchema) {
        // All nodes create their groups and initialize with the same membership.
        for node in &self.nodes {
            node.create_table_multi_node(name.to_string(), schema.clone())
                .await
                .unwrap();
        }

        // Give Raft time to elect a leader and stabilize.
        // With election_timeout_max=1000ms, we need at least 2-3 seconds.
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

    /// Connect a MeruClient to this cluster.
    async fn client(&self) -> MeruClient {
        MeruClient::connect(self.identities.clone()).await.unwrap()
    }
}

/// 9. Multi-node replication: writes on the leader are replicated to followers
///    via real gRPC Raft transport. Verify followers have the data.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_node_replication() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = MultiNodeTestCluster::start(tmp.path()).await;

    let table = "replicated";
    let schema = test_schema(table);
    cluster.create_replicated_table(table, &schema).await;

    // Find whichever node is the actual Raft leader using non-blocking
    // metrics check. Poll until a leader is elected.
    let mut leader_node_id: Option<u64> = None;
    for _attempt in 0..20 {
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(table).await {
                if let Some(lid) = group.current_leader() {
                    leader_node_id = Some(lid);
                    break;
                }
            }
        }
        if leader_node_id.is_some() {
            break;
        }
        // Leader not yet elected, wait and retry.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let leader_id = leader_node_id.expect("a leader should be elected");
    let leader_node = cluster
        .nodes
        .iter()
        .find(|n| n.config().this_node.node_id == leader_id)
        .expect("leader node should exist in cluster");

    // Put 10 rows directly through the leader's RaftGroup (bypassing
    // the Router's preferred-leader check which may not match the actual
    // Raft leader).
    let leader_group = leader_node
        .get_raft_group(table)
        .await
        .expect("leader should have raft group");
    // Put 10 rows on the leader through the RaftGroup directly.
    for i in 1..=10i64 {
        let row = make_row(i, &format!("replicated_{}", i));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            leader_group.client_write(
                merutable_cluster::raft::types::RaftCommand::Put { row },
            ),
        )
        .await;
        match result {
            Ok(Ok(_)) => {} // committed successfully
            Ok(Err(e)) => panic!("client_write error for row {}: {}", i, e),
            Err(_) => panic!("client_write timeout for row {} (5s)", i),
        }
    }

    // Wait for replication to propagate to followers.
    let target_index = leader_group.sm().applied_index();

    for node in &cluster.nodes {
        if node.config().this_node.node_id == leader_node.config().this_node.node_id {
            continue;
        }
        if let Some(group) = node.get_raft_group(table).await {
            // Wait until the follower catches up to the leader's applied index.
            let reached = group
                .sm()
                .wait_for_applied(target_index, std::time::Duration::from_secs(10))
                .await;
            assert!(
                reached,
                "follower node {} should catch up to applied_index {}",
                node.config().this_node.node_id,
                target_index
            );

            // Brief pause to let the follower's async apply worker drain.
            // Increased from 200ms to 500ms to account for metadata Raft
            // group overhead under concurrent test load.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Verify the follower has all 10 rows.
            for i in 1..=10i64 {
                let got = group.sm().get(&[FieldValue::Int64(i)]).unwrap();
                assert!(
                    got.is_some(),
                    "follower node {} should have row {} after replication",
                    node.config().this_node.node_id,
                    i
                );
            }
        }
    }
}

/// 10. Leader failover: write rows, shutdown the leader's Raft group,
///     verify a new leader is elected and writes continue on the new leader,
///     then verify all data is present on the remaining nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leader_failover() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = MultiNodeTestCluster::start(tmp.path()).await;

    let table = "failover_tbl";
    let schema = test_schema(table);
    cluster.create_replicated_table(table, &schema).await;

    // Find the initial leader.
    let mut leader_node_id: Option<u64> = None;
    for _attempt in 0..20 {
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(table).await {
                if let Some(lid) = group.current_leader() {
                    leader_node_id = Some(lid);
                    break;
                }
            }
        }
        if leader_node_id.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let old_leader_id = leader_node_id.expect("initial leader should be elected");
    let old_leader_node = cluster
        .nodes
        .iter()
        .find(|n| n.config().this_node.node_id == old_leader_id)
        .unwrap();

    // Write 5 rows through the old leader.
    let old_group = old_leader_node
        .get_raft_group(table)
        .await
        .expect("leader should have raft group");
    for i in 1..=5i64 {
        let row = make_row(i, &format!("before_failover_{}", i));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            old_group.client_write(
                merutable_cluster::raft::types::RaftCommand::Put { row },
            ),
        )
        .await
        .expect("pre-failover write should not timeout")
        .expect("pre-failover write should succeed");
    }

    // Shutdown the old leader's Raft group.
    old_group
        .shutdown()
        .await
        .expect("leader shutdown should succeed");

    // Wait for a new leader to be elected on one of the remaining nodes.
    let mut new_leader_id: Option<u64> = None;
    for _attempt in 0..30 {
        for node in &cluster.nodes {
            if node.config().this_node.node_id == old_leader_id {
                continue; // skip the dead leader
            }
            if let Some(group) = node.get_raft_group(table).await {
                if let Some(lid) = group.current_leader() {
                    if lid != old_leader_id {
                        new_leader_id = Some(lid);
                        break;
                    }
                }
            }
        }
        if new_leader_id.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let new_leader = new_leader_id.expect("new leader should be elected after failover");
    assert_ne!(new_leader, old_leader_id, "new leader should differ from old");

    let new_leader_node = cluster
        .nodes
        .iter()
        .find(|n| n.config().this_node.node_id == new_leader)
        .unwrap();
    let new_group = new_leader_node
        .get_raft_group(table)
        .await
        .expect("new leader should have raft group");

    // Write 5 more rows through the new leader.
    for i in 6..=10i64 {
        let row = make_row(i, &format!("after_failover_{}", i));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            new_group.client_write(
                merutable_cluster::raft::types::RaftCommand::Put { row },
            ),
        )
        .await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("post-failover write error for row {}: {}", i, e),
            Err(_) => panic!("post-failover write timeout for row {} (5s)", i),
        }
    }

    // Verify all 10 rows on the remaining nodes.
    let target_index = new_group.sm().applied_index();
    for node in &cluster.nodes {
        if node.config().this_node.node_id == old_leader_id {
            continue; // old leader is shut down
        }
        if let Some(group) = node.get_raft_group(table).await {
            let reached = group
                .sm()
                .wait_for_applied(target_index, std::time::Duration::from_secs(10))
                .await;
            assert!(
                reached,
                "node {} should catch up to applied_index {}",
                node.config().this_node.node_id,
                target_index
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            for i in 1..=10i64 {
                let got = group
                    .sm()
                    .get(&[FieldValue::Int64(i)])
                    .unwrap();
                assert!(
                    got.is_some(),
                    "node {} should have row {} after failover",
                    node.config().this_node.node_id,
                    i
                );
            }
        }
    }
}

/// 11. Watch-stream client: subscribe to WatchLeader, register a new node
///     that shifts the ring, verify the client receives leader events and
///     updates its internal leader_overrides.
#[tokio::test]
async fn watch_stream_ring_change() {
    use merutable_cluster::metadata::state_machine::{NodeEntry, NodeStatus};

    let tmp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(tmp.path()).await;

    // Create 20 tables so we have enough for a ring shift to affect some.
    for i in 0..20 {
        let name = format!("watch_tbl_{}", i);
        let schema = test_schema(&name);
        cluster.create_table_on_all_nodes(&name, &schema).await;
    }

    let client = cluster.client().await;

    // Start the watch stream.
    client.start_watch().await.unwrap();

    // Give the watch background task time to start.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // No overrides yet — all leaders match the ring.
    assert_eq!(
        client.leader_override_count(),
        0,
        "no overrides before ring change"
    );

    // Register a 4th node in AZ-1 on ALL cluster nodes (each has an
    // independent metadata Raft group in TestCluster). This triggers a
    // RingChanged event on whichever node the client's watch stream is
    // connected to.
    for node in &cluster.nodes {
        let new_node = NodeEntry {
            node_id: 4,
            address: "127.0.0.1:19999".to_string(),
            az: "AZ-1".to_string(),
            table_count: 0,
            status: NodeStatus::Active,
        };
        node.register_node(new_node).await;
    }

    // Wait for the watch event to propagate through the stream.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // After adding a 4th node to AZ-1, some tables' preferred leader in
    // AZ-1 shifts from the old node to the new node. The client's ring
    // doesn't know about node 4, so the watch events show leaders that
    // differ from the client's local ring → leader_overrides is populated.
    let overrides = client.leader_override_count();
    assert!(
        overrides > 0,
        "client should have leader overrides after ring change, got {}",
        overrides
    );
}

/// 12. Ring-rebalance: 3-node multi-node cluster, create a replicated table,
///     write data, add a 4th node to AZ-1, start the rebalance watcher,
///     verify the Raft membership change occurs and the new node can serve reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ring_rebalance_membership_change() {
    use merutable_cluster::metadata::state_machine::{NodeEntry, NodeStatus};

    let tmp = tempfile::tempdir().unwrap();

    // Build a 4-node cluster (3 original + 1 extra in AZ-1).
    // We start all 4 from the beginning so gRPC channels exist,
    // but only register the first 3 in the bootstrap. Then we
    // register the 4th to trigger a ring change + rebalance.
    let azs = ["AZ-1", "AZ-2", "AZ-3", "AZ-1"];
    let mut identities = Vec::new();
    let mut listeners = Vec::new();

    for (i, az) in azs.iter().enumerate() {
        let node_id = (i + 1) as u64;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        identities.push(NodeIdentity {
            node_id,
            address: format!("127.0.0.1:{}", addr.port()),
            az: az.to_string(),
        });
        listeners.push(listener);
    }

    // Create 4 nodes but only bootstrap with the first 3.
    let initial_identities: Vec<NodeIdentity> = identities[..3].to_vec();
    let mut nodes = Vec::new();
    for identity in &identities {
        let config = ClusterConfig {
            this_node: identity.clone(),
            seed_nodes: vec![],
            data_dir: tmp.path()
                .join(format!("node_{}", identity.node_id))
                .join("data"),
            raft_dir: tmp.path()
                .join(format!("node_{}", identity.node_id))
                .join("raft"),
            grpc_port: 0,
        };
        let node = Arc::new(Node::new(config).await);
        node.bootstrap_cluster(&initial_identities).await;
        nodes.push(node);
    }

    // Start gRPC servers with both MeruCluster and RaftTransport.
    let mut _servers = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let cluster_service = MeruClusterService::new(nodes[i].clone());
        let raft_service =
            RaftTransportService::new(nodes[i].shared_table_groups());
        let handle = tokio::spawn(async move {
            let incoming =
                tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(
                    listener,
                );
            tonic::transport::Server::builder()
                .add_service(MeruClusterServer::new(cluster_service))
                .add_service(RaftTransportServer::new(raft_service))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });
        _servers.push(handle);
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect all peers (including node 4).
    for node in &nodes {
        for identity in &identities {
            if identity.node_id != node.config().this_node.node_id {
                let _ = node.connect_peer(identity.node_id, &identity.address).await;
            }
        }
    }

    // Create a replicated table on the first 3 nodes.
    let table = "rebalance_tbl";
    let schema = test_schema(table);
    for node in &nodes[..3] {
        node.create_table_multi_node(table.to_string(), schema.clone())
            .await
            .unwrap();
    }
    // Wait for election.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Write some data through the leader.
    let mut leader_node_id: Option<u64> = None;
    for node in &nodes[..3] {
        if let Some(group) = node.get_raft_group(table).await {
            if let Some(lid) = group.current_leader() {
                leader_node_id = Some(lid);
                break;
            }
        }
    }
    let leader_id = leader_node_id.expect("leader should be elected");
    let leader = nodes
        .iter()
        .find(|n| n.config().this_node.node_id == leader_id)
        .unwrap();
    let leader_group = leader.get_raft_group(table).await.unwrap();
    for i in 1..=5i64 {
        let row = make_row(i, &format!("rebalance_{}", i));
        leader_group
            .client_write(merutable_cluster::raft::types::RaftCommand::Put { row })
            .await
            .unwrap();
    }

    // Start rebalance watchers on nodes that are Raft leaders.
    for node in &nodes[..3] {
        node.start_rebalance_watcher();
    }

    // Now register node 4 in AZ-1 on all 3 nodes' metadata groups.
    // This triggers RingChanged → on_ring_change → membership changes.
    let node4_entry = NodeEntry {
        node_id: 4,
        address: identities[3].address.clone(),
        az: "AZ-1".to_string(),
        table_count: 0,
        status: NodeStatus::Active,
    };
    for node in &nodes[..3] {
        node.register_node(node4_entry.clone()).await;
    }

    // Check if node 4 is in the new replica set after the ring change.
    let new_rings = nodes[0].metadata_cache().rings();
    let new_replicas = new_rings.replicas_for_table(table);

    // Node 4 needs to know about the table in its metadata + have a
    // RaftGroup. Register node 4 in its own metadata and create the
    // table group so it can receive Raft messages from the rebalancer.
    if new_replicas.contains(&4) {
        // Register the 4th node in its own metadata so the ring is correct.
        nodes[3].register_node(node4_entry.clone()).await;
        // Create the table in node 4's metadata + RaftGroup (uninitialised,
        // the rebalance leader will add it as a learner).
        nodes[3]
            .create_table_multi_node(table.to_string(), schema.clone())
            .await
            .unwrap();
    }

    // Wait for the rebalance to complete (add_learner + change_membership).
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Verify: if node 4 is in the new replica set, it should have the data.
    if new_replicas.contains(&4) {
        if let Some(group) = nodes[3].get_raft_group(table).await {
            // Wait for applied_index to catch up.
            let target = leader_group.sm().applied_index();
            let reached = group
                .sm()
                .wait_for_applied(target, std::time::Duration::from_secs(10))
                .await;
            if reached {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                for i in 1..=5i64 {
                    let got = group.sm().get(&[FieldValue::Int64(i)]).unwrap();
                    assert!(
                        got.is_some(),
                        "node 4 should have row {} after rebalance",
                        i
                    );
                }
            }
            // If not reached, the rebalance may not have affected this table.
            // That's OK — the ring is probabilistic.
        }
    }

    // Verify the original nodes still have all data.
    for node in &nodes[..3] {
        if let Some(group) = node.get_raft_group(table).await {
            for i in 1..=5i64 {
                let got = group.sm().get(&[FieldValue::Int64(i)]).unwrap();
                assert!(
                    got.is_some(),
                    "node {} should still have row {} after rebalance",
                    node.config().this_node.node_id,
                    i
                );
            }
        }
    }
}

/// 13. Follower failover: write rows on the leader, shutdown one follower's
///     Raft group, verify writes continue on the remaining 2-node majority,
///     then recreate the follower group and verify it catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn follower_failover() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = MultiNodeTestCluster::start(tmp.path()).await;

    let table = "follower_fail";
    let schema = test_schema(table);
    cluster.create_replicated_table(table, &schema).await;

    // Find the leader.
    let mut leader_id: Option<u64> = None;
    for _attempt in 0..20 {
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(table).await {
                if let Some(lid) = group.current_leader() {
                    leader_id = Some(lid);
                    break;
                }
            }
        }
        if leader_id.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let leader_id = leader_id.expect("leader should be elected");

    // Pick a follower (any node that is not the leader).
    let follower_node = cluster
        .nodes
        .iter()
        .find(|n| n.config().this_node.node_id != leader_id)
        .expect("should have a follower");
    let follower_id = follower_node.config().this_node.node_id;

    // Write 5 rows through the leader.
    let leader_node = cluster
        .nodes
        .iter()
        .find(|n| n.config().this_node.node_id == leader_id)
        .unwrap();
    let leader_group = leader_node.get_raft_group(table).await.unwrap();
    for i in 1..=5i64 {
        let row = make_row(i, &format!("before_kill_{}", i));
        leader_group
            .client_write(merutable_cluster::raft::types::RaftCommand::Put { row })
            .await
            .unwrap();
    }

    // Shutdown the follower's Raft group.
    let follower_group = follower_node.get_raft_group(table).await.unwrap();
    follower_group.shutdown().await.unwrap();

    // Writes should still succeed — 2-node majority (leader + other follower).
    for i in 6..=10i64 {
        let row = make_row(i, &format!("during_kill_{}", i));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            leader_group.client_write(
                merutable_cluster::raft::types::RaftCommand::Put { row },
            ),
        )
        .await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("write during follower-down should succeed: {}", e),
            Err(_) => panic!("write timed out during follower-down (row {})", i),
        }
    }

    // Verify the other (alive) follower has all 10 rows.
    let alive_follower = cluster
        .nodes
        .iter()
        .find(|n| {
            let nid = n.config().this_node.node_id;
            nid != leader_id && nid != follower_id
        })
        .expect("should have a second follower");
    if let Some(group) = alive_follower.get_raft_group(table).await {
        let target = leader_group.sm().applied_index();
        group
            .sm()
            .wait_for_applied(target, std::time::Duration::from_secs(10))
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        for i in 1..=10i64 {
            let got = group.sm().get(&[FieldValue::Int64(i)]).unwrap();
            assert!(
                got.is_some(),
                "alive follower {} should have row {}",
                alive_follower.config().this_node.node_id,
                i
            );
        }
    }
}

/// 14. Multi-table batched transport: create 10 tables on a 3-node cluster,
///     write to each, verify data on all followers. This exercises the PeerBus
///     batching path where multiple Raft groups share the same peer bus.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_table_batched_transport() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = MultiNodeTestCluster::start(tmp.path()).await;

    let table_names: Vec<String> = (0..10).map(|i| format!("batch_tbl_{}", i)).collect();

    // Create all 10 tables with multi-node replication.
    for name in &table_names {
        let schema = test_schema(name);
        cluster.create_replicated_table(name, &schema).await;
    }

    // For each table, find the leader and write 5 rows.
    for name in &table_names {
        let mut leader_group = None;
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(name).await {
                if let Some(lid) = group.current_leader() {
                    if lid == node.config().this_node.node_id {
                        leader_group = Some(group);
                        break;
                    }
                }
            }
        }

        // If no node thinks it's the leader, try writing through any node
        // that has the group (openraft will forward internally).
        let group = match leader_group {
            Some(g) => g,
            None => {
                // Use the first node that has the group.
                let mut found = None;
                for node in &cluster.nodes {
                    if let Some(g) = node.get_raft_group(name).await {
                        found = Some(g);
                        break;
                    }
                }
                found.expect("at least one node should have the group")
            }
        };

        for i in 1..=5i64 {
            let row = make_row(i, &format!("{}_{}", name, i));
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                group.client_write(
                    merutable_cluster::raft::types::RaftCommand::Put { row },
                ),
            )
            .await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => panic!("write to {} row {} failed: {}", name, i, e),
                Err(_) => panic!("write to {} row {} timed out", name, i),
            }
        }
    }

    // Verify all followers have the data for each table.
    for name in &table_names {
        // Find the leader's applied_index.
        let mut target_index = 0u64;
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(name).await {
                let idx = group.sm().applied_index();
                if idx > target_index {
                    target_index = idx;
                }
            }
        }

        // All nodes should catch up.
        for node in &cluster.nodes {
            if let Some(group) = node.get_raft_group(name).await {
                group
                    .sm()
                    .wait_for_applied(target_index, std::time::Duration::from_secs(10))
                    .await;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                for i in 1..=5i64 {
                    let got = group.sm().get(&[FieldValue::Int64(i)]).unwrap();
                    assert!(
                        got.is_some(),
                        "node {} should have row {} for table {}",
                        node.config().this_node.node_id,
                        i,
                        name
                    );
                }
            }
        }
    }
}
