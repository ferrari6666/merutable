// GAP-6: merutable-node binary — bootstraps a cluster node.
//
// Usage: merutable-node <node_id> <az> <data_dir> <grpc_port> [<peer_node_id>:<peer_az>:<peer_addr> ...]
// Example: merutable-node 1 AZ-1 /data/node1 9100 2:AZ-2:10.0.2.1:9100 3:AZ-3:10.0.3.1:9100

use std::path::PathBuf;
use std::sync::Arc;

use merutable_cluster::config::{ClusterConfig, NodeIdentity};
use merutable_cluster::harness::node::Node;
use merutable_cluster::metadata::state_machine::{NodeEntry, NodeStatus};
use merutable_cluster::proto::cluster::meru_cluster_server::MeruClusterServer;
use merutable_cluster::proto::raft_transport::raft_transport_server::RaftTransportServer;
use merutable_cluster::rpc::raft_server::RaftTransportService;
use merutable_cluster::rpc::server::MeruClusterService;

fn print_usage() {
    eprintln!(
        "Usage: merutable-node <node_id> <az> <data_dir> <grpc_port> [<peer_node_id>:<peer_az>:<peer_addr> ...]"
    );
    eprintln!(
        "Example: merutable-node 1 AZ-1 /data/node1 9100 2:AZ-2:10.0.2.1:9100 3:AZ-3:10.0.3.1:9100"
    );
}

/// Parse a seed peer specification of the form `<node_id>:<az>:<address>`.
///
/// The address portion may itself contain colons (e.g. `10.0.2.1:9100`),
/// so we split on the first two colons only.
fn parse_seed_peer(spec: &str) -> Option<NodeEntry> {
    // Split into at most 3 parts on ':'.
    let mut parts = spec.splitn(3, ':');
    let node_id_str = parts.next()?;
    let az = parts.next()?;
    let address = parts.next()?;

    let node_id: u64 = node_id_str.parse().ok()?;

    if az.is_empty() || address.is_empty() {
        return None;
    }

    Some(NodeEntry {
        node_id,
        address: address.to_string(),
        az: az.to_string(),
        table_count: 0,
        status: NodeStatus::Active,
    })
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 4 {
        print_usage();
        std::process::exit(1);
    }

    let node_id: u64 = args[0].parse().unwrap_or_else(|_| {
        eprintln!("error: node_id must be a positive integer");
        print_usage();
        std::process::exit(1);
    });

    let az = args[1].clone();
    let data_dir = PathBuf::from(&args[2]);
    let grpc_port: u16 = args[3].parse().unwrap_or_else(|_| {
        eprintln!("error: grpc_port must be a valid port number");
        print_usage();
        std::process::exit(1);
    });

    let seed_specs: Vec<String> = args[4..].to_vec();

    // Install Prometheus metrics exporter on :9101 (configurable via METRICS_PORT).
    // Exposes the `metrics` facade counters/histograms (BUS_FLUSH_COUNT,
    // BUS_BATCH_SIZE, raft.*) for scraping during benchmarks.
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9101);
    match metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(std::net::SocketAddr::from(([0, 0, 0, 0], metrics_port)))
        .install()
    {
        Ok(()) => eprintln!(
            "merutable-node: Prometheus metrics listening on 0.0.0.0:{}",
            metrics_port
        ),
        Err(e) => eprintln!(
            "merutable-node: warning: failed to install Prometheus exporter on :{}: {}",
            metrics_port, e
        ),
    }

    // §13.3 Divergence 3 fix: use ADVERTISE_ADDR env var for the
    // address that peers will dial. Falls back to HOSTNAME-based
    // Kubernetes DNS, then to 0.0.0.0 (dev mode only).
    let advertise_addr = std::env::var("ADVERTISE_ADDR").unwrap_or_else(|_| {
        // Kubernetes: derive from pod hostname + headless service.
        if let (Ok(hostname), Ok(domain)) = (
            std::env::var("HOSTNAME"),
            std::env::var("CLUSTER_DOMAIN"),
        ) {
            format!("{}:{}", format!("{}.{}", hostname, domain), grpc_port)
        } else {
            format!("0.0.0.0:{}", grpc_port)
        }
    });

    let config = ClusterConfig {
        this_node: NodeIdentity {
            node_id,
            address: advertise_addr.clone(),
            az: az.clone(),
        },
        seed_nodes: vec![],
        data_dir: data_dir.clone(),
        raft_dir: data_dir.join("raft"),
        grpc_port,
    };

    eprintln!(
        "merutable-node: starting node {} in {} on port {} (advertise: {})",
        node_id, az, grpc_port, advertise_addr
    );

    // Parse seed peers upfront for all_node_ids.
    let seed_peers: Vec<NodeEntry> = seed_specs.iter().filter_map(|s| parse_seed_peer(s)).collect();
    let all_node_ids: Vec<u64> = std::iter::once(node_id)
        .chain(seed_peers.iter().map(|e| e.node_id))
        .collect();
    let is_multi_node = !seed_peers.is_empty();
    let bootstrapper = *all_node_ids.iter().min().unwrap();

    // Step 4: Create node.
    // §13.3 Div 2 fix: multi-node uses DynamicBatchedNetworkFactory for
    // the metadata group so peers become reachable as connect_peer runs.
    let node = if is_multi_node {
        Arc::new(Node::new_multi_node(config, &all_node_ids).await)
    } else {
        Arc::new(Node::new(config).await)
    };

    // Step 6: Start gRPC server BEFORE connecting peers / initializing.
    // The server must be accepting Raft messages before the bootstrapper
    // calls initialize — otherwise heartbeats to peers fail.
    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port)
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("error: failed to parse bind address");
            std::process::exit(1);
        });

    let cluster_service = MeruClusterService::new(node.clone());
    let raft_service = RaftTransportService::with_metadata(
        node.shared_table_groups(),
        Arc::clone(node.metadata_raft_group()),
    );
    eprintln!("merutable-node: starting gRPC server on {}", bind_addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let server = tonic::transport::Server::builder()
            .add_service(MeruClusterServer::new(cluster_service))
            .add_service(RaftTransportServer::new(raft_service));
        server
            .serve_with_shutdown(bind_addr, async { let _ = shutdown_rx.await; })
            .await
            .ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Step 7: Connect to all seed peers. Retry with backoff — peers may
    // still be starting. Populates peer_buses which the metadata group's
    // DynamicBatchedNetworkFactory reads at RPC time.
    for entry in &seed_peers {
        let mut attempt = 0u32;
        loop {
            match node.connect_peer(entry.node_id, &entry.address).await {
                Ok(()) => {
                    eprintln!(
                        "merutable-node: connected to peer {} at {}",
                        entry.node_id, entry.address
                    );
                    break;
                }
                Err(e) if attempt < 30 => {
                    attempt += 1;
                    eprintln!(
                        "merutable-node: connect_peer({}) attempt {} failed ({}); retrying in 1s",
                        entry.node_id, attempt, e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(e) => {
                    eprintln!(
                        "merutable-node: giving up on connect_peer({}): {}",
                        entry.node_id, e
                    );
                    break;
                }
            }
        }
    }

    // Step 8: Bootstrapper initializes the metadata Raft group with all
    // members, then writes registrations. Non-bootstrappers sit idle —
    // the bootstrapper's initialize sends them AppendEntries and the
    // register_* writes replicate to them automatically.
    if !is_multi_node {
        // Single-node: group was initialized in Node::new; safe to write.
        node.register_self().await;
        eprintln!("merutable-node: registered self");
    } else {
        if node_id == bootstrapper {
            eprintln!("merutable-node: I am the metadata bootstrapper (node {})", node_id);
            match node.initialize_metadata(&all_node_ids).await {
                Ok(()) => eprintln!(
                    "merutable-node: metadata group initialized with {} members",
                    all_node_ids.len()
                ),
                Err(e) => eprintln!(
                    "merutable-node: warning: metadata initialize failed: {}",
                    e
                ),
            }
        } else {
            eprintln!(
                "merutable-node: waiting for bootstrapper (node {}) to initialize metadata group",
                bootstrapper
            );
        }

        // All nodes: wait for leader election.
        let mut leader_id: Option<u64> = None;
        let mut waited_ms = 0u32;
        loop {
            if let Some(leader) = node.metadata_raft_group().current_leader() {
                eprintln!("merutable-node: metadata leader elected: node {}", leader);
                leader_id = Some(leader);
                break;
            }
            if waited_ms >= 10_000 {
                eprintln!("merutable-node: warning: no metadata leader after 10s; continuing anyway");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            waited_ms += 100;
        }

        // Only the elected leader writes registrations (replicated to all peers).
        if leader_id == Some(node_id) {
            eprintln!("merutable-node: I am the leader — registering all peers");
            node.register_self().await;
            eprintln!("merutable-node: registered self");
            for entry in &seed_peers {
                node.register_node(entry.clone()).await;
                eprintln!(
                    "merutable-node: registered seed peer {} (az={}) at {}",
                    entry.node_id, entry.az, entry.address
                );
            }
        } else {
            eprintln!(
                "merutable-node: not leader (leader={:?}); skipping registration writes",
                leader_id
            );
        }
    }

    // Step 9: Start rebalance watcher + leader metrics sampler + table opener.
    node.start_rebalance_watcher();
    node.start_leader_metrics_sampler();
    node.start_table_opener();
    eprintln!("merutable-node: ready");

    // Step 10: Wait for SIGINT/SIGTERM.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl_c");
    eprintln!("\nmerutable-node: shutting down");

    // Step 11: Graceful shutdown.
    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
    node.shutdown().await;

    eprintln!("merutable-node: goodbye");
}
