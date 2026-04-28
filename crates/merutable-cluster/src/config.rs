use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub type NodeId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub address: String,
    pub az: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub this_node: NodeIdentity,
    pub seed_nodes: Vec<NodeIdentity>,
    pub data_dir: PathBuf,
    pub raft_dir: PathBuf,
    pub grpc_port: u16,
}
