pub mod config;
pub mod io;
pub mod metadata;
pub mod observability;
pub mod proto_convert;
pub mod raft;
pub mod ring;
pub mod table;
pub mod harness;
pub mod rpc;

pub mod proto {
    pub mod cluster {
        tonic::include_proto!("merutable.cluster");
    }
    pub mod raft_transport {
        tonic::include_proto!("merutable.raft_transport");
    }
}
