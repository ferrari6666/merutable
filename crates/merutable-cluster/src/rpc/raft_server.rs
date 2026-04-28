// Multi-node Raft transport: gRPC server handler for RaftTransport::Batch.
//
// When a peer node sends a batched Raft message, this handler:
// 1. Demuxes each GroupMessage by group_id (table name)
// 2. Finds the local RaftGroup for that table
// 3. Delivers the message to the local openraft instance
// 4. Collects responses and returns them as BatchRaftResponse

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tokio::sync::RwLock;

use crate::proto::raft_transport as pb;
use crate::proto::raft_transport::group_message::Payload as PbPayload;
use crate::proto::raft_transport::raft_transport_server::RaftTransport;
use crate::metadata::raft_group::{MetadataRaftGroup, METADATA_GROUP_ID};
use crate::raft::group::RaftGroup;
use crate::raft::types::{
    log_id_from_proto, log_id_to_proto, vote_from_proto, vote_to_proto, MeruTypeConfig,
};

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{Entry, LogId, SnapshotMeta};

use crate::raft::group::deserialize_entry_payload;

// ---------------------------------------------------------------------------
// Proto -> openraft request conversion helpers
// ---------------------------------------------------------------------------

fn append_req_from_proto(
    p: &pb::AppendEntriesRequest,
) -> Result<AppendEntriesRequest<MeruTypeConfig>, Status> {
    let vote = p
        .vote
        .as_ref()
        .map(vote_from_proto)
        .ok_or_else(|| Status::invalid_argument("missing vote in AppendEntriesRequest"))?;

    let prev_log_id = p.prev_log_id.as_ref().map(log_id_from_proto);

    let entries: Vec<Entry<MeruTypeConfig>> = p
        .entries
        .iter()
        .map(|e| {
            let log_id = e
                .log_id
                .as_ref()
                .map(log_id_from_proto)
                .unwrap_or_else(|| LogId::new(openraft::CommittedLeaderId::new(0, 0), 0));
            let payload = deserialize_entry_payload(&e.payload)
                .unwrap_or_else(|err| {
                    tracing::error!("corrupt entry payload at index {}: {err}", log_id.index);
                    openraft::EntryPayload::Blank
                });
            Entry { log_id, payload }
        })
        .collect();

    let leader_commit = p.leader_commit.as_ref().map(log_id_from_proto);

    Ok(AppendEntriesRequest {
        vote,
        prev_log_id,
        entries,
        leader_commit,
    })
}

fn append_resp_to_proto(
    group_id: &str,
    resp: AppendEntriesResponse<u64>,
) -> pb::GroupMessage {
    let (vote_pb, success, conflict) = match resp {
        AppendEntriesResponse::Success => (None, true, None),
        AppendEntriesResponse::PartialSuccess(_) => (None, true, None),
        AppendEntriesResponse::Conflict => (None, false, Some(pb::LogId { term: 0, index: 0, node_id: 0 })),
        AppendEntriesResponse::HigherVote(v) => (Some(vote_to_proto(&v)), false, None),
    };
    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::AppendResponse(pb::AppendEntriesResponse {
            vote: vote_pb,
            success,
            conflict,
        })),
    }
}

fn vote_req_from_proto(p: &pb::VoteRequest) -> Result<VoteRequest<u64>, Status> {
    let vote = p
        .vote
        .as_ref()
        .map(vote_from_proto)
        .ok_or_else(|| Status::invalid_argument("missing vote in VoteRequest"))?;
    let last_log_id = p.last_log_id.as_ref().map(log_id_from_proto);
    Ok(VoteRequest { vote, last_log_id })
}

fn vote_resp_to_proto(group_id: &str, resp: VoteResponse<u64>) -> pb::GroupMessage {
    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::VoteResponse(pb::VoteResponse {
            vote: Some(vote_to_proto(&resp.vote)),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.as_ref().map(log_id_to_proto),
        })),
    }
}

fn install_snap_req_from_proto(
    p: &pb::InstallSnapshotRequest,
) -> Result<InstallSnapshotRequest<MeruTypeConfig>, Status> {
    let vote = p
        .vote
        .as_ref()
        .map(vote_from_proto)
        .ok_or_else(|| Status::invalid_argument("missing vote in InstallSnapshotRequest"))?;

    let last_log_id = p.last_included.as_ref().map(log_id_from_proto);
    let meta = SnapshotMeta {
        last_log_id,
        last_membership: openraft::StoredMembership::new(None, openraft::Membership::new(vec![], ())),
        snapshot_id: String::new(),
    };

    Ok(InstallSnapshotRequest {
        vote,
        meta,
        offset: p.offset,
        data: p.data.clone(),
        done: p.done,
    })
}

fn install_snap_resp_to_proto(
    group_id: &str,
    resp: InstallSnapshotResponse<u64>,
) -> pb::GroupMessage {
    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::InstallSnapshotResponse(
            pb::InstallSnapshotResponse {
                vote: Some(vote_to_proto(&resp.vote)),
            },
        )),
    }
}

// ---------------------------------------------------------------------------
// RaftTransportService
// ---------------------------------------------------------------------------

/// gRPC service handler for the RaftTransport::Batch RPC.
///
/// Receives batched Raft messages from peer nodes, dispatches each message
/// to the correct local RaftGroup (or the metadata Raft group), and collects
/// responses.
pub struct RaftTransportService {
    /// group_id (table name) -> the openraft Raft instance for that table.
    /// Shared with the Node that owns the RaftGroups.
    raft_groups: Arc<RwLock<HashMap<String, Arc<RaftGroup>>>>,
    /// The metadata Raft group. Messages with `group_id == "__metadata__"` are
    /// dispatched here instead of the table groups map.
    metadata_group: Option<Arc<MetadataRaftGroup>>,
}

impl RaftTransportService {
    pub fn new(raft_groups: Arc<RwLock<HashMap<String, Arc<RaftGroup>>>>) -> Self {
        Self {
            raft_groups,
            metadata_group: None,
        }
    }

    /// Create a `RaftTransportService` that also handles metadata Raft
    /// messages (group_id `__metadata__`).
    pub fn with_metadata(
        raft_groups: Arc<RwLock<HashMap<String, Arc<RaftGroup>>>>,
        metadata_group: Arc<MetadataRaftGroup>,
    ) -> Self {
        Self {
            raft_groups,
            metadata_group: Some(metadata_group),
        }
    }

    /// Process a single GroupMessage and return the response GroupMessage.
    async fn handle_one(
        &self,
        msg: &pb::GroupMessage,
    ) -> Result<pb::GroupMessage, Status> {
        let group_id = &msg.group_id;

        // Dispatch to the metadata Raft group if the group_id matches.
        if group_id == METADATA_GROUP_ID {
            let mg = self.metadata_group.as_ref().ok_or_else(|| {
                Status::not_found("no metadata Raft group on this node")
            })?;
            return self.dispatch_to_raft(group_id, mg.raft(), msg).await;
        }

        // Look up the local RaftGroup for this table.
        let groups = self.raft_groups.read().await;
        let group = groups.get(group_id).cloned();
        drop(groups);

        let group = group.ok_or_else(|| {
            Status::not_found(format!(
                "no local RaftGroup for table '{}'",
                group_id
            ))
        })?;

        self.dispatch_to_raft(group_id, group.raft(), msg).await
    }

    /// Dispatch a Raft message to the given openraft instance.
    async fn dispatch_to_raft(
        &self,
        group_id: &str,
        raft: &openraft::Raft<MeruTypeConfig>,
        msg: &pb::GroupMessage,
    ) -> Result<pb::GroupMessage, Status> {

        match &msg.payload {
            Some(PbPayload::AppendRequest(req)) => {
                let rpc = append_req_from_proto(req)?;
                match raft.append_entries(rpc).await {
                    Ok(resp) => Ok(append_resp_to_proto(group_id, resp)),
                    Err(e) => {
                        Err(Status::internal(format!(
                            "append_entries error for '{}': {}",
                            group_id, e
                        )))
                    }
                }
            }
            Some(PbPayload::VoteRequest(req)) => {
                let rpc = vote_req_from_proto(req)?;
                match raft.vote(rpc).await {
                    Ok(resp) => Ok(vote_resp_to_proto(group_id, resp)),
                    Err(e) => Err(Status::internal(format!(
                        "vote error for '{}': {}",
                        group_id, e
                    ))),
                }
            }
            Some(PbPayload::InstallSnapshotRequest(req)) => {
                let rpc = install_snap_req_from_proto(req)?;
                match raft.install_snapshot(rpc).await {
                    Ok(resp) => Ok(install_snap_resp_to_proto(group_id, resp)),
                    Err(e) => Err(Status::internal(format!(
                        "install_snapshot error for '{}': {}",
                        group_id, e
                    ))),
                }
            }
            _ => Err(Status::invalid_argument(format!(
                "unexpected or missing payload for group '{}'",
                group_id
            ))),
        }
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn batch(
        &self,
        request: Request<pb::BatchRaftMessage>,
    ) -> Result<Response<pb::BatchRaftResponse>, Status> {
        let batch = request.into_inner();
        let mut responses = Vec::with_capacity(batch.messages.len());

        for msg in &batch.messages {
            match self.handle_one(msg).await {
                Ok(resp) => responses.push(resp),
                Err(e) => {
                    // If processing fails, return the error to the caller.
                    // This lets the caller's RaftNetwork return RPCError to
                    // openraft, which handles it correctly (backoff/retry).
                    tracing::warn!(
                        group = %msg.group_id,
                        from = %batch.from_node,
                        error = %e,
                        "error handling raft message"
                    );
                    return Err(e);
                }
            }
        }

        Ok(Response::new(pb::BatchRaftResponse { responses }))
    }
}
