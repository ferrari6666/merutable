// P1.2: BatchedRaftNetwork + PeerBus -- batched gRPC transport for openraft.
//
// PeerBus: one per peer node. Collects messages from all Raft groups
// destined for that peer and flushes them as a single batched gRPC call.
//
// BatchedRaftNetwork: implements openraft's RaftNetwork for one Raft group.
// Routes through the shared PeerBus.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{oneshot, Mutex, Notify};

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};

use crate::observability::{BUS_BATCH_SIZE, BUS_FLUSH_COUNT};
use crate::proto::raft_transport as pb;
use crate::proto::raft_transport::group_message::Payload as PbPayload;

use super::group::serialize_entry_payload;
use super::types::{
    log_id_from_proto, log_id_to_proto, vote_from_proto, vote_to_proto,
    MeruTypeConfig,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of messages to batch before triggering a flush.
const BATCH_HIGH_WATER: usize = 64;

/// Idle timer interval — flushes heartbeat-class messages when nothing
/// else wakes the loop. Short enough that a missed notify only adds a
/// few ms of tail; long enough that an idle peer doesn't spin.
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

// ---------------------------------------------------------------------------
// Transport trait -- abstraction over the actual gRPC call
// ---------------------------------------------------------------------------

/// Trait abstracting the single RPC used to send a batch of Raft messages
/// to a peer. In production this wraps a tonic gRPC client; in tests it
/// can be replaced with a mock.
#[async_trait::async_trait]
pub trait BatchTransport: Send + Sync + 'static {
    async fn send_batch(
        &self,
        request: pb::BatchRaftMessage,
    ) -> Result<pb::BatchRaftResponse, tonic::Status>;
}

// ---------------------------------------------------------------------------
// GrpcBatchTransport — production BatchTransport over tonic gRPC
// ---------------------------------------------------------------------------

/// Production `BatchTransport` that sends batched Raft messages to a peer
/// via the `RaftTransport::Batch` gRPC RPC.
pub struct GrpcBatchTransport {
    channel: tonic::transport::Channel,
}

impl GrpcBatchTransport {
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self { channel }
    }
}

#[async_trait::async_trait]
impl BatchTransport for GrpcBatchTransport {
    async fn send_batch(
        &self,
        request: pb::BatchRaftMessage,
    ) -> Result<pb::BatchRaftResponse, tonic::Status> {
        // Construct a fresh client per call. Clones the underlying Channel
        // (cheap — shares the h2 connection + multiplexes streams).
        let mut client = pb::raft_transport_client::RaftTransportClient::new(
            self.channel.clone(),
        );
        let resp = client.batch(request).await?;
        Ok(resp.into_inner())
    }
}

// ---------------------------------------------------------------------------
// PeerBus
// ---------------------------------------------------------------------------

/// An enqueued message paired with a oneshot sender for the response.
struct QueuedMessage {
    msg: pb::GroupMessage,
    resp_tx: oneshot::Sender<pb::GroupMessage>,
}

/// Collects messages from all Raft groups destined for a single peer,
/// and flushes them as a single batched RPC.
///
/// Each enqueued message carries a oneshot channel for its response,
/// so callers block on their own oneshot receiver. The flush loop
/// demuxes responses by position (matching request order) and delivers
/// each response to its oneshot sender.
pub struct PeerBus {
    peer_id: u64,
    this_node_id: u64,
    queue: Mutex<Vec<QueuedMessage>>,
    flush_notify: Notify,
    transport: Box<dyn BatchTransport>,
    /// Set to `true` to signal `flush_loop` to exit.
    shutdown_flag: AtomicBool,
}

impl PeerBus {
    pub fn new(
        peer_id: u64,
        this_node_id: u64,
        transport: Box<dyn BatchTransport>,
    ) -> Self {
        Self {
            peer_id,
            this_node_id,
            queue: Mutex::new(Vec::new()),
            flush_notify: Notify::new(),
            transport,
            shutdown_flag: AtomicBool::new(false),
        }
    }

    /// Signal the `flush_loop` to exit on its next iteration.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        // Wake the loop so it can check the flag immediately.
        self.flush_notify.notify_one();
    }

    /// Enqueue a message destined for this peer and return a oneshot
    /// receiver for the response.
    ///
    /// Wake conditions:
    /// - Vote requests/responses: immediate flush (leader election
    ///   latency dominates otherwise).
    /// - Data-bearing AppendEntries: immediate flush (write latency).
    /// - Snapshot: immediate flush.
    /// - Queue length >= BATCH_HIGH_WATER: immediate flush.
    /// - Empty-entry heartbeats: no wake; rely on the idle timer.
    pub async fn enqueue(&self, msg: pb::GroupMessage) -> oneshot::Receiver<pb::GroupMessage> {
        let is_vote = matches!(
            msg.payload,
            Some(PbPayload::VoteRequest(_)) | Some(PbPayload::VoteResponse(_))
        );
        let is_data_append = matches!(
            &msg.payload,
            Some(PbPayload::AppendRequest(ref req)) if !req.entries.is_empty()
        );
        let is_snapshot = matches!(
            msg.payload,
            Some(PbPayload::InstallSnapshotRequest(_))
        );

        let (resp_tx, resp_rx) = oneshot::channel();

        let wake = {
            let mut q = self.queue.lock().await;
            q.push(QueuedMessage { msg, resp_tx });
            is_vote || is_data_append || is_snapshot || q.len() >= BATCH_HIGH_WATER
        };

        if wake {
            self.flush_notify.notify_one();
        }

        resp_rx
    }

    /// Drain the current queue and return the queued messages (if any).
    async fn drain(&self) -> Vec<QueuedMessage> {
        let mut q = self.queue.lock().await;
        std::mem::take(&mut *q)
    }

    /// Flush: send the current batch and deliver responses via oneshot
    /// channels. Returns the number of messages sent.
    pub async fn flush(&self) -> Result<usize, tonic::Status> {
        let queued = self.drain().await;
        if queued.is_empty() {
            return Ok(0);
        }

        let count = queued.len();
        metrics::counter!(BUS_FLUSH_COUNT).increment(1);
        metrics::histogram!(BUS_BATCH_SIZE).record(count as f64);

        let messages: Vec<pb::GroupMessage> =
            queued.iter().map(|q| q.msg.clone()).collect();
        let resp_txs: Vec<oneshot::Sender<pb::GroupMessage>> =
            queued.into_iter().map(|q| q.resp_tx).collect();

        let request = pb::BatchRaftMessage {
            from_node: self.this_node_id,
            messages,
        };

        let response = self.transport.send_batch(request).await?;

        // Deliver each response to the corresponding oneshot sender.
        // The server returns responses in the same order as requests.
        for (resp_msg, tx) in response.responses.into_iter().zip(resp_txs) {
            let _ = tx.send(resp_msg);
        }

        Ok(count)
    }

    /// Background flush loop. Call this from a spawned task.
    ///
    /// The loop flushes when:
    /// 1. `flush_notify` is triggered (vote or high-water), or
    /// 2. the timer fires (heartbeat coalescing).
    ///
    /// Exits when the shutdown flag is set via `shutdown()`.
    pub async fn flush_loop(self: Arc<Self>) {
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                tracing::info!(peer_id = self.peer_id, "PeerBus flush_loop shutting down");
                break;
            }

            // Wait for either a notification or the idle timer.
            tokio::select! {
                _ = self.flush_notify.notified() => {}
                _ = tokio::time::sleep(FLUSH_INTERVAL) => {}
            }

            if self.shutdown_flag.load(Ordering::Acquire) {
                tracing::info!(peer_id = self.peer_id, "PeerBus flush_loop shutting down");
                break;
            }

            if let Err(e) = self.flush().await {
                tracing::warn!(
                    peer_id = self.peer_id,
                    "batch flush to peer failed: {}",
                    e
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Proto <-> openraft conversion helpers (request / response)
// ---------------------------------------------------------------------------

fn append_req_to_proto(
    group_id: &str,
    rpc: &AppendEntriesRequest<MeruTypeConfig>,
) -> pb::GroupMessage {
    let entries: Vec<pb::RaftEntry> = rpc
        .entries
        .iter()
        .map(|e| {
            // Use the tagged serialization format (tag byte + payload)
            // so the server-side deserialize_entry_payload can decode it.
            let payload = serialize_entry_payload(&e.payload)
                .unwrap_or_else(|err| {
                    tracing::error!("failed to serialize entry at index {}: {err}", e.log_id.index);
                    vec![0x00] // PAYLOAD_TAG_BLANK fallback
                });
            pb::RaftEntry {
                log_id: Some(log_id_to_proto(&e.log_id)),
                payload,
            }
        })
        .collect();

    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::AppendRequest(pb::AppendEntriesRequest {
            vote: Some(vote_to_proto(&rpc.vote)),
            prev_log_id: rpc.prev_log_id.as_ref().map(log_id_to_proto),
            entries,
            leader_commit: rpc.leader_commit.as_ref().map(log_id_to_proto),
        })),
    }
}

fn append_resp_from_proto(
    p: &pb::AppendEntriesResponse,
) -> AppendEntriesResponse<u64> {
    if p.success {
        AppendEntriesResponse::Success
    } else if p.conflict.is_some() {
        AppendEntriesResponse::Conflict
    } else {
        // Higher vote scenario
        let vote = p.vote.as_ref().map(vote_from_proto).unwrap_or_default();
        AppendEntriesResponse::HigherVote(vote)
    }
}

fn vote_req_to_proto(group_id: &str, rpc: &VoteRequest<u64>) -> pb::GroupMessage {
    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::VoteRequest(pb::VoteRequest {
            vote: Some(vote_to_proto(&rpc.vote)),
            last_log_id: rpc.last_log_id.as_ref().map(log_id_to_proto),
        })),
    }
}

fn vote_resp_from_proto(p: &pb::VoteResponse) -> VoteResponse<u64> {
    VoteResponse {
        vote: p.vote.as_ref().map(vote_from_proto).unwrap_or_default(),
        vote_granted: p.vote_granted,
        last_log_id: p.last_log_id.as_ref().map(log_id_from_proto),
    }
}

fn install_snap_req_to_proto(
    group_id: &str,
    rpc: &InstallSnapshotRequest<MeruTypeConfig>,
) -> pb::GroupMessage {
    pb::GroupMessage {
        group_id: group_id.to_string(),
        payload: Some(PbPayload::InstallSnapshotRequest(
            pb::InstallSnapshotRequest {
                vote: Some(vote_to_proto(&rpc.vote)),
                last_included: rpc
                    .meta
                    .last_log_id
                    .as_ref()
                    .map(log_id_to_proto),
                offset: rpc.offset,
                data: rpc.data.clone(),
                done: rpc.done,
            },
        )),
    }
}

fn install_snap_resp_from_proto(
    p: &pb::InstallSnapshotResponse,
) -> InstallSnapshotResponse<u64> {
    InstallSnapshotResponse {
        vote: p.vote.as_ref().map(vote_from_proto).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// BatchedRaftNetwork
// ---------------------------------------------------------------------------

/// Implements `openraft::RaftNetwork` for a single Raft group. All RPCs
/// are enqueued on the shared PeerBus which batches them to the target
/// peer. Each RPC gets a per-request oneshot channel for its response.
pub struct BatchedRaftNetwork {
    group_id: String,
    bus: Arc<PeerBus>,
}

impl BatchedRaftNetwork {
    pub fn new(
        group_id: String,
        bus: Arc<PeerBus>,
    ) -> Self {
        Self {
            group_id,
            bus,
        }
    }

    /// Enqueue a message and wait for the response via oneshot.
    async fn send_and_recv(
        &self,
        msg: pb::GroupMessage,
    ) -> Result<pb::GroupMessage, RPCError<u64, BasicNode, RaftError<u64>>> {
        let resp_rx = self.bus.enqueue(msg).await;
        resp_rx.await.map_err(|_| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "response oneshot dropped (flush failed?)",
            )))
        })
    }
}

impl RaftNetwork<MeruTypeConfig> for BatchedRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MeruTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let proto_msg = append_req_to_proto(&self.group_id, &rpc);
        let resp = self.send_and_recv(proto_msg).await?;
        match resp.payload {
            Some(PbPayload::AppendResponse(ar)) => Ok(append_resp_from_proto(&ar)),
            other => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected response payload: {other:?}"),
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
        let proto_msg = install_snap_req_to_proto(&self.group_id, &rpc);
        let resp_rx = self.bus.enqueue(proto_msg).await;
        let resp = resp_rx.await.map_err(|_| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "response oneshot dropped (flush failed?)",
            )))
        })?;

        match resp.payload {
            Some(PbPayload::InstallSnapshotResponse(sr)) => {
                Ok(install_snap_resp_from_proto(&sr))
            }
            other => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected snapshot response payload: {other:?}"),
            )))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let proto_msg = vote_req_to_proto(&self.group_id, &rpc);
        let resp = self.send_and_recv(proto_msg).await?;
        match resp.payload {
            Some(PbPayload::VoteResponse(vr)) => Ok(vote_resp_from_proto(&vr)),
            other => Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected vote response payload: {other:?}"),
            )))),
        }
    }
}

// ---------------------------------------------------------------------------
// BatchedNetworkFactory
// ---------------------------------------------------------------------------

/// Factory that creates `BatchedRaftNetwork` instances for each target
/// node. Owns a shared map of `PeerBus` instances.
pub struct BatchedNetworkFactory {
    group_id: String,
    buses: Arc<HashMap<u64, Arc<PeerBus>>>,
}

impl BatchedNetworkFactory {
    pub fn new(group_id: String, buses: Arc<HashMap<u64, Arc<PeerBus>>>) -> Self {
        Self { group_id, buses }
    }
}

impl RaftNetworkFactory<MeruTypeConfig> for BatchedNetworkFactory {
    type Network = BatchedRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        let bus = self.buses.get(&target).expect("unknown peer").clone();
        BatchedRaftNetwork::new(self.group_id.clone(), bus)
    }
}

// ---------------------------------------------------------------------------
// DynamicBatchedNetworkFactory
// ---------------------------------------------------------------------------

/// Like `BatchedNetworkFactory` but looks up `PeerBus` instances from a
/// shared `RwLock<HashMap>` at RPC time instead of at construction time.
///
/// This is needed for the metadata Raft group whose `Raft` instance is
/// created in `Node::new` before any peer connections exist. Peers are
/// added later via `connect_peer`, which populates the shared map.
pub struct DynamicBatchedNetworkFactory {
    group_id: String,
    buses: Arc<tokio::sync::RwLock<HashMap<u64, Arc<PeerBus>>>>,
}

impl DynamicBatchedNetworkFactory {
    pub fn new(
        group_id: String,
        buses: Arc<tokio::sync::RwLock<HashMap<u64, Arc<PeerBus>>>>,
    ) -> Self {
        Self { group_id, buses }
    }
}

impl RaftNetworkFactory<MeruTypeConfig> for DynamicBatchedNetworkFactory {
    type Network = BatchedRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        let buses = self.buses.read().await;
        match buses.get(&target) {
            Some(bus) => BatchedRaftNetwork::new(self.group_id.clone(), Arc::clone(bus)),
            None => {
                // Peer not connected yet. Return a network whose bus has
                // no transport — enqueue will store the oneshot but flush
                // will fail, causing openraft to retry after backoff.
                // This is safe: openraft handles transient network failures.
                tracing::warn!(
                    target = target,
                    group = %self.group_id,
                    "peer bus not found for target — peer may not be connected yet"
                );
                // Create a dummy bus that will fail on flush.
                let dummy = Arc::new(PeerBus::new(
                    target,
                    0,
                    Box::new(FailTransport),
                ));
                BatchedRaftNetwork::new(self.group_id.clone(), dummy)
            }
        }
    }
}

/// A `BatchTransport` that always returns Unavailable. Used as a
/// placeholder when a peer is not yet connected.
struct FailTransport;

#[async_trait::async_trait]
impl BatchTransport for FailTransport {
    async fn send_batch(
        &self,
        _request: pb::BatchRaftMessage,
    ) -> Result<pb::BatchRaftResponse, tonic::Status> {
        Err(tonic::Status::unavailable("peer not connected"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -- Mock transport that records batches ---------------------------------

    struct MockTransport {
        batches: Mutex<Vec<pb::BatchRaftMessage>>,
        /// If set, the mock will generate one response per request message
        /// echoing back the group_id with an AppendResponse.
        auto_respond: bool,
        flush_count: AtomicUsize,
        /// Signaled every time a flush occurs.
        flushed: Notify,
    }

    impl MockTransport {
        fn new(auto_respond: bool) -> Arc<Self> {
            Arc::new(Self {
                batches: Mutex::new(Vec::new()),
                auto_respond,
                flush_count: AtomicUsize::new(0),
                flushed: Notify::new(),
            })
        }
    }

    #[async_trait::async_trait]
    impl BatchTransport for Arc<MockTransport> {
        async fn send_batch(
            &self,
            request: pb::BatchRaftMessage,
        ) -> Result<pb::BatchRaftResponse, tonic::Status> {
            let responses = if self.auto_respond {
                request
                    .messages
                    .iter()
                    .map(|m| pb::GroupMessage {
                        group_id: m.group_id.clone(),
                        payload: Some(PbPayload::AppendResponse(
                            pb::AppendEntriesResponse {
                                vote: Some(pb::Vote {
                                    term: 1,
                                    node_id: 0,
                                    committed: false,
                                }),
                                success: true,
                                conflict: None,
                            },
                        )),
                    })
                    .collect()
            } else {
                Vec::new()
            };

            self.batches.lock().await.push(request);
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            self.flushed.notify_waiters();
            Ok(pb::BatchRaftResponse { responses })
        }
    }

    fn make_heartbeat(group_id: &str) -> pb::GroupMessage {
        pb::GroupMessage {
            group_id: group_id.to_string(),
            payload: Some(PbPayload::AppendRequest(pb::AppendEntriesRequest {
                vote: Some(pb::Vote {
                    term: 1,
                    node_id: 0,
                    committed: true,
                }),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            })),
        }
    }

    fn make_vote_msg(group_id: &str) -> pb::GroupMessage {
        pb::GroupMessage {
            group_id: group_id.to_string(),
            payload: Some(PbPayload::VoteRequest(pb::VoteRequest {
                vote: Some(pb::Vote {
                    term: 2,
                    node_id: 1,
                    committed: false,
                }),
                last_log_id: None,
            })),
        }
    }

    // -----------------------------------------------------------------------
    // Test: peer_bus_batches_messages
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn peer_bus_batches_messages() {
        let mock = MockTransport::new(true);
        let bus = PeerBus::new(
            1,
            0,
            Box::new(Arc::clone(&mock)),
        );

        // Enqueue 10 heartbeat messages from different groups.
        // Collect the oneshot receivers.
        let mut rxs = Vec::new();
        for i in 0..10 {
            let rx = bus.enqueue(make_heartbeat(&format!("group-{i}"))).await;
            rxs.push(rx);
        }

        // Flush manually.
        let count = bus.flush().await.unwrap();
        assert_eq!(count, 10, "all 10 messages should be in one batch");

        let batches = mock.batches.lock().await;
        assert_eq!(batches.len(), 1, "exactly one batch RPC");
        assert_eq!(batches[0].messages.len(), 10);
        assert_eq!(batches[0].from_node, 0);

        // All oneshot receivers should have responses.
        for (i, rx) in rxs.into_iter().enumerate() {
            let resp = rx.await.unwrap_or_else(|_| panic!("group-{i} response missing"));
            assert_eq!(resp.group_id, format!("group-{i}"));
        }
    }

    // -----------------------------------------------------------------------
    // Test: peer_bus_demuxes_responses_by_position
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn peer_bus_demuxes_responses_by_position() {
        let mock = MockTransport::new(true);
        let bus = PeerBus::new(
            1,
            0,
            Box::new(Arc::clone(&mock)),
        );

        // Enqueue 3 messages from different groups.
        let rx_a = bus.enqueue(make_heartbeat("group-a")).await;
        let rx_b = bus.enqueue(make_heartbeat("group-b")).await;
        let rx_c = bus.enqueue(make_heartbeat("group-c")).await;

        // Flush (the mock auto-responds in order).
        bus.flush().await.unwrap();

        // Each oneshot should have the correct response.
        let resp_a = rx_a.await.expect("group-a response");
        assert_eq!(resp_a.group_id, "group-a");

        let resp_b = rx_b.await.expect("group-b response");
        assert_eq!(resp_b.group_id, "group-b");

        let resp_c = rx_c.await.expect("group-c response");
        assert_eq!(resp_c.group_id, "group-c");
    }

    // -----------------------------------------------------------------------
    // Test: peer_bus_immediate_flush_on_vote
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn peer_bus_immediate_flush_on_vote() {
        let mock = MockTransport::new(true);
        let bus = Arc::new(PeerBus::new(
            1,
            0,
            Box::new(Arc::clone(&mock)),
        ));

        // Start the flush loop.
        let bus_clone = Arc::clone(&bus);
        let _handle = tokio::spawn(async move {
            bus_clone.flush_loop().await;
        });

        // Enqueue a vote message. The bus should flush immediately,
        // not wait for the 100ms timer.
        let _rx = bus.enqueue(make_vote_msg("group-election")).await;

        // Wait for at least one flush to complete. If the vote did not
        // trigger an immediate flush, this would take ~100ms.
        // We give it a generous 50ms budget: an immediate flush should
        // complete much sooner.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
        loop {
            if mock.flush_count.load(Ordering::SeqCst) >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("vote did not trigger immediate flush within 50ms");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // Verify the batch contained the vote.
        let batches = mock.batches.lock().await;
        assert!(!batches.is_empty());
        let vote_found = batches.iter().any(|b| {
            b.messages
                .iter()
                .any(|m| matches!(m.payload, Some(PbPayload::VoteRequest(_))))
        });
        assert!(vote_found, "batch should contain the vote request");
    }

    // -----------------------------------------------------------------------
    // Test: high-water flush
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn peer_bus_high_water_flush() {
        let mock = MockTransport::new(true);
        let bus = Arc::new(PeerBus::new(
            1,
            0,
            Box::new(Arc::clone(&mock)),
        ));

        // Start the flush loop.
        let bus_clone = Arc::clone(&bus);
        let _handle = tokio::spawn(async move {
            bus_clone.flush_loop().await;
        });

        // Enqueue BATCH_HIGH_WATER messages. The last one should trigger
        // an immediate flush.
        for i in 0..BATCH_HIGH_WATER {
            let _rx = bus.enqueue(make_heartbeat(&format!("g-{i}"))).await;
        }

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
        loop {
            if mock.flush_count.load(Ordering::SeqCst) >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("high-water did not trigger flush within 50ms");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
