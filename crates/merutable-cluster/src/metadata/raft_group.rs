// Metadata Raft group: replicates MetadataCommand through the same openraft
// infrastructure used by table Raft groups. Uses the special group_id
// `__metadata__` and stores its Raft log under `raft_dir/__metadata__/`.
//
// MetadataRaftStorage wraps DiskRaftStorage and overrides
// `apply_to_state_machine` to delegate to `MetadataStateMachine::apply`,
// then drains events through a broadcast channel.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{Adaptor, LogState, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};

use tokio::sync::RwLock;

use crate::metadata::cache::LocalMetadataCache;
use crate::metadata::state_machine::{MetadataCommand, MetadataEvent, MetadataStateMachine};
use crate::raft::disk_storage::DiskRaftStorage;
use crate::raft::group::GrpcNetworkFactory;
use crate::raft::network::{DynamicBatchedNetworkFactory, PeerBus};
use crate::raft::types::{MeruTypeConfig, RaftCommand, RaftResponse};

/// The well-known group_id used for the metadata Raft group.
pub const METADATA_GROUP_ID: &str = "__metadata__";

// ---------------------------------------------------------------------------
// MetadataRaftStorage
// ---------------------------------------------------------------------------

/// A `RaftStorage` that wraps `DiskRaftStorage` and overrides
/// `apply_to_state_machine` to apply metadata commands to the
/// `MetadataStateMachine`.
#[derive(Clone)]
pub struct MetadataRaftStorage {
    disk: DiskRaftStorage,
    sm: Arc<RwLock<MetadataStateMachine>>,
    cache: Arc<LocalMetadataCache>,
    watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
}

impl MetadataRaftStorage {
    pub fn new(
        disk: DiskRaftStorage,
        sm: Arc<RwLock<MetadataStateMachine>>,
        cache: Arc<LocalMetadataCache>,
        watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
    ) -> Self {
        Self { disk, sm, cache, watch_tx }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader — delegate to DiskRaftStorage
// ---------------------------------------------------------------------------

impl RaftLogReader<MeruTypeConfig> for MetadataRaftStorage {
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

impl RaftSnapshotBuilder<MeruTypeConfig> for MetadataRaftStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MeruTypeConfig>, StorageError<u64>> {
        self.disk.build_snapshot().await
    }
}

// ---------------------------------------------------------------------------
// RaftStorage — delegate everything except apply_to_state_machine
// ---------------------------------------------------------------------------

impl RaftStorage<MeruTypeConfig> for MetadataRaftStorage {
    type LogReader = MetadataRaftStorage;
    type SnapshotBuilder = MetadataRaftStorage;

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
        // 1. Let DiskRaftStorage track last_applied / membership.
        let responses = self.disk.apply_to_state_machine(entries).await?;

        // 2. Apply metadata commands to the MetadataStateMachine.
        let mut sm = self.sm.write().await;
        for entry in entries {
            if let EntryPayload::Normal(RaftCommand::Metadata { cmd }) = &entry.payload {
                let result = sm.apply(cmd.clone());

                // 3. Sync the cache immediately so that consumers who
                //    wake up on the broadcast event see the updated state.
                match cmd {
                    MetadataCommand::RegisterNode { .. }
                    | MetadataCommand::DeregisterNode { .. } => {
                        self.cache.update_rings(sm.rings().clone());
                    }
                    MetadataCommand::CreateTable { name, schema }
                    | MetadataCommand::UpdateSchema { table: name, schema } => {
                        self.cache.update_schema(name.clone(), schema.clone());
                    }
                    MetadataCommand::DropTable { name } => {
                        self.cache.remove_table(name);
                    }
                    _ => {}
                }
                let _ = result;
            }
        }

        // 4. Drain events and broadcast them.
        let events = sm.drain_events();
        for event in events {
            let _ = self.watch_tx.send(event);
        }

        Ok(responses)
    }

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
// MetadataRaftGroup
// ---------------------------------------------------------------------------

/// Wraps an openraft Raft instance dedicated to metadata replication.
///
/// All metadata mutations (CreateTable, RegisterNode, etc.) are proposed
/// through `client_write` and applied by `MetadataRaftStorage` to the
/// `MetadataStateMachine`.
pub struct MetadataRaftGroup {
    raft: openraft::Raft<MeruTypeConfig>,
    sm: Arc<RwLock<MetadataStateMachine>>,
}

impl MetadataRaftGroup {
    /// Create and initialize a single-node metadata Raft group.
    pub async fn create_and_initialize(
        node_id: u64,
        members: BTreeSet<u64>,
        sm: Arc<RwLock<MetadataStateMachine>>,
        cache: Arc<LocalMetadataCache>,
        raft_dir: PathBuf,
        watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: "meru-metadata".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        };

        let network_factory = GrpcNetworkFactory::new(
            METADATA_GROUP_ID.to_string(),
            node_id,
            HashMap::new(), // single-node: no peers
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = MetadataRaftStorage::new(disk, sm.clone(), cache, watch_tx);
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        // On a fresh disk, initialize sets up the first membership. On a
        // pod restart the log already has entries; openraft returns
        // NotAllowed, which is safe to swallow — the group is already
        // initialized.
        match raft.initialize(members).await {
            Ok(()) => tracing::info!("metadata raft group created and initialized"),
            Err(e) if e.to_string().contains("NotAllowed") => {
                tracing::info!("metadata raft group already initialized on disk — skipping");
            }
            Err(e) => return Err(e.into()),
        }

        Ok(Self { raft, sm })
    }

    /// Create a metadata Raft group with real gRPC network for multi-node
    /// replication. NOT initialized — call `initialize()` separately.
    pub async fn create_with_network(
        node_id: u64,
        sm: Arc<RwLock<MetadataStateMachine>>,
        cache: Arc<LocalMetadataCache>,
        raft_dir: PathBuf,
        watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
        peer_channels: HashMap<u64, tonic::transport::Channel>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: "meru-metadata".to_string(),
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 200,
            max_payload_entries: 1024,
            ..Default::default()
        };

        let network_factory = GrpcNetworkFactory::new(
            METADATA_GROUP_ID.to_string(),
            node_id,
            peer_channels,
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = MetadataRaftStorage::new(disk, sm.clone(), cache, watch_tx);
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        tracing::info!(node_id = node_id, "metadata raft group created (multi-node, uninit)");

        Ok(Self { raft, sm })
    }

    /// Create a metadata Raft group backed by `DynamicBatchedNetworkFactory`
    /// which looks up `PeerBus` instances at RPC time from the shared map.
    ///
    /// Use this for the **non-bootstrapper** path in multi-node startup:
    /// the group is created uninitialised and waits for the bootstrapper
    /// to include it via `initialize(members={all})`.
    ///
    /// Also used by the **bootstrapper** — it calls `initialize()` after
    /// construction, once all peers are reachable.
    pub async fn create_with_dynamic_network(
        node_id: u64,
        sm: Arc<RwLock<MetadataStateMachine>>,
        cache: Arc<LocalMetadataCache>,
        raft_dir: PathBuf,
        watch_tx: tokio::sync::broadcast::Sender<MetadataEvent>,
        peer_buses: Arc<tokio::sync::RwLock<HashMap<u64, Arc<PeerBus>>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            cluster_name: "meru-metadata".to_string(),
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 200,
            max_payload_entries: 1024,
            ..Default::default()
        };

        let network_factory = DynamicBatchedNetworkFactory::new(
            METADATA_GROUP_ID.to_string(),
            peer_buses,
        );

        let disk = DiskRaftStorage::open(raft_dir)?;
        let storage = MetadataRaftStorage::new(disk, sm.clone(), cache, watch_tx);
        let (log_store, state_machine) = Adaptor::new(storage);

        let raft = openraft::Raft::<MeruTypeConfig>::new(
            node_id,
            Arc::new(config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        tracing::info!(node_id = node_id, "metadata raft group created (dynamic network, uninit)");

        Ok(Self { raft, sm })
    }

    /// Initialize this group's membership.
    pub async fn initialize(
        &self,
        members: BTreeSet<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.initialize(members).await?;
        Ok(())
    }

    /// Propose a metadata command through Raft consensus.
    pub async fn client_write(
        &self,
        cmd: MetadataCommand,
    ) -> Result<RaftResponse, Box<dyn std::error::Error>> {
        let resp = self
            .raft
            .client_write(RaftCommand::Metadata { cmd })
            .await?;
        Ok(resp.data)
    }

    /// Access the underlying `MetadataStateMachine`.
    pub fn sm(&self) -> &Arc<RwLock<MetadataStateMachine>> {
        &self.sm
    }

    /// Access the underlying openraft `Raft` instance.
    ///
    /// Used by `RaftTransportService` to deliver incoming Raft messages
    /// from peers for the metadata group.
    pub fn raft(&self) -> &openraft::Raft<MeruTypeConfig> {
        &self.raft
    }

    /// Non-blocking check: who does this node think is the current leader?
    pub fn current_leader(&self) -> Option<u64> {
        let rx = self.raft.metrics();
        let leader = rx.borrow().current_leader;
        leader
    }

    /// Add a learner to the metadata Raft group.
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

    /// Change the membership of the metadata Raft group.
    pub async fn change_membership(
        &self,
        members: BTreeSet<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.change_membership(members, false).await?;
        Ok(())
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
    use crate::metadata::state_machine::MetadataCommand;
    use crate::ring::per_az::PerAzRings;
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};

    fn empty_cache() -> Arc<LocalMetadataCache> {
        Arc::new(LocalMetadataCache::new(
            PerAzRings::new(&[], 32),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    fn make_schema(name: &str) -> TableSchema {
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

    #[tokio::test]
    async fn metadata_raft_group_create_table() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(RwLock::new(MetadataStateMachine::new(32)));
        let (watch_tx, _rx) = tokio::sync::broadcast::channel(64);

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("metadata_raft");
        let group = MetadataRaftGroup::create_and_initialize(
            1,
            members,
            sm.clone(),
            empty_cache(),
            raft_dir,
            watch_tx,
        )
        .await
        .unwrap();

        // Create a table through the metadata Raft group.
        group
            .client_write(MetadataCommand::CreateTable {
                name: "users".into(),
                schema: make_schema("users"),
            })
            .await
            .unwrap();

        // Verify the SM has the schema.
        let sm_guard = sm.read().await;
        assert!(sm_guard.table_schema("users").is_some());
    }

    #[tokio::test]
    async fn metadata_raft_group_register_node() {
        use crate::metadata::state_machine::{NodeEntry, NodeStatus};

        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(RwLock::new(MetadataStateMachine::new(32)));
        let (watch_tx, _rx) = tokio::sync::broadcast::channel(64);

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("metadata_raft");
        let group = MetadataRaftGroup::create_and_initialize(
            1,
            members,
            sm.clone(),
            empty_cache(),
            raft_dir,
            watch_tx,
        )
        .await
        .unwrap();

        // Register a node.
        group
            .client_write(MetadataCommand::RegisterNode {
                node: NodeEntry {
                    node_id: 1,
                    address: "127.0.0.1:9001".to_string(),
                    az: "AZ-1".to_string(),
                    table_count: 0,
                    status: NodeStatus::Active,
                },
            })
            .await
            .unwrap();

        let sm_guard = sm.read().await;
        assert!(sm_guard.node(1).is_some());
    }

    #[tokio::test]
    async fn metadata_raft_group_broadcasts_events() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(RwLock::new(MetadataStateMachine::new(32)));
        let (watch_tx, mut rx) = tokio::sync::broadcast::channel(64);

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("metadata_raft");
        let group = MetadataRaftGroup::create_and_initialize(
            1,
            members,
            sm.clone(),
            empty_cache(),
            raft_dir,
            watch_tx,
        )
        .await
        .unwrap();

        // Create a table and verify the event is broadcast.
        group
            .client_write(MetadataCommand::CreateTable {
                name: "events_test".into(),
                schema: make_schema("events_test"),
            })
            .await
            .unwrap();

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx.recv(),
        )
        .await
        .expect("timed out waiting for event")
        .expect("broadcast closed");

        match event {
            MetadataEvent::TableCreated { name, .. } => {
                assert_eq!(name, "events_test");
            }
            other => panic!("expected TableCreated, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn metadata_raft_group_drop_table() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = Arc::new(RwLock::new(MetadataStateMachine::new(32)));
        let (watch_tx, _rx) = tokio::sync::broadcast::channel(64);

        let mut members = BTreeSet::new();
        members.insert(1u64);

        let raft_dir = tmp.path().join("metadata_raft");
        let group = MetadataRaftGroup::create_and_initialize(
            1,
            members,
            sm.clone(),
            empty_cache(),
            raft_dir,
            watch_tx,
        )
        .await
        .unwrap();

        // Create then drop.
        group
            .client_write(MetadataCommand::CreateTable {
                name: "temp".into(),
                schema: make_schema("temp"),
            })
            .await
            .unwrap();

        group
            .client_write(MetadataCommand::DropTable {
                name: "temp".into(),
            })
            .await
            .unwrap();

        let sm_guard = sm.read().await;
        assert!(sm_guard.table_schema("temp").is_none());
    }
}
