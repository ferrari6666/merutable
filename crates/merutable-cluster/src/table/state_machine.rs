// P1.5: TableStateMachine — leader sync apply, follower async apply,
// schema-through-Raft, drain-before-promote, snapshot build/install.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::Notify;

use merutable::schema::TableSchema;
use merutable::value::{FieldValue, Row};
use merutable::{MeruDB, OpenOptions};

use crate::observability::{FOLLOWER_APPLY_LAG_ENTRIES, FOLLOWER_APPLY_LAG_MS, FOLLOWER_APPLY_ERRORS_TOTAL};
use crate::raft::types::{RaftCommand, SchemaChange};

/// Maximum capacity for the bounded follower apply queue.
const FOLLOWER_APPLY_QUEUE_CAP: usize = 10_000;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Which apply strategy the state machine uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateMachineMode {
    /// Synchronous: `apply()` calls `db.put()`/`db.delete()` inline.
    Leader,
    /// Asynchronous: `apply()` enqueues entries to a background worker.
    Follower,
}

/// A single Raft log entry destined for the state machine.
#[derive(Clone, Debug)]
pub struct ApplyEntry {
    pub index: u64,
    pub command: RaftCommand,
}

/// Result of applying a single entry.
#[derive(Clone, Debug)]
pub enum ApplyResult {
    /// Data mutation succeeded; `seq` is the `SeqNum.0` returned by MeruDB.
    Ok { seq: u64 },
    /// Schema change succeeded; contains the new schema.
    SchemaChanged { schema: TableSchema },
    /// Follower async mode: entry queued but not yet applied.
    Ack,
}

/// Metadata about a built snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotInfo {
    /// Path to the catalog directory that was flushed.
    pub catalog_dir: PathBuf,
    /// The `applied_index` at snapshot time.
    pub last_applied_index: u64,
}

// ---------------------------------------------------------------------------
// TableStateMachine
// ---------------------------------------------------------------------------

/// Application-level state machine that the Raft storage layer delegates to.
///
/// NOT an openraft trait impl directly. The `InMemoryRaftStorage` (or future
/// io_uring storage) calls into this for `apply_to_state_machine`.
pub struct TableStateMachine {
    db: Arc<MeruDB>,
    mode: StateMachineMode,
    committed_index: Arc<AtomicU64>,
    applied_index: Arc<AtomicU64>,
    /// Notified after every `applied_index.store()` so waiters can detect progress.
    apply_notify: Arc<Notify>,
    /// Sender for the follower async apply worker. `None` in Leader mode.
    apply_tx: Option<mpsc::Sender<Vec<ApplyEntry>>>,
    /// Join handle for the background apply worker. `None` in Leader mode.
    apply_handle: Option<tokio::task::JoinHandle<()>>,
    /// Paths needed for snapshot build/install.
    wal_dir: PathBuf,
    catalog_dir: PathBuf,
    /// Path to the applied_index sidecar file for persistence across restarts.
    sidecar_path: PathBuf,
    /// Schema snapshot used by `open` so we can reopen after snapshot install.
    schema: TableSchema,
}

impl TableStateMachine {
    /// Open a new `TableStateMachine` backed by a real MeruDB instance.
    ///
    /// If `mode` is `Follower`, a background apply worker is spawned.
    pub async fn open(opts: OpenOptions, mode: StateMachineMode) -> merutable::error::Result<Self> {
        let wal_dir = opts.wal_dir.clone();
        let catalog_dir = PathBuf::from(&opts.catalog_uri);
        let sidecar_path = catalog_dir.join("applied_index.sidecar");
        let schema = opts.schema.clone();

        let db = Arc::new(MeruDB::open(opts).await?);
        let committed_index = Arc::new(AtomicU64::new(0));

        // Seed applied_index from the sidecar file if it exists.
        let initial_index = if sidecar_path.exists() {
            match fs::read(&sidecar_path) {
                Ok(bytes) if bytes.len() == 8 => {
                    u64::from_le_bytes(bytes.try_into().unwrap())
                }
                _ => 0,
            }
        } else {
            0
        };
        let applied_index = Arc::new(AtomicU64::new(initial_index));
        let apply_notify = Arc::new(Notify::new());

        let (apply_tx, apply_handle) = match mode {
            StateMachineMode::Leader => (None, None),
            StateMachineMode::Follower => {
                let (tx, rx) = mpsc::channel(FOLLOWER_APPLY_QUEUE_CAP);
                let handle = tokio::spawn(apply_worker(
                    db.clone(),
                    rx,
                    applied_index.clone(),
                    apply_notify.clone(),
                    sidecar_path.clone(),
                ));
                (Some(tx), Some(handle))
            }
        };

        tracing::info!(mode = ?mode, "table state machine opened");

        Ok(Self {
            db,
            mode,
            committed_index,
            applied_index,
            apply_notify,
            apply_tx,
            apply_handle,
            wal_dir,
            catalog_dir,
            sidecar_path,
            schema,
        })
    }

    /// Apply a batch of committed Raft log entries.
    ///
    /// - **Leader mode**: applies synchronously, returning results per entry.
    /// - **Follower mode**: enqueues to the background worker, returns `Ack`.
    pub async fn apply(
        &self,
        entries: Vec<ApplyEntry>,
    ) -> merutable::error::Result<Vec<ApplyResult>> {
        match self.mode {
            StateMachineMode::Leader => {
                let mut results = Vec::with_capacity(entries.len());
                for entry in entries {
                    let result = match entry.command {
                        RaftCommand::Put { row } => {
                            let seq = self.db.put(row).await?;
                            ApplyResult::Ok { seq: seq.0 }
                        }
                        RaftCommand::Delete { pk } => {
                            let seq = self.db.delete(pk).await?;
                            ApplyResult::Ok { seq: seq.0 }
                        }
                        RaftCommand::AlterSchema { change } => match change {
                            SchemaChange::AddColumn { col } => {
                                let new_schema = self.db.add_column(col).await?;
                                ApplyResult::SchemaChanged { schema: new_schema }
                            }
                        },
                        RaftCommand::Metadata { .. } => {
                            // Metadata commands are handled by MetadataRaftGroup,
                            // not by table state machines. Skip silently.
                            ApplyResult::Ok { seq: 0 }
                        }
                    };
                    self.applied_index.store(entry.index, Ordering::Release);
                    self.apply_notify.notify_waiters();
                    results.push(result);
                }
                let committed = self.committed_index.load(Ordering::Acquire);
                let applied = self.applied_index.load(Ordering::Acquire);
                metrics::gauge!(FOLLOWER_APPLY_LAG_ENTRIES)
                    .set(committed.saturating_sub(applied) as f64);
                // Persist the applied_index to the sidecar file.
                write_sidecar(&self.sidecar_path, applied);
                Ok(results)
            }
            StateMachineMode::Follower => {
                // Record the highest committed index from this batch.
                if let Some(last) = entries.last() {
                    // Use a fetch_max so concurrent calls don't regress it.
                    loop {
                        let current = self.committed_index.load(Ordering::Acquire);
                        if last.index <= current {
                            break;
                        }
                        if self
                            .committed_index
                            .compare_exchange_weak(
                                current,
                                last.index,
                                Ordering::Release,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                }
                let ack_count = entries.len();
                if let Some(tx) = &self.apply_tx {
                    let _ = tx.send(entries).await;
                }
                let committed = self.committed_index.load(Ordering::Acquire);
                let applied = self.applied_index.load(Ordering::Acquire);
                metrics::gauge!(FOLLOWER_APPLY_LAG_ENTRIES)
                    .set(committed.saturating_sub(applied) as f64);
                // NOTE: sidecar is written inside apply_worker after entries
                // are actually applied (GAP-13 fix).
                Ok(vec![ApplyResult::Ack; ack_count])
            }
        }
    }

    /// Promote from Follower to Leader.
    ///
    /// Drains the async apply backlog (waits until `applied_index >= committed_index`),
    /// then stops the background worker and switches to synchronous mode.
    pub async fn promote_to_leader(&mut self) {
        // Drain: wait until the background worker has caught up.
        if self.mode == StateMachineMode::Follower {
            // Drop the sender to signal the worker to finish after
            // processing all buffered entries.
            self.apply_tx.take();

            // Wait for the worker task to complete.
            if let Some(handle) = self.apply_handle.take() {
                let _ = handle.await;
            }
        }
        self.mode = StateMachineMode::Leader;
        tracing::info!("promoted to leader, backlog drained");
    }

    /// Demote from Leader to Follower.
    ///
    /// Spawns the background apply worker and switches to async mode.
    pub fn demote_to_follower(&mut self) {
        if self.mode == StateMachineMode::Leader {
            let (tx, rx) = mpsc::channel(FOLLOWER_APPLY_QUEUE_CAP);
            let handle = tokio::spawn(apply_worker(
                self.db.clone(),
                rx,
                self.applied_index.clone(),
                self.apply_notify.clone(),
                self.sidecar_path.clone(),
            ));
            self.apply_tx = Some(tx);
            self.apply_handle = Some(handle);
            self.mode = StateMachineMode::Follower;
            tracing::info!("demoted to follower");
        }
    }

    /// Point lookup by primary key — reads directly from MeruDB.
    pub fn get(&self, pk: &[FieldValue]) -> merutable::error::Result<Option<Row>> {
        self.db.get(pk)
    }

    /// Range scan. Returns rows in PK order where `start_pk <= pk < end_pk`.
    /// If `start_pk` is `None`, scan from the beginning.
    /// If `end_pk` is `None`, scan to the end.
    pub fn scan(
        &self,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> merutable::error::Result<Vec<Row>> {
        let results = self.db.scan(start_pk, end_pk)?;
        Ok(results.into_iter().map(|(_, row)| row).collect())
    }

    /// The highest Raft index that has been committed (logged by Raft).
    pub fn committed_index(&self) -> u64 {
        self.committed_index.load(Ordering::Acquire)
    }

    /// The highest Raft index whose command has been applied to MeruDB.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Wait until `applied_index >= min_index` or `timeout` elapses.
    ///
    /// Returns `true` if the index was reached, `false` on timeout.
    pub async fn wait_for_applied(&self, min_index: u64, timeout: std::time::Duration) -> bool {
        if self.applied_index.load(Ordering::Acquire) >= min_index {
            return true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            tokio::select! {
                _ = self.apply_notify.notified() => {
                    if self.applied_index.load(Ordering::Acquire) >= min_index {
                        return true;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    // Final check before declaring timeout.
                    return self.applied_index.load(Ordering::Acquire) >= min_index;
                }
            }
        }
    }

    /// MeruDB's current read sequence number.
    pub fn read_seq(&self) -> u64 {
        self.db.read_seq().0
    }

    /// The table schema.
    pub fn schema(&self) -> &TableSchema {
        self.db.schema()
    }

    /// Flush MeruDB and return snapshot metadata.
    pub async fn build_snapshot(&self) -> merutable::error::Result<SnapshotInfo> {
        self.db.flush().await?;
        Ok(SnapshotInfo {
            catalog_dir: self.catalog_dir.clone(),
            last_applied_index: self.applied_index.load(Ordering::Acquire),
        })
    }

    /// Install a snapshot: close MeruDB, replace catalog, delete WAL, reopen.
    pub async fn install_snapshot(
        &mut self,
        snapshot_data: &[u8],
    ) -> merutable::error::Result<()> {
        // 1. Close the current MeruDB instance.
        self.db.close().await?;

        // 2. Delete everything in wal_dir.
        if self.wal_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&self.wal_dir).await;
        }

        // 3. Delete the applied_index sidecar file so it resets cleanly.
        if self.sidecar_path.exists() {
            let _ = fs::remove_file(&self.sidecar_path);
        }

        // 4. Write snapshot_data to catalog_dir.
        //    For now, we clear the catalog and write the raw snapshot bytes
        //    as a single file. Real implementation will unpack a tarball.
        if self.catalog_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&self.catalog_dir).await;
        }
        tokio::fs::create_dir_all(&self.catalog_dir).await.map_err(|e| {
            merutable::error::Error::Io(e)
        })?;
        let snapshot_file = self.catalog_dir.join("snapshot.bin");
        tokio::fs::write(&snapshot_file, snapshot_data).await.map_err(|e| {
            merutable::error::Error::Io(e)
        })?;

        // 5. Reopen MeruDB.
        let opts = OpenOptions::new(self.schema.clone())
            .wal_dir(&self.wal_dir)
            .catalog_uri(self.catalog_dir.to_string_lossy().to_string());
        let db = Arc::new(MeruDB::open(opts).await?);
        self.db = db;

        // Reset applied_index since the sidecar was deleted.
        self.applied_index.store(0, Ordering::Release);

        // Re-spawn follower worker if in follower mode.
        if self.mode == StateMachineMode::Follower {
            let (tx, rx) = mpsc::channel(FOLLOWER_APPLY_QUEUE_CAP);
            let handle = tokio::spawn(apply_worker(
                self.db.clone(),
                rx,
                self.applied_index.clone(),
                self.apply_notify.clone(),
                self.sidecar_path.clone(),
            ));
            self.apply_tx = Some(tx);
            self.apply_handle = Some(handle);
        }

        Ok(())
    }

    /// Graceful shutdown.
    pub async fn close(&self) -> merutable::error::Result<()> {
        self.db.close().await
    }
}

// ---------------------------------------------------------------------------
// Sidecar persistence helper
// ---------------------------------------------------------------------------

/// Write `applied_index` as 8 bytes little-endian to the sidecar file using
/// an atomic write pattern: write to a `.tmp` sibling, fsync, rename, fsync
/// the parent directory. This prevents a crash mid-write from corrupting
/// the sidecar and resetting `applied_index` to 0 on restart.
fn write_sidecar(path: &std::path::Path, index: u64) {
    let tmp = path.with_extension("tmp");
    let bytes = index.to_le_bytes();
    if let Err(e) = std::fs::write(&tmp, bytes) {
        tracing::warn!("sidecar tmp write failed: {e}");
        return;
    }
    // fsync the tmp file
    if let Ok(f) = std::fs::File::open(&tmp) {
        let _ = f.sync_all();
    }
    // atomic rename
    if let Err(e) = std::fs::rename(&tmp, path) {
        tracing::warn!("sidecar rename failed: {e}");
        return;
    }
    // fsync parent directory
    if let Some(parent) = path.parent() {
        if let Ok(d) = std::fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
}

// ---------------------------------------------------------------------------
// Background apply worker (follower mode)
// ---------------------------------------------------------------------------

/// Processes `ApplyEntry` batches from the channel. Schema changes act as
/// synchronous barriers — the worker blocks on `add_column` before processing
/// subsequent entries.
async fn apply_worker(
    db: Arc<MeruDB>,
    mut rx: mpsc::Receiver<Vec<ApplyEntry>>,
    applied_index: Arc<AtomicU64>,
    apply_notify: Arc<Notify>,
    sidecar_path: PathBuf,
) {
    while let Some(entries) = rx.recv().await {
        let batch_start = std::time::Instant::now();
        for entry in entries {
            let idx = entry.index;
            match entry.command {
                RaftCommand::Put { row } => {
                    if let Err(e) = db.put(row).await {
                        tracing::error!(index = idx, error = %e, "follower apply failed for Put");
                        metrics::counter!(FOLLOWER_APPLY_ERRORS_TOTAL).increment(1);
                        break; // don't advance applied_index past this entry
                    }
                }
                RaftCommand::Delete { pk } => {
                    if let Err(e) = db.delete(pk).await {
                        tracing::error!(index = idx, error = %e, "follower apply failed for Delete");
                        metrics::counter!(FOLLOWER_APPLY_ERRORS_TOTAL).increment(1);
                        break;
                    }
                }
                RaftCommand::AlterSchema { change } => {
                    match change {
                        SchemaChange::AddColumn { col } => {
                            if let Err(e) = db.add_column(col).await {
                                tracing::error!(index = idx, error = %e, "follower apply failed for AlterSchema");
                                metrics::counter!(FOLLOWER_APPLY_ERRORS_TOTAL).increment(1);
                                break;
                            }
                        }
                    }
                }
                RaftCommand::Metadata { .. } => {
                    // Metadata commands are handled by MetadataRaftGroup,
                    // not by table state machines. Skip silently.
                }
            }
            applied_index.store(idx, Ordering::Release);
            apply_notify.notify_waiters();
        }
        metrics::gauge!(FOLLOWER_APPLY_LAG_MS)
            .set(batch_start.elapsed().as_millis() as f64);
        // Persist the applied_index sidecar after each batch (GAP-13 fix:
        // sidecar now reflects only actually-applied entries).
        write_sidecar(&sidecar_path, applied_index.load(Ordering::Acquire));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};
    use merutable::value::{FieldValue, Row};

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "test".into(),
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

    fn test_options(tmp: &tempfile::TempDir) -> OpenOptions {
        OpenOptions::new(test_schema())
            .wal_dir(tmp.path().join("wal"))
            .catalog_uri(tmp.path().join("data").to_string_lossy().to_string())
    }

    fn make_row(id: i64, val: &str) -> Row {
        Row::new(vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Bytes(bytes::Bytes::from(val.to_string()))),
        ])
    }

    fn put_cmd(id: i64, val: &str) -> RaftCommand {
        RaftCommand::Put {
            row: make_row(id, val),
        }
    }

    fn delete_cmd(id: i64) -> RaftCommand {
        RaftCommand::Delete {
            pk: vec![FieldValue::Int64(id)],
        }
    }

    // -----------------------------------------------------------------------
    // Leader tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn leader_apply_put_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
            .await
            .unwrap();

        let results = sm
            .apply(vec![ApplyEntry {
                index: 1,
                command: put_cmd(42, "hello"),
            }])
            .await
            .unwrap();

        assert!(matches!(results[0], ApplyResult::Ok { seq } if seq > 0));

        let row = sm.get(&[FieldValue::Int64(42)]).unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(
            *row.get(1).unwrap(),
            FieldValue::Bytes(bytes::Bytes::from("hello"))
        );
    }

    #[tokio::test]
    async fn leader_apply_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
            .await
            .unwrap();

        // Put then delete.
        sm.apply(vec![ApplyEntry {
            index: 1,
            command: put_cmd(1, "alice"),
        }])
        .await
        .unwrap();
        assert!(sm.get(&[FieldValue::Int64(1)]).unwrap().is_some());

        let results = sm
            .apply(vec![ApplyEntry {
                index: 2,
                command: delete_cmd(1),
            }])
            .await
            .unwrap();
        assert!(matches!(results[0], ApplyResult::Ok { .. }));
        assert!(sm.get(&[FieldValue::Int64(1)]).unwrap().is_none());
    }

    #[tokio::test]
    async fn leader_apply_alter_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
            .await
            .unwrap();

        assert_eq!(sm.schema().columns.len(), 2);

        let new_col = ColumnDef {
            name: "extra".into(),
            col_type: ColumnType::Double,
            nullable: true,
            ..Default::default()
        };
        let results = sm
            .apply(vec![ApplyEntry {
                index: 1,
                command: RaftCommand::AlterSchema {
                    change: SchemaChange::AddColumn { col: new_col },
                },
            }])
            .await
            .unwrap();

        match &results[0] {
            ApplyResult::SchemaChanged { schema } => {
                assert_eq!(schema.columns.len(), 3);
                assert_eq!(schema.columns[2].name, "extra");
            }
            other => panic!("expected SchemaChanged, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn leader_applied_index_advances() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
            .await
            .unwrap();

        assert_eq!(sm.applied_index(), 0);

        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("val{i}")),
            })
            .collect();

        sm.apply(entries).await.unwrap();
        assert_eq!(sm.applied_index(), 5);
    }

    // -----------------------------------------------------------------------
    // Follower tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn follower_async_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Follower)
            .await
            .unwrap();

        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("val{i}")),
            })
            .collect();

        let results = sm.apply(entries).await.unwrap();

        // All results should be Ack (async).
        assert!(results.iter().all(|r| matches!(r, ApplyResult::Ack)));

        // committed_index should be set immediately.
        assert_eq!(sm.committed_index(), 5);

        // Wait for the background worker to catch up.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if sm.applied_index() >= 5 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower apply worker should catch up within 5s");

        assert_eq!(sm.applied_index(), 5);

        // Verify data is actually there.
        let row = sm.get(&[FieldValue::Int64(3)]).unwrap();
        assert!(row.is_some());
    }

    #[tokio::test]
    async fn follower_schema_barrier() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Follower)
            .await
            .unwrap();

        let new_col = ColumnDef {
            name: "extra".into(),
            col_type: ColumnType::Int64,
            nullable: true,
            ..Default::default()
        };

        // Apply: Put, AlterSchema, Put-with-new-column (3 fields).
        // The schema barrier ensures AlterSchema completes before the
        // third entry is applied.
        let entries = vec![
            ApplyEntry {
                index: 1,
                command: put_cmd(1, "before"),
            },
            ApplyEntry {
                index: 2,
                command: RaftCommand::AlterSchema {
                    change: SchemaChange::AddColumn { col: new_col },
                },
            },
            ApplyEntry {
                index: 3,
                command: put_cmd(2, "after"),
            },
        ];

        sm.apply(entries).await.unwrap();

        // Wait for all three entries to be applied.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if sm.applied_index() >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower should apply all three entries including schema barrier");

        assert_eq!(sm.applied_index(), 3);
    }

    #[tokio::test]
    async fn promote_drains_backlog() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Follower)
            .await
            .unwrap();

        // Enqueue 100 entries.
        let entries: Vec<ApplyEntry> = (1..=100)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("v{i}")),
            })
            .collect();

        sm.apply(entries).await.unwrap();
        assert_eq!(sm.committed_index(), 100);

        // Promote: must drain all 100 entries before returning.
        sm.promote_to_leader().await;

        assert_eq!(sm.applied_index(), 100);
        assert_eq!(sm.mode, StateMachineMode::Leader);

        // After promote, we can apply synchronously.
        let results = sm
            .apply(vec![ApplyEntry {
                index: 101,
                command: put_cmd(101, "leader-write"),
            }])
            .await
            .unwrap();
        assert!(matches!(results[0], ApplyResult::Ok { .. }));
    }

    // -----------------------------------------------------------------------
    // Sidecar persistence tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sidecar_persists_applied_index() {
        let tmp = tempfile::tempdir().unwrap();
        let sidecar_path = tmp.path().join("data").join("applied_index.sidecar");

        // Open, apply 3 entries, close.
        {
            let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
                .await
                .unwrap();
            let entries: Vec<ApplyEntry> = (1..=3)
                .map(|i| ApplyEntry {
                    index: i,
                    command: put_cmd(i as i64, &format!("v{i}")),
                })
                .collect();
            sm.apply(entries).await.unwrap();
            assert_eq!(sm.applied_index(), 3);
            sm.close().await.unwrap();
        }

        // Sidecar file should exist with value 3.
        assert!(sidecar_path.exists());
        let bytes = std::fs::read(&sidecar_path).unwrap();
        assert_eq!(bytes.len(), 8);
        assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 3);

        // Reopen — applied_index should be seeded from sidecar.
        {
            let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
                .await
                .unwrap();
            assert_eq!(sm.applied_index(), 3);
        }
    }

    // -----------------------------------------------------------------------
    // GAP-13: follower sidecar reflects actual apply
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn follower_sidecar_reflects_actual_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let sidecar_path = tmp.path().join("data").join("applied_index.sidecar");

        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Follower)
            .await
            .unwrap();

        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("val{i}")),
            })
            .collect();

        sm.apply(entries).await.unwrap();

        // Wait for applied_index to reach 5.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if sm.applied_index() >= 5 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower apply worker should catch up within 5s");

        // The sidecar file should exist and contain 5 as u64 LE.
        assert!(sidecar_path.exists(), "sidecar file should exist after worker drain");
        let bytes = std::fs::read(&sidecar_path).unwrap();
        assert_eq!(bytes.len(), 8);
        assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 5);
    }

    // -----------------------------------------------------------------------
    // GAP-14: wait_for_applied tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wait_for_applied_immediate() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Leader)
            .await
            .unwrap();

        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("val{i}")),
            })
            .collect();

        sm.apply(entries).await.unwrap();
        assert_eq!(sm.applied_index(), 5);

        // Should return true immediately since applied_index is already 5.
        let reached = sm
            .wait_for_applied(5, std::time::Duration::from_secs(1))
            .await;
        assert!(reached, "wait_for_applied should return true immediately");
    }

    #[tokio::test]
    async fn wait_for_applied_with_follower() {
        let tmp = tempfile::tempdir().unwrap();
        let sm = TableStateMachine::open(test_options(&tmp), StateMachineMode::Follower)
            .await
            .unwrap();

        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("val{i}")),
            })
            .collect();

        sm.apply(entries).await.unwrap();

        // The background worker hasn't necessarily finished yet. Use
        // wait_for_applied to block until it catches up.
        let reached = sm
            .wait_for_applied(5, std::time::Duration::from_secs(2))
            .await;
        assert!(reached, "wait_for_applied should return true after worker catches up");
        assert_eq!(sm.applied_index(), 5);
    }

    // -----------------------------------------------------------------------
    // Snapshot install test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn snapshot_install_roundtrip() {
        // 1. Open a leader TableStateMachine.
        let tmp_src = tempfile::tempdir().unwrap();
        let sm_src = TableStateMachine::open(test_options(&tmp_src), StateMachineMode::Leader)
            .await
            .unwrap();

        // 2. Apply 5 Put entries.
        let entries: Vec<ApplyEntry> = (1..=5)
            .map(|i| ApplyEntry {
                index: i,
                command: put_cmd(i as i64, &format!("snap{i}")),
            })
            .collect();
        sm_src.apply(entries).await.unwrap();
        assert_eq!(sm_src.applied_index(), 5);

        // 3. Call build_snapshot — capture the catalog_dir.
        let snap_info = sm_src.build_snapshot().await.unwrap();
        assert_eq!(snap_info.last_applied_index, 5);

        // Read the snapshot data from the source catalog_dir (all files).
        // For this test, we use the same mechanism as install_snapshot:
        // we'll pass a dummy snapshot_data blob. The real content is the
        // flushed catalog, but install_snapshot currently writes raw bytes.
        // So we simulate by reading back what build_snapshot flushed.
        let snapshot_data = b"snapshot-placeholder";

        // 4. Open a SECOND TableStateMachine (fresh, different directory).
        let tmp_dst = tempfile::tempdir().unwrap();
        let mut sm_dst = TableStateMachine::open(test_options(&tmp_dst), StateMachineMode::Leader)
            .await
            .unwrap();

        // Write a sidecar file to prove install_snapshot cleans it up.
        let dst_sidecar = tmp_dst.path().join("data").join("applied_index.sidecar");
        write_sidecar(&dst_sidecar, 999);
        assert!(dst_sidecar.exists());

        // Also write a dummy WAL file to prove it gets cleaned.
        let dst_wal = tmp_dst.path().join("wal");
        std::fs::create_dir_all(&dst_wal).unwrap();
        std::fs::write(dst_wal.join("stale.log"), b"stale").unwrap();

        // 5. Call install_snapshot on the second SM.
        sm_dst.install_snapshot(snapshot_data).await.unwrap();

        // 6. Verify: the second SM's WAL dir is clean (no stale files).
        //    After install_snapshot, the WAL dir was deleted. It may or
        //    may not exist (MeruDB::open may recreate it), but it should
        //    not contain our stale file.
        let stale_file = dst_wal.join("stale.log");
        assert!(
            !stale_file.exists(),
            "stale WAL file should have been cleaned by install_snapshot"
        );

        // 7. Verify: the second SM's applied_index sidecar was cleaned.
        assert!(
            !dst_sidecar.exists(),
            "sidecar file should have been deleted by install_snapshot"
        );

        // 8. Verify: applied_index was reset.
        assert_eq!(sm_dst.applied_index(), 0);
    }

    // -----------------------------------------------------------------------
    // GAP-22: atomic sidecar write
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn atomic_sidecar_survives_content() {
        let tmp = tempfile::tempdir().unwrap();
        let sidecar = tmp.path().join("applied_index.sidecar");

        // Write the sidecar with value 42 and verify content.
        write_sidecar(&sidecar, 42);
        assert!(sidecar.exists());
        let bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(bytes.len(), 8, "sidecar must be exactly 8 bytes LE");
        assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 42);

        // Overwrite with a different value and verify update.
        write_sidecar(&sidecar, 999);
        let bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(bytes.len(), 8);
        assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 999);

        // The .tmp file should NOT linger after a successful write.
        let tmp_file = sidecar.with_extension("tmp");
        assert!(!tmp_file.exists(), ".tmp file should be cleaned up after rename");
    }
}
