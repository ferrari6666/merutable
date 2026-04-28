// GAP-2: DiskRaftStorage — io_uring/fallback-backed RaftStorage adapter.
//
// Wraps `RaftLog` (from src/io/uring_log.rs) to implement openraft's
// `RaftStorage`, `RaftLogReader`, and `RaftSnapshotBuilder` traits. Log
// entries are persisted to the Raft log file; hard state (vote) is stored
// in a small sidecar file. An in-memory `LogIndex` maps log_index to
// (file_offset, payload_length) for fast lookups.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use openraft::storage::{LogState, Snapshot};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, LogId, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote, ErrorSubject, ErrorVerb,
};

use crate::io::uring_log::RaftLog;
use super::store_parts::{LogIndex, StateMachineState, StoredSnapshot};
use super::types::{MeruTypeConfig, RaftCommand, RaftResponse};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Alignment used by `RaftLog` framing (must match uring_log::ALIGN).
const ALIGN: usize = 4096;

/// Size of the `RaftLog` entry header (payload_len: u32 + crc32c: u32).
const HEADER_SIZE: usize = 8;

/// Size of the hard state file: term(8) + node_id(8) + committed(1).
const HARD_STATE_SIZE: usize = 17;

/// Entry-type tag for blank / no-op entries.
const ENTRY_TYPE_BLANK: u8 = 0;

/// Entry-type tag for normal command entries.
const ENTRY_TYPE_NORMAL: u8 = 1;

/// Entry-type tag for membership change entries.
const ENTRY_TYPE_MEMBERSHIP: u8 = 2;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Round `n` up to the next multiple of `align` (mirrors uring_log::round_up).
fn round_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

/// Compute the on-disk frame size for a payload of `payload_len` bytes.
/// This must match the framing in `RaftLog::append`.
fn framed_size(payload_len: usize) -> usize {
    round_up(HEADER_SIZE + payload_len, ALIGN)
}

// ---------------------------------------------------------------------------
// On-disk entry serialization
// ---------------------------------------------------------------------------

/// Serialize a Raft log entry into the payload bytes written to `RaftLog`.
///
/// Layout: `[log_index: u64 LE][term: u64 LE][entry_type: u8][command_bytes]`
fn serialize_entry(entry: &Entry<MeruTypeConfig>) -> Vec<u8> {
    let index = entry.log_id.index;
    let term = entry.log_id.leader_id.term;

    let (entry_type, command_bytes) = match &entry.payload {
        EntryPayload::Blank => (ENTRY_TYPE_BLANK, Vec::new()),
        EntryPayload::Normal(cmd) => {
            let bytes = postcard::to_stdvec(cmd).expect("postcard serialize RaftCommand");
            (ENTRY_TYPE_NORMAL, bytes)
        }
        EntryPayload::Membership(m) => {
            let bytes = postcard::to_stdvec(m).expect("postcard serialize Membership");
            (ENTRY_TYPE_MEMBERSHIP, bytes)
        }
    };

    let mut buf = Vec::with_capacity(17 + command_bytes.len());
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&term.to_le_bytes());
    buf.push(entry_type);
    buf.extend_from_slice(&command_bytes);
    buf
}

/// Deserialize a Raft log entry from the payload bytes read from `RaftLog`.
fn deserialize_entry(payload: &[u8]) -> Result<Entry<MeruTypeConfig>, String> {
    if payload.len() < 17 {
        return Err(format!("payload too short: {} bytes", payload.len()));
    }

    let index = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let term = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let entry_type = payload[16];
    let command_bytes = &payload[17..];

    let payload_enum = match entry_type {
        ENTRY_TYPE_BLANK => EntryPayload::Blank,
        ENTRY_TYPE_NORMAL => {
            let cmd: RaftCommand = postcard::from_bytes(command_bytes)
                .map_err(|e| format!("postcard deserialize RaftCommand: {e}"))?;
            EntryPayload::Normal(cmd)
        }
        ENTRY_TYPE_MEMBERSHIP => {
            let m = postcard::from_bytes(command_bytes)
                .map_err(|e| format!("postcard deserialize Membership: {e}"))?;
            EntryPayload::Membership(m)
        }
        other => return Err(format!("unknown entry_type: {other}")),
    };

    Ok(Entry {
        log_id: LogId::new(
            openraft::CommittedLeaderId::new(term, 0),
            index,
        ),
        payload: payload_enum,
    })
}

// ---------------------------------------------------------------------------
// Hard state persistence
// ---------------------------------------------------------------------------

fn serialize_vote(vote: &Vote<u64>) -> [u8; HARD_STATE_SIZE] {
    let mut buf = [0u8; HARD_STATE_SIZE];
    buf[0..8].copy_from_slice(&vote.leader_id.term.to_le_bytes());
    buf[8..16].copy_from_slice(&vote.leader_id.voted_for().unwrap_or(0).to_le_bytes());
    buf[16] = if vote.committed { 1 } else { 0 };
    buf
}

fn deserialize_vote(buf: &[u8; HARD_STATE_SIZE]) -> Vote<u64> {
    let term = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let node_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let committed = buf[16] != 0;
    if committed {
        Vote::new_committed(term, node_id)
    } else {
        Vote::new(term, node_id)
    }
}

fn write_hard_state(path: &std::path::Path, vote: &Vote<u64>) -> std::io::Result<()> {
    use std::io::Write;
    let buf = serialize_vote(vote);
    let mut f = std::fs::File::create(path)?;
    f.write_all(&buf)?;
    f.sync_data()?;
    Ok(())
}

fn read_hard_state(path: &std::path::Path) -> std::io::Result<Option<Vote<u64>>> {
    use std::io::Read;
    match std::fs::File::open(path) {
        Ok(mut f) => {
            let mut buf = [0u8; HARD_STATE_SIZE];
            f.read_exact(&mut buf)?;
            Ok(Some(deserialize_vote(&buf)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Inner state
// ---------------------------------------------------------------------------

struct Inner {
    /// The underlying append-only log writer.
    raft_log: RaftLog,
    /// In-memory log index: log_index -> (file_offset, payload_length).
    /// The file still contains bytes for purged entries; only the map shrinks.
    log: LogIndex<(u64, usize)>,
    /// Current vote / hard state (cached in memory, persisted to disk).
    vote: Option<Vote<u64>>,
    /// Path to the directory containing the log file and hard state file.
    dir: PathBuf,
    sm: StateMachineState,
    snapshot: Option<StoredSnapshot>,
}

impl Inner {
    fn hard_state_path(&self) -> PathBuf {
        self.dir.join("hard_state.bin")
    }
}

// ---------------------------------------------------------------------------
// DiskRaftStorage
// ---------------------------------------------------------------------------

/// Disk-backed `RaftStorage` implementation using `RaftLog` for log
/// persistence and a sidecar file for hard state (vote).
#[derive(Clone)]
pub struct DiskRaftStorage {
    inner: Arc<Mutex<Inner>>,
}

impl DiskRaftStorage {
    /// Open (or create) a `DiskRaftStorage` rooted at `dir`.
    ///
    /// The directory will contain:
    /// - `raft.log` — the append-only entry log (managed by `RaftLog`)
    /// - `hard_state.bin` — the persisted vote (17 bytes)
    ///
    /// On open, the log is scanned to rebuild the in-memory index, and the
    /// hard state file is read if it exists.
    pub fn open(dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(&dir)?;

        let log_path = dir.join("raft.log");
        let raft_log = RaftLog::open(&log_path)?;

        // Scan the log to rebuild the in-memory index.
        let payloads = raft_log.scan_all()?;
        let mut log: LogIndex<(u64, usize)> = LogIndex::default();
        let mut file_offset: u64 = 0;

        for payload in &payloads {
            let payload_len = payload.len();
            // Parse the log_index from the first 8 bytes of the payload.
            if payload_len >= 8 {
                let log_index = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                log.insert(log_index, (file_offset, payload_len));
            }
            // Advance the file offset by the full framed size.
            file_offset += framed_size(payload_len) as u64;
        }

        // Read hard state.
        let hs_path = dir.join("hard_state.bin");
        let vote = read_hard_state(&hs_path)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                raft_log,
                log,
                vote,
                dir,
                sm: StateMachineState::default(),
                snapshot: None,
            })),
        })
    }

    /// Acquire the inner lock, converting PoisonError into StorageError.
    fn lock(&self) -> Result<MutexGuard<'_, Inner>, StorageError<u64>> {
        self.inner.lock().map_err(|_: PoisonError<_>| {
            StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                AnyError::error("lock poisoned"),
            )
            .into()
        })
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader
// ---------------------------------------------------------------------------

impl RaftLogReader<MeruTypeConfig> for DiskRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MeruTypeConfig>>, StorageError<u64>> {
        let guard = self.lock()?;

        // Collect the offsets and lengths for entries in the requested range.
        let positions: Vec<(u64, u64, usize)> = guard
            .log
            .range(range)
            .map(|(&log_idx, &(offset, len))| (log_idx, offset, len))
            .collect();

        let mut entries = Vec::with_capacity(positions.len());
        for (_log_idx, offset, _len) in &positions {
            let payload = guard.raft_log.read_entry(*offset).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::LogIndex(*_log_idx),
                    ErrorVerb::Read,
                    AnyError::error(format!("read_entry at offset {offset}: {e}")),
                )
            })?;

            let entry = deserialize_entry(&payload).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::LogIndex(*_log_idx),
                    ErrorVerb::Read,
                    AnyError::error(e),
                )
            })?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder
// ---------------------------------------------------------------------------

impl RaftSnapshotBuilder<MeruTypeConfig> for DiskRaftStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MeruTypeConfig>, StorageError<u64>> {
        let mut guard = self.lock()?;

        let data = Vec::new();

        let meta = SnapshotMeta {
            last_log_id: guard.sm.last_applied.clone(),
            last_membership: guard.sm.last_membership.clone(),
            snapshot_id: format!(
                "snap-{}-{}",
                guard.sm.last_applied.as_ref().map_or(0, |l| l.leader_id.term),
                guard.sm.last_applied.as_ref().map_or(0, |l| l.index),
            ),
        };

        guard.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ---------------------------------------------------------------------------
// RaftStorage
// ---------------------------------------------------------------------------

impl RaftStorage<MeruTypeConfig> for DiskRaftStorage {
    type LogReader = DiskRaftStorage;
    type SnapshotBuilder = DiskRaftStorage;

    // --- Vote ---

    async fn save_vote(
        &mut self,
        vote: &Vote<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.lock()?;
        write_hard_state(&guard.hard_state_path(), vote).map_err(|e| {
            StorageIOError::new(
                ErrorSubject::Vote,
                ErrorVerb::Write,
                AnyError::error(format!("write hard_state.bin: {e}")),
            )
        })?;
        guard.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.lock()?;
        Ok(guard.vote)
    }

    // --- Log ---

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<MeruTypeConfig>, StorageError<u64>> {
        let guard = self.lock()?;
        let last_log_id = guard
            .log
            .last()
            .map(|(&log_idx, &(offset, _len))| {
                // Read the entry payload to extract term for the LogId.
                let payload = guard.raft_log.read_entry(offset).ok();
                if let Some(ref p) = payload {
                    if p.len() >= 16 {
                        let index = u64::from_le_bytes(p[0..8].try_into().unwrap());
                        let term = u64::from_le_bytes(p[8..16].try_into().unwrap());
                        return LogId::new(openraft::CommittedLeaderId::new(term, 0), index);
                    }
                }
                // Fallback: use log_idx with term 0 (should not happen).
                LogId::new(openraft::CommittedLeaderId::new(0, 0), log_idx)
            })
            .or_else(|| guard.log.last_purged_log_id());

        Ok(LogState {
            last_purged_log_id: guard.log.last_purged_log_id(),
            last_log_id,
        })
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
        let mut guard = self.lock()?;
        let start = Instant::now();

        for entry in entries {
            let log_index = entry.log_id.index;
            let payload = serialize_entry(&entry);
            let payload_len = payload.len();

            let offset = guard.raft_log.append(&payload).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::Log(entry.log_id.clone()),
                    ErrorVerb::Write,
                    AnyError::error(format!("RaftLog::append: {e}")),
                )
            })?;

            guard.log.insert(log_index, (offset, payload_len));
        }

        metrics::histogram!(crate::observability::RAFT_LOG_WRITE_LATENCY_US)
            .record(start.elapsed().as_micros() as f64);

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.lock()?;
        guard.log.remove_range(log_id.index..);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.lock()?;
        guard.log.remove_range(..=log_id.index);
        guard.log.mark_purged(log_id);
        Ok(())
    }

    // --- State machine ---

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, BasicNode>,
        ),
        StorageError<u64>,
    > {
        let guard = self.lock()?;
        Ok((guard.sm.last_applied.clone(), guard.sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<MeruTypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<u64>> {
        let mut guard = self.lock()?;
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            guard.sm.last_applied = Some(entry.log_id.clone());

            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(_cmd) => {
                    // Real state machine application happens in a later phase.
                }
                EntryPayload::Membership(m) => {
                    guard.sm.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m.clone());
                }
            }
            responses.push(RaftResponse);
        }
        Ok(responses)
    }

    // --- Snapshot ---

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let snapshot_size = snapshot.get_ref().len();
        let start = Instant::now();

        let mut guard = self.lock()?;

        guard.sm.last_applied = meta.last_log_id.clone();
        guard.sm.last_membership = meta.last_membership.clone();

        if let Some(ref lid) = meta.last_log_id {
            guard.log.remove_range(..=lid.index);
            guard.log.mark_purged(lid.clone());
        }

        guard.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        });

        tracing::info!(
            last_log_id = ?meta.last_log_id,
            snapshot_id = %meta.snapshot_id,
            size_bytes = snapshot_size,
            duration_ms = start.elapsed().as_millis(),
            "snapshot installed"
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MeruTypeConfig>>, StorageError<u64>> {
        let guard = self.lock()?;
        Ok(guard.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    fn make_entry(term: u64, index: u64, cmd: RaftCommand) -> Entry<MeruTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn make_put_cmd(val: i64) -> RaftCommand {
        RaftCommand::Put {
            row: merutable::value::Row::new(vec![Some(
                merutable::value::FieldValue::Int64(val),
            )]),
        }
    }

    #[tokio::test]
    async fn disk_storage_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DiskRaftStorage::open(dir.path().to_path_buf()).unwrap();

        let entries: Vec<_> = (1..=5)
            .map(|i| make_entry(1, i, make_put_cmd(i as i64)))
            .collect();

        store
            .append_to_log(entries.clone())
            .await
            .unwrap();

        let mut reader = store.get_log_reader().await;
        let read_back = reader.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(read_back.len(), 5);

        for (i, entry) in read_back.iter().enumerate() {
            let expected = &entries[i];
            assert_eq!(entry.log_id.index, expected.log_id.index);
            assert_eq!(entry.log_id.leader_id.term, expected.log_id.leader_id.term);
            assert_eq!(entry.payload, expected.payload);
        }
    }

    #[tokio::test]
    async fn disk_storage_hard_state_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let vote = Vote::new(3, 7u64);

        // Write vote and drop storage.
        {
            let mut store = DiskRaftStorage::open(path.clone()).unwrap();
            assert!(store.read_vote().await.unwrap().is_none());
            store.save_vote(&vote).await.unwrap();
            let read_back = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read_back, vote);
        }

        // Reopen and verify vote is loaded from disk.
        {
            let mut store = DiskRaftStorage::open(path.clone()).unwrap();
            let read_back = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read_back, vote);
        }

        // Overwrite with a committed vote and verify persistence.
        {
            let mut store = DiskRaftStorage::open(path.clone()).unwrap();
            let committed = Vote::new_committed(5, 2u64);
            store.save_vote(&committed).await.unwrap();
        }
        {
            let mut store = DiskRaftStorage::open(path).unwrap();
            let read_back = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read_back, Vote::new_committed(5, 2u64));
            assert!(read_back.committed);
        }
    }

    #[tokio::test]
    async fn disk_storage_recovery_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let entries: Vec<_> = (1..=10)
            .map(|i| make_entry(1, i, make_put_cmd(i as i64 * 100)))
            .collect();

        // Write 10 entries, drop.
        {
            let mut store = DiskRaftStorage::open(path.clone()).unwrap();
            store.append_to_log(entries.clone()).await.unwrap();
        }

        // Reopen: scan rebuilds the index.
        {
            let mut store = DiskRaftStorage::open(path).unwrap();
            let mut reader = store.get_log_reader().await;
            let read_back = reader.try_get_log_entries(1..=10).await.unwrap();
            assert_eq!(read_back.len(), 10);

            for (i, entry) in read_back.iter().enumerate() {
                let expected = &entries[i];
                assert_eq!(entry.log_id.index, expected.log_id.index);
                assert_eq!(entry.payload, expected.payload);
            }
        }
    }

    #[tokio::test]
    async fn disk_storage_delete_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DiskRaftStorage::open(dir.path().to_path_buf()).unwrap();

        let entries: Vec<_> = (1..=5)
            .map(|i| make_entry(1, i, make_put_cmd(i as i64)))
            .collect();

        store.append_to_log(entries).await.unwrap();

        // Delete from index 3 onwards.
        store
            .delete_conflict_logs_since(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..=10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);

        // Entries 3-5 are gone.
        let gone = reader.try_get_log_entries(3..=5).await.unwrap();
        assert!(gone.is_empty());
    }

    #[tokio::test]
    async fn disk_storage_purge() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DiskRaftStorage::open(dir.path().to_path_buf()).unwrap();

        let entries: Vec<_> = (1..=5)
            .map(|i| make_entry(1, i, make_put_cmd(i as i64)))
            .collect();

        store.append_to_log(entries).await.unwrap();

        // Purge up to index 3.
        store
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..=10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 4);
        assert_eq!(remaining[1].log_id.index, 5);

        // 1-3 are gone.
        let gone = reader.try_get_log_entries(1..=3).await.unwrap();
        assert!(gone.is_empty());
    }

    #[tokio::test]
    async fn disk_storage_get_log_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DiskRaftStorage::open(dir.path().to_path_buf()).unwrap();

        // Initially empty.
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());

        // Append entries 1-5.
        let entries: Vec<_> = (1..=5)
            .map(|i| make_entry(2, i, make_put_cmd(i as i64)))
            .collect();
        store.append_to_log(entries).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.as_ref().unwrap().index, 5);
        assert_eq!(state.last_log_id.as_ref().unwrap().leader_id.term, 2);
        assert!(state.last_purged_log_id.is_none());

        // Purge up to 3.
        store
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(2, 0), 3))
            .await
            .unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.as_ref().unwrap().index, 3);
        assert_eq!(state.last_log_id.as_ref().unwrap().index, 5);

        // Purge everything.
        store
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(2, 0), 5))
            .await
            .unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.as_ref().unwrap().index, 5);
        // When all entries are purged, last_log_id should fall back to last_purged_log_id.
        assert_eq!(state.last_log_id.as_ref().unwrap().index, 5);
    }
}
