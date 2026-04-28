// P1.2: In-memory RaftStorage stub for testing the network/transport layer.
//
// The real io_uring-backed storage (P1.1) will replace this later. This
// version stores log entries in a BTreeMap and hard state (vote) in memory.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use openraft::storage::{LogState, Snapshot};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, LogId, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote, ErrorSubject, ErrorVerb,
};

use super::store_parts::{LogIndex, StateMachineState, StoredSnapshot};
use super::types::{MeruTypeConfig, RaftResponse};

// ---------------------------------------------------------------------------
// Shared inner state
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Inner {
    vote: Option<Vote<u64>>,
    log: LogIndex<Entry<MeruTypeConfig>>,
    sm: StateMachineState,
    snapshot: Option<StoredSnapshot>,
}

// ---------------------------------------------------------------------------
// InMemoryRaftStorage
// ---------------------------------------------------------------------------

/// Minimal in-memory `RaftStorage` implementation.
///
/// Thread-safety: all state is behind `Arc<Mutex<..>>` so that the log
/// reader clone can operate concurrently with the main storage path.
#[derive(Clone, Debug)]
pub struct InMemoryRaftStorage {
    inner: Arc<Mutex<Inner>>,
}

impl Default for InMemoryRaftStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRaftStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Acquire the inner lock, converting PoisonError into StorageError.
    ///
    /// We return the MutexGuard scoped to the inner Arc's lifetime (not
    /// `self`), which satisfies the `'static` future requirement from
    /// `#[add_async_trait]`.
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

impl RaftLogReader<MeruTypeConfig> for InMemoryRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MeruTypeConfig>>, StorageError<u64>> {
        let guard = self.lock()?;
        let entries: Vec<_> = guard.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder
// ---------------------------------------------------------------------------

impl RaftSnapshotBuilder<MeruTypeConfig> for InMemoryRaftStorage {
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
// RaftStorage (the combined trait)
// ---------------------------------------------------------------------------

impl RaftStorage<MeruTypeConfig> for InMemoryRaftStorage {
    type LogReader = InMemoryRaftStorage;
    type SnapshotBuilder = InMemoryRaftStorage;

    // --- Vote ---

    async fn save_vote(
        &mut self,
        vote: &Vote<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.lock()?;
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
            .map(|(_, e)| e.log_id.clone())
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
        for entry in entries {
            guard.log.insert(entry.log_id.index, entry);
        }
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

    use super::super::types::RaftCommand;

    fn make_entry(term: u64, index: u64, cmd: RaftCommand) -> Entry<MeruTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn in_memory_storage_append_and_read() {
        let mut store = InMemoryRaftStorage::new();

        let e1 = make_entry(
            1,
            1,
            RaftCommand::Put {
                row: merutable::value::Row::new(vec![Some(
                    merutable::value::FieldValue::Int64(1),
                )]),
            },
        );
        let e2 = make_entry(
            1,
            2,
            RaftCommand::Delete {
                pk: vec![merutable::value::FieldValue::Int32(7)],
            },
        );
        let e3 = make_entry(
            2,
            3,
            RaftCommand::Put {
                row: merutable::value::Row::new(vec![Some(
                    merutable::value::FieldValue::Boolean(false),
                )]),
            },
        );

        // Append
        store
            .append_to_log(vec![e1.clone(), e2.clone(), e3.clone()])
            .await
            .unwrap();

        // Read all
        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[2].log_id.index, 3);

        // Read sub-range
        let entries = reader.try_get_log_entries(2..3).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 2);

        // Verify log state
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.as_ref().unwrap().index, 3);
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn in_memory_storage_hard_state() {
        let mut store = InMemoryRaftStorage::new();

        // Initially no vote
        assert!(store.read_vote().await.unwrap().is_none());

        // Save and read vote
        let vote = Vote::new(3, 7u64);
        store.save_vote(&vote).await.unwrap();
        let read_back = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read_back, vote);

        // Overwrite with committed vote
        let committed = Vote::new_committed(5, 2u64);
        store.save_vote(&committed).await.unwrap();
        let read_back = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read_back, committed);
        assert!(read_back.committed);
    }

    #[tokio::test]
    async fn in_memory_storage_delete_conflict() {
        let mut store = InMemoryRaftStorage::new();

        let entries: Vec<_> = (1..=5)
            .map(|i| {
                make_entry(
                    1,
                    i,
                    RaftCommand::Put {
                        row: merutable::value::Row::new(vec![Some(
                            merutable::value::FieldValue::Int64(i as i64),
                        )]),
                    },
                )
            })
            .collect();

        store.append_to_log(entries).await.unwrap();

        // Delete from index 3 onwards
        store
            .delete_conflict_logs_since(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn in_memory_storage_purge() {
        let mut store = InMemoryRaftStorage::new();

        let entries: Vec<_> = (1..=5)
            .map(|i| {
                make_entry(
                    1,
                    i,
                    RaftCommand::Put {
                        row: merutable::value::Row::new(vec![Some(
                            merutable::value::FieldValue::Int64(i as i64),
                        )]),
                    },
                )
            })
            .collect();

        store.append_to_log(entries).await.unwrap();

        // Purge up to index 3
        store
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.as_ref().unwrap().index, 3);

        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(remaining.len(), 2); // 4 and 5
        assert_eq!(remaining[0].log_id.index, 4);
    }
}
