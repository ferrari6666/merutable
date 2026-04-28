// Shared building blocks for RaftStorage backends.
//
// The in-memory and disk-backed RaftStorage implementations have the same
// high-level shape: a log index keyed by log_index, hard state (vote),
// state-machine progress (last_applied + last_membership), and an optional
// snapshot. This module factors those concerns into typed substructs so both
// backends can compose them rather than duplicating a flat `Inner` struct.
//
// The substructs are plain data — no locking, no I/O. They are meant to live
// inside a single outer `Mutex<Inner>` per backend.

use std::collections::BTreeMap;
use std::ops::RangeBounds;

use openraft::{BasicNode, LogId, SnapshotMeta, StoredMembership};

// ---------------------------------------------------------------------------
// LogIndex<V>
// ---------------------------------------------------------------------------

/// Log-index keyed map. `V` is whatever a backend wants to store per entry:
/// for the in-memory backend that's the full `Entry<MeruTypeConfig>`, for the
/// disk-backed backend it's the on-disk `(file_offset, payload_length)` tuple.
///
/// Also tracks `last_purged_log_id`, which is the highest id whose entry has
/// been removed from the map via `purge_logs_upto`.
#[derive(Debug)]
pub(super) struct LogIndex<V> {
    entries: BTreeMap<u64, V>,
    last_purged_log_id: Option<LogId<u64>>,
}

impl<V> Default for LogIndex<V> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_purged_log_id: None,
        }
    }
}

impl<V> LogIndex<V> {
    pub(super) fn insert(&mut self, log_index: u64, value: V) {
        self.entries.insert(log_index, value);
    }

    pub(super) fn range<RB: RangeBounds<u64>>(
        &self,
        range: RB,
    ) -> std::collections::btree_map::Range<'_, u64, V> {
        self.entries.range(range)
    }

    pub(super) fn last(&self) -> Option<(&u64, &V)> {
        self.entries.iter().next_back()
    }

    pub(super) fn last_purged_log_id(&self) -> Option<LogId<u64>> {
        self.last_purged_log_id.clone()
    }

    /// Remove every entry in `range` from the in-memory map. The caller is
    /// responsible for any side effects (e.g. updating `last_purged_log_id`
    /// via [`mark_purged`](Self::mark_purged) for a purge).
    pub(super) fn remove_range<RB: RangeBounds<u64>>(&mut self, range: RB) {
        let keys: Vec<u64> = self.entries.range(range).map(|(&k, _)| k).collect();
        for k in keys {
            self.entries.remove(&k);
        }
    }

    /// Record that entries at or below `log_id.index` have been purged.
    pub(super) fn mark_purged(&mut self, log_id: LogId<u64>) {
        self.last_purged_log_id = Some(log_id);
    }
}

// ---------------------------------------------------------------------------
// StateMachineState
// ---------------------------------------------------------------------------

/// The bookkeeping every backend's state machine carries: the highest log id
/// applied and the last membership configuration observed. The actual
/// application of commands lives outside (in the state machine crate).
#[derive(Debug, Default)]
pub(super) struct StateMachineState {
    pub(super) last_applied: Option<LogId<u64>>,
    pub(super) last_membership: StoredMembership<u64, BasicNode>,
}

// ---------------------------------------------------------------------------
// StoredSnapshot
// ---------------------------------------------------------------------------

/// A snapshot cached in the storage backend. Both backends hold snapshot
/// bytes in memory for now; the only variation is the surrounding struct
/// name. This type replaces both `InMemSnapshot` and `DiskSnapshot`.
#[derive(Debug, Clone)]
pub(super) struct StoredSnapshot {
    pub(super) meta: SnapshotMeta<u64, BasicNode>,
    pub(super) data: Vec<u8>,
}
