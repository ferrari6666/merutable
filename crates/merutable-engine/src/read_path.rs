//! Read path: point lookup (3-stop) and range scan via K-way merge.
//!
//! ## Point lookup algorithm
//!
//! 1. **Memtable** (active → immutable queue, newest first)
//!    → return immediately if found.
//! 2. **L0 files** (ALL checked, sorted by seq_max DESC — can overlap)
//!    → bloom filter gate first.
//! 3. **L1..LN** (binary search per level by key_max — non-overlapping)
//!    → bloom filter gate first.
//!
//! ## Range scan
//!
//! K-way merge of:
//! - MemtableIterator snapshots (one per memtable)
//! - ParquetScanIterator per file
//!   Deduplication: skip duplicate user_keys; `Delete` at top = skip key.

use merutable_memtable::iterator::MemEntry;
use merutable_types::{
    key::InternalKey,
    sequence::OpType,
    value::{FieldValue, Row},
    Result,
};

use crate::engine::MeruEngine;

// ── Point lookup ─────────────────────────────────────────────────────────────

/// 3-stop point lookup: memtable → L0 → L1..LN.
pub fn point_lookup(engine: &MeruEngine, pk_values: &[FieldValue]) -> Result<Option<Row>> {
    let read_seq = engine.read_seq();

    // Encode user key bytes for the lookup.
    let ikey = InternalKey::encode(pk_values, read_seq, OpType::Put, &engine.schema)?;
    let user_key_bytes = ikey.user_key_bytes();

    // Stop 1: Memtable.
    if let Some(entry) = engine.memtable.get(user_key_bytes, read_seq) {
        if entry.op_type == OpType::Delete {
            return Ok(None); // tombstone
        }
        // Deserialize row from value bytes.
        let row: Row = if entry.value.is_empty() {
            Row::default()
        } else {
            serde_json::from_slice(&entry.value).unwrap_or_default()
        };
        return Ok(Some(row));
    }

    // Get current version for file lookups.
    let _version = engine.version_set.current();

    // Stop 2: L0 files (check all, sorted by seq_max DESC = newest first).
    // In the current Phase 4 implementation, ParquetReader requires a Read+Seek
    // source. For files on disk, we'd open them. For now (placeholder files),
    // we skip the file lookup.
    // TODO: Wire up ParquetReader for real file reads in Phase 7 completion.

    // Stop 3: L1..LN (binary search per level).
    // Same limitation as Stop 2 — deferred to full Phase 7.

    Ok(None)
}

// ── Range scan ───────────────────────────────────────────────────────────────

/// Range scan with K-way merge.
pub fn range_scan(
    engine: &MeruEngine,
    start_pk: Option<&[FieldValue]>,
    end_pk: Option<&[FieldValue]>,
) -> Result<Vec<(InternalKey, Row)>> {
    let read_seq = engine.read_seq();

    // Encode start/end user key bytes.
    let start_bytes = start_pk
        .map(|pk| {
            InternalKey::encode(pk, read_seq, OpType::Put, &engine.schema)
                .map(|ik| ik.user_key_bytes().to_vec())
        })
        .transpose()?;
    let end_bytes = end_pk
        .map(|pk| {
            InternalKey::encode(pk, read_seq, OpType::Put, &engine.schema)
                .map(|ik| ik.user_key_bytes().to_vec())
        })
        .transpose()?;

    // Collect memtable entries.
    let mem_snapshots = engine.memtable.snapshot_entries(read_seq);

    // Merge all memtable entries into a single sorted stream.
    // Since each snapshot is already sorted and deduplicated within its
    // memtable, we do a K-way merge across all memtable snapshots.
    let mut all_entries: Vec<MemEntry> = Vec::new();
    for snapshot in mem_snapshots {
        all_entries.extend(snapshot);
    }

    // Deduplicate: keep only the entry with highest seq for each user_key.
    // Sort by (user_key ASC, seq DESC).
    all_entries.sort_by(|a, b| a.user_key.cmp(&b.user_key).then(b.seq.cmp(&a.seq)));

    let mut results: Vec<(InternalKey, Row)> = Vec::new();
    let mut last_uk: Option<Vec<u8>> = None;

    for entry in &all_entries {
        let uk = entry.user_key.to_vec();

        // Range filter.
        if let Some(ref start) = start_bytes {
            if uk.as_slice() < start.as_slice() {
                continue;
            }
        }
        if let Some(ref end) = end_bytes {
            if uk.as_slice() >= end.as_slice() {
                break;
            }
        }

        // Dedup: skip older versions of same user_key.
        if let Some(ref last) = last_uk {
            if *last == uk {
                continue;
            }
        }
        last_uk = Some(uk.clone());

        // Skip deletes.
        if entry.entry.op_type == OpType::Delete {
            continue;
        }

        // Reconstruct InternalKey.
        let tag = (merutable_types::sequence::SEQNUM_MAX.0 - entry.seq.0) << 8
            | (entry.entry.op_type as u64);
        let mut wire = Vec::with_capacity(uk.len() + 8);
        wire.extend_from_slice(&uk);
        wire.extend_from_slice(&tag.to_be_bytes());
        let ikey = InternalKey::decode(&wire, &engine.schema)?;

        // Deserialize row.
        let row: Row = if entry.entry.value.is_empty() {
            Row::default()
        } else {
            serde_json::from_slice(&entry.entry.value).unwrap_or_default()
        };

        results.push((ikey, row));
    }

    // TODO: merge with L0/L1+ Parquet file entries when ParquetReader
    // is fully wired up (Phase 7 completion).

    Ok(results)
}
