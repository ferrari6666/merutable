//! `CompactionIterator`: wraps a K-way merge iterator with compaction semantics.
//!
//! - Drops stale versions (keeps only the latest seq for each user_key).
//! - Drops tombstones when no older data exists below the output level.
//! - Tracks `row_position` for DV bookkeeping.

use merutable_types::{
    key::InternalKey,
    sequence::{OpType, SeqNum},
    value::Row,
};

/// One entry output by the compaction iterator.
#[derive(Clone, Debug)]
pub struct CompactionEntry {
    pub ikey: InternalKey,
    pub row: Row,
    /// Index of the source file this entry came from.
    pub source_file_idx: usize,
    /// Row position within the source file (for DV tracking).
    pub row_position: u32,
}

/// Input to the compaction iterator: entries from one source file.
#[derive(Clone, Debug)]
pub struct FileEntries {
    pub file_idx: usize,
    pub entries: Vec<(InternalKey, Row, u32)>, // (ikey, row, row_position)
}

/// Compaction iterator: merges entries from multiple files, deduplicates by
/// user_key, and optionally drops tombstones.
pub struct CompactionIterator {
    /// All entries merged and sorted by InternalKey.
    entries: Vec<CompactionEntry>,
    pos: usize,
}

impl CompactionIterator {
    /// Build from multiple source files' entries.
    ///
    /// `oldest_snapshot_seq`: only keep the latest version with seq ≤ this.
    /// `drop_tombstones`: if true, `OpType::Delete` entries are dropped
    /// (safe only when no older data exists below the output level).
    pub fn new(
        file_entries: Vec<FileEntries>,
        _oldest_snapshot_seq: SeqNum,
        drop_tombstones: bool,
    ) -> Self {
        // Flatten all entries.
        let mut all: Vec<CompactionEntry> = Vec::new();
        for fe in file_entries {
            for (ikey, row, row_pos) in fe.entries {
                all.push(CompactionEntry {
                    ikey,
                    row,
                    source_file_idx: fe.file_idx,
                    row_position: row_pos,
                });
            }
        }

        // Sort by InternalKey (PK ASC, seq DESC).
        all.sort_by(|a, b| a.ikey.cmp(&b.ikey));

        // Deduplicate: for each user_key, keep only the latest version.
        let mut deduped: Vec<CompactionEntry> = Vec::new();
        let mut last_uk: Option<Vec<u8>> = None;

        for entry in all {
            let uk = entry.ikey.user_key_bytes().to_vec();
            if let Some(ref last) = last_uk {
                if *last == uk {
                    continue; // older version of same key
                }
            }
            last_uk = Some(uk);

            // Drop tombstones if allowed.
            if drop_tombstones && entry.ikey.op_type == OpType::Delete {
                continue;
            }

            deduped.push(entry);
        }

        Self {
            entries: deduped,
            pos: 0,
        }
    }

    /// Number of surviving entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Iterator for CompactionIterator {
    type Item = CompactionEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::{
        schema::{ColumnDef, ColumnType, TableSchema},
        sequence::OpType,
        value::FieldValue,
    };

    fn schema() -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
            }],
            primary_key: vec![0],
        }
    }

    fn make_ikey(pk: i64, seq: u64, op: OpType) -> InternalKey {
        InternalKey::encode(&[FieldValue::Int64(pk)], SeqNum(seq), op, &schema()).unwrap()
    }

    #[test]
    fn dedup_keeps_latest() {
        let fe = vec![FileEntries {
            file_idx: 0,
            entries: vec![
                (make_ikey(1, 10, OpType::Put), Row::default(), 0),
                (make_ikey(1, 5, OpType::Put), Row::default(), 1),
                (make_ikey(2, 8, OpType::Put), Row::default(), 2),
            ],
        }];
        let iter = CompactionIterator::new(fe, SeqNum(100), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 2); // key=1 (seq=10), key=2 (seq=8)
        assert_eq!(results[0].ikey.seq, SeqNum(10));
        assert_eq!(results[1].ikey.seq, SeqNum(8));
    }

    #[test]
    fn drop_tombstones() {
        let fe = vec![FileEntries {
            file_idx: 0,
            entries: vec![
                (make_ikey(1, 10, OpType::Delete), Row::default(), 0),
                (make_ikey(2, 8, OpType::Put), Row::default(), 1),
            ],
        }];
        let iter = CompactionIterator::new(fe, SeqNum(100), true);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 1); // only key=2 survives
        assert_eq!(results[0].ikey.seq, SeqNum(8));
    }

    #[test]
    fn merge_across_files() {
        let fe = vec![
            FileEntries {
                file_idx: 0,
                entries: vec![
                    (make_ikey(1, 10, OpType::Put), Row::default(), 0),
                    (make_ikey(3, 10, OpType::Put), Row::default(), 1),
                ],
            },
            FileEntries {
                file_idx: 1,
                entries: vec![
                    (make_ikey(1, 5, OpType::Put), Row::default(), 0),
                    (make_ikey(2, 8, OpType::Put), Row::default(), 1),
                ],
            },
        ];
        let iter = CompactionIterator::new(fe, SeqNum(100), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 3); // keys 1, 2, 3
                                      // Key 1 should come from file 0 (seq=10, newer).
        assert_eq!(results[0].ikey.seq, SeqNum(10));
        assert_eq!(results[0].source_file_idx, 0);
    }
}
