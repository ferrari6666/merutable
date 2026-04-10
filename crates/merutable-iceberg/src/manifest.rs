//! `ManifestReader`: reads an Iceberg snapshot and reconstructs the LSM
//! level-file map. Each `DataFile` in the Iceberg manifest carries the
//! `"merutable.meta"` KV footer from which we extract the `Level`.
//!
//! For the embedded (file-system catalog) case, the manifest is a JSON file
//! on disk rather than a full Iceberg catalog scan. We keep the interface
//! generic enough for both paths.

use std::{collections::HashMap, sync::Arc};

use merutable_types::{
    level::{Level, ParquetFileMeta},
    schema::TableSchema,
    MeruError, Result,
};

use crate::version::{DataFileMeta, Version};

// ── ManifestEntry ────────────────────────────────────────────────────────────

/// A single file entry as stored in our manifest (simplified Iceberg manifest
/// subset). Full Iceberg catalogs use `DataFile` from the iceberg crate;
/// for the embedded FS catalog we use this lightweight representation.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ManifestEntry {
    /// Object-store path of the Parquet file.
    pub path: String,
    /// Serialized `ParquetFileMeta` (same as the Parquet KV footer).
    pub meta: ParquetFileMeta,
    /// `.puffin` DV file path, if any.
    pub dv_path: Option<String>,
    /// Byte offset of the DV blob within the `.puffin` file.
    pub dv_offset: Option<i64>,
    /// Byte length of the DV blob.
    pub dv_length: Option<i64>,
    /// Status: "existing", "added", or "deleted".
    #[serde(default = "default_status")]
    pub status: String,
}

fn default_status() -> String {
    "existing".to_string()
}

impl ManifestEntry {
    /// Convert to a `DataFileMeta` for the version layer.
    pub fn to_data_file_meta(&self) -> DataFileMeta {
        DataFileMeta {
            path: self.path.clone(),
            meta: self.meta.clone(),
            dv_path: self.dv_path.clone(),
            dv_offset: self.dv_offset,
            dv_length: self.dv_length,
        }
    }
}

// ── Manifest ─────────────────────────────────────────────────────────────────

/// Serializable manifest — a list of file entries plus snapshot metadata.
/// This is the embedded FS catalog's equivalent of an Iceberg manifest list.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// Monotonically increasing snapshot ID.
    pub snapshot_id: i64,
    /// Schema of the table at this snapshot.
    pub schema: TableSchema,
    /// All live file entries (status != "deleted").
    pub entries: Vec<ManifestEntry>,
    /// Snapshot summary properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl Manifest {
    /// Build a `Version` from this manifest.
    pub fn to_version(&self, schema: Arc<TableSchema>) -> Version {
        let mut levels: HashMap<Level, Vec<DataFileMeta>> = HashMap::new();
        for entry in &self.entries {
            if entry.status == "deleted" {
                continue;
            }
            levels
                .entry(entry.meta.level)
                .or_default()
                .push(entry.to_data_file_meta());
        }

        // Sort L0 by seq_max descending (newest first).
        if let Some(l0_files) = levels.get_mut(&Level(0)) {
            l0_files.sort_by(|a, b| b.meta.seq_max.cmp(&a.meta.seq_max));
        }
        // Sort L1+ by key_min ascending (for binary search).
        for (level, files) in levels.iter_mut() {
            if level.0 >= 1 {
                files.sort_by(|a, b| a.meta.key_min.cmp(&b.meta.key_min));
            }
        }

        Version {
            snapshot_id: self.snapshot_id,
            levels,
            schema,
        }
    }

    /// Serialize manifest to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| MeruError::Iceberg(format!("manifest serialize: {e}")))
    }

    /// Deserialize manifest from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| MeruError::Iceberg(format!("manifest deserialize: {e}")))
    }

    /// Create an empty initial manifest.
    pub fn empty(schema: TableSchema) -> Self {
        Self {
            snapshot_id: 0,
            schema,
            entries: Vec::new(),
            properties: HashMap::new(),
        }
    }

    /// Apply a `SnapshotTransaction` to produce a new manifest.
    /// This is the core commit logic for the embedded FS catalog.
    pub fn apply(&self, txn: &crate::snapshot::SnapshotTransaction, new_snapshot_id: i64) -> Self {
        let remove_set: std::collections::HashSet<&str> =
            txn.removes.iter().map(|s| s.as_str()).collect();

        let mut new_entries: Vec<ManifestEntry> = Vec::new();

        // Carry forward existing entries that aren't removed.
        for entry in &self.entries {
            if entry.status == "deleted" {
                continue;
            }
            if remove_set.contains(entry.path.as_str()) {
                continue; // fully compacted — drop
            }
            let mut e = entry.clone();
            // Apply DV update if present.
            if let Some(_dv) = txn.dvs.get(&entry.path) {
                let dv_path = format!(
                    "{}.dv-{}.puffin",
                    entry.path.trim_end_matches(".parquet"),
                    new_snapshot_id
                );
                // DV offset/length are set to 0 here; the catalog commit path
                // fills in the real values after uploading the puffin file.
                e.dv_path = Some(dv_path);
                e.dv_offset = Some(0);
                e.dv_length = Some(0);
                // Also update the embedded ParquetFileMeta DV fields.
                e.meta.dv_path = e.dv_path.clone();
                e.meta.dv_offset = e.dv_offset;
                e.meta.dv_length = e.dv_length;
            }
            new_entries.push(e);
        }

        // Add new files.
        for add in &txn.adds {
            new_entries.push(ManifestEntry {
                path: add.path.clone(),
                meta: add.meta.clone(),
                dv_path: None,
                dv_offset: None,
                dv_length: None,
                status: "added".to_string(),
            });
        }

        let mut props = self.properties.clone();
        props.extend(txn.props.iter().map(|(k, v)| (k.clone(), v.clone())));

        Manifest {
            snapshot_id: new_snapshot_id,
            schema: self.schema.clone(),
            entries: new_entries,
            properties: props,
        }
    }

    /// Number of live (non-deleted) file entries.
    pub fn live_file_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.status != "deleted")
            .count()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deletion_vector::DeletionVector;
    use crate::snapshot::{IcebergDataFile, SnapshotTransaction};
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType, TableSchema},
    };

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: ColumnType::ByteArray,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn test_meta(
        level: u8,
        seq_min: u64,
        seq_max: u64,
        key_min: &[u8],
        key_max: &[u8],
    ) -> ParquetFileMeta {
        ParquetFileMeta {
            level: Level(level),
            seq_min,
            seq_max,
            key_min: key_min.to_vec(),
            key_max: key_max.to_vec(),
            num_rows: 100,
            file_size: 1024,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        }
    }

    #[test]
    fn empty_manifest_roundtrip() {
        let m = Manifest::empty(test_schema());
        let json = m.to_json().unwrap();
        let decoded = Manifest::from_json(&json).unwrap();
        assert_eq!(decoded.snapshot_id, 0);
        assert_eq!(decoded.entries.len(), 0);
        assert_eq!(decoded.schema.table_name, "test");
    }

    #[test]
    fn apply_flush_txn() {
        let m = Manifest::empty(test_schema());
        let mut txn = SnapshotTransaction::new();
        txn.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0, 1, 10, b"\x01", b"\x05"),
        });
        txn.set_prop("merutable.job", "flush");

        let m2 = m.apply(&txn, 1);
        assert_eq!(m2.snapshot_id, 1);
        assert_eq!(m2.live_file_count(), 1);
        assert_eq!(m2.entries[0].path, "data/L0/a.parquet");
        assert_eq!(m2.properties.get("merutable.job").unwrap(), "flush");
    }

    #[test]
    fn apply_compaction_with_remove() {
        // Start with 2 L0 files.
        let mut m = Manifest::empty(test_schema());
        m.snapshot_id = 1;
        m.entries.push(ManifestEntry {
            path: "data/L0/a.parquet".into(),
            meta: test_meta(0, 1, 10, b"\x01", b"\x05"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });
        m.entries.push(ManifestEntry {
            path: "data/L0/b.parquet".into(),
            meta: test_meta(0, 11, 20, b"\x03", b"\x08"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });

        // Compact both into one L1 file.
        let mut txn = SnapshotTransaction::new();
        txn.remove_file("data/L0/a.parquet".into());
        txn.remove_file("data/L0/b.parquet".into());
        txn.add_file(IcebergDataFile {
            path: "data/L1/merged.parquet".into(),
            file_size: 2048,
            num_rows: 200,
            meta: test_meta(1, 1, 20, b"\x01", b"\x08"),
        });

        let m2 = m.apply(&txn, 2);
        assert_eq!(m2.snapshot_id, 2);
        assert_eq!(m2.live_file_count(), 1);
        assert_eq!(m2.entries[0].path, "data/L1/merged.parquet");
    }

    #[test]
    fn apply_partial_compaction_with_dv() {
        let mut m = Manifest::empty(test_schema());
        m.snapshot_id = 1;
        m.entries.push(ManifestEntry {
            path: "data/L0/a.parquet".into(),
            meta: test_meta(0, 1, 10, b"\x01", b"\x05"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });

        let mut txn = SnapshotTransaction::new();
        let mut dv = DeletionVector::new();
        dv.mark_deleted(0);
        dv.mark_deleted(5);
        dv.mark_deleted(10);
        txn.add_dv("data/L0/a.parquet".into(), dv);
        txn.add_file(IcebergDataFile {
            path: "data/L1/promoted.parquet".into(),
            file_size: 512,
            num_rows: 3,
            meta: test_meta(1, 1, 10, b"\x01", b"\x03"),
        });

        let m2 = m.apply(&txn, 2);
        assert_eq!(m2.live_file_count(), 2);
        // L0 file still exists but now has a DV path.
        let l0_entry = m2
            .entries
            .iter()
            .find(|e| e.path == "data/L0/a.parquet")
            .unwrap();
        assert!(l0_entry.dv_path.is_some());
    }

    #[test]
    fn to_version_sort_order() {
        let mut m = Manifest::empty(test_schema());
        m.snapshot_id = 5;
        // L0 files with different seq_max — should be sorted DESC.
        m.entries.push(ManifestEntry {
            path: "l0_old.parquet".into(),
            meta: test_meta(0, 1, 10, b"\x01", b"\x05"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });
        m.entries.push(ManifestEntry {
            path: "l0_new.parquet".into(),
            meta: test_meta(0, 11, 20, b"\x03", b"\x08"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });
        // L1 files — should be sorted by key_min ASC.
        m.entries.push(ManifestEntry {
            path: "l1_b.parquet".into(),
            meta: test_meta(1, 1, 20, b"\x05", b"\x0A"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });
        m.entries.push(ManifestEntry {
            path: "l1_a.parquet".into(),
            meta: test_meta(1, 1, 20, b"\x01", b"\x04"),
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            status: "existing".into(),
        });

        let v = m.to_version(Arc::new(test_schema()));
        // L0: newest first (seq_max=20 before seq_max=10).
        let l0 = v.files_at(Level(0));
        assert_eq!(l0[0].path, "l0_new.parquet");
        assert_eq!(l0[1].path, "l0_old.parquet");
        // L1: sorted by key_min ASC.
        let l1 = v.files_at(Level(1));
        assert_eq!(l1[0].path, "l1_a.parquet");
        assert_eq!(l1[1].path, "l1_b.parquet");
    }
}
