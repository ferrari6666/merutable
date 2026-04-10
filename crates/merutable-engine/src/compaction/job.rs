//! `CompactionJob`: orchestrates one compaction run with DV bookkeeping.
//!
//! Sequence:
//! 1. Pick compaction (level, input files).
//! 2. Read input files (TODO: via ParquetReader with DV applied).
//! 3. Merge via `CompactionIterator` (dedup, tombstone drop).
//! 4. Write output files at the target level.
//! 5. Track promoted row positions for DV updates.
//! 6. Build `SnapshotTransaction` (adds + removes + dvs).
//! 7. Commit Iceberg snapshot.
//! 8. Install new version.

use std::{collections::HashMap, sync::Arc};

use merutable_iceberg::{DeletionVector, IcebergDataFile, SnapshotTransaction};
use merutable_types::{level::ParquetFileMeta, MeruError, Result};
use tracing::{debug, info};

use crate::{
    compaction::{
        iterator::{CompactionIterator, FileEntries},
        picker,
    },
    engine::MeruEngine,
};

/// Run one compaction job. Picks the best level and compacts.
pub async fn run_compaction(engine: &Arc<MeruEngine>) -> Result<()> {
    let version = engine.version_set.current();
    let pick = match picker::pick_compaction(&version, &engine.config) {
        Some(p) => p,
        None => {
            debug!("no compaction needed");
            return Ok(());
        }
    };

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        score = pick.score,
        input_files = pick.input_files.len(),
        "starting compaction"
    );

    // Collect input file entries.
    // TODO: Read from actual Parquet files via ParquetReader.
    // For now, create an empty compaction since ParquetReader isn't fully wired.
    let file_entries: Vec<FileEntries> = Vec::new();

    // Build compaction iterator.
    let read_seq = engine.read_seq();
    let drop_tombstones = pick.output_level.0 as usize >= engine.config.level_target_bytes.len();
    let iter = CompactionIterator::new(file_entries, read_seq, drop_tombstones);

    if iter.is_empty() {
        // If we can't read files yet, at least build the transaction structure.
        // This path will be fully functional when ParquetReader is wired up.
        debug!("compaction produced no output (reader not wired)");
        return Ok(());
    }

    // Collect surviving entries and track promoted rows per source file.
    let mut output_rows: Vec<(
        merutable_types::key::InternalKey,
        merutable_types::value::Row,
    )> = Vec::new();
    let mut promoted_per_file: HashMap<usize, Vec<u32>> = HashMap::new();

    for entry in iter {
        output_rows.push((entry.ikey, entry.row));
        promoted_per_file
            .entry(entry.source_file_idx)
            .or_default()
            .push(entry.row_position);
    }

    let num_output_rows = output_rows.len();

    // Write output Parquet file(s).
    let file_id = uuid::Uuid::new_v4().to_string();
    let output_path = format!("data/L{}/{file_id}.parquet", pick.output_level.0);

    let (parquet_bytes, _, _) = merutable_parquet::writer::write_sorted_rows(
        output_rows,
        engine.schema.clone(),
        pick.output_level,
        engine.config.bloom_bits_per_key,
    )?;

    let file_size = parquet_bytes.len() as u64;

    // Write to disk.
    engine.catalog.ensure_level_dir(pick.output_level).await?;
    let full_path = engine.catalog.data_file_path(pick.output_level, &file_id);
    if !parquet_bytes.is_empty() {
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(MeruError::Io)?;
        }
        tokio::fs::write(&full_path, &parquet_bytes)
            .await
            .map_err(MeruError::Io)?;
    }

    // Build snapshot transaction.
    let mut txn = SnapshotTransaction::new();

    // Add output file.
    let meta = ParquetFileMeta {
        level: pick.output_level,
        seq_min: 0, // TODO: track from iterator
        seq_max: 0,
        key_min: Vec::new(),
        key_max: Vec::new(),
        num_rows: num_output_rows as u64,
        file_size,
        dv_path: None,
        dv_offset: None,
        dv_length: None,
    };

    txn.add_file(IcebergDataFile {
        path: output_path.clone(),
        file_size,
        num_rows: num_output_rows as u64,
        meta,
    });

    // Build DVs for source files or remove them if fully compacted.
    let input_files = version.files_at(pick.input_level);
    for (file_idx, file_meta) in input_files.iter().enumerate() {
        let promoted = promoted_per_file.get(&file_idx);
        let promoted_count = promoted.map(|v| v.len() as u64).unwrap_or(0);

        if promoted_count >= file_meta.meta.num_rows {
            // All rows promoted → remove file from manifest.
            txn.remove_file(file_meta.path.clone());
        } else if promoted_count > 0 {
            // Partial promotion → build DV for promoted positions.
            let mut dv = DeletionVector::new();
            for &pos in promoted.unwrap() {
                dv.mark_deleted(pos);
            }
            txn.add_dv(file_meta.path.clone(), dv);
        }
    }

    txn.set_prop("merutable.job", "compaction");
    txn.set_prop("merutable.input_level", pick.input_level.0.to_string());
    txn.set_prop("merutable.output_level", pick.output_level.0.to_string());

    // Commit.
    let new_version = engine.catalog.commit(&txn, engine.schema.clone()).await?;
    engine.version_set.install(new_version);

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        output_rows = num_output_rows,
        "compaction committed"
    );

    Ok(())
}
