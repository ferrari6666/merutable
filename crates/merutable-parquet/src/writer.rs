//! `ParquetWriter`: writes a sorted stream of `(InternalKey, Row)` to a Parquet file.
//!
//! Output file is a valid Apache Parquet file that:
//! - Sorts rows by `InternalKey` (PK ASC, seq DESC) within each row group
//! - Stores bloom filter in KV metadata (`merutable.bloom`)
//! - Stores `ParquetFileMeta` + `TableSchema` in Parquet KV footer
//! - Uses row group sizing tuned to the target LSM level

use std::sync::Arc;

use bytes::Bytes;
use merutable_types::{
    key::InternalKey,
    level::{Level, ParquetFileMeta},
    schema::TableSchema,
    value::Row,
    MeruError, Result,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{bloom::FastLocalBloom, codec};

/// Assumed average row width in bytes. Used to convert byte budgets into
/// row counts for `set_max_row_group_size` (which takes rows, not bytes).
/// This is a rough heuristic; production tuning should measure actual widths.
const ASSUMED_ROW_BYTES: usize = 256;

/// Target row-group **byte** budget per LSM level.
///
/// The LSM tree treats L0 as the *hot* tier (just flushed from the memtable,
/// point-lookup heavy, write-amplification sensitive) and L2+ as the *cold*
/// tier (compacted, scan-heavy, analytics target). We therefore size row
/// groups *small* at the hot tier (row-store-like — favors selective reads
/// and reduces flush latency) and *large* at the cold tier (columnar
/// analytics — maximizes scan throughput and compression ratio).
pub fn target_row_group_bytes(level: Level) -> usize {
    match level.0 {
        0 => 4 * 1024 * 1024,   // 4 MiB  — hot
        1 => 32 * 1024 * 1024,  // 32 MiB — warm
        _ => 128 * 1024 * 1024, // 128 MiB — cold (analytics)
    }
}

/// Target data-page **byte** size per LSM level. Same hot→cold rationale
/// as `target_row_group_bytes`: small pages favor selective reads at L0,
/// large pages favor scan throughput at L2+.
pub fn target_data_page_bytes(level: Level) -> usize {
    match level.0 {
        0 => 64 * 1024,   // 64 KiB  — hot
        1 => 256 * 1024,  // 256 KiB — warm
        _ => 1024 * 1024, // 1 MiB   — cold (parquet default analytics)
    }
}

/// Row count to pass to `set_max_row_group_size` for a given level,
/// derived from `target_row_group_bytes` and `ASSUMED_ROW_BYTES`. Floored
/// at 1024 rows so very narrow heuristics never produce a degenerate group.
pub fn target_rows_per_row_group(level: Level) -> usize {
    (target_row_group_bytes(level) / ASSUMED_ROW_BYTES).max(1024)
}

pub struct WriterStats {
    pub num_rows: u64,
    pub file_size: u64,
    pub key_min: Vec<u8>,
    pub key_max: Vec<u8>,
    pub seq_min: u64,
    pub seq_max: u64,
}

/// Convenience: write all rows at once into a `Vec<u8>` buffer.
/// Returns `(parquet_bytes, bloom_bytes, meta)`.
pub fn write_sorted_rows(
    rows: Vec<(InternalKey, Row)>,
    schema: Arc<TableSchema>,
    level: Level,
    bloom_bits_per_key: u8,
) -> Result<(Vec<u8>, Bytes, ParquetFileMeta)> {
    if rows.is_empty() {
        let meta = ParquetFileMeta {
            level,
            seq_min: 0,
            seq_max: 0,
            key_min: Vec::new(),
            key_max: Vec::new(),
            num_rows: 0,
            file_size: 0,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        };
        return Ok((Vec::new(), Bytes::new(), meta));
    }

    let estimated = rows.len();
    let arrow_schema = codec::arrow_schema(&schema);

    // Track stats.
    let mut bloom = FastLocalBloom::new(estimated.max(1000), bloom_bits_per_key);
    let mut key_min: Option<Vec<u8>> = None;
    let mut key_max: Option<Vec<u8>> = None;
    let mut seq_min = u64::MAX;
    let mut seq_max = 0u64;

    for (ikey, _) in &rows {
        let uk = ikey.user_key_bytes().to_vec();
        bloom.add(&uk);
        if key_min.is_none() {
            key_min = Some(uk.clone());
        }
        key_max = Some(uk);
        if ikey.seq.0 < seq_min {
            seq_min = ikey.seq.0;
        }
        if ikey.seq.0 > seq_max {
            seq_max = ikey.seq.0;
        }
    }

    // Build KV metadata.
    let meta = ParquetFileMeta {
        level,
        seq_min: if seq_min == u64::MAX { 0 } else { seq_min },
        seq_max,
        key_min: key_min.unwrap_or_default(),
        key_max: key_max.unwrap_or_default(),
        num_rows: rows.len() as u64,
        file_size: 0, // filled after writing
        dv_path: None,
        dv_offset: None,
        dv_length: None,
    };

    let bloom_bytes = bloom.to_bytes();
    let footer_kv = crate::footer::encode_footer_kv(&meta, &schema)?;

    // Build Parquet KV metadata entries.
    let mut kv_meta: Vec<parquet::format::KeyValue> = footer_kv
        .into_iter()
        .map(|(k, v)| parquet::format::KeyValue {
            key: k,
            value: Some(v),
        })
        .collect();
    kv_meta.push(parquet::format::KeyValue {
        key: "merutable.bloom".to_string(),
        value: Some(hex::encode(&bloom_bytes)),
    });

    // Build Arrow schema with metadata.
    let arrow_schema_with_meta = {
        let mut metadata = arrow_schema.metadata().clone();
        for kv in &kv_meta {
            if let Some(ref v) = kv.value {
                metadata.insert(kv.key.clone(), v.clone());
            }
        }
        Arc::new(arrow::datatypes::Schema::new_with_metadata(
            arrow_schema.fields().to_vec(),
            metadata,
        ))
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(target_rows_per_row_group(level))
        .set_data_page_size_limit(target_data_page_bytes(level))
        .build();

    let buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(buf, arrow_schema_with_meta, Some(props))
        .map_err(|e| MeruError::Parquet(e.to_string()))?;

    // Write rows in batches.
    let batch = codec::rows_to_record_batch(&rows, &schema)?;
    writer
        .write(&batch)
        .map_err(|e| MeruError::Parquet(e.to_string()))?;

    let file_bytes: Vec<u8> = writer
        .into_inner()
        .map_err(|e| MeruError::Parquet(e.to_string()))?;

    let mut final_meta = meta;
    final_meta.file_size = file_bytes.len() as u64;

    Ok((file_bytes, bloom_bytes, final_meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L0 is the *hot* tier (just flushed, point-lookup heavy). L2+ is the
    /// *cold* analytics tier. Row-group byte budget must grow with level so
    /// that hot data behaves like a row store and cold data like a columnar
    /// analytics store. A regression here silently destroys read latency at
    /// L0 or scan throughput at L2+.
    #[test]
    fn row_group_bytes_grow_from_hot_to_cold() {
        let l0 = target_row_group_bytes(Level(0));
        let l1 = target_row_group_bytes(Level(1));
        let l2 = target_row_group_bytes(Level(2));
        let l3 = target_row_group_bytes(Level(3));
        assert!(l0 < l1, "L0 ({l0}) must be smaller than L1 ({l1})");
        assert!(l1 < l2, "L1 ({l1}) must be smaller than L2 ({l2})");
        assert_eq!(l2, l3, "L2+ should plateau at the cold-tier size");
    }

    /// Same hot→cold rationale for data pages: small pages at L0 favor
    /// selective decoding for point lookups; large pages at L2+ favor
    /// scan throughput and compression.
    #[test]
    fn data_page_bytes_grow_from_hot_to_cold() {
        let l0 = target_data_page_bytes(Level(0));
        let l1 = target_data_page_bytes(Level(1));
        let l2 = target_data_page_bytes(Level(2));
        let l3 = target_data_page_bytes(Level(3));
        assert!(l0 < l1, "L0 ({l0}) must be smaller than L1 ({l1})");
        assert!(l1 < l2, "L1 ({l1}) must be smaller than L2 ({l2})");
        assert_eq!(l2, l3, "L2+ should plateau at the cold-tier size");
    }

    /// The cold tier should be *significantly* larger than the hot tier —
    /// not just nominally larger. If the ratio collapses below 8x, the
    /// HTAP tradeoff has been weakened and the level differentiation is
    /// no longer meaningful.
    #[test]
    fn cold_tier_significantly_larger_than_hot() {
        let rg_ratio = target_row_group_bytes(Level(2)) / target_row_group_bytes(Level(0));
        let pg_ratio = target_data_page_bytes(Level(2)) / target_data_page_bytes(Level(0));
        assert!(
            rg_ratio >= 8,
            "row-group cold/hot ratio {rg_ratio}x is too small (need ≥8x)"
        );
        assert!(
            pg_ratio >= 8,
            "data-page cold/hot ratio {pg_ratio}x is too small (need ≥8x)"
        );
    }

    /// L0 specifically must stay row-store-sized so flushes are fast and
    /// point lookups are cheap. Anchor the absolute upper bound so that
    /// future "tuning" doesn't drift L0 back into analytics territory.
    #[test]
    fn l0_stays_row_store_sized() {
        assert!(
            target_row_group_bytes(Level(0)) <= 8 * 1024 * 1024,
            "L0 row group must stay ≤ 8 MiB to behave like a row store"
        );
        assert!(
            target_data_page_bytes(Level(0)) <= 128 * 1024,
            "L0 data page must stay ≤ 128 KiB to behave like a row store"
        );
    }

    /// L2+ must remain analytics-friendly. Anchor the absolute lower bound
    /// so a future tweak can't accidentally shrink the cold tier into
    /// row-store territory.
    #[test]
    fn cold_tier_stays_analytics_sized() {
        assert!(
            target_row_group_bytes(Level(2)) >= 64 * 1024 * 1024,
            "Cold tier row group must stay ≥ 64 MiB for analytics scans"
        );
        assert!(
            target_data_page_bytes(Level(2)) >= 512 * 1024,
            "Cold tier data page must stay ≥ 512 KiB for analytics scans"
        );
    }

    /// `set_max_row_group_size` takes a row count, not a byte count. The
    /// converted row count must be both nonzero and at least the floor
    /// (1024 rows), and must still grow monotonically with level.
    #[test]
    fn rows_per_row_group_monotonic_and_floored() {
        let r0 = target_rows_per_row_group(Level(0));
        let r1 = target_rows_per_row_group(Level(1));
        let r2 = target_rows_per_row_group(Level(2));
        assert!(r0 >= 1024, "row floor violated: {r0}");
        assert!(r0 < r1, "L0 rows ({r0}) must be < L1 rows ({r1})");
        assert!(r1 < r2, "L1 rows ({r1}) must be < L2 rows ({r2})");
    }
}
