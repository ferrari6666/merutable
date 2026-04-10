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

/// Row group size tuned by level.
pub fn row_group_size_for_level(level: Level) -> usize {
    match level.0 {
        0 => 128 * 1024 * 1024,
        1 => 64 * 1024 * 1024,
        _ => 8 * 1024 * 1024,
    }
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
        .set_max_row_group_size(row_group_size_for_level(level) / 256)
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
