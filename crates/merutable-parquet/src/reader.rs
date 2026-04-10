//! `ParquetReader`: point lookup and range scan with Deletion Vector masking.
//!
//! Reads via the `parquet` crate's Arrow record-batch reader, projecting
//! only the columns needed for the file's level:
//! - L0: `[_merutable_ikey, _merutable_value]` — two-column read; the
//!   postcard blob carries the entire row, so a point lookup decodes one
//!   row from one column chunk instead of N typed columns.
//! - L1+: `[_merutable_ikey, ...all user columns]` — typed-column path;
//!   no value blob exists at this tier.

use std::sync::Arc;

use merutable_types::{
    key::InternalKey,
    level::ParquetFileMeta,
    schema::TableSchema,
    sequence::{OpType, SeqNum},
    value::Row,
    MeruError, Result,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use roaring::RoaringBitmap;

use crate::{
    bloom::FastLocalBloom,
    codec::{self, IKEY_COLUMN_NAME, VALUE_BLOB_COLUMN_NAME},
};

pub struct ParquetReader<R: ChunkReader + Clone> {
    /// Original source, kept so we can rebuild a fresh
    /// `ParquetRecordBatchReaderBuilder` per read. `Bytes` (the production
    /// case) is cheaply `Clone`; `File` users should wrap with `try_clone`.
    source: R,
    schema: Arc<TableSchema>,
    meta: ParquetFileMeta,
    bloom: Option<FastLocalBloom>,
}

impl<R: ChunkReader + Clone + 'static> ParquetReader<R> {
    /// Open a Parquet file for reading. Loads bloom filter from KV metadata if present.
    pub fn open(source: R, schema: Arc<TableSchema>) -> Result<Self> {
        let file_reader = SerializedFileReader::new(source.clone())
            .map_err(|e| MeruError::Parquet(e.to_string()))?;

        // Read KV metadata from the file footer.
        let file_meta = file_reader.metadata().file_metadata();
        let kv_map: std::collections::HashMap<String, String> = file_meta
            .key_value_metadata()
            .map(|kv| {
                kv.iter()
                    .filter_map(|e| e.value.as_ref().map(|v| (e.key.clone(), v.clone())))
                    .collect()
            })
            .unwrap_or_default();

        let meta = if let Some(meta_json) = kv_map.get(merutable_types::level::FOOTER_KEY) {
            serde_json::from_str(meta_json)
                .map_err(|e| MeruError::Corruption(format!("footer parse: {e}")))?
        } else {
            // No footer key — construct minimal meta from file metadata.
            ParquetFileMeta {
                level: merutable_types::level::Level(0),
                seq_min: 0,
                seq_max: 0,
                key_min: Vec::new(),
                key_max: Vec::new(),
                num_rows: file_reader.metadata().file_metadata().num_rows() as u64,
                file_size: 0,
                dv_path: None,
                dv_offset: None,
                dv_length: None,
            }
        };

        // Load bloom filter from "merutable.bloom" KV entry.
        let bloom = kv_map.get("merutable.bloom").and_then(|hex_str| {
            hex::decode(hex_str)
                .ok()
                .and_then(|b| FastLocalBloom::from_bytes(&b).ok())
        });

        Ok(Self {
            source,
            schema,
            meta,
            bloom,
        })
    }

    /// Point lookup. Returns `None` if definitely absent (bloom) or not found.
    pub fn get(
        &self,
        user_key_bytes: &[u8],
        read_seq: SeqNum,
        deleted_rows: Option<&RoaringBitmap>,
    ) -> Result<Option<(InternalKey, Row)>> {
        // Bloom gate.
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(user_key_bytes) {
                return Ok(None);
            }
        }

        // Check file-level key range.
        if !self.meta.key_min.is_empty() && user_key_bytes < self.meta.key_min.as_slice() {
            return Ok(None);
        }
        if !self.meta.key_max.is_empty() && user_key_bytes > self.meta.key_max.as_slice() {
            return Ok(None);
        }

        let rows = self.read_all_rows()?;
        for (global_pos, (ikey, row)) in rows.into_iter().enumerate() {
            if let Some(dv) = deleted_rows {
                if dv.contains(global_pos as u32) {
                    continue;
                }
            }
            if ikey.user_key_bytes() != user_key_bytes {
                continue;
            }
            if ikey.seq > read_seq {
                continue;
            }
            return Ok(Some((ikey, row)));
        }
        Ok(None)
    }

    /// Scan rows in key order with optional range and DV filtering.
    pub fn scan(
        &self,
        start_user_key: Option<&[u8]>,
        end_user_key: Option<&[u8]>,
        read_seq: SeqNum,
        deleted_rows: Option<&RoaringBitmap>,
    ) -> Result<Vec<(InternalKey, Row)>> {
        let rows = self.read_all_rows()?;
        let mut results = Vec::new();
        let mut last_uk: Option<Vec<u8>> = None;

        for (global_pos, (ikey, row)) in rows.into_iter().enumerate() {
            if let Some(dv) = deleted_rows {
                if dv.contains(global_pos as u32) {
                    continue;
                }
            }
            if ikey.seq > read_seq {
                continue;
            }
            let uk = ikey.user_key_bytes().to_vec();
            if let Some(start) = start_user_key {
                if uk.as_slice() < start {
                    continue;
                }
            }
            if let Some(end) = end_user_key {
                if uk.as_slice() >= end {
                    break;
                }
            }
            if let Some(ref last) = last_uk {
                if *last == uk {
                    continue;
                }
            }
            last_uk = Some(uk);
            if ikey.op_type == OpType::Delete {
                continue;
            }
            results.push((ikey, row));
        }
        Ok(results)
    }

    pub fn meta(&self) -> &ParquetFileMeta {
        &self.meta
    }

    /// Read every row in the file as a fully-decoded `(InternalKey, Row)`
    /// pair using a column-projected Arrow record-batch reader.
    ///
    /// Projection is level-aware:
    /// - At L0 we ask only for `_merutable_ikey` and `_merutable_value`,
    ///   then `codec::record_batch_to_rows` takes the postcard fast path
    ///   and decodes each `Row` from a single column chunk's bytes.
    /// - At L1+ we ask for `_merutable_ikey` plus every user column, and
    ///   `codec::record_batch_to_rows` materializes each `Row` field by
    ///   field from the typed Arrow arrays.
    fn read_all_rows(&self) -> Result<Vec<(InternalKey, Row)>> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(self.source.clone())
            .map_err(|e| MeruError::Parquet(e.to_string()))?;

        let parquet_schema = builder.parquet_schema();
        let mut leaf_indices: Vec<usize> = Vec::new();

        // Always project the ikey column.
        leaf_indices.push(find_leaf(parquet_schema, IKEY_COLUMN_NAME)?);

        if codec::level_has_value_blob(self.meta.level) {
            // L0 fast path: only the value blob is needed; typed columns
            // would be redundant work.
            leaf_indices.push(find_leaf(parquet_schema, VALUE_BLOB_COLUMN_NAME)?);
        } else {
            // L1+: project every user-defined typed column.
            for col in &self.schema.columns {
                leaf_indices.push(find_leaf(parquet_schema, &col.name)?);
            }
        }

        let mask = ProjectionMask::leaves(parquet_schema, leaf_indices);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|e| MeruError::Parquet(e.to_string()))?;

        let mut out = Vec::with_capacity(self.meta.num_rows as usize);
        for batch_result in reader {
            let batch = batch_result.map_err(|e| MeruError::Parquet(e.to_string()))?;
            let mut decoded = codec::record_batch_to_rows(&batch, &self.schema)?;
            out.append(&mut decoded);
        }
        Ok(out)
    }
}

fn find_leaf(schema: &parquet::schema::types::SchemaDescriptor, name: &str) -> Result<usize> {
    for i in 0..schema.num_columns() {
        if schema.column(i).name() == name {
            return Ok(i);
        }
    }
    Err(MeruError::Corruption(format!(
        "column '{name}' not found in Parquet schema"
    )))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes as BBytes;
    use merutable_types::{
        level::Level,
        schema::{ColumnDef, ColumnType},
        value::{FieldValue, Row},
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

    fn write_test_file(rows: Vec<(InternalKey, Row)>, schema: &TableSchema) -> Vec<u8> {
        let (parquet_bytes, _bloom, _meta) =
            crate::writer::write_sorted_rows(rows, Arc::new(schema.clone()), Level(0), 10).unwrap();
        parquet_bytes
    }

    fn make_ikey(id: i64, seq: u64) -> InternalKey {
        InternalKey::encode(
            &[FieldValue::Int64(id)],
            SeqNum(seq),
            OpType::Put,
            &test_schema(),
        )
        .unwrap()
    }

    #[test]
    fn write_and_read_roundtrip() {
        let schema = test_schema();
        let rows = vec![
            (
                make_ikey(1, 1),
                Row::new(vec![
                    Some(FieldValue::Int64(1)),
                    Some(FieldValue::Bytes(BBytes::from("hello"))),
                ]),
            ),
            (
                make_ikey(2, 2),
                Row::new(vec![
                    Some(FieldValue::Int64(2)),
                    Some(FieldValue::Bytes(BBytes::from("world"))),
                ]),
            ),
        ];

        let bytes = write_test_file(rows, &schema);
        assert!(!bytes.is_empty());

        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();
        assert_eq!(reader.meta().num_rows, 2);
    }

    #[test]
    fn point_lookup_found() {
        let schema = test_schema();
        let ikey = make_ikey(42, 1);
        let uk = ikey.user_key_bytes().to_vec();
        let original_row = Row::new(vec![
            Some(FieldValue::Int64(42)),
            Some(FieldValue::Bytes(BBytes::from("found_me"))),
        ]);
        let rows = vec![(ikey, original_row.clone())];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();

        let result = reader.get(&uk, SeqNum(10), None).unwrap();
        assert!(result.is_some());
        let (ikey, row) = result.unwrap();
        assert_eq!(ikey.seq, SeqNum(1));
        // Real value decode — not a Row::default() placeholder. The row
        // returned by the reader must equal the row written, field-for-field.
        assert_eq!(
            row, original_row,
            "reader must return the actual decoded row, not a default placeholder"
        );
    }

    #[test]
    fn point_lookup_not_found() {
        let schema = test_schema();
        let rows = vec![(
            make_ikey(1, 1),
            Row::new(vec![Some(FieldValue::Int64(1)), None]),
        )];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema.clone())).unwrap();

        // Look up key 99 — should not be found.
        let missing_ikey = make_ikey(99, 1);
        let result = reader
            .get(missing_ikey.user_key_bytes(), SeqNum(10), None)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn point_lookup_bloom_rejects() {
        let schema = test_schema();
        let rows = vec![(
            make_ikey(1, 1),
            Row::new(vec![Some(FieldValue::Int64(1)), None]),
        )];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema.clone())).unwrap();

        // Random key very likely rejected by bloom.
        let random_ikey = make_ikey(999_999, 1);
        let result = reader
            .get(random_ikey.user_key_bytes(), SeqNum(10), None)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn point_lookup_respects_read_seq() {
        let schema = test_schema();
        let rows = vec![(
            make_ikey(1, 10),
            Row::new(vec![Some(FieldValue::Int64(1)), None]),
        )];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema.clone())).unwrap();

        let ikey = make_ikey(1, 10);
        // Read at seq 5: should not see seq=10 write.
        let result = reader.get(ikey.user_key_bytes(), SeqNum(5), None).unwrap();
        assert!(result.is_none());

        // Read at seq 10: should see it.
        let result = reader.get(ikey.user_key_bytes(), SeqNum(10), None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn point_lookup_with_deletion_vector() {
        let schema = test_schema();
        let rows = vec![
            (
                make_ikey(1, 1),
                Row::new(vec![Some(FieldValue::Int64(1)), None]),
            ),
            (
                make_ikey(2, 2),
                Row::new(vec![Some(FieldValue::Int64(2)), None]),
            ),
        ];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema.clone())).unwrap();

        // DV marks row 0 (key=1) as deleted.
        let mut dv = RoaringBitmap::new();
        dv.insert(0);

        let ikey1 = make_ikey(1, 1);
        let result = reader
            .get(ikey1.user_key_bytes(), SeqNum(10), Some(&dv))
            .unwrap();
        assert!(result.is_none(), "row 0 should be masked by DV");

        let ikey2 = make_ikey(2, 2);
        let result = reader
            .get(ikey2.user_key_bytes(), SeqNum(10), Some(&dv))
            .unwrap();
        assert!(result.is_some(), "row 1 should still be visible");
    }

    #[test]
    fn scan_returns_all_rows() {
        let schema = test_schema();
        let originals: Vec<(InternalKey, Row)> = (1..=5i64)
            .map(|i| {
                (
                    make_ikey(i, i as u64),
                    Row::new(vec![
                        Some(FieldValue::Int64(i)),
                        Some(FieldValue::Bytes(BBytes::from(format!("v{i}")))),
                    ]),
                )
            })
            .collect();

        let bytes = write_test_file(originals.clone(), &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();

        let results = reader.scan(None, None, SeqNum(100), None).unwrap();
        assert_eq!(results.len(), 5);
        // Field-for-field equality: real value decode end-to-end.
        for ((orig_ik, orig_row), (got_ik, got_row)) in originals.iter().zip(results.iter()) {
            assert_eq!(orig_ik.seq, got_ik.seq);
            assert_eq!(orig_ik.user_key_bytes(), got_ik.user_key_bytes());
            assert_eq!(orig_row, got_row);
        }
    }

    #[test]
    fn scan_with_dv_excludes_deleted() {
        let schema = test_schema();
        let rows: Vec<_> = (1..=5i64)
            .map(|i| {
                (
                    make_ikey(i, i as u64),
                    Row::new(vec![Some(FieldValue::Int64(i)), None]),
                )
            })
            .collect();

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();

        let mut dv = RoaringBitmap::new();
        dv.insert(1); // Delete row at position 1 (key=2).
        dv.insert(3); // Delete row at position 3 (key=4).

        let results = reader.scan(None, None, SeqNum(100), Some(&dv)).unwrap();
        assert_eq!(results.len(), 3); // rows 0, 2, 4 survive
    }

    /// L1 files have no `_merutable_value` blob — the reader must take the
    /// typed-column decode path and still reconstruct each `Row`
    /// field-for-field. This is the cold-tier read contract: pure columnar
    /// shape, full row materialization.
    #[test]
    fn l1_typed_only_decode_roundtrip() {
        let schema = test_schema();
        let originals: Vec<(InternalKey, Row)> = (1..=10i64)
            .map(|i| {
                let val = if i % 2 == 0 {
                    Some(FieldValue::Bytes(BBytes::from(format!("payload_{i}"))))
                } else {
                    None
                };
                (
                    make_ikey(i, i as u64),
                    Row::new(vec![Some(FieldValue::Int64(i)), val]),
                )
            })
            .collect();

        // Write at Level(1) → no value blob column.
        let (bytes, _bloom, _meta) = crate::writer::write_sorted_rows(
            originals.clone(),
            Arc::new(schema.clone()),
            Level(1),
            10,
        )
        .unwrap();
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();

        let scanned = reader.scan(None, None, SeqNum(100), None).unwrap();
        assert_eq!(scanned.len(), originals.len());
        for ((orig_ik, orig_row), (got_ik, got_row)) in originals.iter().zip(scanned.iter()) {
            assert_eq!(orig_ik.seq, got_ik.seq);
            assert_eq!(orig_ik.user_key_bytes(), got_ik.user_key_bytes());
            assert_eq!(
                orig_row, got_row,
                "L1 typed-column decode must reproduce written rows exactly"
            );
        }

        // Point lookup also goes through the typed-column path at L1.
        let probe = make_ikey(4, 4);
        let got = reader
            .get(probe.user_key_bytes(), SeqNum(100), None)
            .unwrap();
        let (_, row) = got.expect("L1 point lookup must find existing key");
        assert_eq!(&row, &originals[3].1);
    }

    #[test]
    fn empty_file_roundtrip() {
        let schema = test_schema();
        let (bytes, _bloom, meta) =
            crate::writer::write_sorted_rows(vec![], Arc::new(schema.clone()), Level(0), 10)
                .unwrap();
        assert!(bytes.is_empty());
        assert_eq!(meta.num_rows, 0);
    }
}
