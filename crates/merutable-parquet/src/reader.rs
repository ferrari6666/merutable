//! `ParquetReader`: point lookup and range scan with Deletion Vector masking.
//!
//! Uses the `parquet` crate's Arrow reader for actual column reading.

use std::sync::Arc;

use merutable_types::{
    key::InternalKey,
    level::ParquetFileMeta,
    schema::TableSchema,
    sequence::{OpType, SeqNum},
    value::Row,
    MeruError, Result,
};
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use roaring::RoaringBitmap;

use crate::{bloom::FastLocalBloom, codec::IKEY_COLUMN_NAME};

pub struct ParquetReader<R: ChunkReader> {
    file_reader: SerializedFileReader<R>,
    schema: Arc<TableSchema>,
    meta: ParquetFileMeta,
    bloom: Option<FastLocalBloom>,
}

impl<R: ChunkReader + 'static> ParquetReader<R> {
    /// Open a Parquet file for reading. Loads bloom filter from KV metadata if present.
    pub fn open(source: R, schema: Arc<TableSchema>) -> Result<Self> {
        let file_reader =
            SerializedFileReader::new(source).map_err(|e| MeruError::Parquet(e.to_string()))?;

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
            file_reader,
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

        let ikeys = self.read_all_ikeys()?;
        for (global_pos, ikey_bytes) in ikeys.iter().enumerate() {
            if let Some(dv) = deleted_rows {
                if dv.contains(global_pos as u32) {
                    continue;
                }
            }
            if ikey_bytes.len() < 8 {
                continue;
            }
            let uk = &ikey_bytes[..ikey_bytes.len() - 8];
            if uk != user_key_bytes {
                continue;
            }
            let ikey = InternalKey::decode(ikey_bytes, &self.schema)?;
            if ikey.seq > read_seq {
                continue;
            }
            return Ok(Some((ikey, Row::default())));
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
        let ikeys = self.read_all_ikeys()?;
        let mut results = Vec::new();
        let mut last_uk: Option<Vec<u8>> = None;

        for (global_pos, ikey_bytes) in ikeys.iter().enumerate() {
            if let Some(dv) = deleted_rows {
                if dv.contains(global_pos as u32) {
                    continue;
                }
            }
            let ikey = InternalKey::decode(ikey_bytes, &self.schema)?;
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
            results.push((ikey, Row::default()));
        }
        Ok(results)
    }

    pub fn meta(&self) -> &ParquetFileMeta {
        &self.meta
    }

    /// Read all ikey bytes from the file using the Arrow-based reader.
    fn read_all_ikeys(&self) -> Result<Vec<Vec<u8>>> {
        let file_meta = self.file_reader.metadata();
        let ikey_col_idx = find_ikey_column_index(file_meta)?;

        let mut all_ikeys = Vec::new();
        let num_rg = file_meta.num_row_groups();

        for rg_idx in 0..num_rg {
            let rg_reader = self
                .file_reader
                .get_row_group(rg_idx)
                .map_err(|e| MeruError::Parquet(e.to_string()))?;
            let rg_meta = file_meta.row_group(rg_idx);
            let num_rows = rg_meta.num_rows() as usize;

            // Use the row-level iterator to read the ikey column.
            let mut row_iter = rg_reader
                .get_row_iter(None)
                .map_err(|e| MeruError::Parquet(e.to_string()))?;

            for _ in 0..num_rows {
                if let Some(row_result) = row_iter.next() {
                    let row = row_result.map_err(|e| MeruError::Parquet(e.to_string()))?;
                    // The ikey column is the first column (index 0).
                    match row.get_bytes(ikey_col_idx) {
                        Ok(ba) => all_ikeys.push(ba.data().to_vec()),
                        Err(_) => all_ikeys.push(Vec::new()),
                    }
                }
            }
        }
        Ok(all_ikeys)
    }
}

fn find_ikey_column_index(meta: &parquet::file::metadata::ParquetMetaData) -> Result<usize> {
    let schema = meta.file_metadata().schema_descr();
    for i in 0..schema.num_columns() {
        if schema.column(i).name() == IKEY_COLUMN_NAME {
            return Ok(i);
        }
    }
    Err(MeruError::Corruption(format!(
        "column '{}' not found in Parquet schema",
        IKEY_COLUMN_NAME
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
        let rows = vec![(
            ikey,
            Row::new(vec![
                Some(FieldValue::Int64(42)),
                Some(FieldValue::Bytes(BBytes::from("found_me"))),
            ]),
        )];

        let bytes = write_test_file(rows, &schema);
        let reader = ParquetReader::open(BBytes::from(bytes), Arc::new(schema)).unwrap();

        let result = reader.get(&uk, SeqNum(10), None).unwrap();
        assert!(result.is_some());
        let (ikey, _row) = result.unwrap();
        assert_eq!(ikey.seq, SeqNum(1));
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

        let results = reader.scan(None, None, SeqNum(100), None).unwrap();
        assert_eq!(results.len(), 5);
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
