//! Parquet KV metadata footer encode/decode for merutable-specific fields.
//!
//! Two keys are written into every merutable-managed Parquet file's footer:
//! - `"merutable.meta"` → JSON-serialized `ParquetFileMeta`
//! - `"merutable.schema"` → JSON-serialized `TableSchema`

use merutable_types::{level::ParquetFileMeta, schema::TableSchema, MeruError, Result};
use std::collections::HashMap;

/// Encode merutable metadata as Parquet KV footer entries.
pub fn encode_footer_kv(
    meta: &ParquetFileMeta,
    schema: &TableSchema,
) -> Result<Vec<(String, String)>> {
    let meta_json = meta.serialize()?;
    let schema_json =
        serde_json::to_string(schema).map_err(|e| MeruError::Parquet(e.to_string()))?;
    Ok(vec![
        (ParquetFileMeta::FOOTER_KEY.to_string(), meta_json),
        (ParquetFileMeta::SCHEMA_KEY.to_string(), schema_json),
    ])
}

/// Decode merutable metadata from the Parquet KV footer entries.
pub fn decode_footer_kv(kv: &HashMap<String, String>) -> Result<(ParquetFileMeta, TableSchema)> {
    let meta_json = kv.get(ParquetFileMeta::FOOTER_KEY).ok_or_else(|| {
        MeruError::Corruption(format!(
            "missing '{}' in Parquet KV footer",
            ParquetFileMeta::FOOTER_KEY
        ))
    })?;
    let schema_json = kv.get(ParquetFileMeta::SCHEMA_KEY).ok_or_else(|| {
        MeruError::Corruption(format!(
            "missing '{}' in Parquet KV footer",
            ParquetFileMeta::SCHEMA_KEY
        ))
    })?;

    let meta: ParquetFileMeta = ParquetFileMeta::deserialize(meta_json)?;
    let schema: TableSchema =
        serde_json::from_str(schema_json).map_err(|e| MeruError::Corruption(e.to_string()))?;

    Ok((meta, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType, TableSchema},
    };

    fn sample_meta() -> ParquetFileMeta {
        ParquetFileMeta {
            level: Level(0),
            seq_min: 1,
            seq_max: 100,
            key_min: vec![0x01, 0x02],
            key_max: vec![0xFF, 0xFE],
            num_rows: 500,
            file_size: 1024 * 1024,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            table_name: "events".into(),
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

    #[test]
    fn roundtrip() {
        let meta = sample_meta();
        let schema = sample_schema();
        let kv = encode_footer_kv(&meta, &schema).unwrap();
        let map: HashMap<_, _> = kv.into_iter().collect();
        let (decoded_meta, decoded_schema) = decode_footer_kv(&map).unwrap();

        assert_eq!(decoded_meta.level, meta.level);
        assert_eq!(decoded_meta.num_rows, meta.num_rows);
        assert_eq!(decoded_schema.table_name, schema.table_name);
    }

    #[test]
    fn missing_key_is_error() {
        let kv: HashMap<String, String> = HashMap::new();
        assert!(decode_footer_kv(&kv).is_err());
    }
}
