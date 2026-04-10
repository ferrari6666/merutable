//! Arrow `RecordBatch` ↔ `(InternalKey, Row)` conversion.
//!
//! Used in the flush pipeline:
//!   memtable entries → `rows_to_record_batch` → Parquet writer
//! And in the read pipeline:
//!   Parquet reader → `record_batch_to_rows` → engine merge iterator
//!
//! The Parquet schema produced here is:
//!   - Column `_merutable_ikey` (Binary, required): full encoded InternalKey bytes
//!   - One column per `TableSchema::columns` entry, in definition order
//!
//! The `_merutable_ikey` column is hidden from the public API but is the
//! sort key and bloom filter target for every Parquet file.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use merutable_types::{
    key::InternalKey,
    level::Level,
    schema::{ColumnType, TableSchema},
    value::{FieldValue, Row},
    MeruError, Result,
};

/// Hidden column carrying the encoded `InternalKey` for every row. Always
/// present, always column 0. Sort key + bloom filter target + kv_index target.
pub const IKEY_COLUMN_NAME: &str = "_merutable_ikey";

/// Hidden column carrying a postcard-encoded `Row` blob. Present at L0 only
/// (the hot tier where point lookups dominate and want a single
/// column-chunk decode), absent at L1+ (cold analytics tier — typed columns
/// are sufficient and reading a redundant blob would waste bytes).
pub const VALUE_BLOB_COLUMN_NAME: &str = "_merutable_value";

/// Whether files at the given level carry a `_merutable_value` blob column.
/// Centralized so writer + reader + codec all agree on the per-level
/// schema shape.
#[inline]
pub fn level_has_value_blob(level: Level) -> bool {
    level.0 == 0
}

/// Build the Arrow schema for a given `TableSchema` and target LSM level.
///
/// Layout:
/// - Column 0: `_merutable_ikey` (always)
/// - Column 1: `_merutable_value` (L0 only)
/// - Remaining: one typed column per `TableSchema::columns` entry, in
///   schema order. These are the columns external HTAP readers see.
pub fn arrow_schema(schema: &TableSchema, level: Level) -> Arc<Schema> {
    let mut fields = vec![Field::new(IKEY_COLUMN_NAME, DataType::Binary, false)];
    if level_has_value_blob(level) {
        fields.push(Field::new(VALUE_BLOB_COLUMN_NAME, DataType::Binary, false));
    }
    for col in &schema.columns {
        let dtype = column_type_to_arrow(&col.col_type);
        fields.push(Field::new(&col.name, dtype, col.nullable));
    }
    Arc::new(Schema::new(fields))
}

fn column_type_to_arrow(ct: &ColumnType) -> DataType {
    match ct {
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Int32 => DataType::Int32,
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float => DataType::Float32,
        ColumnType::Double => DataType::Float64,
        ColumnType::ByteArray => DataType::Binary,
        ColumnType::FixedLenByteArray(n) => DataType::FixedSizeBinary(*n),
    }
}

/// Convert a slice of `(InternalKey, Row)` pairs into an Arrow `RecordBatch`
/// laid out for the given LSM level.
///
/// At L0 the batch carries the `_merutable_value` blob column in addition to
/// the typed user columns; at L1+ only typed user columns. The `_merutable_ikey`
/// column is always column 0.
pub fn rows_to_record_batch(
    rows: &[(InternalKey, Row)],
    schema: &TableSchema,
    level: Level,
) -> Result<RecordBatch> {
    let arrow_sch = arrow_schema(schema, level);
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_sch));
    }
    let n = rows.len();

    // Build _merutable_ikey column.
    let ikey_col: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        rows.iter().map(|(ik, _)| ik.as_bytes()),
    ));
    let mut col_arrays: Vec<ArrayRef> = vec![ikey_col];

    // Optional _merutable_value blob column at L0.
    if level_has_value_blob(level) {
        let blobs: Vec<Vec<u8>> = rows
            .iter()
            .map(|(_, row)| {
                postcard::to_allocvec(row)
                    .map_err(|e| MeruError::Parquet(format!("postcard encode row: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let blob_col: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            blobs.iter().map(|b| b.as_slice()),
        ));
        col_arrays.push(blob_col);
    }

    // Typed user columns, in schema order.
    for (col_idx, col_def) in schema.columns.iter().enumerate() {
        let arr = build_column(rows, col_idx, &col_def.col_type, n)?;
        col_arrays.push(arr);
    }

    RecordBatch::try_new(arrow_sch, col_arrays).map_err(|e| MeruError::Parquet(e.to_string()))
}

fn build_column(
    rows: &[(InternalKey, Row)],
    col_idx: usize,
    col_type: &ColumnType,
    _n: usize,
) -> Result<ArrayRef> {
    match col_type {
        ColumnType::Boolean => {
            let vals: Vec<Option<bool>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Boolean(b) => *b,
                        _ => false,
                    })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        ColumnType::Int32 => {
            let vals: Vec<Option<i32>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Int32(i) => *i,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(Int32Array::from(vals)))
        }
        ColumnType::Int64 => {
            let vals: Vec<Option<i64>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Int64(i) => *i,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(vals)))
        }
        ColumnType::Float => {
            let vals: Vec<Option<f32>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Float(f) => *f,
                        _ => 0.0,
                    })
                })
                .collect();
            Ok(Arc::new(Float32Array::from(vals)))
        }
        ColumnType::Double => {
            let vals: Vec<Option<f64>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Double(d) => *d,
                        _ => 0.0,
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(vals)))
        }
        ColumnType::ByteArray | ColumnType::FixedLenByteArray(_) => {
            let vals: Vec<Option<&[u8]>> = rows
                .iter()
                .map(|(_, row)| {
                    row.get(col_idx).map(|v| match v {
                        FieldValue::Bytes(b) => b.as_ref(),
                        _ => &[],
                    })
                })
                .collect();
            // Use BinaryArray for both variable and fixed-length in Arrow.
            let arr = BinaryArray::from_iter(vals);
            Ok(Arc::new(arr))
        }
    }
}

/// Convert an Arrow `RecordBatch` (from a Parquet read) back into
/// `(InternalKey, Row)` pairs.
///
/// Column lookup is **by name**, not position, so this function works
/// uniformly against:
/// - L0 batches (with `_merutable_value` blob present) — fast path:
///   decode each `Row` from postcard bytes; the typed columns are ignored.
/// - L1+ batches (typed columns only) — materialize each `Row` field by
///   field from the per-column Arrow arrays.
/// - Projected batches that include only a subset of columns, as long as
///   `_merutable_ikey` is present.
pub fn record_batch_to_rows(
    batch: &RecordBatch,
    schema: &TableSchema,
) -> Result<Vec<(InternalKey, Row)>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(vec![]);
    }

    let arrow_schema = batch.schema();
    let ikey_idx = arrow_schema
        .index_of(IKEY_COLUMN_NAME)
        .map_err(|_| MeruError::Parquet(format!("missing {IKEY_COLUMN_NAME} column")))?;
    let ikey_col = batch
        .column(ikey_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| MeruError::Parquet(format!("{IKEY_COLUMN_NAME} not BinaryArray")))?;

    // L0 fast path: if the blob column is present, decode rows directly
    // from postcard bytes and skip per-column field extraction entirely.
    if let Ok(blob_idx) = arrow_schema.index_of(VALUE_BLOB_COLUMN_NAME) {
        let blob_col = batch
            .column(blob_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                MeruError::Parquet(format!("{VALUE_BLOB_COLUMN_NAME} not BinaryArray"))
            })?;

        let mut result = Vec::with_capacity(n);
        for row_idx in 0..n {
            let ikey_bytes = ikey_col.value(row_idx);
            let ikey = InternalKey::decode(ikey_bytes, schema)?;
            let blob = blob_col.value(row_idx);
            let row: Row = postcard::from_bytes(blob)
                .map_err(|e| MeruError::Parquet(format!("postcard decode row: {e}")))?;
            result.push((ikey, row));
        }
        return Ok(result);
    }

    // L1+ path: rebuild Row from typed user columns by name.
    let mut user_col_indices: Vec<usize> = Vec::with_capacity(schema.columns.len());
    for col_def in &schema.columns {
        let idx = arrow_schema.index_of(&col_def.name).map_err(|_| {
            MeruError::Parquet(format!(
                "missing user column '{}' in record batch",
                col_def.name
            ))
        })?;
        user_col_indices.push(idx);
    }

    let mut result = Vec::with_capacity(n);
    for row_idx in 0..n {
        let ikey_bytes = ikey_col.value(row_idx);
        let ikey = InternalKey::decode(ikey_bytes, schema)?;

        let mut fields = Vec::with_capacity(schema.columns.len());
        for (col_def, &arrow_col_idx) in schema.columns.iter().zip(&user_col_indices) {
            let fv = extract_field(batch.column(arrow_col_idx), row_idx, &col_def.col_type)?;
            fields.push(fv);
        }
        result.push((ikey, Row::new(fields)));
    }
    Ok(result)
}

fn extract_field(
    arr: &dyn arrow::array::Array,
    row: usize,
    col_type: &ColumnType,
) -> Result<Option<FieldValue>> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let val = match col_type {
        ColumnType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            FieldValue::Boolean(a.value(row))
        }
        ColumnType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            FieldValue::Int32(a.value(row))
        }
        ColumnType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            FieldValue::Int64(a.value(row))
        }
        ColumnType::Float => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            FieldValue::Float(a.value(row))
        }
        ColumnType::Double => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            FieldValue::Double(a.value(row))
        }
        ColumnType::ByteArray | ColumnType::FixedLenByteArray(_) => {
            let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
            FieldValue::Bytes(Bytes::copy_from_slice(a.value(row)))
        }
    };
    Ok(Some(val))
}
