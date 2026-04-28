// Shared proto <-> domain conversion helpers.
//
// Three modules (rpc/server, rpc/client, harness/node) used to carry
// near-identical copies of these conversions. They differed only in the
// error type wrapped around the fallible direction (`Status`, `ClientError`,
// `NodeError`). This module owns the canonical implementation and returns
// `ProtoConvertError` from the fallible direction; callers implement
// `From<ProtoConvertError>` for their own error type.

use crate::proto::cluster as pb;
use merutable::schema::{ColumnDef, ColumnType, TableSchema};
use merutable::value::{FieldValue, Row};

/// Error returned by the fallible `proto -> domain` conversions.
/// Carries a human-readable message; callers typically wrap it in their own
/// error enum via `From`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoConvertError(pub String);

impl std::fmt::Display for ProtoConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProtoConvertError {}

// ---------------------------------------------------------------------------
// domain -> proto (infallible)
// ---------------------------------------------------------------------------

pub fn field_value_to_proto(fv: &FieldValue) -> pb::FieldValue {
    use pb::field_value::Value;
    let value = match fv {
        FieldValue::Boolean(b) => Value::Boolean(*b),
        FieldValue::Int32(v) => Value::Int32(*v),
        FieldValue::Int64(v) => Value::Int64(*v),
        FieldValue::Float(v) => Value::FloatVal(*v),
        FieldValue::Double(v) => Value::DoubleVal(*v),
        FieldValue::Bytes(b) => Value::ByteArray(b.to_vec()),
    };
    pb::FieldValue { value: Some(value) }
}

pub fn row_to_proto_row(row: &Row) -> pb::Row {
    let fields = row
        .fields
        .iter()
        .map(|opt| match opt {
            None => pb::OptionalFieldValue {
                is_null: true,
                value: None,
            },
            Some(fv) => pb::OptionalFieldValue {
                is_null: false,
                value: Some(field_value_to_proto(fv)),
            },
        })
        .collect();
    pb::Row { fields }
}

pub fn column_type_to_proto(ct: &ColumnType) -> i32 {
    match ct {
        ColumnType::Boolean => pb::ColumnType::Boolean as i32,
        ColumnType::Int32 => pb::ColumnType::Int32 as i32,
        ColumnType::Int64 => pb::ColumnType::Int64 as i32,
        ColumnType::Float => pb::ColumnType::Float as i32,
        ColumnType::Double => pb::ColumnType::Double as i32,
        ColumnType::ByteArray => pb::ColumnType::ByteArray as i32,
        ColumnType::FixedLenByteArray(_) => pb::ColumnType::FixedLenByteArray as i32,
    }
}

pub fn column_def_to_proto(cd: &ColumnDef) -> pb::ColumnDef {
    pb::ColumnDef {
        name: cd.name.clone(),
        col_type: column_type_to_proto(&cd.col_type),
        nullable: cd.nullable,
        field_id: cd.field_id,
        initial_default: cd.initial_default.as_ref().map(field_value_to_proto),
        write_default: cd.write_default.as_ref().map(field_value_to_proto),
        fixed_len: match &cd.col_type {
            ColumnType::FixedLenByteArray(n) => *n,
            _ => 0,
        },
    }
}

pub fn schema_to_proto(schema: &TableSchema) -> pb::TableSchema {
    pb::TableSchema {
        table_name: schema.table_name.clone(),
        columns: schema.columns.iter().map(column_def_to_proto).collect(),
        primary_key: schema.primary_key.iter().map(|&i| i as u32).collect(),
        schema_id: schema.schema_id,
        last_column_id: schema.last_column_id,
    }
}

// ---------------------------------------------------------------------------
// proto -> domain (fallible)
// ---------------------------------------------------------------------------

pub fn proto_field_value_to_field_value(
    proto: &pb::FieldValue,
) -> Result<FieldValue, ProtoConvertError> {
    use pb::field_value::Value;
    match &proto.value {
        Some(Value::Boolean(b)) => Ok(FieldValue::Boolean(*b)),
        Some(Value::Int32(v)) => Ok(FieldValue::Int32(*v)),
        Some(Value::Int64(v)) => Ok(FieldValue::Int64(*v)),
        Some(Value::FloatVal(v)) => Ok(FieldValue::Float(*v)),
        Some(Value::DoubleVal(v)) => Ok(FieldValue::Double(*v)),
        Some(Value::ByteArray(b)) => Ok(FieldValue::Bytes(bytes::Bytes::from(b.clone()))),
        None => Err(ProtoConvertError(
            "FieldValue has no value set".to_string(),
        )),
    }
}

pub fn proto_field_values_to_field_values(
    fields: &[pb::FieldValue],
) -> Result<Vec<FieldValue>, ProtoConvertError> {
    fields.iter().map(proto_field_value_to_field_value).collect()
}

pub fn proto_field_value_opt_to_field_value(
    proto: &Option<pb::FieldValue>,
) -> Result<Option<FieldValue>, ProtoConvertError> {
    match proto {
        None => Ok(None),
        Some(pv) => Ok(Some(proto_field_value_to_field_value(pv)?)),
    }
}

pub fn proto_row_to_row(proto: &pb::Row) -> Result<Row, ProtoConvertError> {
    let fields: Result<Vec<Option<FieldValue>>, ProtoConvertError> = proto
        .fields
        .iter()
        .map(|opt| {
            if opt.is_null {
                Ok(None)
            } else {
                match &opt.value {
                    Some(fv) => Ok(Some(proto_field_value_to_field_value(fv)?)),
                    None => Ok(None),
                }
            }
        })
        .collect();
    Ok(Row::new(fields?))
}

pub fn proto_column_type(
    val: i32,
    fixed_len: i32,
) -> Result<ColumnType, ProtoConvertError> {
    match pb::ColumnType::try_from(val) {
        Ok(pb::ColumnType::Boolean) => Ok(ColumnType::Boolean),
        Ok(pb::ColumnType::Int32) => Ok(ColumnType::Int32),
        Ok(pb::ColumnType::Int64) => Ok(ColumnType::Int64),
        Ok(pb::ColumnType::Float) => Ok(ColumnType::Float),
        Ok(pb::ColumnType::Double) => Ok(ColumnType::Double),
        Ok(pb::ColumnType::ByteArray) => Ok(ColumnType::ByteArray),
        Ok(pb::ColumnType::FixedLenByteArray) => Ok(ColumnType::FixedLenByteArray(fixed_len)),
        Ok(pb::ColumnType::Unspecified) | Err(_) => Err(ProtoConvertError(format!(
            "unknown column type: {}",
            val
        ))),
    }
}

pub fn proto_column_def(proto: &pb::ColumnDef) -> Result<ColumnDef, ProtoConvertError> {
    let col_type = proto_column_type(proto.col_type, proto.fixed_len)?;
    let initial_default = proto_field_value_opt_to_field_value(&proto.initial_default)?;
    let write_default = proto_field_value_opt_to_field_value(&proto.write_default)?;
    Ok(ColumnDef {
        name: proto.name.clone(),
        col_type,
        nullable: proto.nullable,
        field_id: proto.field_id,
        initial_default,
        write_default,
    })
}

pub fn proto_schema_to_schema(
    proto: &pb::TableSchema,
) -> Result<TableSchema, ProtoConvertError> {
    let columns: Result<Vec<ColumnDef>, ProtoConvertError> =
        proto.columns.iter().map(proto_column_def).collect();
    Ok(TableSchema {
        table_name: proto.table_name.clone(),
        columns: columns?,
        primary_key: proto.primary_key.iter().map(|&i| i as usize).collect(),
        schema_id: proto.schema_id,
        last_column_id: proto.last_column_id,
    })
}
