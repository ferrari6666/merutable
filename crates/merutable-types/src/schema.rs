use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::{MeruError, Result};

/// Parquet-native column types. Schema is immutable after table creation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Boolean,
    Int32,
    Int64,
    Float,
    Double,
    /// Variable-length binary or UTF-8 string.
    ByteArray,
    /// Fixed-length binary; `i32` is the byte length.
    FixedLenByteArray(i32),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    /// PK columns must be non-nullable.
    pub nullable: bool,
}

/// The single logical table schema for a merutable instance.
/// `primary_key` is an ordered list of column indices; order determines sort order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
}

impl TableSchema {
    /// Validate schema invariants. Call before use.
    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(MeruError::InvalidArgument(
                "schema must have at least one column".into(),
            ));
        }
        if self.primary_key.is_empty() {
            return Err(MeruError::InvalidArgument(
                "primary key must have at least one column".into(),
            ));
        }
        let mut seen = HashSet::new();
        for &idx in &self.primary_key {
            if idx >= self.columns.len() {
                return Err(MeruError::InvalidArgument(format!(
                    "primary_key index {idx} out of bounds (columns len={})",
                    self.columns.len()
                )));
            }
            if !seen.insert(idx) {
                return Err(MeruError::InvalidArgument(format!(
                    "duplicate primary_key column index {idx}"
                )));
            }
            if self.columns[idx].nullable {
                return Err(MeruError::InvalidArgument(format!(
                    "primary key column '{}' must be non-nullable",
                    self.columns[idx].name
                )));
            }
        }
        Ok(())
    }

    pub fn column_by_name(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }

    /// Number of PK columns.
    pub fn pk_len(&self) -> usize {
        self.primary_key.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema(pk_nullable: bool) -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: pk_nullable,
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
    fn valid_schema_passes() {
        make_schema(false).validate().unwrap();
    }

    #[test]
    fn nullable_pk_rejected() {
        assert!(make_schema(true).validate().is_err());
    }

    #[test]
    fn empty_pk_rejected() {
        let mut s = make_schema(false);
        s.primary_key.clear();
        assert!(s.validate().is_err());
    }

    #[test]
    fn out_of_bounds_pk_rejected() {
        let mut s = make_schema(false);
        s.primary_key = vec![99];
        assert!(s.validate().is_err());
    }

    #[test]
    fn duplicate_pk_col_rejected() {
        let mut s = make_schema(false);
        s.primary_key = vec![0, 0];
        assert!(s.validate().is_err());
    }
}
