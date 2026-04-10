use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A typed field value. `Bytes` covers both `ByteArray` and `FixedLenByteArray`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Bytes(Bytes),
}

/// A complete table row. Fields are parallel to `TableSchema::columns`.
/// `None` = SQL NULL (only valid for nullable columns).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Row {
    pub fields: Vec<Option<FieldValue>>,
}

impl Row {
    pub fn new(fields: Vec<Option<FieldValue>>) -> Self {
        Self { fields }
    }

    /// Access a field by column index.
    pub fn get(&self, col_idx: usize) -> Option<&FieldValue> {
        self.fields.get(col_idx).and_then(Option::as_ref)
    }

    /// Extract PK field values in `primary_key` index order.
    /// Returns `Err` if any PK field is NULL.
    pub fn pk_values(&self, primary_key: &[usize]) -> crate::Result<Vec<FieldValue>> {
        primary_key
            .iter()
            .map(|&idx| {
                self.fields
                    .get(idx)
                    .and_then(Option::as_ref)
                    .cloned()
                    .ok_or_else(|| {
                        crate::MeruError::InvalidArgument(format!(
                            "PK column at index {idx} is NULL"
                        ))
                    })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_new_and_get() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(42)),
            Some(FieldValue::Boolean(true)),
            None,
        ]);
        assert_eq!(row.get(0), Some(&FieldValue::Int64(42)));
        assert_eq!(row.get(1), Some(&FieldValue::Boolean(true)));
        assert_eq!(row.get(2), None); // NULL field
        assert_eq!(row.get(3), None); // out of bounds
    }

    #[test]
    fn row_default_is_empty() {
        let row = Row::default();
        assert!(row.fields.is_empty());
        assert_eq!(row.get(0), None);
    }

    #[test]
    fn pk_values_extracts_correctly() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(1)),
            Some(FieldValue::Bytes(Bytes::from("hello"))),
            Some(FieldValue::Boolean(false)),
        ]);
        let pk = row.pk_values(&[0, 2]).unwrap();
        assert_eq!(pk, vec![FieldValue::Int64(1), FieldValue::Boolean(false)]);
    }

    #[test]
    fn pk_values_errors_on_null() {
        let row = Row::new(vec![Some(FieldValue::Int64(1)), None]);
        let result = row.pk_values(&[1]);
        assert!(result.is_err());
    }

    #[test]
    fn field_value_serde_roundtrip() {
        let values = vec![
            FieldValue::Boolean(true),
            FieldValue::Int32(42),
            FieldValue::Int64(-100),
            FieldValue::Float(1.23),
            FieldValue::Double(4.56789),
            FieldValue::Bytes(Bytes::from("test")),
        ];
        for v in &values {
            let json = serde_json::to_string(v).unwrap();
            let back: FieldValue = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, v);
        }
    }

    #[test]
    fn row_serde_roundtrip() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(1)),
            None,
            Some(FieldValue::Bytes(Bytes::from("data"))),
        ]);
        let json = serde_json::to_vec(&row).unwrap();
        let back: Row = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.fields.len(), 3);
        assert_eq!(back.get(0), Some(&FieldValue::Int64(1)));
        assert_eq!(back.get(1), None);
        assert_eq!(back.get(2), Some(&FieldValue::Bytes(Bytes::from("data"))));
    }
}
