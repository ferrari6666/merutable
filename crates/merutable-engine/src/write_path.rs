//! Write path helpers.
//!
//! The primary write path is `MeruEngine::put()` and `MeruEngine::delete()`
//! (implemented in `engine.rs`). This module provides batch-write support
//! and helper utilities.

use std::sync::Arc;

use bytes::Bytes;
use merutable_types::{
    key::InternalKey,
    sequence::{OpType, SeqNum},
    value::{FieldValue, Row},
    Result,
};
use merutable_wal::batch::WriteBatch;

use crate::engine::MeruEngine;

/// A batch of mutations to apply atomically to the engine.
/// Assigns sequential `SeqNum`s starting from the allocated base.
pub struct MutationBatch {
    pub(crate) ops: Vec<Mutation>,
}

pub struct Mutation {
    pub pk_values: Vec<FieldValue>,
    pub row: Option<Row>,
    pub op_type: OpType,
}

impl MutationBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put(&mut self, pk_values: Vec<FieldValue>, row: Row) {
        self.ops.push(Mutation {
            pk_values,
            row: Some(row),
            op_type: OpType::Put,
        });
    }

    pub fn delete(&mut self, pk_values: Vec<FieldValue>) {
        self.ops.push(Mutation {
            pk_values,
            row: None,
            op_type: OpType::Delete,
        });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for MutationBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply a `MutationBatch` to the engine atomically.
/// All mutations share the same WAL record and are applied to the memtable
/// in a single `WriteBatch`.
pub async fn apply_batch(engine: &Arc<MeruEngine>, batch: MutationBatch) -> Result<SeqNum> {
    if batch.is_empty() {
        return Ok(engine.global_seq.current());
    }

    // Flow control.
    while engine.memtable.should_stall() {
        engine.memtable.flush_complete.notified().await;
    }

    // Allocate a base seq for the entire batch.
    let base_seq = engine.global_seq.allocate();
    let mut wal_batch = WriteBatch::new(base_seq);

    for mutation in &batch.ops {
        let ikey = InternalKey::encode(
            &mutation.pk_values,
            base_seq,
            mutation.op_type,
            &engine.schema,
        )?;
        let user_key_bytes = Bytes::from(ikey.user_key_bytes().to_vec());

        match mutation.op_type {
            OpType::Put => {
                let value_bytes = mutation
                    .row
                    .as_ref()
                    .map(|r| {
                        let json = serde_json::to_vec(r).unwrap_or_default();
                        Bytes::from(json)
                    })
                    .unwrap_or_default();
                wal_batch.put(user_key_bytes, value_bytes);
            }
            OpType::Delete => {
                wal_batch.delete(user_key_bytes);
            }
        }
    }

    // WAL first.
    {
        let mut wal = engine.wal.lock().await;
        wal.append(&wal_batch)?;
    }

    // Apply to memtable.
    let should_flush = engine.memtable.apply_batch(&wal_batch)?;

    if should_flush {
        let engine = Arc::clone(engine);
        tokio::spawn(async move {
            if let Err(e) = crate::flush::run_flush(&engine).await {
                tracing::error!(error = %e, "flush failed");
            }
        });
    }

    Ok(base_seq)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::{
        schema::{ColumnDef, ColumnType, TableSchema},
        value::Row,
    };

    #[test]
    fn mutation_batch_builder() {
        let mut batch = MutationBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.put(
            vec![FieldValue::Int64(1)],
            Row::new(vec![Some(FieldValue::Int64(1))]),
        );
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());

        batch.delete(vec![FieldValue::Int64(2)]);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn mutation_batch_default() {
        let batch = MutationBatch::default();
        assert!(batch.is_empty());
    }

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

    #[tokio::test]
    async fn apply_empty_batch_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::EngineConfig {
            schema: test_schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        };
        let engine = crate::engine::MeruEngine::open(config).await.unwrap();
        let batch = MutationBatch::new();
        let seq = apply_batch(&engine, batch).await.unwrap();
        // Should return current seq without advancing.
        assert!(seq.0 > 0);
    }

    #[tokio::test]
    async fn apply_batch_writes_and_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::EngineConfig {
            schema: test_schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        };
        let engine = crate::engine::MeruEngine::open(config).await.unwrap();

        let mut batch = MutationBatch::new();
        batch.put(
            vec![FieldValue::Int64(42)],
            Row::new(vec![
                Some(FieldValue::Int64(42)),
                Some(FieldValue::Bytes(bytes::Bytes::from("hello"))),
            ]),
        );
        batch.put(
            vec![FieldValue::Int64(99)],
            Row::new(vec![
                Some(FieldValue::Int64(99)),
                Some(FieldValue::Bytes(bytes::Bytes::from("world"))),
            ]),
        );

        apply_batch(&engine, batch).await.unwrap();

        // Both keys should be readable.
        let row1 = engine.get(&[FieldValue::Int64(42)]).unwrap();
        assert!(row1.is_some());
        let row2 = engine.get(&[FieldValue::Int64(99)]).unwrap();
        assert!(row2.is_some());
    }

    #[tokio::test]
    async fn apply_batch_with_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::EngineConfig {
            schema: test_schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        };
        let engine = crate::engine::MeruEngine::open(config).await.unwrap();

        // Put then delete via batch.
        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![Some(FieldValue::Int64(1)), None]),
            )
            .await
            .unwrap();

        let mut batch = MutationBatch::new();
        batch.delete(vec![FieldValue::Int64(1)]);
        apply_batch(&engine, batch).await.unwrap();

        // Should not be visible (deleted).
        let row = engine.get(&[FieldValue::Int64(1)]).unwrap();
        assert!(row.is_none());
    }
}
