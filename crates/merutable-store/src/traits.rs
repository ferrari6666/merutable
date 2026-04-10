//! `MeruStore` trait: abstraction over object stores (local, S3, etc.).
//!
//! The engine writes Parquet files and Puffin DVs through this trait.
//! WAL always uses direct filesystem I/O — it does not go through MeruStore.

use async_trait::async_trait;
use bytes::Bytes;
use merutable_types::Result;

/// Unified object-store interface for merutable data files.
#[async_trait]
pub trait MeruStore: Send + Sync + 'static {
    /// Write `data` atomically to `path`. If the path already exists, overwrite.
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;

    /// Read the entire object at `path`.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Read a byte range `[offset, offset+length)` from the object at `path`.
    async fn get_range(&self, path: &str, offset: usize, length: usize) -> Result<Bytes>;

    /// Delete the object at `path`. No error if it doesn't exist.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Check whether the object exists.
    async fn exists(&self, path: &str) -> Result<bool>;

    /// List all objects with the given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}
