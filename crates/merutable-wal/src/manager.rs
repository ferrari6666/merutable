//! `WalManager`: lifecycle management for WAL files in a directory.
//!
//! Responsibilities:
//! - Create the initial WAL file on open.
//! - Rotate to a new WAL file when the memtable flushes (old log can be reclaimed).
//! - GC WAL files whose max sequence has been persisted to Parquet.
//! - Recover all valid `WriteBatch`es from a WAL directory on startup.
//!
//! File naming: `{log_number:06}.wal` (e.g., `000001.wal`).

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use merutable_types::{sequence::SeqNum, MeruError, Result};
use tracing::{debug, info};

use crate::{
    batch::WriteBatch,
    reader::{FileSource, WalReader},
    writer::{FileSink, WalWriter},
};

pub struct WalManager {
    dir: PathBuf,
    current: WalWriter,
    log_number: u64,
    next_log: AtomicU64,
    /// Max sequence number that has been durably flushed to Parquet.
    /// WAL files with `max_seq <= flushed_seq` are safe to delete.
    flushed_seq: AtomicU64,
}

impl WalManager {
    /// Open (or create) a WAL directory. Starts writing to a new log file.
    pub fn open(dir: &Path, initial_log_number: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let log_number = initial_log_number;
        let path = log_path(dir, log_number);
        info!(log_number, path = %path.display(), "opening WAL");
        let sink = FileSink::create(&path)?;
        let writer = WalWriter::new(Box::new(sink), log_number, false);
        Ok(Self {
            dir: dir.to_path_buf(),
            current: writer,
            log_number,
            next_log: AtomicU64::new(log_number + 1),
            flushed_seq: AtomicU64::new(0),
        })
    }

    /// Append a `WriteBatch` to the current WAL and fsync.
    pub fn append(&mut self, batch: &WriteBatch) -> Result<()> {
        let encoded = batch.encode();
        self.current.add_record(&encoded)?;
        self.current.sync()
    }

    /// Rotate: close the current WAL and open a new one.
    /// Returns the log number of the file that was closed (the caller
    /// associates it with the immutable memtable that needs flushing).
    pub fn rotate(&mut self) -> Result<u64> {
        let old_log = self.log_number;
        let new_log = self.next_log.fetch_add(1, Ordering::Relaxed);
        debug!(old_log, new_log, "rotating WAL");

        self.current.sync()?;
        // Replace writer — old WalWriter dropped here (file closed by OS).
        let path = log_path(&self.dir, new_log);
        let sink = FileSink::create(&path)?;
        self.current = WalWriter::new(Box::new(sink), new_log, false);
        self.log_number = new_log;
        Ok(old_log)
    }

    /// Mark that all sequences up to (and including) `seq` have been persisted
    /// to Parquet. This allows GC of WAL files whose `max_seq <= seq`.
    pub fn mark_flushed_seq(&self, seq: SeqNum) {
        let mut current = self.flushed_seq.load(Ordering::Acquire);
        loop {
            if seq.0 <= current {
                break;
            }
            match self.flushed_seq.compare_exchange_weak(
                current,
                seq.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => current = now,
            }
        }
    }

    /// Delete WAL files whose log number is strictly less than `before_log`.
    /// Called after a successful Iceberg snapshot commit.
    pub fn gc_logs_before(&self, before_log: u64) -> Result<()> {
        let entries = std::fs::read_dir(&self.dir)?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(log_num) = parse_log_number(&name) {
                if log_num < before_log {
                    let path = entry.path();
                    debug!(log_num, path = %path.display(), "GC WAL file");
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }

    /// Recover all valid `WriteBatch`es from a WAL directory in log-number order.
    /// Returns the batches and the maximum sequence number seen.
    pub fn recover_from_dir(dir: &Path) -> Result<(Vec<WriteBatch>, SeqNum)> {
        let mut log_files = wal_files_in_dir(dir)?;
        log_files.sort_by_key(|(num, _)| *num);

        let mut batches = Vec::new();
        let mut max_seq = SeqNum(0);

        for (log_num, path) in log_files {
            info!(log_num, path = %path.display(), "recovering WAL");
            let source = FileSource::open(&path)?;
            let mut reader = WalReader::new(source, false);
            for record in reader.records() {
                match record {
                    Ok(payload) => {
                        match WriteBatch::decode(&payload) {
                            Ok(batch) => {
                                let last = batch.last_seq();
                                if last > max_seq {
                                    max_seq = last;
                                }
                                batches.push(batch);
                            }
                            Err(e) => {
                                // Corrupt batch in an otherwise valid record — stop recovery.
                                tracing::warn!(error = %e, "corrupt WriteBatch during recovery, stopping");
                                return Ok((batches, max_seq));
                            }
                        }
                    }
                    Err(e) => {
                        // CRC mismatch / truncation — stop cleanly (partial tail is expected).
                        tracing::warn!(error = %e, "WAL read error during recovery, stopping at this point");
                        return Ok((batches, max_seq));
                    }
                }
            }
        }

        Ok((batches, max_seq))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn log_path(dir: &Path, log_number: u64) -> PathBuf {
    dir.join(format!("{log_number:06}.wal"))
}

fn parse_log_number(name: &str) -> Option<u64> {
    name.strip_suffix(".wal")?.parse().ok()
}

fn wal_files_in_dir(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir).map_err(MeruError::Io)?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(log_num) = parse_log_number(&name) {
            files.push((log_num, entry.path()));
        }
    }
    Ok(files)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use merutable_types::sequence::SeqNum;

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn open_write_recover() {
        let dir = tmp_dir();
        {
            let mut mgr = WalManager::open(dir.path(), 1).unwrap();
            let mut batch = WriteBatch::new(SeqNum(1));
            batch.put(Bytes::from("hello"), Bytes::from("world"));
            mgr.append(&batch).unwrap();

            let mut batch2 = WriteBatch::new(SeqNum(2));
            batch2.delete(Bytes::from("gone"));
            mgr.append(&batch2).unwrap();
        }

        let (batches, max_seq) = WalManager::recover_from_dir(dir.path()).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].sequence, SeqNum(1));
        assert_eq!(batches[1].sequence, SeqNum(2));
        assert_eq!(max_seq, SeqNum(2));
    }

    #[test]
    fn rotate_creates_new_file() {
        let dir = tmp_dir();
        let mut mgr = WalManager::open(dir.path(), 1).unwrap();

        let mut b = WriteBatch::new(SeqNum(1));
        b.put(Bytes::from("k"), Bytes::from("v"));
        mgr.append(&b).unwrap();

        let old = mgr.rotate().unwrap();
        assert_eq!(old, 1);
        assert_eq!(mgr.log_number, 2);

        let mut b2 = WriteBatch::new(SeqNum(2));
        b2.put(Bytes::from("k2"), Bytes::from("v2"));
        mgr.append(&b2).unwrap();

        let (batches, _) = WalManager::recover_from_dir(dir.path()).unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn recovery_stops_on_truncation() {
        let dir = tmp_dir();
        {
            let mut mgr = WalManager::open(dir.path(), 1).unwrap();
            let mut b = WriteBatch::new(SeqNum(1));
            b.put(Bytes::from("key"), Bytes::from("val"));
            mgr.append(&b).unwrap();
        }
        // Truncate the WAL file to simulate a crash.
        let wal_path = dir.path().join("000001.wal");
        let meta = std::fs::metadata(&wal_path).unwrap();
        let truncated_len = meta.len() / 2;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        f.set_len(truncated_len).unwrap();

        // Recovery should not panic; it returns whatever it successfully read.
        let (batches, _) = WalManager::recover_from_dir(dir.path()).unwrap();
        // May get 0 or 1 batches depending on where truncation fell.
        let _ = batches;
    }

    #[test]
    fn gc_removes_old_logs() {
        let dir = tmp_dir();
        let mut mgr = WalManager::open(dir.path(), 1).unwrap();
        mgr.rotate().unwrap(); // log 1 → log 2
        mgr.rotate().unwrap(); // log 2 → log 3

        mgr.gc_logs_before(3).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        // Only log 3 (current) should remain.
        assert!(files.iter().all(|f| f.starts_with("000003")));
    }
}
