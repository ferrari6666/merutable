//! WAL writer: appends `WriteBatch`es as physical records into 32 KiB blocks.
//!
//! A `WriteBatch` payload is fragmented across block boundaries using the
//! standard First/Middle/Last record types (same as RocksDB).
//!
//! CRC32c checksum covers `[record_type_byte ++ payload_fragment]`.

use std::io::Write;

use crc32fast::Hasher as Crc32;
use merutable_types::{MeruError, Result};

use crate::format::{RecordType, BLOCK_SIZE, HEADER_SIZE, MIN_REMAINING};

/// Pluggable WAL sink. Local filesystem: `std::fs::File`.
/// Distributed/consensus WAL: implement this trait (e.g., Raft append log).
pub trait WalSink: Send + Sync {
    /// Append bytes to the log. Must be durable after `sync()`.
    fn append(&mut self, data: &[u8]) -> Result<()>;
    /// Fsync/flush so data is durable.
    fn sync(&mut self) -> Result<()>;
    /// Close the sink cleanly.
    fn close(self: Box<Self>) -> Result<()>;
}

/// `WalSink` backed by a local file.
pub struct FileSink {
    file: std::fs::File,
}

impl FileSink {
    pub fn create(path: &std::path::Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }
}

impl WalSink for FileSink {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data).map_err(MeruError::Io)
    }
    fn sync(&mut self) -> Result<()> {
        self.file.sync_data().map_err(MeruError::Io)
    }
    fn close(self: Box<Self>) -> Result<()> {
        // `file` dropped on return — OS closes fd.
        Ok(())
    }
}

/// Writes `WriteBatch` payloads as WAL records.
pub struct WalWriter {
    sink: Box<dyn WalSink>,
    /// Byte offset within the current 32 KiB block.
    block_offset: usize,
    log_number: u64,
    recyclable: bool,
}

impl WalWriter {
    pub fn new(sink: Box<dyn WalSink>, log_number: u64, recyclable: bool) -> Self {
        Self {
            sink,
            block_offset: 0,
            log_number,
            recyclable,
        }
    }

    /// Append one encoded `WriteBatch` payload. Fragments across block boundaries.
    /// Caller must have encoded the batch via `WriteBatch::encode()`.
    pub fn add_record(&mut self, payload: &[u8]) -> Result<()> {
        let mut remaining = payload;
        let mut is_first = true;

        while !remaining.is_empty() {
            let available = self.available_in_block();

            // If the block can't even fit a header, pad with zeros and start a new one.
            if available < MIN_REMAINING {
                let pad = vec![0u8; available];
                self.sink.append(&pad)?;
                self.block_offset = 0;
            }

            let available = self.available_in_block();
            let header_size = if self.recyclable {
                crate::format::RECYCLABLE_HEADER_SIZE
            } else {
                HEADER_SIZE
            };
            let capacity = available - header_size;
            let fragment = &remaining[..remaining.len().min(capacity)];
            remaining = &remaining[fragment.len()..];

            let rtype = if is_first && remaining.is_empty() {
                if self.recyclable {
                    RecordType::RecyclableFull
                } else {
                    RecordType::Full
                }
            } else if is_first {
                if self.recyclable {
                    RecordType::RecyclableFirst
                } else {
                    RecordType::First
                }
            } else if remaining.is_empty() {
                if self.recyclable {
                    RecordType::RecyclableLast
                } else {
                    RecordType::Last
                }
            } else {
                if self.recyclable {
                    RecordType::RecyclableMiddle
                } else {
                    RecordType::Middle
                }
            };

            self.emit_physical_record(rtype, fragment)?;
            is_first = false;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.sink.sync()
    }

    pub fn close(self) -> Result<()> {
        Box::new(self.sink).close()
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    fn available_in_block(&self) -> usize {
        BLOCK_SIZE - self.block_offset
    }

    fn emit_physical_record(&mut self, rtype: RecordType, payload: &[u8]) -> Result<()> {
        debug_assert!(payload.len() <= u16::MAX as usize);
        let header_size = if self.recyclable {
            crate::format::RECYCLABLE_HEADER_SIZE
        } else {
            HEADER_SIZE
        };

        // CRC covers: [type_byte ++ payload].
        let mut hasher = Crc32::new();
        hasher.update(&[rtype as u8]);
        hasher.update(payload);
        let crc = hasher.finalize();

        // Build header.
        let mut header = [0u8; 11]; // max recyclable header
        header[..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(payload.len() as u16).to_le_bytes());
        header[6] = rtype as u8;
        if self.recyclable {
            header[7..11].copy_from_slice(&(self.log_number as u32).to_le_bytes());
        }

        self.sink.append(&header[..header_size])?;
        self.sink.append(payload)?;
        self.block_offset += header_size + payload.len();
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{VecSource, WalReader};

    fn make_writer(recyclable: bool) -> (WalWriter, std::sync::Arc<std::sync::Mutex<Vec<u8>>>) {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = VecSink { buf: buf.clone() };
        let writer = WalWriter::new(Box::new(sink), 1, recyclable);
        (writer, buf)
    }

    struct VecSink {
        buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }
    impl WalSink for VecSink {
        fn append(&mut self, data: &[u8]) -> Result<()> {
            self.buf.lock().unwrap().extend_from_slice(data);
            Ok(())
        }
        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
        fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_small_record_and_read_back() {
        let (mut writer, buf) = make_writer(false);
        let payload = b"hello merutable";
        writer.add_record(payload).unwrap();
        let data = buf.lock().unwrap().clone();
        let mut reader = WalReader::new(VecSource::new(data), false);
        let records: Vec<_> = reader.records().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].as_ref().unwrap()[..], payload);
    }

    #[test]
    fn write_multiple_records() {
        let (mut writer, buf) = make_writer(false);
        let payloads: &[&[u8]] = &[b"alpha", b"beta", b"gamma delta"];
        for p in payloads {
            writer.add_record(p).unwrap();
        }
        let data = buf.lock().unwrap().clone();
        let mut reader = WalReader::new(VecSource::new(data), false);
        let records: Vec<_> = reader.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), payloads.len());
        for (got, expected) in records.iter().zip(payloads.iter()) {
            assert_eq!(&got[..], *expected);
        }
    }

    #[test]
    fn fragmentation_across_block_boundary() {
        let (mut writer, buf) = make_writer(false);
        // Payload larger than one block to force fragmentation.
        let payload = vec![0xAAu8; BLOCK_SIZE + 100];
        writer.add_record(&payload).unwrap();
        let data = buf.lock().unwrap().clone();
        let mut reader = WalReader::new(VecSource::new(data), false);
        let records: Vec<_> = reader.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], payload);
    }

    #[test]
    fn recyclable_format_roundtrip() {
        let (mut writer, buf) = make_writer(true);
        writer.add_record(b"recyclable test").unwrap();
        let data = buf.lock().unwrap().clone();
        let mut reader = WalReader::new(VecSource::new(data), true);
        let records: Vec<_> = reader.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 1);
        assert_eq!(&records[0][..], b"recyclable test");
    }
}
