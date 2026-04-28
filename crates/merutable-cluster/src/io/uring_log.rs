// P1.1: IoUringRaftLog — io_uring + O_DIRECT Raft log writer.
// AlignedBufferPool — pre-allocated 4 KiB DMA buffers.
//
// On linux: io_uring submission/completion with O_DIRECT | O_DSYNC.
// On non-linux (macOS dev): fallback to std::fs::File + write + sync_data.
// The public API is identical on both paths.

use std::sync::Mutex;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Alignment required by O_DIRECT (and used for entry padding).
const ALIGN: usize = 4096;

/// Size of the entry header: payload_len (u32) + crc32c (u32).
const HEADER_SIZE: usize = 8;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Error, Debug)]
pub enum RaftLogError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error("truncated entry at offset {0}")]
    Truncated(u64),
}

pub type Result<T> = std::result::Result<T, RaftLogError>;

// ---------------------------------------------------------------------------
// AlignedBuf — a heap-allocated, 4096-byte-aligned buffer
// ---------------------------------------------------------------------------

/// A heap buffer whose base address is aligned to [`ALIGN`] bytes.
/// Implements `Drop` to deallocate safely.
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
}

// SAFETY: the buffer is just a raw byte slab with no interior aliasing.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate a zero-initialized buffer of at least `size` bytes, rounded up
    /// to a multiple of [`ALIGN`].
    fn new(size: usize) -> Self {
        let aligned_size = round_up(size.max(ALIGN), ALIGN);
        let layout =
            std::alloc::Layout::from_size_align(aligned_size, ALIGN).expect("invalid layout");
        // SAFETY: layout is non-zero and properly aligned.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self {
            ptr,
            len: aligned_size,
            layout,
        }
    }

    /// Returns a mutable slice over the full buffer.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `len` bytes and we have unique access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Returns a read-only slice over the full buffer.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: same as above, read-only.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns the raw pointer (for io_uring iov).
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Returns the allocated length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Re-zero the buffer for reuse.
    fn zero(&mut self) {
        // SAFETY: ptr is valid for `len` bytes.
        unsafe {
            std::ptr::write_bytes(self.ptr, 0, self.len);
        }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout via alloc_zeroed.
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

// ---------------------------------------------------------------------------
// AlignedBufferPool
// ---------------------------------------------------------------------------

/// A simple pool of reusable [`AlignedBuf`] buffers, keyed by size class
/// (each size class is the rounded-up-to-ALIGN allocation size).
pub struct AlignedBufferPool {
    pool: Mutex<Vec<AlignedBuf>>,
}

impl AlignedBufferPool {
    pub fn new() -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
        }
    }

    /// Acquire a zero-initialized buffer of at least `size` bytes.
    pub fn acquire(&self, size: usize) -> AlignedBuf {
        let needed = round_up(size.max(ALIGN), ALIGN);
        let mut pool = self.pool.lock().unwrap();
        // Try to find a buffer that is large enough.
        if let Some(idx) = pool.iter().position(|b| b.len >= needed) {
            let mut buf = pool.swap_remove(idx);
            buf.zero();
            buf
        } else {
            drop(pool);
            AlignedBuf::new(needed)
        }
    }

    /// Return a buffer to the pool for later reuse.
    pub fn release(&self, buf: AlignedBuf) {
        let mut pool = self.pool.lock().unwrap();
        // Cap pool size to avoid unbounded growth.
        if pool.len() < 64 {
            pool.push(buf);
        }
        // else: buf is dropped
    }
}

impl Default for AlignedBufferPool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Round `n` up to the next multiple of `align`.
fn round_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

/// Compute the on-disk size of a framed entry (header + payload + padding).
fn framed_size(payload_len: usize) -> usize {
    round_up(HEADER_SIZE + payload_len, ALIGN)
}

/// Serialise one entry into the front of `buf`. The caller must ensure
/// `buf.len() >= framed_size(payload.len())`. The buffer should already be
/// zeroed (padding bytes will be 0x00).
fn write_frame(buf: &mut [u8], payload: &[u8]) {
    let len = payload.len() as u32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());

    let crc = crc32fast::hash(payload);
    buf[4..8].copy_from_slice(&crc.to_le_bytes());

    buf[HEADER_SIZE..HEADER_SIZE + payload.len()].copy_from_slice(payload);
    // Remaining bytes stay zero (padding).
}

/// Decode an entry from raw bytes starting at `data[0..]`.
/// Returns `(payload, total_frame_bytes_consumed)` on success.
fn read_frame(data: &[u8], file_offset: u64) -> Result<(Vec<u8>, usize)> {
    if data.len() < HEADER_SIZE {
        return Err(RaftLogError::Truncated(file_offset));
    }
    let payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(data[4..8].try_into().unwrap());

    let frame_total = framed_size(payload_len);
    if data.len() < HEADER_SIZE + payload_len {
        return Err(RaftLogError::Truncated(file_offset));
    }

    let payload = &data[HEADER_SIZE..HEADER_SIZE + payload_len];
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != stored_crc {
        return Err(RaftLogError::CrcMismatch {
            offset: file_offset,
            expected: stored_crc,
            actual: actual_crc,
        });
    }

    Ok((payload.to_vec(), frame_total))
}

// ===========================================================================
// Linux path: io_uring + O_DIRECT (cfg-gated, not compiled on macOS)
// ===========================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    use io_uring::{opcode, types, IoUring};

    /// Raft log writer backed by io_uring with O_DIRECT | O_DSYNC.
    pub struct RaftLog {
        ring: IoUring,
        file: std::fs::File,
        write_offset: u64,
        pool: AlignedBufferPool,
    }

    impl RaftLog {
        /// Open (or create) the log file at `path`.
        ///
        /// Tries `O_DIRECT | O_DSYNC` first; on filesystems that don't
        /// support O_DIRECT (overlayfs / tmpfs — i.e. kind/Docker Desktop
        /// emptyDir volumes), falls back to `O_DSYNC` alone.
        pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
            let p = path.as_ref();
            let file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
                .open(p)
            {
                Ok(f) => f,
                Err(e)
                    if matches!(
                        e.raw_os_error(),
                        Some(libc::EINVAL)
                            | Some(libc::ENOTSUP)
                            | Some(libc::ENOSYS)
                            | Some(libc::EOPNOTSUPP)
                    ) =>
                {
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .custom_flags(libc::O_DSYNC)
                        .open(p)?
                }
                Err(e) => return Err(e.into()),
            };

            let len = file.metadata()?.len();

            let ring = IoUring::new(32)?;

            Ok(Self {
                ring,
                file,
                write_offset: len,
                pool: AlignedBufferPool::new(),
            })
        }

        /// Append a single entry. Returns the file offset where it was written.
        pub fn append(&mut self, entry_bytes: &[u8]) -> Result<u64> {
            let frame_len = framed_size(entry_bytes.len());
            let mut buf = self.pool.acquire(frame_len);
            write_frame(buf.as_mut_slice(), entry_bytes);

            let offset = self.write_offset;
            let fd = types::Fd(self.file.as_raw_fd());
            let write_e = opcode::Write::new(fd, buf.as_ptr(), frame_len as u32)
                .offset(offset)
                .build()
                .user_data(0x01);

            // SAFETY: SQE references buf which lives until CQE is reaped.
            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "SQ full"))?;
            }
            self.ring.submit_and_wait(1)?;

            let cqe = self.ring.completion().next().expect("no CQE");
            let res = cqe.result();
            if res < 0 {
                return Err(std::io::Error::from_raw_os_error(-res).into());
            }

            self.write_offset += frame_len as u64;
            self.pool.release(buf);
            Ok(offset)
        }

        /// Append multiple entries in one submission.
        pub fn append_batch(&mut self, entries: &[&[u8]]) -> Result<u64> {
            let first_offset = self.write_offset;
            for entry in entries {
                self.append(entry)?;
            }
            Ok(first_offset)
        }

        /// Read a single entry at the given byte offset.
        pub fn read_entry(&self, offset: u64) -> Result<Vec<u8>> {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = std::fs::File::open(
                format!("/proc/self/fd/{}", self.file.as_raw_fd()),
            )
            .or_else(|_| {
                // Fallback: re-open via /dev/fd (macOS compat, though this is the linux path).
                Err(std::io::Error::new(std::io::ErrorKind::Other, "cannot reopen"))
            })?;
            f.seek(SeekFrom::Start(offset))?;
            let mut header = [0u8; HEADER_SIZE];
            f.read_exact(&mut header)?;
            let payload_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let frame_total = framed_size(payload_len);
            let mut frame_buf = vec![0u8; frame_total];
            frame_buf[..HEADER_SIZE].copy_from_slice(&header);
            if payload_len > 0 {
                f.read_exact(&mut frame_buf[HEADER_SIZE..HEADER_SIZE + payload_len])?;
            }
            let (payload, _) = read_frame(&frame_buf, offset)?;
            Ok(payload)
        }

        /// Scan all entries from the beginning of the file.
        pub fn scan_all(&self) -> Result<Vec<Vec<u8>>> {
            use std::io::Read;
            let mut f = std::fs::File::open(
                format!("/proc/self/fd/{}", self.file.as_raw_fd()),
            )?;
            let file_len = f.metadata()?.len() as usize;
            let mut data = vec![0u8; file_len];
            f.read_exact(&mut data)?;
            scan_entries(&data)
        }
    }
}

// ===========================================================================
// Fallback path: std::fs (non-linux — macOS development)
// ===========================================================================

#[cfg(not(target_os = "linux"))]
mod fallback {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};

    /// Raft log writer backed by `std::fs::File` + `sync_data()`.
    pub struct RaftLog {
        file: File,
        path: PathBuf,
        write_offset: u64,
        pool: AlignedBufferPool,
    }

    impl RaftLog {
        /// Open (or create) the log file at `path`.
        pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
            let path = path.as_ref().to_path_buf();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;

            let len = file.metadata()?.len();

            Ok(Self {
                file,
                path,
                write_offset: len,
                pool: AlignedBufferPool::new(),
            })
        }

        /// Append a single entry. Returns the file offset where it was written.
        pub fn append(&mut self, entry_bytes: &[u8]) -> Result<u64> {
            let frame_len = framed_size(entry_bytes.len());
            let mut buf = self.pool.acquire(frame_len);
            write_frame(buf.as_mut_slice(), entry_bytes);

            let offset = self.write_offset;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&buf.as_slice()[..frame_len])?;
            self.file.sync_data()?;

            self.write_offset += frame_len as u64;
            self.pool.release(buf);
            Ok(offset)
        }

        /// Append multiple entries in a single I/O batch (on fallback this is
        /// a single concatenated write + one sync).
        pub fn append_batch(&mut self, entries: &[&[u8]]) -> Result<u64> {
            if entries.is_empty() {
                return Ok(self.write_offset);
            }

            // Compute total frame size for all entries.
            let total: usize = entries.iter().map(|e| framed_size(e.len())).sum();
            let mut buf = self.pool.acquire(total);

            let mut cursor = 0usize;
            for entry in entries {
                let flen = framed_size(entry.len());
                write_frame(&mut buf.as_mut_slice()[cursor..cursor + flen], entry);
                cursor += flen;
            }

            let first_offset = self.write_offset;
            self.file.seek(SeekFrom::Start(first_offset))?;
            self.file.write_all(&buf.as_slice()[..total])?;
            self.file.sync_data()?;

            self.write_offset += total as u64;
            self.pool.release(buf);
            Ok(first_offset)
        }

        /// Read a single entry at the given byte offset.
        pub fn read_entry(&self, offset: u64) -> Result<Vec<u8>> {
            let mut f = File::open(&self.path)?;
            f.seek(SeekFrom::Start(offset))?;

            let mut header = [0u8; HEADER_SIZE];
            f.read_exact(&mut header)?;
            let payload_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let frame_total = framed_size(payload_len);

            let mut frame_buf = vec![0u8; frame_total];
            frame_buf[..HEADER_SIZE].copy_from_slice(&header);
            if payload_len > 0 {
                f.read_exact(&mut frame_buf[HEADER_SIZE..HEADER_SIZE + payload_len])?;
            }

            let (payload, _) = read_frame(&frame_buf, offset)?;
            Ok(payload)
        }

        /// Scan all entries from the beginning of the file (for crash recovery).
        pub fn scan_all(&self) -> Result<Vec<Vec<u8>>> {
            let mut f = File::open(&self.path)?;
            let file_len = f.metadata()?.len() as usize;
            if file_len == 0 {
                return Ok(Vec::new());
            }
            let mut data = vec![0u8; file_len];
            f.read_exact(&mut data)?;
            scan_entries(&data)
        }
    }
}

// ---------------------------------------------------------------------------
// Shared scan logic
// ---------------------------------------------------------------------------

/// Walk a byte buffer containing concatenated framed entries and decode them all.
fn scan_entries(data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut entries = Vec::new();
    let mut pos = 0usize;

    while pos + HEADER_SIZE <= data.len() {
        let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;

        // A zero-length header with zero CRC indicates we hit padding / EOF.
        if payload_len == 0 {
            let stored_crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
            if stored_crc == 0 {
                break;
            }
        }

        let (payload, frame_bytes) = read_frame(&data[pos..], pos as u64)?;
        entries.push(payload);
        pos += frame_bytes;
    }

    Ok(entries)
}

// ---------------------------------------------------------------------------
// Re-export the platform-appropriate struct as `RaftLog`
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
pub use linux::RaftLog;

#[cfg(not(target_os = "linux"))]
pub use fallback::RaftLog;

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_single_entry_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.log");

        let mut log = RaftLog::open(&path).unwrap();
        let data = b"hello raft";
        let off = log.append(data).unwrap();
        assert_eq!(off, 0);

        let entries = log.scan_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], data);
    }

    #[test]
    fn write_multiple_entries_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.log");

        let mut log = RaftLog::open(&path).unwrap();
        for i in 0u32..100 {
            let payload = format!("entry-{:04}", i);
            log.append(payload.as_bytes()).unwrap();
        }

        let entries = log.scan_all().unwrap();
        assert_eq!(entries.len(), 100);
        for (i, entry) in entries.iter().enumerate() {
            let expected = format!("entry-{:04}", i);
            assert_eq!(entry, expected.as_bytes(), "mismatch at index {}", i);
        }
    }

    #[test]
    fn append_batch_matches_individual() {
        let dir = tempfile::tempdir().unwrap();

        // --- individual appends ---
        let path_ind = dir.path().join("individual.log");
        let mut log_ind = RaftLog::open(&path_ind).unwrap();
        let payloads: Vec<Vec<u8>> = (0..10).map(|i| format!("batch-{}", i).into_bytes()).collect();
        for p in &payloads {
            log_ind.append(p).unwrap();
        }
        let entries_ind = log_ind.scan_all().unwrap();

        // --- batch append ---
        let path_bat = dir.path().join("batch.log");
        let mut log_bat = RaftLog::open(&path_bat).unwrap();
        let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
        log_bat.append_batch(&refs).unwrap();
        let entries_bat = log_bat.scan_all().unwrap();

        assert_eq!(entries_ind.len(), entries_bat.len());
        for (a, b) in entries_ind.iter().zip(entries_bat.iter()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn large_entry_spanning_multiple_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.log");

        let mut log = RaftLog::open(&path).unwrap();
        let big = vec![0xABu8; 16 * 1024]; // 16 KiB
        let off = log.append(&big).unwrap();
        assert_eq!(off, 0);

        // Verify round-trip via scan.
        let entries = log.scan_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], big);

        // Verify round-trip via read_entry.
        let single = log.read_entry(off).unwrap();
        assert_eq!(single, big);
    }

    #[test]
    fn recovery_after_simulated_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.log");

        let payloads: Vec<Vec<u8>> = (0..5)
            .map(|i| format!("recover-{}", i).into_bytes())
            .collect();

        // Write 5 entries, then drop (simulated crash).
        {
            let mut log = RaftLog::open(&path).unwrap();
            for p in &payloads {
                log.append(p).unwrap();
            }
            // log is dropped here — file handle closed.
        }

        // Reopen and scan.
        let log2 = RaftLog::open(&path).unwrap();
        let entries = log2.scan_all().unwrap();
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry, &payloads[i]);
        }
    }

    #[test]
    fn corrupted_entry_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.log");

        {
            let mut log = RaftLog::open(&path).unwrap();
            log.append(b"good data").unwrap();
        }

        // Corrupt a payload byte (byte index 8 is the first payload byte).
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF; // flip all bits
            f.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_data().unwrap();
        }

        let log2 = RaftLog::open(&path).unwrap();
        let err = log2.scan_all().unwrap_err();
        assert!(
            matches!(err, RaftLogError::CrcMismatch { .. }),
            "expected CrcMismatch, got: {:?}",
            err
        );
    }

    #[test]
    fn aligned_buffer_pool_reuse() {
        let pool = AlignedBufferPool::new();

        let buf1 = pool.acquire(100);
        assert!(buf1.len() >= ALIGN);
        assert_eq!(buf1.as_ptr() as usize % ALIGN, 0, "not aligned");
        let ptr1 = buf1.as_ptr();
        pool.release(buf1);

        // Second acquire should reuse.
        let buf2 = pool.acquire(100);
        assert_eq!(buf2.as_ptr(), ptr1, "expected pool reuse");
        // Buffer should be zeroed.
        assert!(buf2.as_slice().iter().all(|&b| b == 0));
    }
}
