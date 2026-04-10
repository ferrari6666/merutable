//! Arena allocator backed by `bumpalo::Bump`.
//!
//! Used for arena-allocating raw bytes within a memtable's lifetime.
//! The arena is tied to one `Memtable` instance and is dropped when
//! the memtable is flushed and released.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Thread-safe arena allocator. `bumpalo::Bump` itself is not `Sync`, so we
/// wrap it in a `Mutex` for the (rare) concurrent alloc case. The hot path
/// (skip list inserts) does not use the arena directly — values are stored
/// as `bytes::Bytes` (reference-counted). The arena is here for future use
/// (e.g., arena-backed key storage to reduce allocator pressure).
pub struct Arena {
    inner: std::sync::Mutex<bumpalo::Bump>,
    allocated: AtomicUsize,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(bumpalo::Bump::new()),
            allocated: AtomicUsize::new(0),
        }
    }

    /// Allocate `n` bytes. Returns a mutable slice backed by the arena.
    /// The slice is valid for the lifetime of the `Arena`.
    #[allow(clippy::mut_from_ref)]
    pub fn alloc_bytes(&self, n: usize) -> &mut [u8] {
        let bump = self.inner.lock().unwrap();
        self.allocated.fetch_add(n, Ordering::Relaxed);
        // SAFETY: We extend the lifetime to 'static here, but the caller
        // must not outlive the Arena. This is safe because Arena holds
        // the Bump and is the sole owner of the allocated memory.
        // In practice, callers copy data into Bytes immediately.
        let slice = bump.alloc_slice_fill_default::<u8>(n);
        // SAFETY: The Bump is heap-allocated and pinned for the Arena's lifetime.
        unsafe { &mut *(slice as *mut [u8]) }
    }

    /// Total bytes allocated through this arena.
    pub fn allocated_bytes(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}
