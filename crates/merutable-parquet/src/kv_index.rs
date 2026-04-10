//! `KvSparseIndex`: front-coded sparse "user_key → Parquet page location" index.
//!
//! # Why this exists
//!
//! Parquet's built-in `ColumnIndex` is general-purpose: it stores per-page
//! min/max statistics for *every* column, truncated at 64 bytes by default.
//! For an LSM hot tier — where data is sorted KV, point lookups dominate,
//! and composite primary keys routinely exceed 64 bytes — that's pure
//! overhead and lossy precision.
//!
//! `KvSparseIndex` is a domain-specific replacement: one entry per data page
//! on the `_merutable_ikey` column only, full keys (no truncation),
//! prefix-compressed because sorted internal keys share huge prefixes,
//! and binary-searchable via restart points (LevelDB / RocksDB index-block
//! style).
//!
//! Stored in the Parquet footer KV under `merutable.kv_index.v1`, sibling
//! to `merutable.bloom`.
//!
//! # Wire format (`merutable.kv_index.v1`)
//!
//! All multi-byte integers are little-endian.
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │ header (24 bytes)                    │
//! │   u8  version              (= 1)     │
//! │   u8  reserved[3]          (= 0)     │
//! │   u32 num_entries                    │
//! │   u32 restart_interval               │
//! │   u32 entries_size                   │
//! │   u32 num_restarts                   │
//! │   u32 reserved              (= 0)    │
//! ├──────────────────────────────────────┤
//! │ entries (entries_size bytes)         │
//! │   per entry:                         │
//! │     u16 shared_prefix_len            │
//! │     u16 suffix_len                   │
//! │     bytes suffix         (suffix_len)│
//! │     u64 page_offset                  │
//! │     u32 page_size                    │
//! │     u64 first_row_index              │
//! ├──────────────────────────────────────┤
//! │ restart_offsets (num_restarts × u32) │
//! │   absolute byte offsets into the     │
//! │   entries section, one per restart   │
//! └──────────────────────────────────────┘
//! ```
//!
//! At every restart point (every `restart_interval` entries), the entry is
//! written with `shared_prefix_len = 0`, so the suffix *is* the full key —
//! no decode state is needed to inspect a restart entry.
//!
//! # Search
//!
//! `find_page(target)` returns the page that contains the largest key ≤ target:
//!
//! 1. Binary-search the restart points by comparing `target` to each
//!    restart entry's full key. Find the largest restart `r` whose key ≤
//!    `target`.
//! 2. Linear-scan from restart `r` for at most `restart_interval` entries,
//!    rebuilding prefix-compressed keys against a running buffer, tracking
//!    the largest entry whose key ≤ `target`.
//! 3. Return that entry's `PageLocation`.
//!
//! Result: O(log(num_restarts) + restart_interval) comparisons, ~3-5 KiB
//! of footer KV bytes for a typical L0 4 MiB row group with 8 KiB pages.
//!
//! # Invariants
//!
//! - Entries are written in strict ascending key order. Build-time `assert!`
//!   panics on out-of-order input — sort before calling [`KvSparseIndex::build`].
//! - Restart interval ≥ 1. Build-time clamp.
//! - The first entry is always a restart (its shared_prefix_len is 0).
//!
//! # Encoding choices (and why)
//!
//! - **Fixed-width header fields, fixed-width page metadata.** Varint would
//!   shave ~4-8 bytes per entry but cost branches on the hot decode path.
//!   The compression we care about is on the *keys*, which is where prefix
//!   coding wins big; the per-entry overhead is small enough that fixed
//!   widths are simpler and faster.
//! - **u64 page_offset / u64 first_row_index.** Absolute byte offsets into
//!   the parquet file and absolute row indices, both safely u64 even for
//!   files measured in tens of GiB.
//! - **u32 page_size.** Page compressed size; capped well under 4 GiB by
//!   Parquet's own page size limits.

use bytes::Bytes;
use merutable_types::{MeruError, Result};

/// Format version stored in the header `version` byte.
pub const KV_INDEX_VERSION: u8 = 1;

/// Footer KV key under which the encoded index lives.
pub const KV_INDEX_FOOTER_KEY: &str = "merutable.kv_index.v1";

/// Default restart interval. Every Nth entry is a "restart" with full
/// (non-prefix-compressed) key, enabling binary search. 16 is a sweet
/// spot: log2(num_restarts) bsearch + ≤16 linear-scan steps inside a
/// restart group, with minimal restart-table overhead.
pub const DEFAULT_RESTART_INTERVAL: u32 = 16;

const HEADER_SIZE: usize = 24;
const ENTRY_FIXED_TAIL: usize = 8 + 4 + 8; // page_offset + page_size + first_row_index
const ENTRY_HEADER: usize = 2 + 2; // shared_prefix_len + suffix_len

/// Location of a Parquet data page within a file. The values come from
/// Parquet's own `OffsetIndex`/`PageLocation`, captured at write time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageLocation {
    /// Absolute byte offset of the data page within the Parquet file.
    pub page_offset: u64,
    /// Compressed size of the data page in bytes.
    pub page_size: u32,
    /// Row index of the first row in this page (within its row group's
    /// global row numbering).
    pub first_row_index: u64,
}

/// Build the on-wire bytes for a `KvSparseIndex` from a strictly-ascending
/// sequence of `(user_key, location)` pairs.
///
/// Panics in debug mode if `entries` is not sorted by `user_key`.
pub fn build(entries: &[(Vec<u8>, PageLocation)], restart_interval: u32) -> Bytes {
    let restart_interval = restart_interval.max(1);
    let num_entries = entries.len() as u32;

    // Compute number of restart points up front so we can pre-size the
    // restart table.
    let num_restarts = if num_entries == 0 {
        0
    } else {
        num_entries.div_ceil(restart_interval)
    };

    // Worst-case capacity: every entry full-key encoded.
    let mut entries_buf: Vec<u8> = Vec::with_capacity(
        entries
            .iter()
            .map(|(k, _)| ENTRY_HEADER + k.len() + ENTRY_FIXED_TAIL)
            .sum(),
    );
    let mut restart_offsets: Vec<u32> = Vec::with_capacity(num_restarts as usize);

    let mut prev_key: &[u8] = &[];

    for (i, (key, loc)) in entries.iter().enumerate() {
        debug_assert!(
            i == 0 || key.as_slice() > prev_key,
            "kv_index::build requires strictly ascending keys; entry {i} is out of order"
        );

        let is_restart = (i as u32).is_multiple_of(restart_interval);
        let shared = if is_restart {
            0
        } else {
            shared_prefix_len(prev_key, key)
        };
        let suffix = &key[shared..];

        if is_restart {
            restart_offsets.push(entries_buf.len() as u32);
        }

        // u16 shared, u16 suffix_len, suffix bytes
        entries_buf.extend_from_slice(&(shared as u16).to_le_bytes());
        entries_buf.extend_from_slice(&(suffix.len() as u16).to_le_bytes());
        entries_buf.extend_from_slice(suffix);
        // u64 page_offset, u32 page_size, u64 first_row_index
        entries_buf.extend_from_slice(&loc.page_offset.to_le_bytes());
        entries_buf.extend_from_slice(&loc.page_size.to_le_bytes());
        entries_buf.extend_from_slice(&loc.first_row_index.to_le_bytes());

        prev_key = key;
    }

    let entries_size = entries_buf.len() as u32;

    let total_size = HEADER_SIZE + entries_size as usize + 4 * restart_offsets.len();
    let mut out = Vec::with_capacity(total_size);

    // Header.
    out.push(KV_INDEX_VERSION);
    out.extend_from_slice(&[0u8, 0, 0]); // reserved
    out.extend_from_slice(&num_entries.to_le_bytes());
    out.extend_from_slice(&restart_interval.to_le_bytes());
    out.extend_from_slice(&entries_size.to_le_bytes());
    out.extend_from_slice(&(restart_offsets.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved
    debug_assert_eq!(out.len(), HEADER_SIZE);

    out.extend_from_slice(&entries_buf);
    for ro in &restart_offsets {
        out.extend_from_slice(&ro.to_le_bytes());
    }

    Bytes::from(out)
}

/// Length of the longest common prefix of `a` and `b`.
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Decoded, searchable view over a `KvSparseIndex` byte buffer.
///
/// Holding a `KvSparseIndex` keeps the underlying `Bytes` alive; lookups
/// allocate only the small per-call key buffer.
///
/// # Evolution
///
/// The footer key carries an explicit `v1` suffix and the wire format
/// starts with a `version` byte, so a future `merutable.kv_index.v2` can
/// coexist with v1 readers. The intended v2 evolution is *partitioned*
/// indexes: split the entries section into N restart-aligned shards, store
/// a tiny top-level shard directory in the footer, and load only the shard
/// covering a given probe — useful once cold-tier files routinely carry
/// indexes in the megabyte range. v1 is monolithic because at expected
/// L0/L1 sizes (a few hundred KiB at most) the savings don't justify the
/// extra indirection.
#[derive(Debug, Clone)]
pub struct KvSparseIndex {
    bytes: Bytes,
    num_entries: u32,
    restart_interval: u32,
    entries_offset: usize,
    entries_size: usize,
    restart_offsets: Vec<u32>,
}

impl KvSparseIndex {
    /// Parse and validate the index header. Does not eagerly decode entries.
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(MeruError::Corruption(format!(
                "kv_index: buffer too small ({} < {HEADER_SIZE})",
                bytes.len()
            )));
        }

        let version = bytes[0];
        if version != KV_INDEX_VERSION {
            return Err(MeruError::Corruption(format!(
                "kv_index: unsupported version {version} (expected {KV_INDEX_VERSION})"
            )));
        }

        let num_entries = u32_at(&bytes, 4);
        let restart_interval = u32_at(&bytes, 8);
        let entries_size = u32_at(&bytes, 12) as usize;
        let num_restarts = u32_at(&bytes, 16) as usize;

        let entries_offset = HEADER_SIZE;
        let restart_section_offset = entries_offset + entries_size;
        let expected_total = restart_section_offset + 4 * num_restarts;
        if bytes.len() < expected_total {
            return Err(MeruError::Corruption(format!(
                "kv_index: truncated buffer (have {}, need {expected_total})",
                bytes.len()
            )));
        }
        if restart_interval == 0 {
            return Err(MeruError::Corruption(
                "kv_index: restart_interval is 0".into(),
            ));
        }

        let mut restart_offsets = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            restart_offsets.push(u32_at(&bytes, restart_section_offset + 4 * i));
        }

        Ok(Self {
            bytes,
            num_entries,
            restart_interval,
            entries_offset,
            entries_size,
            restart_offsets,
        })
    }

    /// Number of (key → page) entries.
    pub fn len(&self) -> usize {
        self.num_entries as usize
    }

    /// Whether the index has zero entries.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Restart interval the index was built with.
    pub fn restart_interval(&self) -> u32 {
        self.restart_interval
    }

    /// On-wire byte size of the encoded index.
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.entries_size + 4 * self.restart_offsets.len()
    }

    /// Find the page that contains the largest key ≤ `target`.
    ///
    /// Returns `None` if the target is strictly less than every key in the
    /// index — i.e., the target precedes the file's smallest key, so the
    /// file cannot contain it.
    pub fn find_page(&self, target: &[u8]) -> Option<PageLocation> {
        if self.num_entries == 0 {
            return None;
        }

        // Phase 1: binary search restart points by full key. Goal: find
        // the largest restart `lo` whose key ≤ target.
        let restarts = &self.restart_offsets;
        let mut lo: i64 = -1; // sentinel: "no restart known to be ≤ target"
        let mut hi: i64 = restarts.len() as i64; // exclusive upper bound

        while hi - lo > 1 {
            let mid = ((lo + hi) / 2) as usize;
            let mid_key = self.read_restart_key(mid);
            match mid_key.cmp(target) {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => lo = mid as i64,
                std::cmp::Ordering::Greater => hi = mid as i64,
            }
        }

        // Phase 2: linear scan from the chosen restart (or from the start
        // if every restart key is > target).
        let scan_start_pos = if lo < 0 {
            // No restart ≤ target. Start scanning from the very first
            // entry; if even entry 0's key > target, no page contains it.
            0
        } else {
            restarts[lo as usize] as usize
        };

        let mut cursor = scan_start_pos;
        let mut prev_key: Vec<u8> = Vec::new();
        let mut best: Option<PageLocation> = None;

        // We may scan beyond `restart_interval` entries when lo == -1, but
        // in that case we still bail out at the next restart (whose key is
        // > target by construction).
        loop {
            if cursor >= self.entries_size {
                break;
            }
            let (entry_key, loc, next_cursor) = self.decode_entry_at(cursor, &prev_key);

            match entry_key.as_slice().cmp(target) {
                std::cmp::Ordering::Greater => break,
                _ => {
                    best = Some(loc);
                    prev_key = entry_key;
                    cursor = next_cursor;
                }
            }
        }

        best
    }

    /// Read the *full* key at restart point `idx`. By construction the
    /// restart entry stores the full key in its suffix (shared_prefix_len = 0).
    fn read_restart_key(&self, idx: usize) -> &[u8] {
        let pos = self.entries_offset + self.restart_offsets[idx] as usize;
        let suffix_len = u16_at(&self.bytes, pos + 2) as usize;
        let key_start = pos + ENTRY_HEADER;
        &self.bytes[key_start..key_start + suffix_len]
    }

    /// Decode an entry starting at `cursor` (offset into the entries
    /// section). Returns `(full_key, page_location, next_cursor)`.
    fn decode_entry_at(&self, cursor: usize, prev_key: &[u8]) -> (Vec<u8>, PageLocation, usize) {
        let abs = self.entries_offset + cursor;
        let shared = u16_at(&self.bytes, abs) as usize;
        let suffix_len = u16_at(&self.bytes, abs + 2) as usize;
        let suffix_start = abs + ENTRY_HEADER;
        let suffix = &self.bytes[suffix_start..suffix_start + suffix_len];

        let mut key = Vec::with_capacity(shared + suffix_len);
        key.extend_from_slice(&prev_key[..shared]);
        key.extend_from_slice(suffix);

        let tail = suffix_start + suffix_len;
        let page_offset = u64_at(&self.bytes, tail);
        let page_size = u32_at(&self.bytes, tail + 8);
        let first_row_index = u64_at(&self.bytes, tail + 12);

        let next_cursor = (tail + 20) - self.entries_offset;
        (
            key,
            PageLocation {
                page_offset,
                page_size,
                first_row_index,
            },
            next_cursor,
        )
    }

    /// Iterator over `(full_key, location)` for every entry in the index,
    /// in ascending key order. Allocates per-call key buffers; intended
    /// for tests and diagnostics, not the hot read path.
    pub fn iter(&self) -> KvSparseIndexIter<'_> {
        KvSparseIndexIter {
            index: self,
            cursor: 0,
            prev_key: Vec::new(),
            remaining: self.num_entries,
        }
    }
}

/// Iterator yielding all `(key, location)` pairs in the index.
pub struct KvSparseIndexIter<'a> {
    index: &'a KvSparseIndex,
    cursor: usize,
    prev_key: Vec<u8>,
    remaining: u32,
}

impl Iterator for KvSparseIndexIter<'_> {
    type Item = (Vec<u8>, PageLocation);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let (key, loc, next) = self.index.decode_entry_at(self.cursor, &self.prev_key);
        self.cursor = next;
        self.prev_key = key.clone();
        self.remaining -= 1;
        Some((key, loc))
    }
}

#[inline]
fn u16_at(buf: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes([buf[pos], buf[pos + 1]])
}

#[inline]
fn u32_at(buf: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
}

#[inline]
fn u64_at(buf: &[u8], pos: usize) -> u64 {
    u64::from_le_bytes([
        buf[pos],
        buf[pos + 1],
        buf[pos + 2],
        buf[pos + 3],
        buf[pos + 4],
        buf[pos + 5],
        buf[pos + 6],
        buf[pos + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn loc(offset: u64, size: u32, row: u64) -> PageLocation {
        PageLocation {
            page_offset: offset,
            page_size: size,
            first_row_index: row,
        }
    }

    /// Empty index encodes, decodes, and reports zero length.
    #[test]
    fn empty_index_round_trip() {
        let bytes = build(&[], DEFAULT_RESTART_INTERVAL);
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert_eq!(idx.find_page(b"anything"), None);
        assert_eq!(idx.iter().count(), 0);
    }

    /// Single-entry index always returns that entry for any key ≥ its key.
    #[test]
    fn single_entry_returns_for_anything_geq() {
        let entries = vec![(b"banana".to_vec(), loc(100, 8192, 0))];
        let bytes = build(&entries, DEFAULT_RESTART_INTERVAL);
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.find_page(b"apple"), None); // before first key
        assert_eq!(idx.find_page(b"banana"), Some(loc(100, 8192, 0)));
        assert_eq!(idx.find_page(b"cherry"), Some(loc(100, 8192, 0)));
    }

    /// Round-trip a small ordered set, exercising prefix compression
    /// across the bucket boundary and the iterator.
    #[test]
    fn small_ordered_round_trip() {
        let raw: Vec<(&[u8], PageLocation)> = vec![
            (b"k/0001/aaaa", loc(0, 8192, 0)),
            (b"k/0001/bbbb", loc(8192, 8192, 100)),
            (b"k/0001/cccc", loc(16384, 8192, 200)),
            (b"k/0002/aaaa", loc(24576, 8192, 300)),
            (b"k/0002/bbbb", loc(32768, 8192, 400)),
        ];
        let entries: Vec<(Vec<u8>, PageLocation)> =
            raw.iter().map(|(k, l)| (k.to_vec(), *l)).collect();

        let bytes = build(&entries, 2); // restart every 2 entries
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();

        assert_eq!(idx.len(), 5);
        assert_eq!(idx.restart_interval(), 2);

        // Iterator returns the same sequence we built from.
        let collected: Vec<(Vec<u8>, PageLocation)> = idx.iter().collect();
        assert_eq!(collected, entries);

        // Exact-key lookups.
        for (k, l) in &entries {
            assert_eq!(idx.find_page(k), Some(*l), "exact lookup failed for {k:?}");
        }

        // Predecessor lookups: a key just past entry i must return entry i.
        assert_eq!(
            idx.find_page(b"k/0001/aaab"),
            Some(loc(0, 8192, 0)),
            "predecessor of /aaab should be /aaaa"
        );
        assert_eq!(idx.find_page(b"k/0001/cccd"), Some(loc(16384, 8192, 200)));
        // Past the end → returns the final entry.
        assert_eq!(idx.find_page(b"k/9999/zzzz"), Some(loc(32768, 8192, 400)));
        // Strictly before all entries → None.
        assert_eq!(idx.find_page(b"k/0000/zzzz"), None);
    }

    /// Compare against a `BTreeMap` oracle on a 1024-entry randomized set.
    /// Every key in the input plus 1024 randomly chosen probes must agree
    /// with the BTreeMap answer.
    #[test]
    fn matches_btreemap_oracle() {
        // Deterministic pseudo-random keys with shared prefixes (mimicking
        // composite PKs that share a tenant/table prefix).
        let mut rng_state: u64 = 0xdeadbeefcafebabe;
        let mut next = || {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng_state
        };

        let mut oracle: BTreeMap<Vec<u8>, PageLocation> = BTreeMap::new();
        for i in 0..1024u64 {
            let r = next();
            let key = format!("tenant/0001/table/users/pk/{:016x}/{:016x}", i, r).into_bytes();
            oracle.insert(key, loc(i * 8192, 8192, i * 100));
        }

        let entries: Vec<(Vec<u8>, PageLocation)> =
            oracle.iter().map(|(k, v)| (k.clone(), *v)).collect();
        let bytes = build(&entries, DEFAULT_RESTART_INTERVAL);
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();

        // Every key in the oracle is found exactly.
        for (k, expected) in &oracle {
            let got = idx.find_page(k);
            assert_eq!(got, Some(*expected), "exact lookup failed for {k:?}");
        }

        // 1024 random probes: compare to BTreeMap::range upper-bound.
        for _ in 0..1024 {
            let r = next();
            let probe =
                format!("tenant/0001/table/users/pk/{:016x}/{:016x}", r % 2048, r).into_bytes();
            let oracle_answer: Option<PageLocation> =
                oracle.range(..=probe.clone()).next_back().map(|(_, v)| *v);
            let idx_answer = idx.find_page(&probe);
            assert_eq!(
                idx_answer, oracle_answer,
                "oracle disagreement for probe {probe:?}"
            );
        }
    }

    /// Prefix compression must actually compress sorted keys with shared
    /// prefixes. Worst-case (everything stored full) would be ~76 bytes per
    /// entry; we expect well under that.
    #[test]
    fn prefix_compression_meaningfully_shrinks_index() {
        let entries: Vec<(Vec<u8>, PageLocation)> = (0..512u64)
            .map(|i| {
                (
                    format!("tenant/0001/table/users/pk/{:032x}", i).into_bytes(),
                    loc(i * 8192, 8192, i * 100),
                )
            })
            .collect();

        let raw_key_bytes: usize = entries.iter().map(|(k, _)| k.len()).sum();
        let bytes = build(&entries, DEFAULT_RESTART_INTERVAL);
        let idx = KvSparseIndex::from_bytes(bytes.clone()).unwrap();

        let on_disk = idx.encoded_size();
        // Sanity: encoded matches Bytes length.
        assert_eq!(on_disk, bytes.len());

        // Without compression, each entry would carry its full key plus
        // 24 bytes of fixed metadata. Demand at least 2x reduction over
        // the raw-key budget alone.
        let raw_lower_bound = raw_key_bytes;
        assert!(
            on_disk * 2 < raw_lower_bound + 24 * entries.len(),
            "kv_index expected ≥2x compression vs raw keys; got {on_disk} bytes for {raw_key_bytes} raw key bytes ({} entries)",
            entries.len()
        );
    }

    /// A truncated buffer must be rejected, not silently misread.
    #[test]
    fn truncated_buffer_is_rejected() {
        let entries: Vec<(Vec<u8>, PageLocation)> = (0..32u64)
            .map(|i| (format!("k{i:04}").into_bytes(), loc(i, 1, i)))
            .collect();
        let bytes = build(&entries, DEFAULT_RESTART_INTERVAL);

        // Drop the last 8 bytes (chops the restart table).
        let truncated = bytes.slice(..bytes.len() - 8);
        let result = KvSparseIndex::from_bytes(truncated);
        assert!(result.is_err(), "truncated buffer should be rejected");

        // Drop everything but a tiny prefix.
        let tiny = bytes.slice(..4);
        let result = KvSparseIndex::from_bytes(tiny);
        assert!(result.is_err(), "tiny buffer should be rejected");
    }

    /// Wrong version byte is rejected.
    #[test]
    fn wrong_version_is_rejected() {
        let entries: Vec<(Vec<u8>, PageLocation)> = vec![(b"hello".to_vec(), loc(0, 1, 0))];
        let bytes = build(&entries, DEFAULT_RESTART_INTERVAL);
        let mut tampered = bytes.to_vec();
        tampered[0] = 99;
        let result = KvSparseIndex::from_bytes(Bytes::from(tampered));
        assert!(matches!(result, Err(MeruError::Corruption(_))));
    }

    /// Restart interval of 1 (every entry is a restart) is valid and
    /// gives correct answers — useful as a worst-case for binary search.
    #[test]
    fn restart_interval_of_one_works() {
        let entries: Vec<(Vec<u8>, PageLocation)> = (0..16u64)
            .map(|i| (format!("k{i:04}").into_bytes(), loc(i * 100, 50, i)))
            .collect();
        let bytes = build(&entries, 1);
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();
        for (k, l) in &entries {
            assert_eq!(idx.find_page(k), Some(*l));
        }
    }

    /// Helper used in proptest-style smoke: build, decode, iterate, search.
    #[test]
    fn iterator_yields_in_order_for_random_lengths() {
        // Mix short and long keys.
        let raw_keys: Vec<&[u8]> = vec![
            b"a",
            b"aa",
            b"aaa",
            b"aab",
            b"ab",
            b"abcdefghijklmnopqrstuvwxyz",
            b"abcdefghijklmnopqrstuvwxyz1",
            b"b",
            b"ba",
            b"baz",
        ];
        let entries: Vec<(Vec<u8>, PageLocation)> = raw_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.to_vec(), loc(i as u64 * 8, 8, i as u64)))
            .collect();
        let bytes = build(&entries, 4);
        let idx = KvSparseIndex::from_bytes(bytes).unwrap();
        let yielded: Vec<Vec<u8>> = idx.iter().map(|(k, _)| k).collect();
        assert_eq!(
            yielded,
            raw_keys.iter().map(|k| k.to_vec()).collect::<Vec<_>>()
        );
    }
}
