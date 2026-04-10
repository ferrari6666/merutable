# merutable

[![CI](https://github.com/merutable/merutable/actions/workflows/ci.yml/badge.svg)](https://github.com/merutable/merutable/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-stable-blue.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

An embeddable Rust HTAP database engine. One logical table backed by an LSM-tree where manifests are Apache Iceberg v3 snapshots and SSTables are Apache Parquet files. Row promotions between LSM levels use Iceberg v3 Deletion Vectors (Puffin format) instead of physical deletes, making every level of the tree live-readable by Spark, Trino, and DuckDB as a standard Iceberg table.

Named after the [Meru Parvatha](https://en.wikipedia.org/wiki/Mount_Meru) from Indian mythology.

## Why merutable

- **HTAP in one binary**: Transactional writes (put/delete/scan) with sub-millisecond memtable lookups, while the on-disk format is standard Iceberg+Parquet readable by any analytics engine.
- **No ETL pipeline**: Every flush and compaction produces a valid Iceberg snapshot. Analytical readers see consistent data without a separate ingestion step.
- **Deletion Vectors, not physical deletes**: Promoted rows are masked via Puffin-format roaring bitmaps. Source files remain readable throughout compaction.
- **SIMD-optimized bloom filter**: AVX2/NEON runtime-dispatched cache-line-aligned bloom filter for fast negative lookups on the read path.
- **Pluggable storage**: Local filesystem for development, S3 with LRU disk cache for production.

## Architecture

```
         put/delete/get/scan
                |
         [ MeruDB API ]
                |
         [ MeruEngine ]
           /    |    \
     [WAL]  [Memtable]  [VersionSet]
              |               |
         [FlushJob]    [IcebergCatalog]
              |               |
       [ParquetWriter]  [Manifest + DV]
              |               |
         [ObjectStore]   [Puffin files]
              |
       L0/ L1/ L2/ ... (Parquet files)
```

**Write path**: Sequence assign -> WAL append -> memtable insert -> flush when threshold crossed.

**Read path**: Memtable (active + immutable queue) -> L0 files (bloom + scan) -> L1..LN (bloom + binary search).

**Compaction**: Leveled compaction with Deletion Vector tracking. Fully compacted files are removed from the manifest; partially compacted files get a DV update.

## Crate map

| Crate | Responsibility |
|---|---|
| `merutable-types` | `InternalKey` encoding, `TableSchema`, `FieldValue`, `SeqNum`, `OpType`, `MeruError` |
| `merutable-wal` | 32 KiB block format WAL with CRC32, recovery, rotation |
| `merutable-memtable` | `crossbeam` skip-list memtable, `bumpalo` arena, rotation, flow control |
| `merutable-parquet` | Parquet SSTable writer/reader, `FastLocalBloom`, footer KV metadata |
| `merutable-iceberg` | Iceberg v3 file-system catalog, manifest, `VersionSet` (ArcSwap), `DeletionVector` (Puffin) |
| `merutable-store` | Pluggable object store: local FS, S3, LRU disk cache |
| `merutable-engine` | `FlushJob`, `CompactionJob`, `MergingIterator`, read/write paths |
| `merutable` | Public embedding API: `MeruDB`, `OpenOptions`, `ScanIterator` |

## Quick start

```rust
use merutable::{MeruDB, OpenOptions};
use merutable::merutable_types::schema::{TableSchema, ColumnDef, ColumnType};
use merutable::merutable_types::value::FieldValue;

#[tokio::main]
async fn main() {
    let schema = TableSchema {
        table_name: "events".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Int64, nullable: false },
            ColumnDef { name: "payload".into(), col_type: ColumnType::ByteArray, nullable: true },
        ],
        primary_key: vec![0],
    };

    let db = MeruDB::open(OpenOptions::new(schema)).await.unwrap();
    db.put(&[FieldValue::Int64(1)], &[FieldValue::Int64(1), FieldValue::Null]).await.unwrap();
    let row = db.get(&[FieldValue::Int64(1)]).unwrap();
    println!("{row:?}");
}
```

## License

Apache-2.0
