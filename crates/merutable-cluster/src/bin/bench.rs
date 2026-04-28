// merutable-bench — single-table write benchmark for the cluster client.
//
// Usage:
//   merutable-bench <target> <table> <rows> <concurrency> <payload_size> [seed1 seed2 ...]
//
// <target> is the first seed: "<node_id>:<az>:host:port", e.g.
//   1:AZ-1:merutable-0.merutable-hl.merutable.svc.cluster.local:9100
// Additional seeds are appended positionally.
//
// Emits a single-line JSON summary on stdout:
//   {"count":N,"total_wall_ms":M,"qps":X,"p50":..,"p95":..,"p99":..,"avg":..,"min":..,"max":..}

use std::sync::Arc;
use std::time::Instant;

use merutable_cluster::config::NodeIdentity;
use merutable_cluster::rpc::client::MeruClient;
use merutable::schema::{ColumnDef, ColumnType, TableSchema};
use merutable::value::{FieldValue, Row};

fn print_usage() {
    eprintln!(
        "Usage: merutable-bench <seed1> <table> <rows> <concurrency> <payload_size> [<seed2> ...]"
    );
    eprintln!("  seed format: <node_id>:<az>:<host>:<port>");
}

fn parse_seed(spec: &str) -> Option<NodeIdentity> {
    let mut parts = spec.splitn(3, ':');
    let node_id: u64 = parts.next()?.parse().ok()?;
    let az = parts.next()?.to_string();
    let address = parts.next()?.to_string();
    if az.is_empty() || address.is_empty() {
        return None;
    }
    Some(NodeIdentity { node_id, address, az })
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 5 {
        print_usage();
        std::process::exit(1);
    }

    let seed1 = &args[0];
    let table = args[1].clone();
    let rows: u64 = args[2].parse().expect("rows must be u64");
    let concurrency: usize = args[3].parse().expect("concurrency must be usize");
    let payload_size: usize = args[4].parse().expect("payload_size must be usize");
    let extra_seeds: Vec<String> = args[5..].to_vec();

    let mut nodes = vec![parse_seed(seed1).expect("invalid seed1")];
    for s in &extra_seeds {
        nodes.push(parse_seed(s).expect("invalid seed"));
    }
    eprintln!(
        "merutable-bench: table={} rows={} concurrency={} payload_size={} seeds={}",
        table, rows, concurrency, payload_size, nodes.len()
    );

    let client = Arc::new(
        MeruClient::connect(nodes.clone())
            .await
            .expect("connect failed"),
    );

    // Ensure table exists. Ignore AlreadyExists-style errors.
    let schema = TableSchema {
        table_name: table.clone(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "payload".into(),
                col_type: ColumnType::ByteArray,
                nullable: false,
                ..Default::default()
            },
        ],
        primary_key: vec![0],
        ..Default::default()
    };

    // create_table is a metadata op and the client's ring routing may
    // target a non-metadata-leader. Retry by rebuilding the client with
    // each seed as the primary until one succeeds or AlreadyExists.
    let mut created = false;
    for seed_idx in 0..nodes.len() {
        let mut rotated = nodes.clone();
        rotated.rotate_left(seed_idx);
        let probe = match MeruClient::connect(rotated).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("merutable-bench: probe connect failed: {}", e);
                continue;
            }
        };
        match probe.create_table(&table, schema.clone()).await {
            Ok(()) => {
                eprintln!(
                    "merutable-bench: created table {} via seed rotation {}",
                    table, seed_idx
                );
                created = true;
                break;
            }
            Err(e) => {
                let s = format!("{}", e);
                if s.contains("AlreadyExists") || s.contains("already exists") {
                    eprintln!("merutable-bench: table {} already exists", table);
                    created = true;
                    break;
                }
                eprintln!("merutable-bench: create_table via seed {} failed: {}", seed_idx, s);
            }
        }
    }
    if !created {
        eprintln!("merutable-bench: WARNING: could not create table — proceeding anyway");
    }

    let payload = vec![0x42u8; payload_size];
    let payload = bytes::Bytes::from(payload);

    // Warmup: 1000 puts serial-ish (still via concurrency pool) to prime caches.
    let warmup: u64 = 1000.min(rows);
    eprintln!("merutable-bench: warmup {} rows...", warmup);
    let warmup_sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut warm_handles = Vec::with_capacity(warmup as usize);
    for i in 0..warmup {
        let sem = warmup_sem.clone();
        let client = client.clone();
        let table = table.clone();
        let payload = payload.clone();
        warm_handles.push(tokio::spawn(async move {
            let _p = sem.acquire_owned().await.unwrap();
            let row = Row::new(vec![
                Some(FieldValue::Int64(-(i as i64) - 1)), // negative ids for warmup
                Some(FieldValue::Bytes(payload)),
            ]);
            let _ = client.put(&table, row).await;
        }));
    }
    for h in warm_handles {
        let _ = h.await;
    }

    // Measured phase.
    eprintln!("merutable-bench: measuring {} rows at concurrency {}...", rows, concurrency);
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Option<u128>>();
    let start = Instant::now();
    let mut handles = Vec::with_capacity(rows as usize);
    for i in 0..rows {
        let sem = sem.clone();
        let client = client.clone();
        let table = table.clone();
        let payload = payload.clone();
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire_owned().await.unwrap();
            let row = Row::new(vec![
                Some(FieldValue::Int64(i as i64)),
                Some(FieldValue::Bytes(payload)),
            ]);
            let t0 = Instant::now();
            let res = client.put(&table, row).await;
            let elapsed = t0.elapsed().as_micros();
            let _ = tx.send(res.ok().map(|_| elapsed));
        }));
    }
    drop(tx);

    let mut latencies: Vec<u128> = Vec::with_capacity(rows as usize);
    let mut errors = 0u64;
    while let Some(entry) = rx.recv().await {
        match entry {
            Some(us) => latencies.push(us),
            None => errors += 1,
        }
    }
    for h in handles {
        let _ = h.await;
    }
    let total_wall = start.elapsed();
    let total_wall_ms = total_wall.as_millis();

    latencies.sort_unstable();
    let count = latencies.len() as u64;
    let qps = if total_wall.as_secs_f64() > 0.0 {
        count as f64 / total_wall.as_secs_f64()
    } else {
        0.0
    };
    let sum: u128 = latencies.iter().sum();
    let avg_us = if count == 0 { 0 } else { sum / count as u128 };
    let min_us = latencies.first().copied().unwrap_or(0);
    let max_us = latencies.last().copied().unwrap_or(0);
    let p50 = percentile(&latencies, 0.50);
    let p95 = percentile(&latencies, 0.95);
    let p99 = percentile(&latencies, 0.99);

    let summary = serde_json::json!({
        "count": count,
        "errors": errors,
        "total_wall_ms": total_wall_ms,
        "qps": qps,
        "concurrency": concurrency,
        "payload_size": payload_size,
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
        "avg_us": avg_us,
        "min_us": min_us,
        "max_us": max_us,
    });
    println!("{}", summary);
}
