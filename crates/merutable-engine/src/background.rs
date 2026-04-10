//! Background Tokio task pool for flush and compaction workers.
//!
//! `BackgroundWorkers` spawns long-lived tasks that poll for work:
//! - **Flush worker**: checks for immutable memtables and flushes them.
//! - **Compaction worker**: checks version for compaction triggers and runs jobs.
//!
//! Both workers use `tokio::sync::Notify` to wake up when new work arrives,
//! rather than polling on a timer.

use std::sync::Arc;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::engine::MeruEngine;

/// Handles for background flush and compaction workers.
pub struct BackgroundWorkers {
    shutdown: Arc<Notify>,
    flush_handles: Vec<tokio::task::JoinHandle<()>>,
    compaction_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BackgroundWorkers {
    /// Spawn background workers. Call after `MeruEngine::open()`.
    pub fn spawn(engine: Arc<MeruEngine>) -> Self {
        let shutdown = Arc::new(Notify::new());
        let flush_parallelism = engine.config.flush_parallelism;
        let compaction_parallelism = engine.config.compaction_parallelism;

        let mut flush_handles = Vec::new();
        for i in 0..flush_parallelism {
            let eng = engine.clone();
            let shut = shutdown.clone();
            let handle = tokio::spawn(async move {
                flush_worker(eng, shut, i).await;
            });
            flush_handles.push(handle);
        }

        let mut compaction_handles = Vec::new();
        for i in 0..compaction_parallelism {
            let eng = engine.clone();
            let shut = shutdown.clone();
            let handle = tokio::spawn(async move {
                compaction_worker(eng, shut, i).await;
            });
            compaction_handles.push(handle);
        }

        Self {
            shutdown,
            flush_handles,
            compaction_handles,
        }
    }

    /// Signal all workers to shut down and wait for them to finish.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        for h in self.flush_handles {
            let _ = h.await;
        }
        for h in self.compaction_handles {
            let _ = h.await;
        }
    }
}

async fn flush_worker(engine: Arc<MeruEngine>, shutdown: Arc<Notify>, id: usize) {
    debug!(worker = id, "flush worker started");
    loop {
        // Wait for either: immutable memtable available, or shutdown.
        tokio::select! {
            _ = shutdown.notified() => {
                info!(worker = id, "flush worker shutting down");
                break;
            }
            _ = engine.memtable.flush_complete.notified() => {}
        }

        // Drain all pending flushes.
        while engine.memtable.oldest_immutable().is_some() {
            match crate::flush::run_flush(&engine).await {
                Ok(_) => debug!(worker = id, "flush completed"),
                Err(e) => {
                    warn!(worker = id, error = %e, "flush failed, will retry");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn compaction_worker(engine: Arc<MeruEngine>, shutdown: Arc<Notify>, id: usize) {
    debug!(worker = id, "compaction worker started");
    loop {
        // Wait for a notification or periodic check.
        tokio::select! {
            _ = shutdown.notified() => {
                info!(worker = id, "compaction worker shutting down");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }

        // Check if compaction is needed.
        let version = engine.version_set.current();
        let l0_count = version.l0_file_count();
        if l0_count >= engine.config.l0_compaction_trigger {
            match crate::compaction::job::run_compaction(&engine).await {
                Ok(_) => debug!(worker = id, "compaction completed"),
                Err(e) => {
                    warn!(worker = id, error = %e, "compaction failed, will retry");
                }
            }
        }
    }
}
