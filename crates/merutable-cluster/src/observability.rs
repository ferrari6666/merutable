//! Cluster-level observability: metric names and tracing helpers.

// -- Raft group metrics --
pub const RAFT_PROPOSALS_TOTAL: &str = "cluster.raft.proposals_total";
pub const RAFT_COMMIT_LATENCY_US: &str = "cluster.raft.commit_latency_us";
pub const RAFT_LOG_WRITE_LATENCY_US: &str = "cluster.raft.log_write_latency_us";
pub const RAFT_ELECTIONS_TOTAL: &str = "cluster.raft.elections_total";
pub const RAFT_LEADER_TENURE_SECS: &str = "cluster.raft.leader_tenure_secs";

// -- Follower metrics --
pub const FOLLOWER_APPLY_LAG_ENTRIES: &str = "cluster.follower.apply_lag_entries";
pub const FOLLOWER_APPLY_LAG_MS: &str = "cluster.follower.apply_lag_ms";
pub const FOLLOWER_APPLY_ERRORS_TOTAL: &str = "cluster.follower.apply_errors_total";

// -- PeerBus metrics --
pub const BUS_BATCH_SIZE: &str = "cluster.bus.batch_size";
pub const BUS_FLUSH_COUNT: &str = "cluster.bus.flush_count";

// -- Ring metrics --
pub const RING_VERSION: &str = "cluster.ring.version";
pub const RING_TABLES_PER_NODE: &str = "cluster.ring.tables_per_node";

// -- Rebalance metrics --
pub const REBALANCE_IN_PROGRESS: &str = "cluster.rebalance.in_progress";

// -- Proxy metrics (GAP-17) --
pub const PROXY_ATTEMPTS_TOTAL: &str = "cluster.proxy.attempts_total";
pub const PROXY_LATENCY_US: &str = "cluster.proxy.latency_us";
pub const NOT_LEADER_RETRIES_TOTAL: &str = "cluster.client.not_leader_retries_total";

// -- Node metrics --
pub const NODE_TABLE_COUNT: &str = "cluster.node.table_count";
pub const NODE_OPEN_RAFT_GROUPS: &str = "cluster.node.open_raft_groups";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify all metric name constants are distinct (catches copy-paste typos).
    #[test]
    fn metrics_constants_are_unique() {
        let all = [
            RAFT_PROPOSALS_TOTAL,
            RAFT_COMMIT_LATENCY_US,
            RAFT_LOG_WRITE_LATENCY_US,
            RAFT_ELECTIONS_TOTAL,
            RAFT_LEADER_TENURE_SECS,
            FOLLOWER_APPLY_LAG_ENTRIES,
            FOLLOWER_APPLY_LAG_MS,
            FOLLOWER_APPLY_ERRORS_TOTAL,
            BUS_BATCH_SIZE,
            BUS_FLUSH_COUNT,
            RING_VERSION,
            RING_TABLES_PER_NODE,
            REBALANCE_IN_PROGRESS,
            PROXY_ATTEMPTS_TOTAL,
            PROXY_LATENCY_US,
            NOT_LEADER_RETRIES_TOTAL,
            NODE_TABLE_COUNT,
            NODE_OPEN_RAFT_GROUPS,
        ];
        let mut set = std::collections::HashSet::new();
        for name in &all {
            assert!(
                set.insert(*name),
                "duplicate metric constant: {}",
                name
            );
        }
        assert_eq!(set.len(), all.len());
    }
}
