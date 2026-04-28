// P1.3: ConsistentHashRing, PerAzRings, replicas_for_table(), leader_for_table().

use std::collections::HashMap;

use crate::config::{NodeId, NodeIdentity};

// ---------------------------------------------------------------------------
// ConsistentHashRing
// ---------------------------------------------------------------------------

/// A consistent hash ring built from virtual nodes (vnodes).
///
/// Each physical node is mapped to `vnodes_per_node` positions on the ring.
/// Lookups use binary search on the sorted position list with wrap-around.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Sorted list of (position, node_id) pairs.
    vnodes: Vec<(u64, NodeId)>,
}

impl ConsistentHashRing {
    /// Build a ring from the given physical nodes.
    ///
    /// Each node gets `vnodes_per_node` virtual positions. The position of
    /// vnode `i` for `node_id` is `xxh3_64("{node_id}:{i}")`.
    pub fn build(nodes: &[NodeId], vnodes_per_node: usize) -> Self {
        let mut vnodes: Vec<(u64, NodeId)> =
            Vec::with_capacity(nodes.len() * vnodes_per_node);

        for &node_id in nodes {
            for i in 0..vnodes_per_node {
                let key = format!("{}:{}", node_id, i);
                let pos = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
                vnodes.push((pos, node_id));
            }
        }

        vnodes.sort_unstable_by_key(|&(pos, _)| pos);

        Self { vnodes }
    }

    /// Return the node responsible for the given hash.
    ///
    /// Finds the first vnode whose position is >= `hash`. If no such vnode
    /// exists (hash is past the last position), wraps around to index 0.
    ///
    /// # Panics
    ///
    /// Panics if the ring is empty.
    pub fn node_at(&self, hash: u64) -> NodeId {
        assert!(!self.vnodes.is_empty(), "node_at called on empty ring");

        let idx = match self.vnodes.binary_search_by_key(&hash, |&(pos, _)| pos) {
            Ok(i) => i,
            Err(i) => {
                if i == self.vnodes.len() {
                    0 // wrap around
                } else {
                    i
                }
            }
        };

        self.vnodes[idx].1
    }

    /// Returns `true` if the ring has no vnodes.
    pub fn is_empty(&self) -> bool {
        self.vnodes.is_empty()
    }
}

// ---------------------------------------------------------------------------
// PerAzRings
// ---------------------------------------------------------------------------

/// Three consistent hash rings — one per availability zone.
///
/// `replicas_for_table` returns one node from each AZ. The first node in the
/// returned list is the preferred Raft leader; which AZ supplies the leader
/// rotates by `hash % 3`.
#[derive(Debug, Clone)]
pub struct PerAzRings {
    /// Sorted AZ names, e.g. `["AZ-1", "AZ-2", "AZ-3"]`.
    az_names: Vec<String>,
    /// One ring per AZ.
    az_rings: HashMap<String, ConsistentHashRing>,
    /// Manual placement overrides checked before the hash path.
    overrides: HashMap<String, Vec<NodeId>>,
    /// Vnodes per physical node — remembered so `rebuild_az` can reuse it.
    vnodes_per_node: usize,
}

impl PerAzRings {
    /// Create a new set of per-AZ rings from a list of node identities.
    ///
    /// Nodes are grouped by their `az` field. AZ names are sorted.
    pub fn new(nodes: &[NodeIdentity], vnodes_per_node: usize) -> Self {
        // Group node IDs by AZ.
        let mut az_nodes: HashMap<String, Vec<NodeId>> = HashMap::new();
        for n in nodes {
            az_nodes
                .entry(n.az.clone())
                .or_default()
                .push(n.node_id);
        }

        let mut az_names: Vec<String> = az_nodes.keys().cloned().collect();
        az_names.sort();

        let az_rings: HashMap<String, ConsistentHashRing> = az_nodes
            .into_iter()
            .map(|(az, node_ids)| {
                let ring = ConsistentHashRing::build(&node_ids, vnodes_per_node);
                (az, ring)
            })
            .collect();

        Self {
            az_names,
            az_rings,
            overrides: HashMap::new(),
            vnodes_per_node,
        }
    }

    /// Return the replica set for `table` — one node per AZ.
    ///
    /// If an override is set for this table it is returned directly.
    /// Otherwise the table name is hashed and each AZ's ring is queried.
    /// The resulting list is rotated so that the AZ at index `hash % len`
    /// supplies the preferred leader (first element).
    pub fn replicas_for_table(&self, table: &str) -> Vec<NodeId> {
        if let Some(nodes) = self.overrides.get(table) {
            return nodes.clone();
        }

        let hash = xxhash_rust::xxh3::xxh3_64(table.as_bytes());

        // Filter out AZs whose rings are empty (e.g. no nodes registered yet)
        // to avoid panicking in `node_at`.
        let mut nodes: Vec<NodeId> = self
            .az_names
            .iter()
            .filter(|az| !self.az_rings[az.as_str()].is_empty())
            .map(|az| self.az_rings[az].node_at(hash))
            .collect();

        if nodes.is_empty() {
            return nodes;
        }

        // Use u64 arithmetic before truncating to usize to avoid data loss
        // on 32-bit platforms where `hash as usize` would silently discard
        // the upper 32 bits.
        let rotation = (hash % nodes.len() as u64) as usize;
        nodes.rotate_left(rotation);
        nodes
    }

    /// Return the preferred Raft leader for `table`.
    ///
    /// This is simply the first element of `replicas_for_table`.
    /// Returns 0 if no AZ has any nodes (bootstrap / empty cluster).
    pub fn leader_for_table(&self, table: &str) -> NodeId {
        self.replicas_for_table(table).first().copied().unwrap_or(0)
    }

    /// Rebuild the ring for a single AZ with a new set of nodes.
    ///
    /// Used when a node joins or leaves. Only the affected AZ is rebuilt;
    /// the other two rings remain untouched.
    pub fn rebuild_az(&mut self, az: &str, nodes: &[NodeId]) {
        let ring = ConsistentHashRing::build(nodes, self.vnodes_per_node);
        self.az_rings.insert(az.to_string(), ring);
    }

    /// Set a manual placement override for `table`.
    pub fn set_override(&mut self, table: String, nodes: Vec<NodeId>) {
        self.overrides.insert(table, nodes);
    }

    /// Remove any manual placement override for `table`.
    pub fn remove_override(&mut self, table: &str) {
        self.overrides.remove(table);
    }

    /// Return the sorted list of AZ names.
    pub fn all_az_names(&self) -> &[String] {
        &self.az_names
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a standard 3-AZ cluster with `per_az` nodes in each AZ.
    fn make_nodes(per_az: usize) -> Vec<NodeIdentity> {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut nodes = Vec::new();
        for (az_idx, az) in azs.iter().enumerate() {
            for i in 0..per_az {
                let node_id = (az_idx * 100 + i + 1) as NodeId;
                nodes.push(NodeIdentity {
                    node_id,
                    address: format!("10.0.{}.{}:9000", az_idx + 1, i + 1),
                    az: az.to_string(),
                });
            }
        }
        nodes
    }

    /// Helper: generate `n` table names.
    fn table_names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("table_{}", i)).collect()
    }

    // -----------------------------------------------------------------------
    // ring_distributes_evenly
    // -----------------------------------------------------------------------
    #[test]
    fn ring_distributes_evenly() {
        let nodes = make_nodes(2); // 2 per AZ = 6 total
        let rings = PerAzRings::new(&nodes, 128);
        let tables = table_names(1000);

        // Count how many tables each node is a replica for (any position).
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for t in &tables {
            for nid in rings.replicas_for_table(t) {
                *counts.entry(nid).or_default() += 1;
            }
        }

        // Each AZ has 2 nodes. Each table assigns one node per AZ, so 1000
        // assignments per AZ. Ideal per node = 500. Allow 20% deviation.
        // We use 128 vnodes per node to reach the desired balance with only
        // 2 physical nodes per ring.
        let min_count = *counts.values().min().unwrap();
        let max_count = *counts.values().max().unwrap();
        let ratio = max_count as f64 / min_count as f64;
        assert!(
            ratio <= 1.20,
            "max/min ratio {:.3} exceeds 1.20 (max={}, min={})",
            ratio,
            max_count,
            min_count,
        );
    }

    // -----------------------------------------------------------------------
    // node_add_shifts_bounded
    // -----------------------------------------------------------------------
    #[test]
    fn node_add_shifts_bounded() {
        // Start with 2 nodes in AZ-1.
        let az1_initial: Vec<NodeId> = vec![1, 2];
        let ring_before = ConsistentHashRing::build(&az1_initial, 32);

        // Add a third node.
        let az1_after: Vec<NodeId> = vec![1, 2, 3];
        let ring_after = ConsistentHashRing::build(&az1_after, 32);

        let tables = table_names(1000);
        let mut shifted = 0usize;
        for t in &tables {
            let hash = xxhash_rust::xxh3::xxh3_64(t.as_bytes());
            if ring_before.node_at(hash) != ring_after.node_at(hash) {
                shifted += 1;
            }
        }

        // Ideal shift = 1/3 of keys. Allow up to 50%.
        let pct = shifted as f64 / tables.len() as f64;
        assert!(
            pct < 0.50,
            "{}% of tables shifted (> 50%); shifted={}",
            (pct * 100.0) as u32,
            shifted,
        );
    }

    // -----------------------------------------------------------------------
    // node_remove_shifts_bounded
    // -----------------------------------------------------------------------
    #[test]
    fn node_remove_shifts_bounded() {
        // Start with 3 nodes in AZ-1.
        let az1_initial: Vec<NodeId> = vec![1, 2, 3];
        let ring_before = ConsistentHashRing::build(&az1_initial, 32);

        // Remove node 3.
        let az1_after: Vec<NodeId> = vec![1, 2];
        let ring_after = ConsistentHashRing::build(&az1_after, 32);

        let tables = table_names(1000);
        let mut shifted = 0usize;
        for t in &tables {
            let hash = xxhash_rust::xxh3::xxh3_64(t.as_bytes());
            let before = ring_before.node_at(hash);
            let after = ring_after.node_at(hash);
            if before != after {
                shifted += 1;
                // Tables that moved must now be on one of the remaining nodes.
                assert!(
                    after == 1 || after == 2,
                    "shifted table landed on unexpected node {}",
                    after,
                );
            }
        }

        // Only the ~1/3 of keys previously on node 3 should move.
        let pct = shifted as f64 / tables.len() as f64;
        assert!(
            pct < 0.50,
            "{}% of tables shifted (> 50%); shifted={}",
            (pct * 100.0) as u32,
            shifted,
        );
    }

    // -----------------------------------------------------------------------
    // az_rotation_spreads_leaders
    // -----------------------------------------------------------------------
    #[test]
    fn az_rotation_spreads_leaders() {
        let nodes = make_nodes(2);
        let rings = PerAzRings::new(&nodes, 32);
        let tables = table_names(1000);

        // Collect the AZ of the preferred leader for each table.
        // Build a reverse map: node_id -> AZ.
        let node_to_az: HashMap<NodeId, String> = nodes
            .iter()
            .map(|n| (n.node_id, n.az.clone()))
            .collect();

        let mut az_leader_counts: HashMap<&str, usize> = HashMap::new();
        for t in &tables {
            let leader = rings.leader_for_table(t);
            let az = node_to_az[&leader].as_str();
            *az_leader_counts.entry(az).or_default() += 1;
        }

        // Each AZ should be preferred leader ~1/3 of the time.
        for (az, &count) in &az_leader_counts {
            let pct = count as f64 / tables.len() as f64;
            assert!(
                (0.25..=0.42).contains(&pct),
                "AZ {} has {:.1}% of leaders (expected 25-42%)",
                az,
                pct * 100.0,
            );
        }
    }

    // -----------------------------------------------------------------------
    // override_takes_precedence
    // -----------------------------------------------------------------------
    #[test]
    fn override_takes_precedence() {
        let nodes = make_nodes(2);
        let mut rings = PerAzRings::new(&nodes, 32);

        let manual = vec![999, 888, 777];
        rings.set_override("hot_table".to_string(), manual.clone());

        assert_eq!(rings.replicas_for_table("hot_table"), manual);
        assert_eq!(rings.leader_for_table("hot_table"), 999);

        // Remove override — should revert to hash-based placement.
        rings.remove_override("hot_table");
        let replicas = rings.replicas_for_table("hot_table");
        assert_ne!(replicas, manual);
        assert_eq!(replicas.len(), 3);
    }

    // -----------------------------------------------------------------------
    // deterministic
    // -----------------------------------------------------------------------
    #[test]
    fn deterministic() {
        let nodes = make_nodes(2);
        let rings = PerAzRings::new(&nodes, 32);

        let first = rings.replicas_for_table("my_table");
        for _ in 0..1000 {
            assert_eq!(
                rings.replicas_for_table("my_table"),
                first,
                "replicas_for_table is not deterministic",
            );
        }
    }

    // -----------------------------------------------------------------------
    // leader_for_table_matches_first_replica
    // -----------------------------------------------------------------------
    #[test]
    fn leader_for_table_matches_first_replica() {
        let nodes = make_nodes(2);
        let rings = PerAzRings::new(&nodes, 32);

        let tables = table_names(1000);
        for t in &tables {
            let replicas = rings.replicas_for_table(t);
            let leader = rings.leader_for_table(t);
            assert_eq!(
                leader, replicas[0],
                "leader_for_table({}) != replicas_for_table()[0]",
                t,
            );
        }
    }

    // -----------------------------------------------------------------------
    // rebuild_az_updates_ring
    // -----------------------------------------------------------------------
    #[test]
    fn rebuild_az_updates_ring() {
        let nodes = make_nodes(2); // AZ-1 has nodes 1, 2
        let mut rings = PerAzRings::new(&nodes, 32);
        let tables = table_names(1000);

        // Record AZ-1 assignments before rebuild.
        let before: Vec<NodeId> = tables
            .iter()
            .map(|t| {
                let hash = xxhash_rust::xxh3::xxh3_64(t.as_bytes());
                rings.az_rings["AZ-1"].node_at(hash)
            })
            .collect();

        // Rebuild AZ-1 with entirely new nodes.
        rings.rebuild_az("AZ-1", &[50, 51]);

        let after: Vec<NodeId> = tables
            .iter()
            .map(|t| {
                let hash = xxhash_rust::xxh3::xxh3_64(t.as_bytes());
                rings.az_rings["AZ-1"].node_at(hash)
            })
            .collect();

        // Every table should now be assigned to one of the new nodes.
        for &nid in &after {
            assert!(
                nid == 50 || nid == 51,
                "after rebuild, unexpected node {} in AZ-1",
                nid,
            );
        }

        // At least some tables should have changed assignment (all will,
        // because old nodes 1/2 are gone).
        let changed = before
            .iter()
            .zip(after.iter())
            .filter(|(a, b)| a != b)
            .count();
        assert!(
            changed > 0,
            "rebuild_az had no effect on AZ-1 assignments",
        );
    }
}
