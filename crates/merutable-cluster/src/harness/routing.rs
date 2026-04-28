// P1.6: Write routing — ring lookup, leader forwarding, internal proxy.

use std::sync::Arc;

use crate::config::NodeId;
use crate::metadata::cache::LocalMetadataCache;

/// Route a table operation to the correct node using the consistent hash ring.
pub struct Router {
    this_node: NodeId,
    cache: Arc<LocalMetadataCache>,
}

impl Router {
    pub fn new(this_node: NodeId, cache: Arc<LocalMetadataCache>) -> Self {
        Self { this_node, cache }
    }

    /// Which nodes are replicas for this table?
    pub fn replicas_for(&self, table: &str) -> Vec<NodeId> {
        self.cache.rings().replicas_for_table(table)
    }

    /// Who is the preferred leader for this table?
    pub fn preferred_leader(&self, table: &str) -> NodeId {
        self.cache.rings().leader_for_table(table)
    }

    /// Am I a replica for this table?
    pub fn is_my_table(&self, table: &str) -> bool {
        self.replicas_for(table).contains(&self.this_node)
    }

    /// Am I the preferred leader for this table?
    pub fn am_i_preferred_leader(&self, table: &str) -> bool {
        self.preferred_leader(table) == self.this_node
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeIdentity;
    use crate::metadata::cache::LocalMetadataCache;
    use crate::ring::per_az::PerAzRings;
    use std::collections::HashMap;

    /// Build a 6-node cluster (2 per AZ) and return (nodes, rings).
    fn make_six_node_rings() -> (Vec<NodeIdentity>, PerAzRings) {
        let azs = ["AZ-1", "AZ-2", "AZ-3"];
        let mut nodes = Vec::new();
        for (az_idx, az) in azs.iter().enumerate() {
            for i in 0..2 {
                let node_id = (az_idx * 100 + i + 1) as NodeId;
                nodes.push(NodeIdentity {
                    node_id,
                    address: format!("10.0.{}.{}:9000", az_idx + 1, i + 1),
                    az: az.to_string(),
                });
            }
        }
        let rings = PerAzRings::new(&nodes, 128);
        (nodes, rings)
    }

    fn make_cache(rings: PerAzRings) -> Arc<LocalMetadataCache> {
        Arc::new(LocalMetadataCache::new(
            rings,
            HashMap::new(),
            HashMap::new(),
        ))
    }

    #[test]
    fn replicas_returns_three_nodes() {
        let (_nodes, rings) = make_six_node_rings();
        let cache = make_cache(rings);
        // Pick any node as "this node" — doesn't matter for this test.
        let router = Router::new(1, cache);

        let replicas = router.replicas_for("test_table");
        assert_eq!(replicas.len(), 3, "should return one replica per AZ");

        // All should be distinct.
        let mut unique = replicas.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3, "all replicas should be distinct nodes");
    }

    #[test]
    fn is_my_table_correct() {
        let (_nodes, rings) = make_six_node_rings();
        let cache = make_cache(rings);

        // Find a table where node 1 IS a replica.
        let mut found_mine = false;
        let mut found_not_mine = false;
        for i in 0..200 {
            let table = format!("probe_table_{}", i);
            let router = Router::new(1, cache.clone());
            let replicas = router.replicas_for(&table);
            if replicas.contains(&1) {
                assert!(
                    router.is_my_table(&table),
                    "is_my_table should be true when node is in replica set"
                );
                found_mine = true;
            } else {
                assert!(
                    !router.is_my_table(&table),
                    "is_my_table should be false when node is NOT in replica set"
                );
                found_not_mine = true;
            }
            if found_mine && found_not_mine {
                break;
            }
        }
        assert!(
            found_mine,
            "should find at least one table assigned to node 1"
        );
        assert!(
            found_not_mine,
            "should find at least one table NOT assigned to node 1"
        );
    }

    #[test]
    fn preferred_leader_matches_ring() {
        let (_nodes, rings) = make_six_node_rings();
        let cache = make_cache(rings.clone());
        let router = Router::new(1, cache);

        for i in 0..100 {
            let table = format!("table_{}", i);
            let router_leader = router.preferred_leader(&table);
            let ring_leader = rings.leader_for_table(&table);
            assert_eq!(
                router_leader, ring_leader,
                "Router.preferred_leader should match ring.leader_for_table for '{}'",
                table
            );
        }
    }
}
