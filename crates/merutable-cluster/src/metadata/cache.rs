// P1.4: LocalMetadataCache — ArcSwap-backed local cache of rings + schemas.
// Updated via watch stream from the metadata Raft group.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::NodeId;
use crate::metadata::state_machine::NodeEntry;
use crate::ring::per_az::PerAzRings;
use merutable::schema::TableSchema;

// ---------------------------------------------------------------------------
// LocalMetadataCache
// ---------------------------------------------------------------------------

/// Local read-only cache of metadata, updated from the watch stream.
/// Every data node holds one of these. Reads are lock-free via `ArcSwap`.
pub struct LocalMetadataCache {
    rings: ArcSwap<PerAzRings>,
    table_schemas: ArcSwap<HashMap<String, TableSchema>>,
    nodes: ArcSwap<HashMap<NodeId, NodeEntry>>,
}

impl LocalMetadataCache {
    /// Create a new cache seeded with initial data.
    pub fn new(
        rings: PerAzRings,
        schemas: HashMap<String, TableSchema>,
        nodes: HashMap<NodeId, NodeEntry>,
    ) -> Self {
        Self {
            rings: ArcSwap::from_pointee(rings),
            table_schemas: ArcSwap::from_pointee(schemas),
            nodes: ArcSwap::from_pointee(nodes),
        }
    }

    /// Load the current per-AZ rings (lock-free).
    pub fn rings(&self) -> arc_swap::Guard<Arc<PerAzRings>> {
        self.rings.load()
    }

    /// Clone the schema for a specific table, if it exists.
    pub fn schema_for(&self, table: &str) -> Option<TableSchema> {
        self.table_schemas.load().get(table).cloned()
    }

    /// Return the names of all cached tables.
    pub fn all_tables(&self) -> Vec<String> {
        self.table_schemas.load().keys().cloned().collect()
    }

    /// Clone a node entry by ID, if it exists.
    pub fn node(&self, id: NodeId) -> Option<NodeEntry> {
        self.nodes.load().get(&id).cloned()
    }

    /// Replace the cached rings with a new snapshot.
    pub fn update_rings(&self, rings: PerAzRings) {
        self.rings.store(Arc::new(rings));
    }

    /// Insert or replace a single table schema in the cache.
    pub fn update_schema(&self, table: String, schema: TableSchema) {
        let mut map = HashMap::clone(&self.table_schemas.load());
        map.insert(table, schema);
        self.table_schemas.store(Arc::new(map));
    }

    /// Remove a table schema from the cache.
    pub fn remove_table(&self, table: &str) {
        let mut map = HashMap::clone(&self.table_schemas.load());
        map.remove(table);
        self.table_schemas.store(Arc::new(map));
    }

    /// Insert or replace a single node entry in the cache.
    pub fn update_node(&self, entry: NodeEntry) {
        let mut map = HashMap::clone(&self.nodes.load());
        map.insert(entry.node_id, entry);
        self.nodes.store(Arc::new(map));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeIdentity;
    use crate::metadata::state_machine::{NodeEntry, NodeStatus};
    use merutable::schema::{ColumnDef, ColumnType, TableSchema};

    fn make_schema(name: &str) -> TableSchema {
        TableSchema {
            table_name: name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: false,
                    ..Default::default()
                },
            ],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn make_node_entry(id: NodeId, az: &str) -> NodeEntry {
        NodeEntry {
            node_id: id,
            address: format!("10.0.0.{}:9000", id),
            az: az.to_string(),
            table_count: 0,
            status: NodeStatus::Active,
        }
    }

    fn make_rings() -> PerAzRings {
        let nodes = vec![
            NodeIdentity { node_id: 1, address: "a".into(), az: "AZ-1".into() },
            NodeIdentity { node_id: 2, address: "b".into(), az: "AZ-2".into() },
            NodeIdentity { node_id: 3, address: "c".into(), az: "AZ-3".into() },
        ];
        PerAzRings::new(&nodes, 32)
    }

    #[test]
    fn cache_read_returns_data() {
        let mut schemas = HashMap::new();
        schemas.insert("users".to_string(), make_schema("users"));
        schemas.insert("orders".to_string(), make_schema("orders"));

        let mut nodes = HashMap::new();
        nodes.insert(1, make_node_entry(1, "AZ-1"));

        let rings = make_rings();
        let cache = LocalMetadataCache::new(rings, schemas, nodes);

        // schema_for
        assert!(cache.schema_for("users").is_some());
        assert_eq!(cache.schema_for("users").unwrap().table_name, "users");
        assert!(cache.schema_for("missing").is_none());

        // all_tables
        let mut tables = cache.all_tables();
        tables.sort();
        assert_eq!(tables, vec!["orders", "users"]);

        // node
        assert!(cache.node(1).is_some());
        assert!(cache.node(999).is_none());

        // rings — verify it returns something usable
        let replicas = cache.rings().replicas_for_table("some_table");
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn cache_update_schema() {
        let schemas = HashMap::new();
        let cache = LocalMetadataCache::new(
            make_rings(),
            schemas,
            HashMap::new(),
        );

        cache.update_schema("t".to_string(), make_schema("t"));
        assert!(cache.schema_for("t").is_some());

        // Replace with a schema that has an extra column.
        let mut updated = make_schema("t");
        updated.columns.push(ColumnDef {
            name: "extra".into(),
            col_type: ColumnType::Double,
            nullable: true,
            ..Default::default()
        });
        cache.update_schema("t".to_string(), updated);

        let stored = cache.schema_for("t").unwrap();
        assert_eq!(stored.columns.len(), 2);
        assert_eq!(stored.columns[1].name, "extra");
    }

    #[test]
    fn cache_update_rings() {
        let cache = LocalMetadataCache::new(
            make_rings(),
            HashMap::new(),
            HashMap::new(),
        );

        // Build a new set of rings with different nodes.
        let new_nodes = vec![
            NodeIdentity { node_id: 10, address: "x".into(), az: "AZ-1".into() },
            NodeIdentity { node_id: 20, address: "y".into(), az: "AZ-2".into() },
            NodeIdentity { node_id: 30, address: "z".into(), az: "AZ-3".into() },
        ];
        let new_rings = PerAzRings::new(&new_nodes, 32);
        cache.update_rings(new_rings);

        // The ring should now only map to the new node IDs.
        let replicas = cache.rings().replicas_for_table("any_table");
        for nid in &replicas {
            assert!(
                *nid == 10 || *nid == 20 || *nid == 30,
                "unexpected node {} after ring update",
                nid,
            );
        }
    }

    #[test]
    fn cache_remove_table() {
        let mut schemas = HashMap::new();
        schemas.insert("t".to_string(), make_schema("t"));
        let cache = LocalMetadataCache::new(
            make_rings(),
            schemas,
            HashMap::new(),
        );

        assert!(cache.schema_for("t").is_some());
        cache.remove_table("t");
        assert!(cache.schema_for("t").is_none());
    }
}
