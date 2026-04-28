// P1.4: Metadata Raft group state machine.
// Stores table schemas, node registry, per-AZ rings.
// Does NOT store table-to-node assignments (derived from rings).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::config::NodeId;
use crate::observability::RING_VERSION;
use crate::ring::per_az::PerAzRings;
use merutable::schema::TableSchema;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub node_id: NodeId,
    pub address: String,
    pub az: String,
    pub table_count: u32,
    pub status: NodeStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Draining,
    Dead,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetadataCommand {
    CreateTable { name: String, schema: TableSchema },
    DropTable { name: String },
    RegisterNode { node: NodeEntry },
    DeregisterNode { node_id: NodeId },
    SetOverride { table: String, nodes: Vec<NodeId> },
    RemoveOverride { table: String },
    /// Informational: a table's Raft group applied a schema change.
    UpdateSchema { table: String, schema: TableSchema },
}

#[derive(Clone, Debug)]
pub enum MetadataEvent {
    TableCreated {
        name: String,
        schema: TableSchema,
        /// The replica set for this table, derived from the rings at
        /// CreateTable commit time.
        replicas: Vec<NodeId>,
        /// The node responsible for calling `Raft::initialize` on the
        /// new table's Raft group. All other replicas create their
        /// group uninitialised and wait for the bootstrapper to add
        /// them. Deterministic: `min(replicas)`.
        bootstrapper: NodeId,
    },
    TableDropped { name: String },
    RingChanged { az: String },
    OverrideSet { table: String, nodes: Vec<NodeId> },
    OverrideRemoved { table: String },
    SchemaUpdated { table: String, schema: TableSchema },
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetadataApplyResult {
    Ok,
    TableCreated,
    TableDropped,
    NodeRegistered,
    NodeDeregistered,
    SchemaUpdated,
    OverrideSet,
    OverrideRemoved,
    Error(String),
}

// ---------------------------------------------------------------------------
// MetadataStateMachine
// ---------------------------------------------------------------------------

pub struct MetadataStateMachine {
    table_schemas: HashMap<String, TableSchema>,
    nodes: HashMap<NodeId, NodeEntry>,
    rings: PerAzRings,
    vnodes_per_node: usize,
    /// Overrides tracked at the SM level so they survive ring rebuilds.
    overrides: HashMap<String, Vec<NodeId>>,
    /// Pending events for watch stream consumers to drain.
    pending_events: Vec<MetadataEvent>,
}

impl MetadataStateMachine {
    /// Create a new, empty state machine.
    pub fn new(vnodes_per_node: usize) -> Self {
        Self {
            table_schemas: HashMap::new(),
            nodes: HashMap::new(),
            rings: PerAzRings::new(&[], vnodes_per_node),
            vnodes_per_node,
            overrides: HashMap::new(),
            pending_events: Vec::new(),
        }
    }

    /// Apply a single command and return the result.
    pub fn apply(&mut self, cmd: MetadataCommand) -> MetadataApplyResult {
        match cmd {
            MetadataCommand::CreateTable { name, schema } => {
                if self.table_schemas.contains_key(&name) {
                    return MetadataApplyResult::Error(format!(
                        "table already exists: {}",
                        name
                    ));
                }
                self.table_schemas.insert(name.clone(), schema.clone());

                // Compute the replica set and bootstrapper at commit time
                // so all nodes agree on who initializes the Raft group.
                let replicas = self.rings.replicas_for_table(&name);
                let bootstrapper = replicas.iter().copied().min().unwrap_or(0);

                tracing::info!(
                    table = %name,
                    ?replicas,
                    bootstrapper = bootstrapper,
                    "metadata: table created"
                );
                self.pending_events.push(MetadataEvent::TableCreated {
                    name,
                    schema,
                    replicas,
                    bootstrapper,
                });
                MetadataApplyResult::TableCreated
            }

            MetadataCommand::DropTable { name } => {
                if self.table_schemas.remove(&name).is_none() {
                    return MetadataApplyResult::Error(format!(
                        "table not found: {}",
                        name
                    ));
                }
                self.pending_events
                    .push(MetadataEvent::TableDropped { name });
                MetadataApplyResult::TableDropped
            }

            MetadataCommand::RegisterNode { node } => {
                let az = node.az.clone();
                tracing::info!(node_id = %node.node_id, az = %node.az, "metadata: node registered");
                self.nodes.insert(node.node_id, node);
                self.rebuild_all_rings();
                metrics::gauge!(RING_VERSION).increment(1.0);
                self.pending_events
                    .push(MetadataEvent::RingChanged { az });
                MetadataApplyResult::NodeRegistered
            }

            MetadataCommand::DeregisterNode { node_id } => {
                let az = match self.nodes.get_mut(&node_id) {
                    Some(entry) => {
                        entry.status = NodeStatus::Dead;
                        entry.az.clone()
                    }
                    None => {
                        return MetadataApplyResult::Error(format!(
                            "node not found: {}",
                            node_id
                        ));
                    }
                };
                tracing::info!(node_id = %node_id, "metadata: node deregistered");
                self.rebuild_all_rings();
                metrics::gauge!(RING_VERSION).increment(1.0);
                self.pending_events
                    .push(MetadataEvent::RingChanged { az });
                MetadataApplyResult::NodeDeregistered
            }

            MetadataCommand::SetOverride { table, nodes } => {
                self.overrides.insert(table.clone(), nodes.clone());
                self.rings.set_override(table.clone(), nodes.clone());
                self.pending_events.push(MetadataEvent::OverrideSet {
                    table,
                    nodes,
                });
                MetadataApplyResult::OverrideSet
            }

            MetadataCommand::RemoveOverride { table } => {
                self.overrides.remove(&table);
                self.rings.remove_override(&table);
                self.pending_events
                    .push(MetadataEvent::OverrideRemoved { table });
                MetadataApplyResult::OverrideRemoved
            }

            MetadataCommand::UpdateSchema { table, schema } => {
                self.table_schemas.insert(table.clone(), schema.clone());
                self.pending_events.push(MetadataEvent::SchemaUpdated {
                    table,
                    schema,
                });
                MetadataApplyResult::SchemaUpdated
            }
        }
    }

    /// Look up a table schema by name.
    pub fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.table_schemas.get(name)
    }

    /// Return the names of all registered tables.
    pub fn all_tables(&self) -> Vec<String> {
        self.table_schemas.keys().cloned().collect()
    }

    /// Look up a node entry by ID.
    pub fn node(&self, id: NodeId) -> Option<&NodeEntry> {
        self.nodes.get(&id)
    }

    /// Return all registered node entries.
    pub fn all_nodes(&self) -> Vec<&NodeEntry> {
        self.nodes.values().collect()
    }

    /// Return only nodes with `Active` status.
    pub fn active_nodes(&self) -> Vec<&NodeEntry> {
        self.nodes
            .values()
            .filter(|n| n.status == NodeStatus::Active)
            .collect()
    }

    /// Borrow the per-AZ rings.
    pub fn rings(&self) -> &PerAzRings {
        &self.rings
    }

    /// Take all pending events, leaving the internal buffer empty.
    pub fn drain_events(&mut self) -> Vec<MetadataEvent> {
        std::mem::take(&mut self.pending_events)
    }

    // -- private helpers ----------------------------------------------------

    /// Reconstruct the full `PerAzRings` from the current active nodes,
    /// then re-apply any stored overrides.
    ///
    /// We rebuild from scratch (rather than using `rebuild_az`) because
    /// `PerAzRings::rebuild_az` does not update the internal `az_names`
    /// list, which is needed when a new AZ appears for the first time.
    fn rebuild_all_rings(&mut self) {
        use crate::config::NodeIdentity;

        let identities: Vec<NodeIdentity> = self
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Active)
            .map(|n| NodeIdentity {
                node_id: n.node_id,
                address: n.address.clone(),
                az: n.az.clone(),
            })
            .collect();

        self.rings = PerAzRings::new(&identities, self.vnodes_per_node);

        // Re-apply overrides to the freshly built rings.
        for (table, nodes) in &self.overrides {
            self.rings.set_override(table.clone(), nodes.clone());
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use merutable::schema::{ColumnDef, ColumnType};

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
                ColumnDef {
                    name: "val".into(),
                    col_type: ColumnType::ByteArray,
                    nullable: true,
                    ..Default::default()
                },
            ],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn make_node(id: NodeId, az: &str) -> NodeEntry {
        NodeEntry {
            node_id: id,
            address: format!("10.0.0.{}:9000", id),
            az: az.to_string(),
            table_count: 0,
            status: NodeStatus::Active,
        }
    }

    /// Register 2 nodes per AZ across 3 AZs (6 nodes total).
    fn register_six_nodes(sm: &mut MetadataStateMachine) {
        for (az_idx, az) in ["AZ-1", "AZ-2", "AZ-3"].iter().enumerate() {
            for i in 0..2 {
                let id = (az_idx * 100 + i + 1) as NodeId;
                sm.apply(MetadataCommand::RegisterNode {
                    node: make_node(id, az),
                });
            }
        }
    }

    #[test]
    fn create_table_stores_schema() {
        let mut sm = MetadataStateMachine::new(32);
        let schema = make_schema("users");
        let result = sm.apply(MetadataCommand::CreateTable {
            name: "users".into(),
            schema: schema.clone(),
        });
        assert_eq!(result, MetadataApplyResult::TableCreated);
        assert_eq!(sm.table_schema("users"), Some(&schema));

        let events = sm.drain_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            MetadataEvent::TableCreated { name, schema: s, .. } => {
                assert_eq!(name, "users");
                assert_eq!(s, &schema);
            }
            other => panic!("expected TableCreated, got {:?}", other),
        }
    }

    #[test]
    fn create_duplicate_table_errors() {
        let mut sm = MetadataStateMachine::new(32);
        sm.apply(MetadataCommand::CreateTable {
            name: "t".into(),
            schema: make_schema("t"),
        });
        let result = sm.apply(MetadataCommand::CreateTable {
            name: "t".into(),
            schema: make_schema("t"),
        });
        assert!(matches!(result, MetadataApplyResult::Error(_)));
    }

    #[test]
    fn drop_table_removes() {
        let mut sm = MetadataStateMachine::new(32);
        sm.apply(MetadataCommand::CreateTable {
            name: "t".into(),
            schema: make_schema("t"),
        });
        sm.drain_events(); // clear

        let result = sm.apply(MetadataCommand::DropTable {
            name: "t".into(),
        });
        assert_eq!(result, MetadataApplyResult::TableDropped);
        assert!(sm.table_schema("t").is_none());

        let events = sm.drain_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MetadataEvent::TableDropped { name } if name == "t"
        ));
    }

    #[test]
    fn drop_nonexistent_errors() {
        let mut sm = MetadataStateMachine::new(32);
        let result = sm.apply(MetadataCommand::DropTable {
            name: "nope".into(),
        });
        assert!(matches!(result, MetadataApplyResult::Error(_)));
    }

    #[test]
    fn register_node_rebuilds_ring() {
        let mut sm = MetadataStateMachine::new(32);
        register_six_nodes(&mut sm);

        // 3 AZs => replicas_for_table should return 3 nodes.
        let replicas = sm.rings().replicas_for_table("some_table");
        assert_eq!(replicas.len(), 3);

        // All returned nodes should be active and distinct.
        let mut seen = std::collections::HashSet::new();
        for nid in &replicas {
            assert!(sm.node(*nid).is_some());
            assert!(seen.insert(*nid));
        }
    }

    #[test]
    fn deregister_node_rebuilds_ring() {
        let mut sm = MetadataStateMachine::new(32);
        register_six_nodes(&mut sm);

        // Deregister node 1 (in AZ-1).
        let target = 1u64;
        sm.apply(MetadataCommand::DeregisterNode { node_id: target });

        // Node should now be Dead.
        assert_eq!(sm.node(target).unwrap().status, NodeStatus::Dead);

        // Ring should not assign the dead node to any table.
        for i in 0..100 {
            let table = format!("tbl_{}", i);
            let replicas = sm.rings().replicas_for_table(&table);
            assert!(
                !replicas.contains(&target),
                "dead node {} still in replicas for {}",
                target,
                table,
            );
        }
    }

    #[test]
    fn update_schema_replaces() {
        let mut sm = MetadataStateMachine::new(32);
        let schema_v1 = make_schema("t");
        sm.apply(MetadataCommand::CreateTable {
            name: "t".into(),
            schema: schema_v1.clone(),
        });

        // Create an updated schema with an extra column.
        let mut schema_v2 = schema_v1;
        schema_v2.columns.push(ColumnDef {
            name: "extra".into(),
            col_type: ColumnType::Double,
            nullable: true,
            ..Default::default()
        });
        sm.drain_events();

        sm.apply(MetadataCommand::UpdateSchema {
            table: "t".into(),
            schema: schema_v2,
        });

        let stored = sm.table_schema("t").unwrap();
        assert_eq!(stored.columns.len(), 3);
        assert_eq!(stored.columns[2].name, "extra");

        let events = sm.drain_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MetadataEvent::SchemaUpdated { table, .. } if table == "t"
        ));
    }

    #[test]
    fn set_and_remove_override() {
        let mut sm = MetadataStateMachine::new(32);
        // Need nodes so the ring is non-empty for fallback after removal.
        register_six_nodes(&mut sm);
        sm.drain_events();

        let override_nodes = vec![999, 888, 777];
        sm.apply(MetadataCommand::SetOverride {
            table: "hot".into(),
            nodes: override_nodes.clone(),
        });
        assert_eq!(sm.rings().replicas_for_table("hot"), override_nodes);

        sm.apply(MetadataCommand::RemoveOverride {
            table: "hot".into(),
        });
        let replicas = sm.rings().replicas_for_table("hot");
        assert_ne!(replicas, override_nodes);
        assert_eq!(replicas.len(), 3);

        let events = sm.drain_events();
        assert!(events.iter().any(|e| matches!(
            e,
            MetadataEvent::OverrideSet { table, .. } if table == "hot"
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            MetadataEvent::OverrideRemoved { table } if table == "hot"
        )));
    }

    #[test]
    fn drain_events_clears() {
        let mut sm = MetadataStateMachine::new(32);
        sm.apply(MetadataCommand::CreateTable {
            name: "a".into(),
            schema: make_schema("a"),
        });
        sm.apply(MetadataCommand::CreateTable {
            name: "b".into(),
            schema: make_schema("b"),
        });
        sm.apply(MetadataCommand::RegisterNode {
            node: make_node(1, "AZ-1"),
        });

        let events = sm.drain_events();
        assert_eq!(events.len(), 3);

        let events2 = sm.drain_events();
        assert!(events2.is_empty());
    }
}
