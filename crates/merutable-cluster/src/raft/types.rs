// P1.2: openraft type config (MeruTypeConfig), RaftCommand enum.

use std::io::Cursor;

use serde::{Deserialize, Serialize};

use crate::proto::raft_transport as pb;

// ---------------------------------------------------------------------------
// RaftCommand: the payload replicated through each table's Raft group
// ---------------------------------------------------------------------------

/// Application data replicated through Raft.
///
/// Each variant maps to a single mutation on one table. The command is
/// serialized with `postcard` into the Raft log entry payload bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftCommand {
    Put {
        row: merutable::value::Row,
    },
    Delete {
        pk: Vec<merutable::value::FieldValue>,
    },
    AlterSchema {
        change: SchemaChange,
    },
    /// Metadata state machine command, replicated through the `__metadata__`
    /// Raft group. Only used by `MetadataRaftGroup`.
    Metadata {
        cmd: crate::metadata::state_machine::MetadataCommand,
    },
}

/// The set of supported schema changes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SchemaChange {
    AddColumn { col: merutable::schema::ColumnDef },
}

/// Application response returned by the state machine after applying a
/// `RaftCommand`. Kept minimal for now.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RaftResponse;

// ---------------------------------------------------------------------------
// openraft type configuration
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    /// Type configuration wiring for merutable's per-table Raft groups.
    pub MeruTypeConfig:
        D            = RaftCommand,
        R            = RaftResponse,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<MeruTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder    = openraft::impls::OneshotResponder<MeruTypeConfig>,
);

// ---------------------------------------------------------------------------
// Proto <-> openraft conversion helpers
// ---------------------------------------------------------------------------

/// Convert an openraft `LogId` to the protobuf representation.
pub fn log_id_to_proto(lid: &openraft::LogId<u64>) -> pb::LogId {
    pb::LogId {
        term: lid.leader_id.term,
        index: lid.index,
        node_id: 0, // single-term-leader: node_id not tracked in CommittedLeaderId
    }
}

/// Convert a protobuf `LogId` to the openraft representation.
pub fn log_id_from_proto(p: &pb::LogId) -> openraft::LogId<u64> {
    openraft::LogId::new(
        openraft::CommittedLeaderId::new(p.term, p.node_id),
        p.index,
    )
}

/// Convert an openraft `Vote` to the protobuf representation.
pub fn vote_to_proto(v: &openraft::Vote<u64>) -> pb::Vote {
    pb::Vote {
        term: v.leader_id.term,
        node_id: v.leader_id.voted_for().unwrap_or(0),
        committed: v.committed,
    }
}

/// Convert a protobuf `Vote` to the openraft representation.
pub fn vote_from_proto(p: &pb::Vote) -> openraft::Vote<u64> {
    if p.committed {
        openraft::Vote::new_committed(p.term, p.node_id)
    } else {
        openraft::Vote::new(p.term, p.node_id)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_command_serialization_roundtrip() {
        use merutable::value::{FieldValue, Row};

        // -- Put
        let put = RaftCommand::Put {
            row: Row::new(vec![
                Some(FieldValue::Int64(42)),
                Some(FieldValue::Boolean(true)),
                None,
            ]),
        };
        let bytes = postcard::to_stdvec(&put).expect("serialize Put");
        let decoded: RaftCommand = postcard::from_bytes(&bytes).expect("deserialize Put");
        assert_eq!(put, decoded);

        // -- Delete
        let delete = RaftCommand::Delete {
            pk: vec![FieldValue::Int32(7), FieldValue::Bytes(bytes::Bytes::from_static(b"key"))],
        };
        let bytes = postcard::to_stdvec(&delete).expect("serialize Delete");
        let decoded: RaftCommand = postcard::from_bytes(&bytes).expect("deserialize Delete");
        assert_eq!(delete, decoded);

        // -- AlterSchema
        let alter = RaftCommand::AlterSchema {
            change: SchemaChange::AddColumn {
                col: merutable::schema::ColumnDef {
                    name: "new_col".to_string(),
                    col_type: merutable::schema::ColumnType::Double,
                    nullable: true,
                    ..Default::default()
                },
            },
        };
        let bytes = postcard::to_stdvec(&alter).expect("serialize AlterSchema");
        let decoded: RaftCommand = postcard::from_bytes(&bytes).expect("deserialize AlterSchema");
        assert_eq!(alter, decoded);
    }

    #[test]
    fn vote_proto_roundtrip() {
        let v = openraft::Vote::new(5, 3u64);
        let p = vote_to_proto(&v);
        let back = vote_from_proto(&p);
        assert_eq!(v, back);

        let vc = openraft::Vote::new_committed(10, 1u64);
        let pc = vote_to_proto(&vc);
        let backc = vote_from_proto(&pc);
        assert_eq!(vc, backc);
    }

    #[test]
    fn log_id_proto_roundtrip() {
        let lid = openraft::LogId::new(
            openraft::CommittedLeaderId::new(3, 0),
            42,
        );
        let p = log_id_to_proto(&lid);
        assert_eq!(p.term, 3);
        assert_eq!(p.index, 42);
        let back = log_id_from_proto(&p);
        assert_eq!(back.index, lid.index);
        assert_eq!(back.leader_id.term, lid.leader_id.term);
    }
}
