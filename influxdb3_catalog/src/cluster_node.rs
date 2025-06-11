use influxdb3_id::NodeId;
use serde::{Deserialize, Serialize};

/// Represents a node in the cluster for membership purposes.
/// This is distinct from the `NodeDefinition` used for self-registration of WAL/Query nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeDefinition {
    /// Unique identifier for the node (shared with internal NodeId if it's the same entity).
    pub id: NodeId,
    /// Address for inter-node RPC communication (e.g., gRPC).
    pub rpc_address: String,
    /// Address for client-facing HTTP APIs.
    pub http_address: String,
    /// Current status of the node within the cluster.
    /// Examples: "Active", "Joining", "Leaving", "Down".
    pub status: String,
    /// Timestamp (nanoseconds since epoch) when the node was first registered or created.
    pub created_at: i64,
    /// Timestamp (nanoseconds since epoch) when the node's definition was last updated.
    pub updated_at: i64,
}

impl ClusterNodeDefinition {
    pub fn new(
        id: NodeId,
        rpc_address: String,
        http_address: String,
        status: String,
        created_at: i64,
        updated_at: i64,
    ) -> Self {
        Self {
            id,
            rpc_address,
            http_address,
            status,
            created_at,
            updated_at,
        }
    }
}

// Add From implementations for snapshotting/restoring

// From ClusterNodeDefinition to ClusterNodeDefinitionSnapshot
impl From<&crate::ClusterNodeDefinition> for crate::snapshot::ClusterNodeDefinitionSnapshot {
    fn from(node_def: &crate::ClusterNodeDefinition) -> Self {
        Self {
            id: node_def.id,
            rpc_address: node_def.rpc_address.clone(),
            http_address: node_def.http_address.clone(),
            status: node_def.status.clone(),
            created_at: node_def.created_at,
            updated_at: node_def.updated_at,
        }
    }
}

// From ClusterNodeDefinitionSnapshot to ClusterNodeDefinition
impl From<crate::snapshot::ClusterNodeDefinitionSnapshot> for crate::ClusterNodeDefinition {
    fn from(snapshot: crate::snapshot::ClusterNodeDefinitionSnapshot) -> Self {
        Self {
            id: snapshot.id,
            rpc_address: snapshot.rpc_address,
            http_address: snapshot.http_address,
            status: snapshot.status,
            created_at: snapshot.created_at,
            updated_at: snapshot.updated_at,
        }
    }
}
