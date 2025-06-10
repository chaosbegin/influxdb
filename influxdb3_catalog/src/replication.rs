use influxdb3_id::NodeId;
use serde::{Deserialize, Serialize};

/// Defines the replication factor for a shard or a set of shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationFactor(u8);

impl ReplicationFactor {
    pub fn new(factor: u8) -> Result<Self, String> {
        if factor == 0 {
            Err("Replication factor cannot be zero.".to_string())
        } else {
            Ok(Self(factor))
        }
    }

    pub fn get(&self) -> u8 {
        self.0
    }
}

/// Represents the replication configuration for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationInfo {
    /// Desired number of replicas for shards within this table.
    pub replication_factor: ReplicationFactor,
    // Potentially, we could have more granular control here, e.g.:
    // - A list of preferred node IDs for replicas.
    // - Strategies for replica placement (e.g., rack-aware, region-aware).
    // For now, keeping it simple with just a replication factor.
    // The actual nodes holding the replicas will be part of ShardDefinition.
}

impl ReplicationInfo {
    pub fn new(replication_factor: ReplicationFactor) -> Self {
        Self { replication_factor }
    }
}

// Example of how it might be extended if needed:
/// Defines the specific nodes where replicas for a given shard are located.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReplicaPlacement {
    /// The primary node for the shard.
    pub primary_node: NodeId,
    /// List of secondary nodes holding replicas of the shard.
    pub secondary_nodes: Vec<NodeId>,
}

impl ShardReplicaPlacement {
    pub fn new(primary_node: NodeId, secondary_nodes: Vec<NodeId>) -> Self {
        Self {
            primary_node,
            secondary_nodes,
        }
    }

    /// Returns all nodes involved in this shard's replication.
    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes = vec![self.primary_node];
        nodes.extend(self.secondary_nodes.iter().cloned());
        nodes
    }
}
