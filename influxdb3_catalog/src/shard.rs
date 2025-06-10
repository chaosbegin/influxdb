use influxdb3_id::NodeId;
use serde::{Deserialize, Serialize};

/// Unique identifier for a shard.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ShardId(u64);

impl ShardId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for ShardId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Defines the time range for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardTimeRange {
    pub start_time: i64, // nanoseconds since epoch
    pub end_time: i64,   // nanoseconds since epoch
}

/// Represents a shard in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDefinition {
    /// Unique identifier for the shard.
    pub id: ShardId,
    /// Time range covered by the shard.
    pub time_range: ShardTimeRange,
    /// List of nodes responsible for this shard.
    /// In a simple setup, this might be a single node.
    /// For replicated setups, this could be multiple nodes.
    pub node_ids: Vec<NodeId>,
    // Add other shard-specific information here, e.g.:
    // - Shard status (e.g., active, inactive, under maintenance)
    // - Storage location (if not implicitly determined by node_ids)
    // - Size of the shard
}

impl ShardDefinition {
    pub fn new(id: ShardId, time_range: ShardTimeRange, node_ids: Vec<NodeId>) -> Self {
        Self {
            id,
            time_range,
            node_ids,
        }
    }
}
