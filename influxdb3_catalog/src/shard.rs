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
    // Status of the shard, e.g., "Stable", "MigratingData", "AwaitingCutover", "CleaningUp"
    pub status: String,
    /// Timestamp of the last update to this shard definition, in nanoseconds since epoch.
    /// This will be set by the catalog operation log's timestamp.
    pub updated_at_ts: Option<i64>,
}

impl ShardDefinition {
    pub fn new(id: ShardId, time_range: ShardTimeRange, node_ids: Vec<NodeId>) -> Self {
        Self {
            id,
            time_range,
            node_ids,
            status: "Stable".to_string(), // Default status
            updated_at_ts: None, // Initialized as None, set upon first catalog operation
        }
    }
}

// --- From implementations for snapshotting ---

// From ShardDefinition to ShardDefinitionSnapshot
impl From<&crate::shard::ShardDefinition> for crate::snapshot::ShardDefinitionSnapshot {
    fn from(shard_def: &crate::shard::ShardDefinition) -> Self {
        crate::snapshot::ShardDefinitionSnapshot {
            id: shard_def.id,
            time_range: shard_def.time_range, // ShardTimeRange is Copy, Serialize, Deserialize
            node_ids: shard_def.node_ids.clone(),
            status: shard_def.status.clone(),
            updated_at_ts: shard_def.updated_at_ts,
        }
    }
}

// From ShardDefinitionSnapshot to ShardDefinition
impl From<crate::snapshot::ShardDefinitionSnapshot> for crate::shard::ShardDefinition {
    fn from(snapshot: crate::snapshot::ShardDefinitionSnapshot) -> Self {
        Self {
            id: snapshot.id,
            time_range: snapshot.time_range,
            node_ids: snapshot.node_ids,
            status: snapshot.status,
            updated_at_ts: snapshot.updated_at_ts,
        }
    }
}
