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
    /// Current migration status of the shard.
    pub migration_status: Option<ShardMigrationStatus>,
    /// If migrating, these are the nodes the shard is intended to move to.
    pub migration_target_node_ids: Option<Vec<NodeId>>,
    /// If migrating, these are the nodes the shard is moving from (for cleanup purposes post-migration).
    pub migration_source_node_ids: Option<Vec<NodeId>>,
    /// Version of this shard definition, incremented on change.
    pub version: u64,
    // Add other shard-specific information here, e.g.:
    // - Shard operational status (e.g., active, inactive, under maintenance)
    // - Storage location (if not implicitly determined by node_ids)
    // - Size of the shard
}

impl ShardDefinition {
    pub fn new(id: ShardId, time_range: ShardTimeRange, node_ids: Vec<NodeId>) -> Self {
        Self {
            id,
            time_range,
            node_ids,
            migration_status: Some(ShardMigrationStatus::Stable),
            migration_target_node_ids: None,
            migration_source_node_ids: None,
            version: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardMigrationStatus {
    Stable,          // Shard is not currently being migrated.
    Preparing,       // Migration initiated, source/target nodes preparing.
    StreamingData,   // Historical data (e.g., Parquet files) is being copied/made available.
    WalSync,         // Delta (WAL entries) is being synchronized.
    AwaitingCutover, // Data sync complete, ready for final ownership switch.
    Finalizing,      // Catalog updates and cleanup in progress.
    Aborting,        // Migration is being rolled back.
    Failed,          // Migration attempt failed.
}

impl Default for ShardMigrationStatus {
    fn default() -> Self {
        Self::Stable
    }
}
