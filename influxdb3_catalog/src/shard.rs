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

/// Represents the migration state of a shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardMigrationStatus {
    /// The shard is stable and not currently undergoing migration.
    Stable,
    /// The shard is actively migrating its data out to the specified target nodes.
    MigratingOutTo(Vec<NodeId>),
    /// This node is in the process of receiving data for this shard from a source node.
    MigratingInFrom(NodeId),
}

impl Default for ShardMigrationStatus {
    fn default() -> Self {
        ShardMigrationStatus::Stable
    }
}

/// Represents a shard in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)] // Removed Copy
pub struct ShardDefinition {
    /// Unique identifier for the shard.
    pub id: ShardId,
    /// Time range covered by the shard.
    pub time_range: ShardTimeRange,
    /// List of nodes responsible for this shard.
    pub node_ids: Vec<NodeId>,
    /// Optional hash key range this shard is responsible for.
    /// (min_hash, max_hash_inclusive)
    pub hash_key_range: Option<(u64, u64)>,
    /// Current migration status of the shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_status: Option<ShardMigrationStatus>,
}

impl ShardDefinition {
    pub fn new(
        id: ShardId,
        time_range: ShardTimeRange,
        node_ids: Vec<NodeId>,
        hash_key_range: Option<(u64, u64)>,
        // migration_status is intentionally not in `new` to force default or explicit update
    ) -> Self {
        Self {
            id,
            time_range,
            node_ids,
            hash_key_range,
            migration_status: Some(ShardMigrationStatus::default()), // Default to Stable
        }
    }
}

/// Defines the sharding strategy for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardingStrategy {
    /// Shard based on time only.
    Time,
    /// Shard based on time and then by a hash of specified shard key columns.
    TimeAndKey,
}

impl Default for ShardingStrategy {
    fn default() -> Self {
        ShardingStrategy::Time
    }
}
