pub mod rebalance;
pub mod error;
pub mod membership;
pub mod migrator; // Added migrator module

#[cfg(any(test, feature = "test_utils"))]
pub mod node_data_client_mock; // Added mock client module

pub use rebalance::initiate_shard_move_conceptual;
// Re-exporting all conceptual steps for potential individual use or testing
pub use rebalance::{
    complete_shard_snapshot_transfer_conceptual,
    complete_shard_wal_sync_conceptual,
    complete_shard_cutover_conceptual,
    complete_shard_cleanup_conceptual,
    RebalanceError
};
pub use error::ClusterManagerError;
pub use membership::StaticClusterMembership;
pub use migrator::{ShardMigrator, ShardMigrationJob}; // Added exports
