use async_trait::async_trait;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::shard::ShardId; // Assuming ShardId is from influxdb3_catalog
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub status: ShardMigrationStatus,
    pub message: String,
    pub progress_pct: Option<f32>, // e.g., percentage of data transferred
    // pub estimated_time_remaining_secs: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ShardMigrationError {
    #[error("Shard {0} not found or not in a migratable state (current state: {1:?})")]
    InvalidShardState(ShardId, Option<ShardMigrationStatus>),
    #[error("Cannot migrate shard {0} to its current owner node {1}")]
    CannotMigrateToSelf(ShardId, String),
    #[error("Node {0} not found, not active, or unsuitable for migration")]
    NodeUnavailable(String),
    #[error("Data transfer failed for shard {0}: {1}")]
    DataTransferError(ShardId, String),
    #[error("Catalog update failed for shard {0}: {1}")]
    CatalogUpdateError(ShardId, String),
    #[error("Operation timed out for shard {0}: {1}")]
    Timeout(ShardId, String),
    #[error("Migration for shard {0} was aborted: {1}")]
    Aborted(ShardId, String),
    #[error("Internal error during shard {0} migration: {1}")]
    Internal(ShardId, String),
    #[error("Generic internal error: {0}")]
    GenericInternal(String), // For errors not specific to one shard
}

/// Trait for managing the migration of a single shard from a source to a target node.
#[async_trait]
pub trait ShardMigrator: Debug + Send + Sync {
    /// Initiates migration for a specific shard.
    /// This involves:
    /// - Validating that the shard, source, and target nodes are in correct states.
    /// - Reserving any necessary resources on the target node.
    /// - Updating the shard's status in the Catalog to `Preparing` or `StreamingData`.
    async fn prepare_migration(
        &self,
        shard_id: ShardId,
        source_node_id: &str,
        target_node_id: &str,
    ) -> Result<(), ShardMigrationError>;

    /// Performs a step of the data transfer process.
    /// This could be copying a Parquet file, a batch of WAL entries, etc.
    /// It's designed to be potentially called multiple times.
    /// Returns the current progress of the migration.
    async fn transfer_data_step(
        &self,
        shard_id: ShardId,
    ) -> Result<MigrationProgress, ShardMigrationError>;

    /// Finalizes the migration after all data is transferred and synced.
    /// This involves:
    /// - Performing the final WAL sync and cutover.
    /// - Updating the shard's ownership and status in the Catalog to `Stable` (on the target).
    /// - Potentially instructing the source node to clean up.
    async fn finalize_migration(&self, shard_id: ShardId) -> Result<(), ShardMigrationError>;

    /// Rolls back a failed or aborted migration.
    /// Attempts to revert catalog changes and clean up any partial state on the target node.
    async fn rollback_migration(&self, shard_id: ShardId, reason: &str) -> Result<(), ShardMigrationError>;

    /// Retrieves the current status and progress of a shard migration.
    async fn get_migration_status(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<MigrationProgress>, ShardMigrationError>;
}

/// A No-Op implementation of ShardMigrator, for testing or when migration is disabled.
#[derive(Debug)]
pub struct NoOpShardMigrator {
    catalog: Arc<Catalog>, // Example dependency, could be used for logging or basic state checks
}

impl NoOpShardMigrator {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ShardMigrator for NoOpShardMigrator {
    async fn prepare_migration(
        &self,
        shard_id: ShardId,
        source_node_id: &str,
        target_node_id: &str,
    ) -> Result<(), ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            source_node_id,
            target_node_id,
            "(NoOpShardMigrator) Prepare migration called."
        );
        // Conceptual: Here you might check catalog if shard exists, if nodes exist etc.
        // For NoOp, we just succeed.
        Ok(())
    }

    async fn transfer_data_step(
        &self,
        shard_id: ShardId,
    ) -> Result<MigrationProgress, ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            "(NoOpShardMigrator) Transfer data step called."
        );
        Ok(MigrationProgress {
            status: ShardMigrationStatus::StreamingData, // Simulate some progress
            message: "No-op: Data transfer step simulated.".to_string(),
            progress_pct: Some(50.0), // Simulate 50% done
        })
    }

    async fn finalize_migration(&self, shard_id: ShardId) -> Result<(), ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            "(NoOpShardMigrator) Finalize migration called."
        );
        // Conceptual: Update catalog shard status to Stable and new owner.
        Ok(())
    }

    async fn rollback_migration(&self, shard_id: ShardId, reason: &str) -> Result<(), ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            reason,
            "(NoOpShardMigrator) Rollback migration called."
        );
        // Conceptual: Revert catalog changes, clean up target node.
        Ok(())
    }

    async fn get_migration_status(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<MigrationProgress>, ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            "(NoOpShardMigrator) Get migration status called."
        );
        // For NoOp, assume shard is always stable or migration doesn't exist unless
        // a more complex in-memory state is desired for testing this NoOp.
        Ok(Some(MigrationProgress {
            status: ShardMigrationStatus::Stable,
            message: "Shard is stable (NoOp implementation).".to_string(),
            progress_pct: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_catalog::shard::ShardId;
    use iox_time::MockProvider;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn setup_noop_migrator() -> (NoOpShardMigrator, Arc<Catalog>) {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        let object_store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_args(
                "test_catalog_noop_migrator".into(),
                object_store,
                time_provider,
                metrics,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        (NoOpShardMigrator::new(Arc::clone(&catalog)), catalog)
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_prepare_migration() {
        let (migrator, _catalog) = setup_noop_migrator().await;
        let res = migrator.prepare_migration(ShardId::new(1), "nodeA", "nodeB").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_transfer_data_step() {
        let (migrator, _catalog) = setup_noop_migrator().await;
        let res = migrator.transfer_data_step(ShardId::new(1)).await;
        assert!(res.is_ok());
        let progress = res.unwrap();
        assert_eq!(progress.status, ShardMigrationStatus::StreamingData);
        assert_eq!(progress.progress_pct, Some(50.0));
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_finalize_migration() {
        let (migrator, _catalog) = setup_noop_migrator().await;
        let res = migrator.finalize_migration(ShardId::new(1)).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_rollback_migration() {
        let (migrator, _catalog) = setup_noop_migrator().await;
        let res = migrator.rollback_migration(ShardId::new(1), "test rollback").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_get_migration_status() {
        let (migrator, _catalog) = setup_noop_migrator().await;
        let res = migrator.get_migration_status(ShardId::new(1)).await;
        assert!(res.is_ok());
        let progress_opt = res.unwrap();
        assert!(progress_opt.is_some());
        let progress = progress_opt.unwrap();
        assert_eq!(progress.status, ShardMigrationStatus::Stable);
    }
}
