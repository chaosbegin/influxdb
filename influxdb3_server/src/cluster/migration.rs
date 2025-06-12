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
        db_name: &str,
        table_name: &str,
        shard_id: ShardId,
        source_node_id: &str,
        target_node_id: &str, // Assuming target_node_id is a ComputeNodeId compatible string or needs conversion
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
    async fn finalize_migration(&self, db_name: &str, table_name: &str, shard_id: ShardId) -> Result<(), ShardMigrationError>;

    /// Rolls back a failed or aborted migration.
    /// Attempts to revert catalog changes and clean up any partial state on the target node.
    async fn rollback_migration(&self, db_name: &str, table_name: &str, shard_id: ShardId, reason: &str) -> Result<(), ShardMigrationError>;

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
        db_name: &str,
        table_name: &str,
        shard_id: ShardId,
        source_node_id: &str,
        target_node_id_str: &str, // Assuming this is a string representation of ComputeNodeId
    ) -> Result<(), ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            %db_name,
            %table_name,
            %source_node_id,
            target_node_id = %target_node_id_str,
            "(NoOpShardMigrator) Prepare migration called."
        );

        // Attempt to parse target_node_id_str to ComputeNodeId (u64)
        // This is a simplification; in a real system, node IDs might be strings (NodeIdentifier)
        // or already u64 (ComputeNodeId). The API layer and this migrator need to agree.
        // For now, assume target_node_id_str can be parsed or looked up to a ComputeNodeId.
        // If your NodeId type used in ShardDefinition is String, then no parse is needed.
        // Based on previous steps, ShardDefinition.node_ids are Vec<ComputeNodeId (u64)>.
        // The API layer's `move_shard_manually` takes `target_compute_node_id: ComputeNodeId`.
        // So, the trait should probably take ComputeNodeId directly.
        // Let's assume for now that the API layer would pass compatible IDs.
        // For this NoOp, we'll assume target_node_id_str can be used if the catalog expects string,
        // or if it expects ComputeNodeId, this NoOp won't be able to provide it without parsing/lookup.
        // The API call `catalog.update_shard_migration_state` takes `Option<Vec<ComputeNodeId>>`.
        // So, `target_node_id_str` must be convertible to `ComputeNodeId`.
        // This is a placeholder for robust ID handling.
        let target_node_compute_id = influxdb3_id::NodeId::new_from_str(target_node_id_str)
            .map_err(|e| ShardMigrationError::Internal(shard_id, format!("Invalid target_node_id format: {} - {}", target_node_id_str, e)))?;
        let source_node_compute_id = influxdb3_id::NodeId::new_from_str(source_node_id)
            .map_err(|e| ShardMigrationError::Internal(shard_id, format!("Invalid source_node_id format: {} - {}", source_node_id, e)))?;


        self.catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            Some(ShardMigrationStatus::Preparing),
            Some(vec![target_node_compute_id]),
            Some(vec![source_node_compute_id]),
        ).await.map_err(|e| ShardMigrationError::CatalogUpdateError(shard_id, e.to_string()))?;
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
            %db_name,
            %table_name,
            "(NoOpShardMigrator) Finalize migration called."
        );
        // Conceptual: Update catalog shard status to Stable and new owner.
        // Also, ShardDefinition.node_ids should be updated to target_node_id.
        // `update_shard_migration_state` currently only handles migration status fields.
        // For NoOp, setting to Stable and clearing migration fields is sufficient for now.
        self.catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            Some(ShardMigrationStatus::Stable),
            None, // Clear target nodes
            None, // Clear source nodes
        ).await.map_err(|e| ShardMigrationError::CatalogUpdateError(shard_id, e.to_string()))?;
        Ok(())
    }

    async fn rollback_migration(&self, db_name: &str, table_name: &str, shard_id: ShardId, reason: &str) -> Result<(), ShardMigrationError> {
        observability_deps::tracing::info!(
            shard_id = shard_id.get(),
            %db_name,
            %table_name,
            %reason,
            "(NoOpShardMigrator) Rollback migration called."
        );
        // Conceptual: Revert catalog changes, clean up target node.
        // Set status to Failed or back to Stable on original source.
        self.catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            Some(ShardMigrationStatus::Failed), // Or Stable, depending on rollback strategy
            None, // Clear target nodes
            None, // Clear source nodes
        ).await.map_err(|e| ShardMigrationError::CatalogUpdateError(shard_id, e.to_string()))?;
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
        let (migrator, catalog) = setup_noop_migrator().await;
        let db_name = "test_db_prep";
        let table_name = "test_table_prep";
        let shard_id = ShardId::new(1);
        let source_node = "nodeA"; // String, will be parsed to ComputeNodeId
        let target_node = "nodeB"; // String, will be parsed to ComputeNodeId

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag"], &[("field", influxdb3_catalog::log::FieldDataType::Integer)]).await.unwrap();
        // Create a dummy shard so update_shard_migration_state can find it
        let initial_shard_def = influxdb3_catalog::shard::ShardDefinition::new(
            shard_id,
            influxdb3_catalog::shard::ShardTimeRange::new(0, i64::MAX),
            vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()]
        );
        catalog.create_shard(db_name, table_name, initial_shard_def).await.unwrap();


        let res = migrator.prepare_migration(db_name, table_name, shard_id, source_node, target_node).await;
        assert!(res.is_ok(), "prepare_migration failed: {:?}", res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let shard_info = table_def.shards.get_by_id(&shard_id).unwrap();

        assert_eq!(shard_info.migration_status, Some(ShardMigrationStatus::Preparing));
        assert_eq!(shard_info.migration_target_node_ids, Some(vec![influxdb3_id::NodeId::new_from_str(target_node).unwrap()]));
        assert_eq!(shard_info.migration_source_node_ids, Some(vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()]));
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
        let (migrator, catalog) = setup_noop_migrator().await;
        let db_name = "test_db_finalize";
        let table_name = "test_table_finalize";
        let shard_id = ShardId::new(1);
        let source_node = "nodeA";
        let target_node = "nodeB";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag"], &[("field", influxdb3_catalog::log::FieldDataType::Integer)]).await.unwrap();
        let initial_shard_def = influxdb3_catalog::shard::ShardDefinition {
            shard_id,
            time_range: influxdb3_catalog::shard::ShardTimeRange::new(0, i64::MAX),
            node_ids: vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()], // Initially on source
            table_name: Arc::from(table_name),
            db_name: Arc::from(db_name),
            migration_status: Some(ShardMigrationStatus::AwaitingCutover), // Pre-condition for finalize
            migration_target_node_ids: Some(vec![influxdb3_id::NodeId::new_from_str(target_node).unwrap()]),
            migration_source_node_ids: Some(vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()]),
            version: 1,
        };
        // Directly insert or update shard to this state, create_shard might reset some fields.
        // For simplicity, we'll use create_shard then update_shard_migration_state if needed,
        // but ideally the test setup would directly place the shard in the AwaitingCutover state.
        // Here, create_shard and then an update_shard_migration_state to set it up.
        let base_shard_def = influxdb3_catalog::shard::ShardDefinition::new(
            shard_id,
            influxdb3_catalog::shard::ShardTimeRange::new(0, i64::MAX),
            vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()]
        );
        catalog.create_shard(db_name, table_name, base_shard_def).await.unwrap();
        catalog.update_shard_migration_state(
            db_name, table_name, shard_id,
            Some(ShardMigrationStatus::AwaitingCutover),
            Some(vec![influxdb3_id::NodeId::new_from_str(target_node).unwrap()]),
            Some(vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()])
        ).await.unwrap();


        let res = migrator.finalize_migration(db_name, table_name, shard_id).await;
        assert!(res.is_ok(), "finalize_migration failed: {:?}", res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let shard_info = table_def.shards.get_by_id(&shard_id).unwrap();

        assert_eq!(shard_info.migration_status, Some(ShardMigrationStatus::Stable));
        assert!(shard_info.migration_target_node_ids.is_none());
        assert!(shard_info.migration_source_node_ids.is_none());
        // Note: This NoOp finalize doesn't change ShardDefinition.node_ids itself.
    }

    #[tokio::test]
    async fn test_noop_shard_migrator_rollback_migration() {
        let (migrator, catalog) = setup_noop_migrator().await;
        let db_name = "test_db_rollback";
        let table_name = "test_table_rollback";
        let shard_id = ShardId::new(1);
        let source_node = "nodeA";
        let target_node = "nodeB";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag"], &[("field", influxdb3_catalog::log::FieldDataType::Integer)]).await.unwrap();
        let initial_shard_def = influxdb3_catalog::shard::ShardDefinition::new(
            shard_id,
            influxdb3_catalog::shard::ShardTimeRange::new(0, i64::MAX),
            vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()]
        );
        catalog.create_shard(db_name, table_name, initial_shard_def).await.unwrap();
        // Simulate it was in a migrating state
         catalog.update_shard_migration_state(
            db_name, table_name, shard_id,
            Some(ShardMigrationStatus::StreamingData),
            Some(vec![influxdb3_id::NodeId::new_from_str(target_node).unwrap()]),
            Some(vec![influxdb3_id::NodeId::new_from_str(source_node).unwrap()])
        ).await.unwrap();

        let res = migrator.rollback_migration(db_name, table_name, shard_id, "test rollback reason").await;
        assert!(res.is_ok(), "rollback_migration failed: {:?}", res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let shard_info = table_def.shards.get_by_id(&shard_id).unwrap();

        assert_eq!(shard_info.migration_status, Some(ShardMigrationStatus::Failed));
        assert!(shard_info.migration_target_node_ids.is_none());
        assert!(shard_info.migration_source_node_ids.is_none());
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
