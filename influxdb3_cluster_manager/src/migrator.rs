use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{NodeId, ShardId};
use std::sync::Arc;
use crate::rebalance::{
    initiate_shard_move_conceptual,
    complete_shard_snapshot_transfer_conceptual, // Renamed function
    complete_shard_wal_sync_conceptual,        // New function
    complete_shard_cutover_conceptual,
    complete_shard_cleanup_conceptual,
    // RebalanceError is implicitly handled by `ClusterManagerError::from`
};
use crate::error::ClusterManagerError;
use observability_deps::tracing::{info, error, debug};

#[derive(Debug, Clone)]
pub struct ShardMigrationJob {
    pub db_name: String,
    pub table_name: String,
    pub shard_id: ShardId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
}

pub struct ShardMigrator {
    catalog: Arc<Catalog>,
}

impl ShardMigrator {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn run_migration_job(&self, job: ShardMigrationJob) -> Result<(), ClusterManagerError> {
        info!("Starting migration job: {:?}", job);

        let db_name = &job.db_name;
        let table_name = &job.table_name;
        let shard_id = job.shard_id;

        // Fetch initial shard status for logging context if needed, or rely on individual function logs
        let initial_shard_def = self.catalog.db_schema(db_name)
            .and_then(|db| db.table_definition(table_name))
            .and_then(|tbl| tbl.shards.get_by_id(&shard_id));

        let old_status = initial_shard_def.map_or_else(
            || "Unknown (shard not found initially)".to_string(),
            |s_def| s_def.status.clone()
        );
        debug!("Initial status for shard {}:{}/{} is '{}'", db_name, table_name, shard_id.get(), old_status);


        // 1. Initiate move (set status to MigratingSnapshot)
        initiate_shard_move_conceptual(
            Arc::clone(&self.catalog),
            db_name,
            table_name,
            shard_id,
            job.target_node_id,
        )
        .await.map_err(ClusterManagerError::from)?;
        // Log is already inside initiate_shard_move_conceptual including old_status if fetched there.
        // If not, we can add a more specific one here using the `old_status` fetched above.
        info!("Job {:?}: Initiated. Status target: MigratingSnapshot.", job.shard_id);


        // 2. Complete snapshot transfer (set status to MigratingWAL)
        complete_shard_snapshot_transfer_conceptual(
            Arc::clone(&self.catalog),
            db_name,
            table_name,
            shard_id,
        )
        .await.map_err(ClusterManagerError::from)?;
        info!("Job {:?}: Snapshot transfer complete. Status target: MigratingWAL.", job.shard_id);


        // 3. Complete WAL sync (set status to AwaitingCutover)
        complete_shard_wal_sync_conceptual(
            Arc::clone(&self.catalog),
            db_name,
            table_name,
            shard_id,
        )
        .await.map_err(ClusterManagerError::from)?;
        info!("Job {:?}: WAL sync complete. Status target: AwaitingCutover.", job.shard_id);


        // 4. Complete cutover (update shard owner to target_node_id, set status to Stable)
        complete_shard_cutover_conceptual(
            Arc::clone(&self.catalog),
            db_name,
            table_name,
            shard_id,
            job.target_node_id,
        )
        .await.map_err(ClusterManagerError::from)?;
        // Log is already inside complete_shard_cutover_conceptual
        info!("Job {:?}: Cutover complete. New owner: {}. Status target: Stable.", job.shard_id, job.target_node_id.get());


        // 5. Complete cleanup on source node (set status to Cleaned)
        complete_shard_cleanup_conceptual(
            Arc::clone(&self.catalog),
            db_name,
            table_name,
            shard_id,
            job.source_node_id,
        )
        .await.map_err(ClusterManagerError::from)?;
        // Log is already inside complete_shard_cleanup_conceptual
        info!("Job {:?}: Cleanup complete on source {}. Status target: Cleaned.", job.shard_id, job.source_node_id.get());

        info!("Successfully completed migration job: {:?}", job);
        Ok(())
    }
}
} // This closes the impl ShardMigrator block. Add tests after this.

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::{Catalog, CatalogArgs}; // Corrected path for CatalogArgs
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_catalog::shard::{ShardDefinition, ShardTimeRange};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn setup_catalog_for_migrator_tests() -> Arc<Catalog> {
        let object_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        Arc::new(
            Catalog::new_with_args(
                "test_node_migrator", // Unique node ID for these tests
                object_store,
                time_provider,
                metrics,
                CatalogArgs::default(),
                Default::default(), // CatalogLimits
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_run_migration_job_full_lifecycle() {
        let catalog = setup_catalog_for_migrator_tests().await;
        let db_name = "mig_db".to_string();
        let table_name = "mig_table".to_string();
        let shard_id = ShardId::new(10);
        let source_node_id = NodeId::new(100);
        let target_node_id = NodeId::new(200);

        // Initial catalog setup
        catalog.create_database(&db_name).await.unwrap();
        catalog.create_table(&db_name, &table_name, &["tagM"], &[(String::from("fieldM"), FieldDataType::Float)]).await.unwrap();

        let initial_ts = catalog.time_provider().now().timestamp_nanos();
        tokio::time::sleep(std::time::Duration::from_nanos(100)).await; // Ensure time advances

        let shard_def = ShardDefinition {
            id: shard_id,
            time_range: ShardTimeRange { start_time: 0, end_time: 1000 },
            node_ids: vec![source_node_id], // Initially on source node
            status: "Stable".to_string(),
            updated_at_ts: Some(initial_ts),
        };
        catalog.create_shard(&db_name, &table_name, shard_def).await.unwrap();

        // Create migration job and migrator
        let job = ShardMigrationJob {
            db_name: db_name.clone(),
            table_name: table_name.clone(),
            shard_id,
            source_node_id,
            target_node_id,
        };
        let migrator = ShardMigrator::new(Arc::clone(&catalog));

        // Run the migration job
        let result = migrator.run_migration_job(job).await;
        assert!(result.is_ok(), "Migration job failed: {:?}", result.err());

        // Verify final state in catalog
        let final_shard_def = catalog.db_schema(&db_name)
            .unwrap().table_definition(&table_name)
            .unwrap().shards.get_by_id(&shard_id).unwrap();

        assert_eq!(final_shard_def.status, "Cleaned");
        assert_eq!(final_shard_def.node_ids, vec![target_node_id]); // Should be on target node
        assert!(final_shard_def.updated_at_ts.unwrap() > initial_ts);
        // Individual step status checks could be done by having the rebalance functions
        // return the new ShardDefinition or by repeatedly querying catalog if these functions
        // were individually awaitable and testable, but run_migration_job runs them sequentially.
        // For a true unit test of run_migration_job, we'd mock the rebalance functions.
        // Given they are conceptual and just update catalog, this end-to-end check of status is okay for now.
    }

    #[tokio::test]
    async fn test_run_migration_job_error_handling() {
        let catalog = setup_catalog_for_migrator_tests().await;
        let db_name = "err_db".to_string();
        let table_name = "err_table".to_string();
        let shard_id_exists = ShardId::new(20);
        let shard_id_not_exists = ShardId::new(21);
        let source_node_id = NodeId::new(300);
        let target_node_id = NodeId::new(400);

        // Setup
        catalog.create_database(&db_name).await.unwrap();
        catalog.create_table(&db_name, &table_name, &["tagE"], &[]).await.unwrap();
        let shard_def = ShardDefinition {
            id: shard_id_exists,
            time_range: ShardTimeRange { start_time: 0, end_time: 1000 },
            node_ids: vec![source_node_id], status: "Stable".to_string(), updated_at_ts: None,
        };
        catalog.create_shard(&db_name, &table_name, shard_def).await.unwrap();

        // Job that will fail because shard does not exist at initiation step
        let job_shard_not_found = ShardMigrationJob {
            db_name: db_name.clone(),
            table_name: table_name.clone(),
            shard_id: shard_id_not_exists, // This shard doesn't exist
            source_node_id,
            target_node_id,
        };
        let migrator = ShardMigrator::new(Arc::clone(&catalog));

        let result = migrator.run_migration_job(job_shard_not_found).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            ClusterManagerError::RebalanceError(RebalanceError::ShardNotFound { shard_id, .. }) => {
                assert_eq!(shard_id, shard_id_not_exists);
            }
            other_error => panic!("Expected ShardNotFound, got {:?}", other_error),
        }

        // Conceptual: To test errors in later stages, one would need to mock the earlier
        // rebalance functions to succeed and a later one to fail.
        // For example, if complete_shard_snapshot_transfer_conceptual failed:
        // - initiate_shard_move_conceptual would succeed.
        // - complete_shard_snapshot_transfer_conceptual would be mocked to return Err(...).
        // - run_migration_job should then propagate that specific error.
        // This requires a more complex mocking setup for the rebalance module functions.
    }
}
