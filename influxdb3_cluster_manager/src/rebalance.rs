use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::CatalogError;
use influxdb3_id::{ShardId, NodeId};
use std::sync::Arc;
use thiserror::Error;
use observability_deps::tracing::{info, error, debug}; // Added debug

#[derive(Debug, Error)]
pub enum RebalanceError {
    #[error("Catalog error: {0}")]
    CatalogError(#[from] CatalogError),

    #[error("Database not found: {0}")]
    DbNotFound(String),

    #[error("Table not found: {db_name}.{table_name}")]
    TableNotFound { db_name: String, table_name: String },

    #[error("Shard not found: {shard_id:?} in table {db_name}.{table_name}")]
    ShardNotFound {
        shard_id: ShardId,
        db_name: String,
        table_name: String,
    },
    #[error("Shard {shard_id:?} has no assigned nodes in catalog")]
    ShardHasNoNodes { shard_id: ShardId },
}

/// Initiates the conceptual move of a shard by setting its status to "MigratingSnapshot".
pub async fn initiate_shard_move_conceptual(
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
    _target_node_id: NodeId, // Kept for context, not used for state change here
) -> Result<(), RebalanceError> {
    debug!(
        %db_name,
        %table_name,
        ?shard_id,
        ?_target_node_id,
        "Attempting to initiate conceptual move for shard (set to MigratingSnapshot)"
    );

    let db_schema = catalog
        .db_schema(db_name)
        .ok_or_else(|| RebalanceError::DbNotFound(db_name.to_string()))?;
    let table_def = db_schema
        .table_definition(table_name)
        .ok_or_else(|| RebalanceError::TableNotFound {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        })?;
    let shard_def = table_def.shards.get_by_id(&shard_id)
        .ok_or_else(|| RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        })?;

    let old_status = shard_def.status.clone();

    match catalog
        .update_shard_metadata(db_name, table_name, shard_id, Some("MigratingSnapshot".to_string()), None)
        .await
    {
        Ok(_) => {
            info!(
                shard_id = %shard_id.get(),
                database_name = %db_name,
                table_name = %table_name,
                old_status = %old_status,
                new_status = "MigratingSnapshot",
                "Shard {}:{}/{} state: {} -> MigratingSnapshot. Conceptual: Source node starts creating/copying base Parquet snapshot.",
                db_name, table_name, shard_id.get(), old_status
            );
            Ok(())
        }
        Err(CatalogError::DbNotFound(db)) => Err(RebalanceError::DbNotFound(db)),
        Err(CatalogError::TableNotFound { db_name: d, table_name: t, .. }) => Err(RebalanceError::TableNotFound {
            db_name: d.into_string(),
            table_name: t.into_string(),
        }),
        Err(CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                %db_name, %table_name, ?shard_id, error = %e,
                "Failed to update shard metadata to 'MigratingSnapshot' in catalog."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}

/// Marks the conceptual completion of base snapshot data transfer for a shard.
pub async fn complete_shard_snapshot_transfer_conceptual( // Renamed
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
) -> Result<(), RebalanceError> {
    debug!(
        %db_name, %table_name, ?shard_id,
        "Attempting to complete conceptual snapshot transfer for shard (set to MigratingWAL)."
    );

    match catalog
        .update_shard_metadata(db_name, table_name, shard_id, Some("MigratingWAL".to_string()), None)
        .await
    {
        Ok(_) => {
            info!(
                shard_id = %shard_id.get(),
                database_name = %db_name,
                table_name = %table_name,
                old_status = "MigratingSnapshot", // Assumed previous state
                new_status = "MigratingWAL",
                "Shard {}:{}/{} state: MigratingSnapshot -> MigratingWAL. Conceptual: Base snapshot copied. Source node starts streaming new WAL entries to target.",
                db_name, table_name, shard_id.get()
            );
            Ok(())
        }
        Err(CatalogError::DbNotFound(db)) => Err(RebalanceError::DbNotFound(db)),
        Err(CatalogError::TableNotFound { db_name: d, table_name: t, .. }) => Err(RebalanceError::TableNotFound {
            db_name: d.into_string(),
            table_name: t.into_string(),
        }),
        Err(CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                %db_name, %table_name, ?shard_id, error = %e,
                "Failed to update shard metadata to 'MigratingWAL' in catalog."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}

/// Marks the conceptual completion of WAL synchronization for a shard.
pub async fn complete_shard_wal_sync_conceptual(
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
) -> Result<(), RebalanceError> {
    debug!(
        %db_name, %table_name, ?shard_id,
        "Attempting to complete conceptual WAL sync for shard (set to AwaitingCutover)."
    );
    match catalog
        .update_shard_metadata(db_name, table_name, shard_id, Some("AwaitingCutover".to_string()), None)
        .await
    {
        Ok(_) => {
            info!(
                shard_id = %shard_id.get(),
                database_name = %db_name,
                table_name = %table_name,
                old_status = "MigratingWAL", // Assumed previous state
                new_status = "AwaitingCutover",
                "Shard {}:{}/{} state: MigratingWAL -> AwaitingCutover. Conceptual: WAL synced. Target node caught up. Ready for cutover.",
                db_name, table_name, shard_id.get()
            );
            Ok(())
        }
        Err(CatalogError::DbNotFound(db)) => Err(RebalanceError::DbNotFound(db)),
        Err(CatalogError::TableNotFound { db_name: d, table_name: t, .. }) => Err(RebalanceError::TableNotFound {
            db_name: d.into_string(),
            table_name: t.into_string(),
        }),
        Err(CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                %db_name, %table_name, ?shard_id, error = %e,
                "Failed to update shard metadata to 'AwaitingCutover' in catalog."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}


/// Conceptually completes the cutover for a shard to a target node.
pub async fn complete_shard_cutover_conceptual(
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
    target_node_id: NodeId,
) -> Result<(), RebalanceError> {
    info!(
        shard_id = %shard_id.get(),
        database_name = %db_name,
        table_name = %table_name,
        target_node_id = %target_node_id.get(),
        old_status = "AwaitingCutover", // Assumed previous state
        new_status = "Stable",
        "Shard {}:{}/{} state: AwaitingCutover -> Stable (on target {}). Conceptual: Locking shard, updating catalog, redirecting writes.",
        db_name, table_name, shard_id.get(), target_node_id.get()
    );

    match catalog
        .update_shard_metadata(db_name, table_name, shard_id, Some("Stable".to_string()), Some(vec![target_node_id]))
        .await
    {
        Ok(_) => {
            info!(
                shard_id = %shard_id.get(),
                database_name = %db_name,
                table_name = %table_name,
                target_node_id = %target_node_id.get(),
                current_status = "Stable",
                "Shard {}:{}/{} state: Stable on target {}. Conceptual: Cutover complete. Old shard on source pending cleanup.",
                db_name, table_name, shard_id.get(), target_node_id.get()
            );
            Ok(())
        }
        Err(CatalogError::DbNotFound(db)) => Err(RebalanceError::DbNotFound(db)),
        Err(CatalogError::TableNotFound { db_name: d, table_name: t, .. }) => Err(RebalanceError::TableNotFound {
            db_name: d.into_string(),
            table_name: t.into_string(),
        }),
        Err(CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                %db_name, %table_name, ?shard_id, ?target_node_id, error = %e,
                "Failed to update shard metadata to 'Stable' and assign to target node in catalog."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}

/// Conceptually completes the cleanup phase for a shard on the source node.
pub async fn complete_shard_cleanup_conceptual(
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
    source_node_id: NodeId, // Added parameter
) -> Result<(), RebalanceError> {
    info!(
        shard_id = %shard_id.get(),
        database_name = %db_name,
        table_name = %table_name,
        source_node_id = %source_node_id.get(),
        new_status = "Cleaned",
        "Shard {}:{}/{} state: -> Cleaned (on source {}). Conceptual: Deleting data from source node.",
        db_name, table_name, shard_id.get(), source_node_id.get()
    );

    // Note: The node_ids list for the shard in the catalog should reflect the target node.
    // This operation primarily updates the status to "Cleaned" to indicate the source node's
    // cleanup is done. It does not change node_ids here.
    match catalog
        .update_shard_metadata(db_name, table_name, shard_id, Some("Cleaned".to_string()), None)
        .await
    {
        Ok(_) => {
            info!(
                shard_id = %shard_id.get(),
                database_name = %db_name,
                table_name = %table_name,
                source_node_id = %source_node_id.get(),
                current_status = "Cleaned",
                "Shard {}:{}/{} state: Cleaned on source {}. Conceptual: Cleanup complete.",
                db_name, table_name, shard_id.get(), source_node_id.get()
            );
            Ok(())
        }
        Err(CatalogError::DbNotFound(db)) => Err(RebalanceError::DbNotFound(db)),
        Err(CatalogError::TableNotFound { db_name: d, table_name: t, .. }) => Err(RebalanceError::TableNotFound {
            db_name: d.into_string(),
            table_name: t.into_string(),
        }),
        Err(CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                %db_name, %table_name, ?shard_id, error = %e,
                "Failed to update shard metadata to 'Cleaned' in catalog."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::CatalogArgs;
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_catalog::shard::{ShardDefinition, ShardTimeRange};
    use iox_time::MockProvider;
    use object_store::memory::InMemory;

    async fn setup_catalog() -> Arc<Catalog> {
        let object_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        Arc::new(
            Catalog::new_with_args(
                "test_node_rebalance",
                object_store,
                time_provider,
                metrics,
                Default::default(), // CatalogArgs
                Default::default(), // CatalogLimits
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_shard_migration_lifecycle() {
        let catalog = setup_catalog().await;
        let db_name = "lifecycle_db";
        let table_name = "lifecycle_table";
        let shard_id = ShardId::new(1);
        let initial_node = NodeId::new(100);
        let target_node = NodeId::new(200);
        let time_provider = catalog.time_provider();

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagL"], &[(String::from("fieldL"), FieldDataType::Integer)]).await.unwrap();

        let created_at_ts = time_provider.now().timestamp_nanos();
        tokio::time::sleep(std::time::Duration::from_nanos(10)).await; // Ensure time advances

        let shard_def = ShardDefinition {
            id: shard_id,
            time_range: ShardTimeRange { start_time: 0, end_time: 1000 },
            node_ids: vec![initial_node],
            status: "Stable".to_string(),
            updated_at_ts: Some(created_at_ts),
        };
        catalog.create_shard(db_name, table_name, shard_def).await.unwrap();
        let initial_shard_state = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(initial_shard_state.status, "Stable");
        assert_eq!(initial_shard_state.updated_at_ts, Some(created_at_ts));


        // 1. Initiate shard move
        let result_initiate = initiate_shard_move_conceptual(
            Arc::clone(&catalog), db_name, table_name, shard_id, target_node,
        ).await;
        assert!(result_initiate.is_ok());
        let shard_after_initiate = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_after_initiate.status, "MigratingSnapshot");
        assert_eq!(shard_after_initiate.node_ids, vec![initial_node]);
        assert!(shard_after_initiate.updated_at_ts.unwrap() > created_at_ts);
        let initiate_ts = shard_after_initiate.updated_at_ts.unwrap();

        // 2. Complete snapshot transfer
        let result_snapshot_transfer = complete_shard_snapshot_transfer_conceptual(
            Arc::clone(&catalog), db_name, table_name, shard_id,
        ).await;
        assert!(result_snapshot_transfer.is_ok());
        let shard_after_snapshot_transfer = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_after_snapshot_transfer.status, "MigratingWAL");
        assert_eq!(shard_after_snapshot_transfer.node_ids, vec![initial_node]);
        assert!(shard_after_snapshot_transfer.updated_at_ts.unwrap() > initiate_ts);
        let snapshot_transfer_ts = shard_after_snapshot_transfer.updated_at_ts.unwrap();

        // 3. Complete WAL sync
        let result_wal_sync = complete_shard_wal_sync_conceptual(
            Arc::clone(&catalog), db_name, table_name, shard_id,
        ).await;
        assert!(result_wal_sync.is_ok());
        let shard_after_wal_sync = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_after_wal_sync.status, "AwaitingCutover");
        assert_eq!(shard_after_wal_sync.node_ids, vec![initial_node]);
        assert!(shard_after_wal_sync.updated_at_ts.unwrap() > snapshot_transfer_ts);
        let wal_sync_ts = shard_after_wal_sync.updated_at_ts.unwrap();

        // 4. Complete cutover
        let result_cutover = complete_shard_cutover_conceptual(
            Arc::clone(&catalog), db_name, table_name, shard_id, target_node,
        ).await;
        assert!(result_cutover.is_ok());
        let shard_after_cutover = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_after_cutover.status, "Stable");
        assert_eq!(shard_after_cutover.node_ids, vec![target_node]);
        assert!(shard_after_cutover.updated_at_ts.unwrap() > wal_sync_ts);
        let cutover_ts = shard_after_cutover.updated_at_ts.unwrap();

        // 5. Complete cleanup (on source node, which was `initial_node`)
        let result_cleanup = complete_shard_cleanup_conceptual(
            Arc::clone(&catalog), db_name, table_name, shard_id, initial_node, // Pass initial_node as source
        ).await;
        assert!(result_cleanup.is_ok());
        let shard_after_cleanup = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_after_cleanup.status, "Cleaned");
        assert_eq!(shard_after_cleanup.node_ids, vec![target_node]); // Node assignment remains target
        assert!(shard_after_cleanup.updated_at_ts.unwrap() > cutover_ts);
    }

    #[tokio::test]
    async fn test_initiate_shard_move_db_not_found() {
        let catalog = setup_catalog().await;
        let result = initiate_shard_move_conceptual(
            catalog, "non_existent_db", "test_table", ShardId::new(1), NodeId::new(1),
        ).await;
        assert!(matches!(result, Err(RebalanceError::DbNotFound(_))));
    }

    #[tokio::test]
    async fn test_initiate_shard_move_table_not_found() {
        let catalog = setup_catalog().await;
        let db_name = "test_db";
        catalog.create_database(db_name).await.unwrap();
        let result = initiate_shard_move_conceptual(
            catalog, db_name, "non_existent_table", ShardId::new(1), NodeId::new(1),
        ).await;
        assert!(matches!(result, Err(RebalanceError::TableNotFound { .. })));
    }

    #[tokio::test]
    async fn test_initiate_shard_move_shard_not_found() {
        let catalog = setup_catalog().await;
        let db_name = "test_db";
        let table_name = "test_table";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag1"], &[(String::from("field1"), FieldDataType::Integer)]).await.unwrap();
        let result = initiate_shard_move_conceptual(
            catalog, db_name, table_name, ShardId::new(999), NodeId::new(1),
        ).await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }

    #[tokio::test]
    async fn test_complete_snapshot_transfer_shard_not_found() { // Renamed test
        let catalog = setup_catalog().await;
        let db_name = "test_db_snap_err";
        let table_name = "test_table_snap_err";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagE"], &[(String::from("fieldE"), FieldDataType::String)]).await.unwrap();
        let result = complete_shard_snapshot_transfer_conceptual( // Renamed function
            catalog, db_name, table_name, ShardId::new(888),
        ).await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }

    #[tokio::test]
    async fn test_complete_wal_sync_shard_not_found() { // New test
        let catalog = setup_catalog().await;
        let db_name = "test_db_wal_err";
        let table_name = "test_table_wal_err";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagW"], &[(String::from("fieldW"), FieldDataType::String)]).await.unwrap();
        let result = complete_shard_wal_sync_conceptual(
            catalog, db_name, table_name, ShardId::new(887),
        ).await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }

    #[tokio::test]
    async fn test_complete_cutover_shard_not_found() {
        let catalog = setup_catalog().await;
        let db_name = "test_db_cutover_err";
        let table_name = "test_table_cutover_err";
        let target_node = NodeId::new(300);
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagF"], &[(String::from("fieldF"), FieldDataType::Boolean)]).await.unwrap();
        let result = complete_shard_cutover_conceptual(
            catalog, db_name, table_name, ShardId::new(777), target_node,
        ).await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }

    #[tokio::test]
    async fn test_complete_cleanup_shard_not_found() {
        let catalog = setup_catalog().await;
        let db_name = "test_db_cleanup_err";
        let table_name = "test_table_cleanup_err";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagG"], &[(String::from("fieldG"), FieldDataType::Float)]).await.unwrap();
        let result = complete_shard_cleanup_conceptual(
            catalog, db_name, table_name, ShardId::new(666), NodeId::new(400), // Pass source_node_id
        ).await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }
}
