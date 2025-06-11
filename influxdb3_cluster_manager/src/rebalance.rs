use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::CatalogError;
use influxdb3_id::{ShardId, NodeId};
use std::sync::Arc;
use thiserror::Error;
use observability_deps::tracing::{info, error};

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
    // Add other specific rebalancing errors here if needed
}

/// Conceptually initiates a shard move by updating its ownership in the catalog.
///
/// This is a rudimentary implementation. A real implementation would involve:
/// - Setting a migration status on the shard.
/// - Orchestrating data transfer (e.g., WAL streaming, Parquet file copy via object store).
/// - A multi-phase commit process for updating the catalog and ensuring consistency.
/// - Handling writes and reads to the shard during migration.
/// - Cleanup of data on the source node.
///
/// For now, this function simply updates the `node_ids` of the ShardDefinition
/// to the `target_node_id`, implying the target node now owns the shard.
pub async fn initiate_shard_move_conceptual(
    catalog: Arc<Catalog>,
    db_name: &str,
    table_name: &str,
    shard_id: ShardId,
    target_node_id: NodeId,
) -> Result<(), RebalanceError> {
    info!(
        db_name,
        table_name,
        ?shard_id,
        ?target_node_id,
        "Attempting to initiate conceptual move for shard"
    );

    // 1. Retrieve DbId and TableId (implicitly handled by catalog methods)
    // Ensure DB and Table exist first to provide clearer errors
    let db_schema = catalog
        .db_schema(db_name)
        .ok_or_else(|| RebalanceError::DbNotFound(db_name.to_string()))?;

    let table_def = db_schema
        .table_definition(table_name)
        .ok_or_else(|| RebalanceError::TableNotFound {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        })?;

    // 3. Find the ShardDefinition
    // The catalog's update_shard_nodes will internally verify if the shard exists.
    // If it doesn't, it will return a CatalogError which will be converted.
    // To provide a more specific ShardNotFound error from here, we could pre-fetch.
    if table_def.shards.get_by_id(&shard_id).is_none() {
        return Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        });
    }

    // 4. Log intent (already done above)

    // 5. Call catalog.update_shard_nodes()
    // This method replaces the existing node_ids list with the new one.
    // For a single-owner model per shard (or if target_node_id is meant to be the sole new owner),
    // this is conceptually what we want for this rudimentary step.
    // In a replicated scenario, this would be more complex (e.g. add target, remove source later).
    match catalog
        .update_shard_nodes(db_name, table_name, shard_id, vec![target_node_id])
        .await
    {
        Ok(_) => {
            // 6. Log success
            info!(
                db_name,
                table_name,
                ?shard_id,
                ?target_node_id,
                "Successfully updated catalog for shard to be owned by target node. Conceptual data migration would follow."
            );
            Ok(())
        }
        Err(e @ CatalogError::TableNotFound { .. }) => Err(RebalanceError::TableNotFound {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e @ CatalogError::ShardNotFound { .. }) => Err(RebalanceError::ShardNotFound {
            shard_id,
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }),
        Err(e) => {
            error!(
                db_name,
                table_name,
                ?shard_id,
                ?target_node_id,
                error = %e,
                "Failed to update shard nodes in catalog during conceptual move."
            );
            Err(RebalanceError::CatalogError(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::CatalogArgs; // For Catalog::new_with_args if needed for limits
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
    async fn test_initiate_shard_move_success() {
        let catalog = setup_catalog().await;
        let db_name = "test_db";
        let table_name = "test_table";
        let shard_id = ShardId::new(1);
        let initial_node = NodeId::new(100);
        let target_node = NodeId::new(200);

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag1"], &[(String::from("field1"), FieldDataType::Integer)]).await.unwrap();

        let shard_def = ShardDefinition::new(
            shard_id,
            ShardTimeRange { start_time: 0, end_time: 1000 },
            vec![initial_node],
        );
        catalog.create_shard(db_name, table_name, shard_def).await.unwrap();

        let result = initiate_shard_move_conceptual(
            Arc::clone(&catalog),
            db_name,
            table_name,
            shard_id,
            target_node,
        )
        .await;
        assert!(result.is_ok());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let updated_shard = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(updated_shard.node_ids, vec![target_node]);
    }

    #[tokio::test]
    async fn test_initiate_shard_move_db_not_found() {
        let catalog = setup_catalog().await;
        let result = initiate_shard_move_conceptual(
            catalog,
            "non_existent_db",
            "test_table",
            ShardId::new(1),
            NodeId::new(1),
        )
        .await;
        assert!(matches!(result, Err(RebalanceError::DbNotFound(_))));
    }

    #[tokio::test]
    async fn test_initiate_shard_move_table_not_found() {
        let catalog = setup_catalog().await;
        let db_name = "test_db";
        catalog.create_database(db_name).await.unwrap();

        let result = initiate_shard_move_conceptual(
            catalog,
            db_name,
            "non_existent_table",
            ShardId::new(1),
            NodeId::new(1),
        )
        .await;
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
            catalog,
            db_name,
            table_name,
            ShardId::new(999), // Non-existent shard
            NodeId::new(1),
        )
        .await;
        assert!(matches!(result, Err(RebalanceError::ShardNotFound { .. })));
    }
}
