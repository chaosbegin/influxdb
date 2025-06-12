#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex; // To avoid conflict with parking_lot::Mutex if Catalog uses it internally for tests
    use std::collections::HashMap;

    use crate::cluster::api::*;
    use crate::cluster::manager::{
        ClusterManager, ClusterManagerError, InMemoryClusterManager, NodeInfo,
        NodeStatus as ClusterManagerNodeStatus, // This is the status within ClusterManager
    };
    use influxdb3_catalog::catalog::{Catalog, TestCatalog}; // Using TestCatalog for easier setup
    use influxdb3_catalog::node::{
        NodeDefinition as CatalogNodeDefinition,
        NodeStatus as CatalogNodeStatus, // This is the status within Catalog
    };
    use influxdb3_catalog::shard::{ShardDefinition, ShardId, ShardMigrationStatus, ShardTimeRange};
    use influxdb3_id::{NodeId as ComputeNodeId, DbId, TableId};
    use iox_time::{MockProvider, Time};
    use crate::cluster::migration::{NoOpShardMigrator, ShardMigrator, MigrationProgress}; // Added for new tests

    // Helper to create a default TestCatalog
    async fn default_test_catalog() -> Arc<Catalog> {
        TestCatalog::new_empty().await
    }

    // Helper to create a default InMemoryClusterManager
    fn default_cluster_manager() -> Arc<InMemoryClusterManager> {
        Arc::new(InMemoryClusterManager::new(Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)))))
    }

    #[tokio::test]
    async fn test_register_new_node() {
        let catalog = default_test_catalog().await;
        let cluster_manager = default_cluster_manager();
        let node_id = "test-node-1".to_string();
        let rpc_address = "http://localhost:8083".to_string();

        let result = register_new_node(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
            node_id.clone(),
            rpc_address.clone(),
        )
        .await;
        assert!(result.is_ok(), "register_new_node failed: {:?}", result.err());

        // Verify in ClusterManager
        let cm_node_info = cluster_manager.get_node(&node_id).await.unwrap().unwrap();
        assert_eq!(cm_node_info.id, node_id);
        assert_eq!(cm_node_info.rpc_address, rpc_address);
        assert_eq!(cm_node_info.status, ClusterManagerNodeStatus::Active); // InMemoryClusterManager sets to Active

        // Verify in Catalog
        let cat_node_info = catalog.get_cluster_node_meta(&node_id).unwrap();
        assert_eq!(cat_node_info.id, node_id);
        assert_eq!(cat_node_info.rpc_address, rpc_address);
        assert_eq!(cat_node_info.status, CatalogNodeStatus::Joining);
    }

    #[tokio::test]
    async fn test_register_existing_node_in_catalog_different_status() {
        // Test re-registration if node is already in catalog but as 'Down' or 'Leaving'
        // (This specific behavior was in a previous iteration of apply_cluster_management_batch,
        //  current version in file might just error. Test will clarify.)
        let catalog = default_test_catalog().await;
        let cluster_manager = default_cluster_manager();
        let node_id = "test-node-reregister".to_string();
        let rpc_address = "http://localhost:9000".to_string();
        let rpc_address_new = "http://localhost:9001".to_string();

        // Initial registration, then simulate it going Down in catalog
        let initial_node_def = CatalogNodeDefinition {
            id: node_id.clone(),
            rpc_address: rpc_address.clone(),
            status: CatalogNodeStatus::Down, // Simulate it was marked Down
            last_heartbeat: None,
        };
        catalog.register_cluster_node_meta(initial_node_def).await.unwrap();

        // Attempt to register again (which might update it if logic allows, or error)
        let result = register_new_node(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
            node_id.clone(),
            rpc_address_new.clone(),
        )
        .await;

        // Based on current apply_cluster_management_batch, this should error if node exists,
        // unless the logic in api::register_new_node or its callees handles this.
        // The current catalog.register_cluster_node_meta will call apply_cluster_management_batch
        // which will attempt to insert if not found, or error if found (unless status is Down/Leaving,
        // in which case it updates in my proposed apply_cluster_management_batch, but the file version is simpler).
        // The version in the file for `apply_cluster_management_batch`'s `RegisterClusterNode` is:
        // `if self.cluster_nodes.contains_name(...) { return Err(NodeAlreadyExists) }`
        // So, this registration attempt should fail.
        assert!(matches!(result, Err(ManagementApiError::Catalog(influxdb3_catalog::CatalogError::NodeAlreadyExists(_)))));
    }


    #[tokio::test]
    async fn test_decommission_cluster_node() {
        let catalog = default_test_catalog().await;
        let cluster_manager = default_cluster_manager();
        let node_id = "test-node-2".to_string();
        let rpc_address = "http://localhost:8084".to_string();

        register_new_node(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
            node_id.clone(),
            rpc_address.clone(),
        )
        .await
        .unwrap();

        let result = decommission_cluster_node(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
            node_id.clone(),
        )
        .await;
        assert!(result.is_ok(), "decommission_cluster_node failed: {:?}", result.err());

        // Verify in ClusterManager
        let cm_node_info = cluster_manager.get_node(&node_id).await.unwrap().unwrap();
        assert_eq!(cm_node_info.status, ClusterManagerNodeStatus::Leaving);

        // Verify in Catalog
        let cat_node_info = catalog.get_cluster_node_meta(&node_id).unwrap();
        assert_eq!(cat_node_info.status, CatalogNodeStatus::Leaving);
    }

    #[tokio::test]
    async fn test_get_cluster_node_info() {
        let cluster_manager = default_cluster_manager();
        let node_id = "test-node-3".to_string();
        let rpc_address = "http://localhost:8085".to_string();

        cluster_manager.register_node(&node_id, &rpc_address).await.unwrap();

        let result = get_cluster_node_info(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            node_id.clone(),
        )
        .await;
        assert!(result.is_ok());
        let node_info = result.unwrap();
        assert_eq!(node_info.id, node_id);
        assert_eq!(node_info.rpc_address, rpc_address);

        // Test Not Found
        let result_not_found = get_cluster_node_info(
            cluster_manager as Arc<dyn ClusterManager>,
            "non-existent-node".to_string(),
        )
        .await;
        assert!(matches!(result_not_found, Err(ManagementApiError::NodeNotFound(_))));
    }

    #[tokio::test]
    async fn test_list_cluster_nodes() {
        let cluster_manager = default_cluster_manager();

        cluster_manager.register_node("node-a", "http://a").await.unwrap();
        cluster_manager.register_node("node-b", "http://b").await.unwrap();

        let result = list_cluster_nodes(cluster_manager as Arc<dyn ClusterManager>).await;
        assert!(result.is_ok());
        let nodes = result.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().any(|n| n.id == "node-a"));
        assert!(nodes.iter().any(|n| n.id == "node-b"));
    }

    #[tokio::test]
    async fn test_get_cluster_status() {
        let catalog = default_test_catalog().await;
        let cluster_manager = default_cluster_manager();

        let result_empty = get_cluster_status(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
        ).await;
        assert!(result_empty.is_ok());
        assert_eq!(result_empty.unwrap(), "Cluster status: No nodes registered.");

        cluster_manager.register_node("node-s1", "http://s1").await.unwrap();
        let result_one = get_cluster_status(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
        ).await;
        assert!(result_one.is_ok());
        let status_one = result_one.unwrap();
        assert!(status_one.contains("Cluster status: 1 nodes registered."));
        assert!(status_one.contains("Node ID: node-s1"));
        assert!(status_one.contains("Status: Active"));
        assert!(status_one.contains("Shard Distribution:")); // Check that shard section starts
        assert!(status_one.contains("No databases found.")); // Initially no dbs
    }

    #[tokio::test]
    async fn test_get_cluster_status_with_shards() {
        let catalog = default_test_catalog().await;
        let cluster_manager = default_cluster_manager();

        // Register nodes in both ClusterManager and Catalog
        let node1_id_str = "node1-status".to_string();
        let node2_id_str = "node2-status".to_string();
        let node1_rpc = "http://node1s:8083".to_string();
        let node2_rpc = "http://node2s:8083".to_string();

        cluster_manager.register_node(&node1_id_str, &node1_rpc).await.unwrap();
        cluster_manager.heartbeat(&node1_id_str, &node1_rpc).await.unwrap(); // Make active
        catalog.register_cluster_node_meta(CatalogNodeDefinition { id: node1_id_str.clone(), rpc_address: node1_rpc.clone(), status: CatalogNodeStatus::Active, last_heartbeat: Some(0) }).await.unwrap();

        cluster_manager.register_node(&node2_id_str, &node2_rpc).await.unwrap();
        catalog.register_cluster_node_meta(CatalogNodeDefinition { id: node2_id_str.clone(), rpc_address: node2_rpc.clone(), status: CatalogNodeStatus::Joining, last_heartbeat: None }).await.unwrap();


        // Setup catalog with a database, table, and shards
        let db_name = "metrics_db";
        let table_name = "cpu_usage";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("value", influxdb3_catalog::log::FieldDataType::Float)]).await.unwrap();

        let shard1_id = ShardId::new(101);
        let shard2_id = ShardId::new(102);
        let node1_compute_id = ComputeNodeId::new_from_str(&node1_id_str).unwrap(); // Assuming node_id can be parsed to ComputeNodeId
        let node2_compute_id = ComputeNodeId::new_from_str(&node2_id_str).unwrap();

        let shard1_def = ShardDefinition::new(shard1_id, ShardTimeRange::new(0, 1000), vec![node1_compute_id]);
        catalog.create_shard(db_name, table_name, shard1_def).await.unwrap();

        let mut shard2_def = ShardDefinition::new(shard2_id, ShardTimeRange::new(1001, 2000), vec![node1_compute_id]);
        // Set shard2 to be migrating
        shard2_def.migration_status = Some(ShardMigrationStatus::StreamingData);
        shard2_def.migration_target_node_ids = Some(vec![node2_compute_id]);
        shard2_def.migration_source_node_ids = Some(vec![node1_compute_id]);
        shard2_def.version = 1; // Simulate an update
        catalog.create_shard(db_name, table_name, shard2_def.clone()).await.unwrap(); // Create first
        // then update its migration state (create_shard might reset migration fields)
        catalog.update_shard_migration_state(db_name, table_name, shard2_id, shard2_def.migration_status.clone(), shard2_def.migration_target_node_ids.clone(), shard2_def.migration_source_node_ids.clone()).await.unwrap();


        let result = get_cluster_status(
            Arc::clone(&cluster_manager) as Arc<dyn ClusterManager>,
            Arc::clone(&catalog),
        ).await;
        assert!(result.is_ok(), "get_cluster_status failed: {:?}", result.err());
        let status_report = result.unwrap();

        println!("Cluster Status Report:\n{}", status_report); // For debugging

        assert!(status_report.contains("Cluster status: 2 nodes registered."));
        assert!(status_report.contains(&format!("Node ID: {}", node1_id_str)));
        assert!(status_report.contains(&format!("Node ID: {}", node2_id_str)));
        assert!(status_report.contains("Shard Distribution:"));
        assert!(status_report.contains(&format!("Database: {}", db_name)));
        assert!(status_report.contains(&format!("Table: {}", table_name)));
        assert!(status_report.contains(&format!("Shard ID: {}", shard1_id.get())));
        assert!(status_report.contains(&format!("Nodes: [{}]", node1_compute_id.get()))); // Shard 1 on node1
        assert!(status_report.contains("Status: Stable")); // Shard 1 default status

        assert!(status_report.contains(&format!("Shard ID: {}", shard2_id.get())));
        assert!(status_report.contains(&format!("Nodes: [{}]", node1_compute_id.get()))); // Shard 2 still on node1 technically
        assert!(status_report.contains("Status: StreamingData"));
        assert!(status_report.contains(&format!("Target: [{}]", node2_compute_id.get())));
        assert!(status_report.contains(&format!("Source: [{}]", node1_compute_id.get())));
        assert!(status_report.contains("Version: 2")); // Version increments on update
    }

    #[tokio::test]
    async fn test_trigger_manual_rebalance() {
        let cluster_manager = default_cluster_manager();
        // InMemoryClusterManager logs "Rebalance initiated"
        // This test just checks if the call succeeds without error.
        // More advanced mocking would be needed to check if logger was called.
        let result = trigger_manual_rebalance(cluster_manager as Arc<dyn ClusterManager>).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_move_shard_manually() {
        let catalog = default_test_catalog().await;
        let shard_migrator = Arc::new(NoOpShardMigrator::new(Arc::clone(&catalog)));

        let db_name = "test_db_move".to_string();
        let table_name = "test_table_move".to_string();
        let shard_id = ShardId::new(5);
        let source_node_id_str = "nodeS".to_string();
        let target_node_id_str = "nodeT".to_string();

        // Register nodes in catalog for validation
        catalog.register_cluster_node_meta(CatalogNodeDefinition { id: source_node_id_str.clone(), rpc_address: "s_addr".into(), status: CatalogNodeStatus::Active, last_heartbeat: None }).await.unwrap();
        catalog.register_cluster_node_meta(CatalogNodeDefinition { id: target_node_id_str.clone(), rpc_address: "t_addr".into(), status: CatalogNodeStatus::Active, last_heartbeat: None }).await.unwrap();

        catalog.db_or_create(&db_name).await.unwrap();
        catalog.create_table(&db_name, &table_name, Default::default()).await.unwrap();

        let source_compute_id = ComputeNodeId::new_from_str(&source_node_id_str).unwrap();
        let target_compute_id = ComputeNodeId::new_from_str(&target_node_id_str).unwrap();

        let shard_def = ShardDefinition {
            shard_id,
            time_range: None,
            node_ids: vec![source_compute_id],
            table_name: Arc::from(table_name.clone()),
            db_name: Arc::from(db_name.clone()),
            migration_status: None,
            migration_target_node_ids: None,
            migration_source_node_ids: None,
            version: 0,
        };
        catalog.create_shard(&db_name, &table_name, shard_def).await.unwrap();

        let result = move_shard_manually(
            shard_migrator,
            Arc::clone(&catalog),
            db_name.clone(),
            table_name.clone(),
            shard_id,
            source_node_id_str.clone(),
            target_node_id_str.clone(),
        )
        .await;

        assert!(result.is_ok(), "move_shard_manually failed: {:?}", result.err());

        // Verify catalog state change (done by NoOpShardMigrator::prepare_migration)
        let db_schema = catalog.db_schema(&db_name).unwrap();
        let table_def = db_schema.table_definition(&table_name).unwrap();
        let updated_shard_def = table_def.shards.get_by_id(&shard_id).unwrap();

        assert_eq!(updated_shard_def.migration_status, Some(ShardMigrationStatus::Preparing));
        assert_eq!(updated_shard_def.migration_target_node_ids, Some(vec![target_compute_id]));
        assert_eq!(updated_shard_def.migration_source_node_ids, Some(vec![source_compute_id]));
        assert_eq!(updated_shard_def.version, 1);
    }

    #[tokio::test]
    async fn test_advance_shard_migration_step() {
        let catalog = default_test_catalog().await;
        let shard_migrator = Arc::new(NoOpShardMigrator::new(Arc::clone(&catalog)));
        let shard_id = ShardId::new(1);

        let result = advance_shard_migration_step(shard_migrator, shard_id).await;
        assert!(result.is_ok());
        let progress = result.unwrap();
        assert_eq!(progress.status, ShardMigrationStatus::StreamingData);
        assert_eq!(progress.progress_pct, Some(50.0));
    }

    #[tokio::test]
    async fn test_finalize_shard_migration_api() {
        let catalog = default_test_catalog().await;
        let shard_migrator = Arc::new(NoOpShardMigrator::new(Arc::clone(&catalog)));
        let db_name = "test_db_fin_api".to_string();
        let table_name = "test_table_fin_api".to_string();
        let shard_id = ShardId::new(1);

        catalog.create_database(&db_name).await.unwrap();
        catalog.create_table(&db_name, &table_name, Default::default()).await.unwrap();
        let initial_shard_def = ShardDefinition::new(shard_id, ShardTimeRange::new(0,1000), vec![ComputeNodeId::new(1)]);
        catalog.create_shard(&db_name, &table_name, initial_shard_def).await.unwrap();
        // Set to a state that can be finalized
        catalog.update_shard_migration_state(&db_name, &table_name, shard_id, Some(ShardMigrationStatus::AwaitingCutover), Some(vec![ComputeNodeId::new(2)]), Some(vec![ComputeNodeId::new(1)])).await.unwrap();


        let result = finalize_shard_migration_api(shard_migrator, db_name.clone(), table_name.clone(), shard_id).await;
        assert!(result.is_ok(), "finalize_shard_migration_api failed: {:?}", result.err());

        let db_schema = catalog.db_schema(&db_name).unwrap();
        let table_def = db_schema.table_definition(&table_name).unwrap();
        let shard_info = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_info.migration_status, Some(ShardMigrationStatus::Stable));
    }

    #[tokio::test]
    async fn test_rollback_shard_migration_api() {
        let catalog = default_test_catalog().await;
        let shard_migrator = Arc::new(NoOpShardMigrator::new(Arc::clone(&catalog)));
        let db_name = "test_db_roll_api".to_string();
        let table_name = "test_table_roll_api".to_string();
        let shard_id = ShardId::new(1);

        catalog.create_database(&db_name).await.unwrap();
        catalog.create_table(&db_name, &table_name, Default::default()).await.unwrap();
        let initial_shard_def = ShardDefinition::new(shard_id, ShardTimeRange::new(0,1000), vec![ComputeNodeId::new(1)]);
        catalog.create_shard(&db_name, &table_name, initial_shard_def).await.unwrap();
        catalog.update_shard_migration_state(&db_name, &table_name, shard_id, Some(ShardMigrationStatus::StreamingData), Some(vec![ComputeNodeId::new(2)]), Some(vec![ComputeNodeId::new(1)])).await.unwrap();

        let result = rollback_shard_migration_api(shard_migrator, db_name.clone(), table_name.clone(), shard_id, "test reason".to_string()).await;
        assert!(result.is_ok(), "rollback_shard_migration_api failed: {:?}", result.err());

        let db_schema = catalog.db_schema(&db_name).unwrap();
        let table_def = db_schema.table_definition(&table_name).unwrap();
        let shard_info = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(shard_info.migration_status, Some(ShardMigrationStatus::Failed));
    }

    #[tokio::test]
    async fn test_get_shard_migration_status_api() {
        let catalog = default_test_catalog().await;
        let shard_migrator = Arc::new(NoOpShardMigrator::new(Arc::clone(&catalog)));
        let shard_id = ShardId::new(1);
        // No specific setup needed as NoOpShardMigrator always returns Stable for now unless state is more complexly mocked

        let result = get_shard_migration_status_api(shard_migrator, shard_id).await;
        assert!(result.is_ok());
        let progress_opt = result.unwrap();
        assert!(progress_opt.is_some());
        assert_eq!(progress_opt.unwrap().status, ShardMigrationStatus::Stable);
    }

    #[tokio::test]
    async fn test_move_shard_manually_shard_not_found() {
        let catalog = default_test_catalog().await;
        let db_name = "test_db_move_nf".to_string();
        let table_name = "test_table_move_nf".to_string();
        let target_compute_node = ComputeNodeId::new(200);

        catalog.db_or_create(&db_name).await.unwrap();
        let _ = catalog.create_table(&db_name, &table_name, Default::default()).await.unwrap();

        let result = move_shard_manually(
            Arc::clone(&catalog),
            ShardId::new(999), // Non-existent shard
            db_name.clone(),
            table_name.clone(),
            target_compute_node,
        )
        .await;

        assert!(matches!(result, Err(ManagementApiError::ShardNotFound{..})));
    }
}
