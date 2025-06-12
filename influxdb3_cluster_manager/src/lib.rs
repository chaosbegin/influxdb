use std::sync::Arc;
use std::fmt::Debug;
use thiserror::Error;
use async_trait::async_trait;
use tracing::{info, warn, error}; // Ensure tracing is used

pub use influxdb3_id::{NodeId, ShardId};
pub use influxdb3_catalog::management::{
    NodeDefinition, NodeStatus,
    ShardDefinition, ShardMigrationStatus,
};
use influxdb3_catalog::catalog::Catalog;


// NodeInfoProvider trait
pub trait NodeInfoProvider: Send + Sync + Debug {
    fn get_node_query_rpc_address(&self, node_id: &NodeId) -> Option<String>;
    fn get_node_management_rpc_address(&self, node_id: &NodeId) -> Option<String>;
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct MockNodeInfoProvider {
    pub query_addresses: std::collections::HashMap<NodeId, String>,
    pub mgmt_addresses: std::collections::HashMap<NodeId, String>,
}

#[cfg(test)]
impl MockNodeInfoProvider {
    pub fn new() -> Self { Self::default() }
    pub fn add_node(&mut self, node_id: NodeId, query_addr: String, mgmt_addr: String) {
        self.query_addresses.insert(node_id, query_addr);
        self.mgmt_addresses.insert(node_id, mgmt_addr);
    }
}

#[cfg(test)]
impl NodeInfoProvider for MockNodeInfoProvider {
    fn get_node_query_rpc_address(&self, node_id: &NodeId) -> Option<String> {
        self.query_addresses.get(node_id).cloned()
    }
    fn get_node_management_rpc_address(&self, node_id: &NodeId) -> Option<String> {
        self.mgmt_addresses.get(node_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_catalog::log::NodeMode;
    use influxdb3_id::NodeId as InfluxNodeId; // Use the actual NodeId type
    use mockall::predicate::*;
    use mockall::*;
    use std::sync::Arc;
    use tokio::runtime::Runtime; // For running async tests

    // Mock Catalog for RebalanceOrchestrator and ClusterManager tests
    mock! {
        pub Catalog {
            // Node Management methods
            async fn add_node(&self, node_def: NodeDefinition) -> influxdb3_catalog::Result<()>;
            async fn remove_node(&self, node_id: &InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn update_node_status(&self, node_id: &InfluxNodeId, status: NodeStatus) -> influxdb3_catalog::Result<()>;
            async fn list_nodes(&self) -> influxdb3_catalog::Result<Vec<Arc<NodeDefinition>>>;
            async fn get_node(&self, node_id: &InfluxNodeId) -> influxdb3_catalog::Result<Option<Arc<NodeDefinition>>>;
            fn next_node_id(&self) -> InfluxNodeId; // Not async

            // Shard Migration methods
            async fn begin_shard_migration_out(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_ids: Vec<InfluxNodeId>) -> influxdb3_catalog::Result<()>;
            async fn commit_shard_migration_on_target(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_id: InfluxNodeId, source_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn finalize_shard_migration_on_source(&self, db_name: &str, table_name: &str, shard_id: &ShardId, source_node_id_to_remove: InfluxNodeId, migrated_to_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;

            // Other methods needed by orchestrator
            async fn list_db_schema(&self) -> influxdb3_catalog::Result<Vec<Arc<influxdb3_catalog::catalog::DatabaseSchema>>>;
        }
    }
    // Implement other necessary traits for MockCatalog if they are needed for Arc<Catalog>
    impl influxdb3_catalog::catalog::CatalogUpdate for MockCatalog {}
    impl influxdb3_authz::TokenProvider for MockCatalog {
        fn get_token(&self, _token_hash: Vec<u8>) -> Option<Arc<influxdb3_authz::TokenInfo>> { None }
    }
    impl influxdb3_telemetry::ProcessingEngineMetrics for MockCatalog {
        fn num_triggers(&self) -> (u64,u64,u64,u64) { (0,0,0,0) }
    }


    fn create_test_node_definition(id: u64, name: &str, status: NodeStatus) -> NodeDefinition {
        NodeDefinition {
            id: InfluxNodeId::new(id),
            node_name: Arc::from(name),
            instance_id: Arc::from(uuid::Uuid::new_v4().to_string()),
            rpc_address: format!("{}:8082", name),
            http_address: format!("{}:8080", name),
            mode: vec![NodeMode::Core],
            core_count: 4,
            status,
            last_heartbeat: Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()),
        }
    }

    #[tokio::test]
    async fn test_cluster_manager_add_node() {
        let mut mock_catalog = MockCatalog::new();
        let node_def = create_test_node_definition(1, "node1", NodeStatus::Joining);
        let node_def_clone = node_def.clone();

        mock_catalog.expect_add_node()
            .with(eq(node_def_clone))
            .times(1)
            .returning(|_| Ok(()));

        let cluster_manager = ClusterManager::new(Arc::new(mock_catalog));
        let result = cluster_manager.add_node(node_def).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_orchestrator_add_new_node_no_shards() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let new_node_def = create_test_node_definition(1, "new_node", NodeStatus::Joining);

        mock_catalog.expect_add_node()
            .with(eq(new_node_def.clone()))
            .times(1)
            .returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema()
            .times(1)
            .returning(|| Ok(vec![])); // No databases, so no shards
        mock_catalog.expect_update_node_status()
            .with(eq(new_node_def.id), eq(NodeStatus::Active))
            .times(1)
            .returning(|_, _| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider);
        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_orchestrator_decommission_node_no_shards() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let node_to_remove_id = InfluxNodeId::new(1);
        let node_to_remove_def = Arc::new(create_test_node_definition(1, "node_to_remove", NodeStatus::Active));

        mock_catalog.expect_get_node()
            .with(eq(node_to_remove_id))
            .times(1)
            .returning(move |_| Ok(Some(Arc::clone(&node_to_remove_def))));
        mock_catalog.expect_update_node_status()
            .with(eq(node_to_remove_id), eq(NodeStatus::Leaving))
            .times(1)
            .returning(|_, _| Ok(()));
        mock_catalog.expect_list_db_schema()
            .times(1)
            .returning(|| Ok(vec![])); // No dbs, so no shards on this node
        mock_catalog.expect_update_node_status() // Called again to set to Down
            .with(eq(node_to_remove_id), eq(NodeStatus::Down))
            .times(1)
            .returning(|_, _| Ok(()));
        mock_catalog.expect_remove_node()
            .with(eq(node_to_remove_id))
            .times(1)
            .returning(|_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider);
        let strategy = RebalanceStrategy::DecommissionNode(node_to_remove_id);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok());
    }

    // More detailed tests for shard selection and migration calls would go here,
    // requiring more complex mocking of catalog's list_db_schema to return tables and shards.

    fn create_test_shard_definition(id: u64, node_ids: Vec<u64>) -> Arc<ShardDefinition> {
        Arc::new(ShardDefinition::new(
            ShardId::new(id),
            influxdb3_catalog::shard::ShardTimeRange { start_time: 0, end_time: 1000 },
            node_ids.into_iter().map(InfluxNodeId::new).collect(),
            None,
            Some(ShardMigrationStatus::Stable),
        ))
    }

    fn create_mock_db_schema_with_shards(
        db_name: &str,
        table_name: &str,
        shards: Vec<Arc<ShardDefinition>>
    ) -> Arc<influxdb3_catalog::catalog::DatabaseSchema> {
        let mut table_repo = influxdb3_catalog::catalog::Repository::<TableId, influxdb3_catalog::catalog::TableDefinition>::new();
        let mut shard_repo = influxdb3_catalog::catalog::Repository::<ShardId, ShardDefinition>::new();

        for shard_def in shards {
            shard_repo.insert(shard_def.id, shard_def).unwrap();
        }

        let table_id = table_repo.next_id();
        // Create a basic TableDefinition for testing purposes
        let columns_for_table = vec![
            (influxdb3_id::ColumnId::new(0), Arc::from("time"), influxdb3_catalog::catalog::InfluxColumnType::Timestamp),
            (influxdb3_id::ColumnId::new(1), Arc::from("tag1"), influxdb3_catalog::catalog::InfluxColumnType::Tag),
            (influxdb3_id::ColumnId::new(2), Arc::from("field1"), influxdb3_catalog::catalog::InfluxColumnType::Field(influxdb3_catalog::catalog::InfluxFieldType::Float)),
        ];
        let series_key_col_ids = vec![influxdb3_id::ColumnId::new(1)];

        let table_definition = influxdb3_catalog::catalog::TableDefinition::new(
            table_id,
            Arc::from(table_name),
            columns_for_table,
            series_key_col_ids,
            None, // replication_info
            None, // shard_key_columns
            influxdb3_catalog::shard::ShardingStrategy::Time, // default strategy
        ).unwrap();

        let mut mutable_table_def = Arc::try_unwrap(Arc::new(table_definition)).unwrap_or_else(|arc| (*arc).clone());
        mutable_table_def.shards = shard_repo; // Assign the shard repository

        table_repo.insert(mutable_table_def.table_id, Arc::new(mutable_table_def)).unwrap();

        Arc::new(influxdb3_catalog::catalog::DatabaseSchema {
            id: influxdb3_id::DbId::new(1), // Dummy ID
            name: Arc::from(db_name),
            tables: table_repo,
            retention_period: influxdb3_catalog::catalog::RetentionPeriod::Indefinite,
            processing_engine_triggers: Default::default(),
            deleted: false,
        })
    }

    #[tokio::test]
    async fn test_orchestrator_add_new_node_with_shards_to_move() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let new_node_def = create_test_node_definition(10, "newly_added_node", NodeStatus::Joining);
        let new_node_id = new_node_def.id;

        // Setup: 2 tables, table1 has 1 shard, table2 has 1 shard. Both should be selected.
        let shard_t1_s1 = create_test_shard_definition(10, vec![1, 2]); // db1.table1, shard 10
        let db_schema1 = create_mock_db_schema_with_shards("db1", "table1", vec![shard_t1_s1.clone()]);

        let shard_t2_s1 = create_test_shard_definition(20, vec![3, 4]); // db1.table2, shard 20
        // For simplicity, adding table2 to the same db_schema1 mock object for this test.
        // A more complex mock could have multiple DbSchemas.
        let mut table_repo_for_db1 = db_schema1.tables.clone(); // Clone the repo to modify
        let mut shard_repo_t2 = influxdb3_catalog::catalog::Repository::<ShardId, ShardDefinition>::new();
        shard_repo_t2.insert(shard_t2_s1.id, shard_t2_s1.clone()).unwrap();

        let table2_id = table_repo_for_db1.next_id();
        let columns_for_table2 = vec![
            (influxdb3_id::ColumnId::new(0), Arc::from("time"), influxdb3_catalog::catalog::InfluxColumnType::Timestamp),
        ];
        let table2_definition = influxdb3_catalog::catalog::TableDefinition::new(
            table2_id, Arc::from("table2"), columns_for_table2, vec![], None, None,
            influxdb3_catalog::shard::ShardingStrategy::Time,
        ).unwrap();
        let mut mutable_table2_def = Arc::try_unwrap(Arc::new(table2_definition)).unwrap_or_else(|arc| (*arc).clone());
        mutable_table2_def.shards = shard_repo_t2;
        table_repo_for_db1.insert(mutable_table2_def.table_id, Arc::new(mutable_table2_def)).unwrap();

        let final_db_schema1 = Arc::new(influxdb3_catalog::catalog::DatabaseSchema {
            id: db_schema1.id, name: Arc::clone(&db_schema1.name), tables: table_repo_for_db1,
            retention_period: db_schema1.retention_period,
            processing_engine_triggers: db_schema1.processing_engine_triggers.clone(),
            deleted: db_schema1.deleted,
        });


        mock_catalog.expect_add_node()
            .with(eq(new_node_def.clone()))
            .times(1)
            .returning(|_| Ok(()));

        mock_catalog.expect_list_db_schema()
            .times(1)
            .returning(move || Ok(vec![final_db_schema1.clone()]));

        // Expectations for shard_t1_s1 from db1.table1
        let primary_source_t1_s1 = shard_t1_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(vec![new_node_id]))
            .times(1).returning(|_, _, _, _| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(new_node_id), eq(primary_source_t1_s1))
            .times(1).returning(|_, _, _, _, _| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(primary_source_t1_s1), eq(new_node_id))
            .times(1).returning(|_, _, _, _, _| Ok(()));
        // Mock the db_schema().await?.and_then(...) call for logging final owners
        // This is tricky with mockall if the state changes. For simplicity, assume it can be called.
        // We might need to relax the .times(1) or use .once() then .returning for subsequent calls if schema is fetched multiple times.
        let db_schema_for_log_t1 = create_mock_db_schema_with_shards("db1", "table1", vec![Arc::new(ShardDefinition::new(
            shard_t1_s1.id, shard_t1_s1.time_range, vec![new_node_id, shard_t1_s1.node_ids[1]], None, Some(ShardMigrationStatus::Stable)
        ))]);
        mock_catalog.expect_db_schema()
            .with(eq("db1")).returning(move |_| Ok(Some(db_schema_for_log_t1.clone())));


        // Expectations for shard_t2_s1 from db1.table2
        let primary_source_t2_s1 = shard_t2_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out()
            .with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(vec![new_node_id]))
            .times(1).returning(|_, _, _, _| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target()
            .with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(new_node_id), eq(primary_source_t2_s1))
            .times(1).returning(|_, _, _, _, _| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source()
            .with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id))
            .times(1).returning(|_, _, _, _, _| Ok(()));
        // Mock the db_schema().await call for logging final owners of shard_t2_s1
        // This is getting complex for mockall due to multiple calls to db_schema with same args but expecting different internal states.
        // A simpler test might not verify the logged final owners as rigorously, or use a more stateful mock.
        // For now, we'll assume the previous mock_catalog.expect_db_schema() might cover this if not scoped by table.
        // Let's make it more specific if possible or accept less rigorous logging check.
        // The .returning sequence for db_schema might be an issue.
        // We will rely on the fact that the previous expect_db_schema for "db1" is general enough.


        mock_catalog.expect_update_node_status()
            .with(eq(new_node_id), eq(NodeStatus::Active))
            .times(1)
            .returning(|_, _| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider);
        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance for AddNewNode failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_orchestrator_decommission_node_with_shards() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());

        let node_to_remove_id = InfluxNodeId::new(1);
        let node_to_remove_def = Arc::new(create_test_node_definition(1, "node_to_remove", NodeStatus::Active));
        let active_target_node_id = InfluxNodeId::new(2);
        let active_target_node_def = Arc::new(create_test_node_definition(2, "active_target_node", NodeStatus::Active));

        let shard1 = create_test_shard_definition(101, vec![1]); // Only on node_to_remove
        let db_schema1 = create_mock_db_schema_with_shards("db_decom", "table_decom", vec![shard1.clone()]);

        mock_catalog.expect_get_node()
            .with(eq(node_to_remove_id))
            .times(1)
            .returning(move |_| Ok(Some(Arc::clone(&node_to_remove_def))));

        mock_catalog.expect_update_node_status()
            .with(eq(node_to_remove_id), eq(NodeStatus::Leaving))
            .times(1)
            .returning(|_, _| Ok(()));

        mock_catalog.expect_list_db_schema()
            .times(1)
            .returning(move || Ok(vec![db_schema1.clone()]));

        mock_catalog.expect_list_nodes()
            .times(1)
            .returning(move || Ok(vec![
                Arc::new(create_test_node_definition(1, "node_to_remove", NodeStatus::Leaving)), // It's now Leaving
                Arc::clone(&active_target_node_def)
            ]));

        mock_catalog.expect_begin_shard_migration_out()
            .with(eq("db_decom"), eq("table_decom"), eq(shard1.id), eq(vec![active_target_node_id]))
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        mock_catalog.expect_commit_shard_migration_on_target()
            .with(eq("db_decom"), eq("table_decom"), eq(shard1.id), eq(active_target_node_id), eq(node_to_remove_id))
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));

        mock_catalog.expect_finalize_shard_migration_on_source()
            .with(eq("db_decom"), eq("table_decom"), eq(shard1.id), eq(node_to_remove_id), eq(active_target_node_id))
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));

        mock_catalog.expect_update_node_status()
            .with(eq(node_to_remove_id), eq(NodeStatus::Down))
            .times(1)
            .returning(|_, _| Ok(()));

        // Note: The current orchestrator logic for DecommissionNode does *not* call remove_node.
        // It only sets status to Leaving, then Down.
        // mock_catalog.expect_remove_node()
        //     .with(eq(node_to_remove_id))
        //     .times(1)
        //     .returning(|_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider);
        let strategy = RebalanceStrategy::DecommissionNode(node_to_remove_id);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance for DecommissionNode failed: {:?}", result.err());
    }
}


#[derive(Error, Debug)]
pub enum ClusterManagerError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] influxdb3_catalog::Error),
    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: NodeId },
    #[error("Node with name '{node_name}' not found")]
    NodeNameNotFound { node_name: String },
    #[error("No suitable target nodes found for shard migration")]
    NoTargetNodes,
    #[error("Shard not found: db={db_name}, table={table_name}, shard_id={shard_id}")]
    ShardNotFound { db_name: String, table_name: String, shard_id: ShardId },
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T, E = ClusterManagerError> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub enum RebalanceStrategy {
    AddNewNode(NodeDefinition),
    DecommissionNode(NodeId),
}

#[derive(Debug)]
pub struct ClusterManager {
    catalog: Arc<Catalog>,
}

impl ClusterManager {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn add_node(&self, node_def_payload: NodeDefinition) -> Result<()> {
        info!("ClusterManager: Adding node {:?} (ID: {:?}) to catalog", node_def_payload.node_name, node_def_payload.id);
        self.catalog.add_node(node_def_payload).await?;
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        info!("ClusterManager: Removing node ID {:?} from catalog", node_id);
        self.catalog.remove_node(node_id).await?;
        Ok(())
    }

    pub async fn update_node_status(&self, node_id: &NodeId, status: NodeStatus) -> Result<()> {
        info!("ClusterManager: Updating status of node ID {:?} to {:?}", node_id, status);
        self.catalog.update_node_status(node_id, status).await?;
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<Arc<NodeDefinition>>> {
        info!("ClusterManager: Listing nodes from catalog");
        Ok(self.catalog.list_nodes().await?)
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Arc<NodeDefinition>>> {
        info!("ClusterManager: Getting node ID {:?} from catalog", node_id);
        Ok(self.catalog.get_node(node_id).await?)
    }
}

#[derive(Debug)]
pub struct RebalanceOrchestrator {
    catalog: Arc<Catalog>,
    #[allow(dead_code)] // Will be used for actual RPC calls later
    node_info_provider: Arc<dyn NodeInfoProvider>,
}

impl RebalanceOrchestrator {
    pub fn new(catalog: Arc<Catalog>, node_info_provider: Arc<dyn NodeInfoProvider>) -> Self {
        Self { catalog, node_info_provider }
    }

    pub async fn initiate_rebalance(&self, strategy: RebalanceStrategy) -> Result<()> {
        info!("RebalanceOrchestrator: Initiating rebalance with strategy: {:?}", strategy);

        match strategy {
            RebalanceStrategy::AddNewNode(new_node_def_payload) => {
                let new_node_id = new_node_def_payload.id;
                let new_node_name = new_node_def_payload.node_name.clone();
                info!("Rebalance: Adding new node {:?} (ID: {:?})", new_node_name, new_node_id);
                self.catalog.add_node(new_node_def_payload.clone()).await?;

                let all_dbs = self.catalog.list_db_schema().await?;
                let mut selected_shards_for_migration = vec![];

                // Simplified shard selection: pick one shard from each table to move.
                // A more advanced strategy would consider shard sizes, node load, replication factor etc.
                for db_schema in all_dbs {
                    for table_def_arc in db_schema.tables() {
                        if let Some(shard_to_move_arc) = table_def_arc.shards.resource_iter().next() { // Pick first shard
                            // Ensure the shard is not already solely on the new node (unlikely for AddNewNode)
                            if !(shard_to_move_arc.node_ids.len() == 1 && shard_to_move_arc.node_ids.contains(&new_node_id)) {
                                selected_shards_for_migration.push((
                                    db_schema.name.clone(),
                                    table_def_arc.table_name.clone(),
                                    shard_to_move_arc.id,
                                    Arc::clone(shard_to_move_arc),
                                ));
                            }
                        }
                    }
                }

                if selected_shards_for_migration.is_empty() {
                    info!("Rebalance: AddNewNode - No suitable shards found to move to the new node {}.", new_node_name);
                }

                for (db_name, table_name, shard_id, current_shard_def) in selected_shards_for_migration {
                    info!(
                        "Rebalance: Starting migration for shard {:?} from table {}.{} to new node {:?} (ID: {:?})",
                        shard_id, db_name, table_name, new_node_name, new_node_id
                    );

                    let original_source_node_ids = current_shard_def.node_ids.clone();
                    if original_source_node_ids.is_empty() {
                        warn!("Shard {:?} in table {}.{} has no current source nodes, skipping migration.", shard_id, db_name, table_name);
                        continue;
                    }
                    // For AddNewNode, we are effectively making the new node an additional owner,
                    // and then removing one of the original owners.
                    // Let's assume the first node in the list is the primary source for the data transfer.
                    let primary_data_source_node_id = original_source_node_ids[0];

                    self.catalog.begin_shard_migration_out(
                        &db_name, &table_name, &shard_id, vec![new_node_id],
                    ).await?;
                    info!("SIMULATE: Source node(s) {:?} flushing WAL and preparing Parquet files for shard {:?}.", original_source_node_ids, shard_id);

                    info!("SIMULATE: Transferring Parquet files for shard {:?} via object store to new node ID {:?}.", shard_id, new_node_id);
                    info!("SIMULATE: New node ID {:?} bootstrapping shard {:?} from Parquet files.", new_node_id, shard_id);

                    // Target node confirms it has bootstrapped the shard data from primary_data_source_node_id
                    self.catalog.commit_shard_migration_on_target(
                        &db_name, &table_name, &shard_id, new_node_id, primary_data_source_node_id
                    ).await?;
                    info!("SIMULATE: New node ID {:?} replaying/synchronizing WAL delta for shard {:?}.", new_node_id, shard_id);

                    // Now, finalize by removing one of the original source nodes.
                    // For simplicity, we remove the primary_data_source_node_id.
                    // If the shard was replicated (e.g., on [S1, S2]) and S1 was primary_data_source,
                    // after this, it will be on [S2, new_node_id].
                    self.catalog.finalize_shard_migration_on_source(
                        &db_name, &table_name, &shard_id, primary_data_source_node_id, new_node_id
                    ).await?;

                    info!("Migration complete for shard {:?} from node {:?} to new node ID {:?}. Final owners: {:?}.",
                        shard_id, primary_data_source_node_id, new_node_id,
                        self.catalog.db_schema(&db_name).await?.and_then(|db| db.table_definition(&table_name)?.shards.get_by_id(&shard_id).map(|s| s.node_ids.clone())).unwrap_or_default()
                    );
                }
                self.catalog.update_node_status(&new_node_id, NodeStatus::Active).await?;
                info!("New node {:?} (ID: {:?}) successfully added and marked Active.", new_node_name, new_node_id);
            }
            RebalanceStrategy::DecommissionNode(node_id_to_remove) => {
                let node_to_remove_def = self.catalog.get_node(&node_id_to_remove).await?
                    .ok_or_else(|| ClusterManagerError::NodeNotFound { node_id: node_id_to_remove })?;

                info!("Rebalance: Decommissioning node {:?} (ID: {:?})", node_to_remove_def.node_name, node_id_to_remove);
                self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Leaving).await?;

                let all_dbs = self.catalog.list_db_schema().await?;
                let mut shards_on_decommissioning_node = vec![];

                for db_schema in all_dbs {
                    for table_def_arc in db_schema.tables() {
                        let table_def = table_def_arc.as_ref();
                        for shard_def_arc in table_def.shards.resource_iter() {
                            if shard_def_arc.node_ids.contains(&node_id_to_remove) {
                                shards_on_decommissioning_node.push((
                                    db_schema.name.clone(),
                                    table_def.table_name.clone(),
                                    shard_def_arc.id,
                                    // Arc::clone(shard_def_arc), // No need to clone here, we re-fetch shard_def later
                                ));
                            }
                        }
                    }
                }

                if shards_on_decommissioning_node.is_empty() {
                    info!("Rebalance: DecommissionNode - Node ID {:?} has no shards. Marking as Down and removing.", node_id_to_remove);
                    self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Down).await?;
                    // self.catalog.remove_node(&node_id_to_remove).await?; // Consider if remove is immediate or after some checks
                    return Ok(());
                }

                let active_nodes = self.catalog.list_nodes().await?
                    .into_iter()
                    .filter(|n| n.id != node_id_to_remove && n.status == NodeStatus::Active)
                    .map(|n| n.id)
                    .collect::<Vec<_>>();

                if active_nodes.is_empty() {
                    error!("Rebalance: DecommissionNode - No active nodes available to migrate shards from ID {:?}. Reverting status to Active.", node_id_to_remove);
                    self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Active).await.unwrap_or_else(|e| {
                        error!("Failed to revert node {:?} status to Active: {}", node_id_to_remove, e);
                    });
                    return Err(ClusterManagerError::NoTargetNodes);
                }

                let mut node_picker = active_nodes.iter().cycle(); // Cycle through active nodes for round-robin

                for (db_name, table_name, shard_id) in shards_on_decommissioning_node {
                     // Fetch the latest shard_def in case it was modified by a previous migration step in this loop
                    let current_shard_def = self.catalog.db_schema(&db_name).await?
                        .and_then(|db| db.table_definition(&table_name)?.shards.get_by_id(&shard_id))
                        .ok_or_else(|| ClusterManagerError::ShardNotFound { db_name: db_name.to_string(), table_name: table_name.to_string(), shard_id })?;

                    // If the shard is still on the decommissioning node (e.g. it was replicated, and another owner was removed)
                    if !current_shard_def.node_ids.contains(&node_id_to_remove) {
                        info!("Shard {:?} in table {}.{} no longer on decommissioning node {:?}. Skipping.", shard_id, db_name, table_name, node_id_to_remove);
                        continue;
                    }

                    // If the shard is ONLY on the decommissioning node, or if we need to move this specific copy.
                    let chosen_target_node_id = *node_picker.next().unwrap_or(&active_nodes[0]); // Should always have a node

                    info!(
                        "Rebalance: Starting migration for shard {:?} from table {}.{} on decommissioning node ID {:?} to target node ID {:?}",
                        shard_id, db_name, table_name, node_id_to_remove, chosen_target_node_id
                    );

                    self.catalog.begin_shard_migration_out(
                        &db_name, &table_name, &shard_id, vec![chosen_target_node_id],
                    ).await?;
                    info!("SIMULATE: Source node ID {:?} flushing WAL and preparing Parquet files for shard {:?}.", node_id_to_remove, shard_id);

                    info!("SIMULATE: Transferring Parquet files for shard {:?} from ID {:?} to ID {:?}.", shard_id, node_id_to_remove, chosen_target_node_id);
                    info!("SIMULATE: Target node ID {:?} bootstrapping shard {:?} from Parquet files.", chosen_target_node_id, shard_id);

                    self.catalog.commit_shard_migration_on_target(
                        &db_name, &table_name, &shard_id, chosen_target_node_id, node_id_to_remove
                    ).await?;
                    info!("SIMULATE: Target node ID {:?} replaying/synchronizing WAL delta for shard {:?}.", chosen_target_node_id, shard_id);

                    self.catalog.finalize_shard_migration_on_source(
                        &db_name, &table_name, &shard_id, node_id_to_remove, chosen_target_node_id
                    ).await?;

                    info!("Migration complete for shard {:?} from ID {:?} to ID {:?}. Final owners: {:?}.",
                        shard_id, node_id_to_remove, chosen_target_node_id,
                        self.catalog.db_schema(&db_name).await?.and_then(|db| db.table_definition(&table_name)?.shards.get_by_id(&shard_id).map(|s| s.node_ids.clone())).unwrap_or_default()
                    );
                }

                info!("All shards migrated off node ID {:?}. Marking as Down.", node_id_to_remove); // Not removing immediately
                self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Down).await?;
                // Consider if self.catalog.remove_node(&node_id_to_remove).await? should be called here or by a separate cleanup process.
                // For now, setting to Down is sufficient.
            }
        }
        Ok(())
    }
}
