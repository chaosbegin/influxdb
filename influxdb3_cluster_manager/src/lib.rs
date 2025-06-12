use std::sync::Arc;
use std::fmt::Debug;
use thiserror::Error;
use async_trait::async_trait;
use tracing::{info, warn, error};
use object_store::{ObjectStore, path::Path as ObjectPath, ObjectMeta};

pub use influxdb3_id::{NodeId, ShardId};
pub use influxdb3_catalog::management::{
    NodeDefinition, NodeStatus,
    ShardDefinition, ShardMigrationStatus,
};
use influxdb3_catalog::catalog::{Catalog, TempParquetFilePlaceholder};


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
    use influxdb3_catalog::catalog::Catalog as CatalogTrait; // Use trait for mock
    use influxdb3_catalog::log::NodeMode;
    use influxdb3_id::NodeId as InfluxNodeId;
    use mockall::predicate::*;
    use mockall::*;
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use object_store::memory::InMemory;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use futures::stream::BoxStream;
    use std::io;
    use std::task::{Context, Poll};
    use object_store::Error as ObjectStoreError;


    // Mock Catalog for RebalanceOrchestrator and ClusterManager tests
    mock! {
        pub Catalog {
            // Node Management methods
            async fn add_node(&self, node_def: NodeDefinition) -> influxdb3_catalog::Result<()>;
            async fn remove_node(&self, node_id: &InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn update_node_status(&self, node_id: &InfluxNodeId, status: NodeStatus) -> influxdb3_catalog::Result<()>;
            async fn list_nodes(&self) -> influxdb3_catalog::Result<Vec<Arc<NodeDefinition>>>;
            async fn get_node(&self, node_id: &InfluxNodeId) -> influxdb3_catalog::Result<Option<Arc<NodeDefinition>>>;
            fn next_node_id(&self) -> InfluxNodeId;

            // Shard Migration methods
            async fn begin_shard_migration_out(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_ids: Vec<InfluxNodeId>) -> influxdb3_catalog::Result<()>;
            async fn mark_source_data_flushed(&self, db_name: &str, table_name: &str, shard_id: &ShardId, targets: Vec<InfluxNodeId>) -> influxdb3_catalog::Result<()>;
            async fn list_parquet_files_for_shard(&self, db_name: &str, table_name: &str, shard_id: ShardId) -> influxdb3_catalog::Result<Vec<TempParquetFilePlaceholder>>;
            async fn mark_target_bootstrapped(&self, db_name: &str, table_name: &str, shard_id: &ShardId, source_node_id: InfluxNodeId, target_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn mark_target_wal_syncing(&self, db_name: &str, table_name: &str, shard_id: &ShardId, source_node_id: InfluxNodeId, target_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn mark_target_ready_for_cutover(&self, db_name: &str, table_name: &str, shard_id: &ShardId, source_node_id: InfluxNodeId, target_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn mark_shard_migration_failed(&self, db_name: &str, table_name: &str, shard_id: &ShardId, error_message: String) -> influxdb3_catalog::Result<()>;
            async fn commit_shard_migration_on_target(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_id: InfluxNodeId, source_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;
            async fn finalize_shard_migration_on_source(&self, db_name: &str, table_name: &str, shard_id: &ShardId, source_node_id_to_remove: InfluxNodeId, migrated_to_node_id: InfluxNodeId) -> influxdb3_catalog::Result<()>;

            async fn list_db_schema(&self) -> influxdb3_catalog::Result<Vec<Arc<influxdb3_catalog::catalog::DatabaseSchema>>>;
            async fn db_schema(&self, db_name: &str) -> influxdb3_catalog::Result<Option<Arc<influxdb3_catalog::catalog::DatabaseSchema>>>;
        }
    }
    impl influxdb3_catalog::catalog::CatalogUpdate for MockCatalog {}
    impl influxdb3_authz::TokenProvider for MockCatalog {
        fn get_token(&self, _token_hash: Vec<u8>) -> Option<Arc<influxdb3_authz::TokenInfo>> { None }
    }
    impl influxdb3_telemetry::ProcessingEngineMetrics for MockCatalog {
        fn num_triggers(&self) -> (u64,u64,u64,u64) { (0,0,0,0) }
    }

    // Faulty ObjectStore Wrapper
    #[derive(Debug)]
    struct FaultyObjectStore {
        inner: Arc<InMemory>,
        fault_on_path: Option<String>, // Path to trigger fault on
        fault_on_op: Option<String>, // "copy" or "delete"
    }

    impl FaultyObjectStore {
        fn new(inner: Arc<InMemory>, fault_on_path: Option<String>, fault_on_op: Option<String>) -> Self {
            Self { inner, fault_on_path, fault_on_op }
        }
    }

    #[async_trait]
    impl ObjectStore for FaultyObjectStore {
        async fn put(&self, location: &ObjectPath, bytes: Bytes) -> object_store::Result<()> {
            self.inner.put(location, bytes).await
        }
        async fn put_multipart(&self, location: &ObjectPath) -> object_store::Result<(String, Box<dyn tokio::io::AsyncWrite + Unpin + Send>)> {
             self.inner.put_multipart(location).await
        }
        async fn abort_multipart(&self, location: &ObjectPath, multipart_id: &str) -> object_store::Result<()> {
            self.inner.abort_multipart(location, multipart_id).await
        }
        async fn get(&self, location: &ObjectPath) -> object_store::Result<object_store::GetResult> {
            self.inner.get(location).await
        }
        async fn get_range(&self, location: &ObjectPath, range: std::ops::Range<usize>) -> object_store::Result<Bytes> {
            self.inner.get_range(location, range).await
        }
        async fn get_ranges(&self, location: &ObjectPath, ranges: &[std::ops::Range<usize>]) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }
        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            if self.fault_on_op.as_deref() == Some("delete") && self.fault_on_path.as_deref() == Some(location.as_ref()) {
                return Err(ObjectStoreError::Generic { store: "FaultyObjectStore", source: Box::new(io::Error::new(io::ErrorKind::Other, "Simulated delete fault"))});
            }
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            if self.fault_on_op.as_deref() == Some("copy") && self.fault_on_path.as_deref() == Some(from.as_ref()) {
                 return Err(ObjectStoreError::Generic { store: "FaultyObjectStore", source: Box::new(io::Error::new(io::ErrorKind::Other, "Simulated copy fault"))});
            }
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            if self.fault_on_op.as_deref() == Some("copy") && self.fault_on_path.as_deref() == Some(from.as_ref()) {
                 return Err(ObjectStoreError::Generic { store: "FaultyObjectStore", source: Box::new(io::Error::new(io::ErrorKind::Other, "Simulated copy_if_not_exists fault"))});
            }
            self.inner.copy_if_not_exists(from, to).await
        }
        fn path_valid(&self, _path: &ObjectPath) -> bool { true }
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
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let new_node_def = create_test_node_definition(1, "new_node", NodeStatus::Joining);

        mock_catalog.expect_add_node()
            .with(eq(new_node_def.clone()))
            .times(1)
            .returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema()
            .times(1)
            .returning(|| Ok(vec![]));
        mock_catalog.expect_update_node_status()
            .with(eq(new_node_def.id), eq(NodeStatus::Active))
            .times(1)
            .returning(|_, _| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, object_store);
        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_orchestrator_decommission_node_no_shards() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
            .returning(|| Ok(vec![]));
        mock_catalog.expect_update_node_status()
            .with(eq(node_to_remove_id), eq(NodeStatus::Down))
            .times(1)
            .returning(|_, _| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, object_store);
        let strategy = RebalanceStrategy::DecommissionNode(node_to_remove_id);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok());
    }


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
        let mut table_repo = influxdb3_catalog::catalog::Repository::<influxdb3_id::TableId, influxdb3_catalog::catalog::TableDefinition>::new();
        let mut shard_repo = influxdb3_catalog::catalog::Repository::<ShardId, ShardDefinition>::new();

        for shard_def in shards {
            shard_repo.insert(shard_def.id, shard_def).unwrap();
        }

        let table_id = influxdb3_id::TableId::new(db_name.len() as u64 + table_name.len() as u64);
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
            None,
            None,
            influxdb3_catalog::shard::ShardingStrategy::Time,
        ).unwrap();

        let mut mutable_table_def = Arc::try_unwrap(Arc::new(table_definition)).unwrap_or_else(|arc| (*arc).clone());
        mutable_table_def.shards = shard_repo;

        table_repo.insert(mutable_table_def.table_id, Arc::new(mutable_table_def)).unwrap();

        Arc::new(influxdb3_catalog::catalog::DatabaseSchema {
            id: influxdb3_id::DbId::new(db_name.len() as u64),
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
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let new_node_def = create_test_node_definition(10, "newly_added_node", NodeStatus::Joining);
        let new_node_id = new_node_def.id;

        let shard_t1_s1 = create_test_shard_definition(10, vec![1, 2]);
        let shard_t2_s1 = create_test_shard_definition(20, vec![3, 4]);

        let db1_schema = create_mock_db_schema_with_shards("db1", "table1", vec![shard_t1_s1.clone()]);
        let mut db1_table_repo_clone = db1_schema.tables.clone();

        let table2_id = influxdb3_id::TableId::new(200);
        let mut shard_repo_t2 = influxdb3_catalog::catalog::Repository::<ShardId, ShardDefinition>::new();
        shard_repo_t2.insert(shard_t2_s1.id, shard_t2_s1.clone()).unwrap();
        let columns_for_table2 = vec![(influxdb3_id::ColumnId::new(0), Arc::from("time"), influxdb3_catalog::catalog::InfluxColumnType::Timestamp)];
        let table2_definition = influxdb3_catalog::catalog::TableDefinition::new(
            table2_id, Arc::from("table2"), columns_for_table2, vec![], None, None,
            influxdb3_catalog::shard::ShardingStrategy::Time).unwrap();
        let mut mutable_table2_def = Arc::try_unwrap(Arc::new(table2_definition)).unwrap_or_else(|arc| (*arc).clone());
        mutable_table2_def.shards = shard_repo_t2;
        db1_table_repo_clone.insert(mutable_table2_def.table_id, Arc::new(mutable_table2_def)).unwrap();

        let final_db_schema1 = Arc::new(influxdb3_catalog::catalog::DatabaseSchema {
            id: db1_schema.id, name: Arc::clone(&db1_schema.name), tables: db1_table_repo_clone,
            retention_period: db1_schema.retention_period,
            processing_engine_triggers: db1_schema.processing_engine_triggers.clone(),
            deleted: db1_schema.deleted,
        });

        let mock_parquet_files = vec![
            TempParquetFilePlaceholder {
                id: 1, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_t1_s1.id,
                path: format!("{}/db1/table1/{}/file1.parquet", shard_t1_s1.node_ids[0].get(), shard_t1_s1.id.get()),
                size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
            },
            TempParquetFilePlaceholder {
                id: 2, db_name: Arc::from("db1"), table_name: Arc::from("table2"), shard_id: shard_t2_s1.id,
                path: format!("{}/db1/table2/{}/file2.parquet", shard_t2_s1.node_ids[0].get(), shard_t2_s1.id.get()),
                size_bytes: 200, row_count: 20, min_time: 0, max_time: 100,
            },
        ];
        let mock_parquet_files_clone1 = mock_parquet_files.clone();
        let mock_parquet_files_clone2 = mock_parquet_files.clone();


        mock_catalog.expect_add_node().times(1).returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema().times(1).returning(move || Ok(vec![final_db_schema1.clone()]));

        let primary_source_t1_s1 = shard_t1_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id))
            .times(1)
            .returning(move |_,_,_| Ok(vec![mock_parquet_files_clone1[0].clone()]));
        mock_catalog.expect_mark_target_bootstrapped().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(primary_source_t1_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(primary_source_t1_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(primary_source_t1_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(new_node_id), eq(primary_source_t1_s1)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(primary_source_t1_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));

        let primary_source_t2_s1 = shard_t2_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard()
            .with(eq("db1"), eq("table2"), eq(shard_t2_s1.id))
            .times(1)
            .returning(move |_,_,_| Ok(vec![mock_parquet_files_clone2[1].clone()]));
        mock_catalog.expect_mark_target_bootstrapped().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(new_node_id), eq(primary_source_t2_s1)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq("db1"), eq("table2"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));

        let db_schema_for_logging = create_mock_db_schema_with_shards("db1", "table1", vec![shard_t1_s1.clone()]);
        mock_catalog.expect_db_schema().with(eq("db1")).returning(move |_| Ok(Some(db_schema_for_logging.clone())));


        mock_catalog.expect_update_node_status().with(eq(new_node_id), eq(NodeStatus::Active)).times(1).returning(|_,_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, Arc::clone(&object_store));

        let dummy_content = Bytes::from_static(b"dummy parquet data");
        for pf_template in &mock_parquet_files {
            let source_path = ObjectPath::from(pf_template.path.as_str());
            object_store.put(&source_path, dummy_content.clone()).await.unwrap();
        }

        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance for AddNewNode failed: {:?}", result.err());

        for pf_template in &mock_parquet_files {
            let original_source_path = ObjectPath::from(pf_template.path.as_str());
            let mut parts = pf_template.path.splitn(2, '/');
            let _original_node_id_part = parts.next().unwrap();
            let rest_of_path = parts.next().unwrap();
            let target_path_str = format!("{}/{}", new_node_id.get(), rest_of_path);
            let target_path = ObjectPath::from(target_path_str.as_str());

            object_store.get(&target_path).await.expect("File not found at target path after copy");
            assert!(matches!(object_store.get(&original_source_path).await, Err(object_store::Error::NotFound { .. })), "File not deleted from source path after finalize");
        }
    }

    #[tokio::test]
    async fn test_orchestrator_decommission_node_with_shards() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let node_to_remove_id = InfluxNodeId::new(1);
        let node_to_remove_def = Arc::new(create_test_node_definition(1, "node_to_remove", NodeStatus::Active));
        let active_target_node_id = InfluxNodeId::new(2);
        let active_target_node_def = Arc::new(create_test_node_definition(2, "active_target_node", NodeStatus::Active));

        let shard1 = create_test_shard_definition(101, vec![1]);
        let db_name_str = "db_decom";
        let table_name_str = "table_decom";
        let db_schema1 = create_mock_db_schema_with_shards(db_name_str, table_name_str, vec![shard1.clone()]);

        let mock_parquet_files_decom = vec![
            TempParquetFilePlaceholder {
                id: 1, db_name: Arc::from(db_name_str), table_name: Arc::from(table_name_str), shard_id: shard1.id,
                path: format!("{}/{}/{}/{}/file_decom.parquet", node_to_remove_id.get(), db_name_str, table_name_str, shard1.id.get()),
                size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
            }
        ];
        let mock_parquet_files_decom_clone = mock_parquet_files_decom.clone();


        mock_catalog.expect_get_node().times(1).returning(move |_| Ok(Some(Arc::clone(&node_to_remove_def))));
        mock_catalog.expect_update_node_status().with(eq(node_to_remove_id), eq(NodeStatus::Leaving)).times(1).returning(|_,_| Ok(()));
        mock_catalog.expect_list_db_schema().times(1).returning(move || Ok(vec![db_schema1.clone()]));
        mock_catalog.expect_list_nodes().times(1).returning(move || Ok(vec![
            Arc::new(create_test_node_definition(1, "node_to_remove", NodeStatus::Leaving)),
            Arc::clone(&active_target_node_def)
        ]));

        let db_schema_for_loop = create_mock_db_schema_with_shards(db_name_str, table_name_str, vec![shard1.clone()]);
        mock_catalog.expect_db_schema()
            .with(eq(db_name_str))
            .returning(move |_| Ok(Some(db_schema_for_loop.clone())));


        mock_catalog.expect_begin_shard_migration_out().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(vec![active_target_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(vec![active_target_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard()
            .with(eq(db_name_str), eq(table_name_str), eq(shard1.id))
            .times(1)
            .returning(move |_,_,_| Ok(mock_parquet_files_decom_clone.clone()));
        mock_catalog.expect_mark_target_bootstrapped().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(node_to_remove_id), eq(active_target_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(node_to_remove_id), eq(active_target_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(node_to_remove_id), eq(active_target_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(active_target_node_id), eq(node_to_remove_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq(db_name_str), eq(table_name_str), eq(shard1.id), eq(node_to_remove_id), eq(active_target_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_update_node_status().with(eq(node_to_remove_id), eq(NodeStatus::Down)).times(1).returning(|_,_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, Arc::clone(&object_store));

        let dummy_content = Bytes::from_static(b"dummy data for decommission");
        for pf_template in &mock_parquet_files_decom {
             let source_path = ObjectPath::from(pf_template.path.as_str());
            object_store.put(&source_path, dummy_content.clone()).await.unwrap();
        }


        let strategy = RebalanceStrategy::DecommissionNode(node_to_remove_id);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance for DecommissionNode failed: {:?}", result.err());

        for pf_template in &mock_parquet_files_decom {
            let original_source_path = ObjectPath::from(pf_template.path.as_str());
            let mut parts = pf_template.path.splitn(2, '/');
            let _original_node_id_part = parts.next().unwrap();
            let rest_of_path = parts.next().unwrap();
            let target_path_str = format!("{}/{}", active_target_node_id.get(), rest_of_path);
            let target_path = ObjectPath::from(target_path_str.as_str());

            object_store.get(&target_path).await.expect("File not found at target path after copy for decommission");
            assert!(matches!(object_store.get(&original_source_path).await, Err(object_store::Error::NotFound { .. })), "File not deleted from source path after decommission");
        }
    }

    #[tokio::test]
    async fn test_orchestrator_add_new_node_catalog_failure_continues() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let new_node_def = create_test_node_definition(10, "newly_added_node", NodeStatus::Joining);
        let new_node_id = new_node_def.id;

        // Shard 1 (will fail), Shard 2 (will succeed)
        let shard_t1_s1 = create_test_shard_definition(10, vec![1]);
        let shard_t2_s1 = create_test_shard_definition(20, vec![2]);

        let db_schema = create_mock_db_schema_with_shards("db1", "table1", vec![shard_t1_s1.clone(), shard_t2_s1.clone()]);

        let mock_parquet_files_s1 = vec![TempParquetFilePlaceholder {
            id: 1, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_t1_s1.id,
            path: format!("{}/db1/table1/{}/s1_file1.parquet", shard_t1_s1.node_ids[0].get(), shard_t1_s1.id.get()),
            size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
        }];
        let mock_parquet_files_s2 = vec![TempParquetFilePlaceholder {
            id: 2, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_t2_s1.id,
            path: format!("{}/db1/table1/{}/s2_file1.parquet", shard_t2_s1.node_ids[0].get(), shard_t2_s1.id.get()),
            size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
        }];
        let mock_parquet_files_s2_clone = mock_parquet_files_s2.clone();


        mock_catalog.expect_add_node().times(1).returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema().times(1).returning(move || Ok(vec![db_schema.clone()]));

        // Shard 1 - begin_shard_migration_out fails
        mock_catalog.expect_begin_shard_migration_out()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), eq(vec![new_node_id]))
            .times(1)
            .returning(|_,_,_,_| Err(influxdb3_catalog::Error::DatabaseNotFound("db1".to_string()))); // Simulate error
        mock_catalog.expect_mark_shard_migration_failed()
            .with(eq("db1"), eq("table1"), eq(shard_t1_s1.id), always()) // any error message
            .times(1)
            .returning(|_,_,_,_| Ok(()));

        // Shard 2 - all operations succeed
        let primary_source_t2_s1 = shard_t2_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard()
            .with(eq("db1"), eq("table1"), eq(shard_t2_s1.id))
            .times(1)
            .returning(move |_,_,_| Ok(mock_parquet_files_s2_clone.clone()));
        mock_catalog.expect_mark_target_bootstrapped().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(new_node_id), eq(primary_source_t2_s1)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq("db1"), eq("table1"), eq(shard_t2_s1.id), eq(primary_source_t2_s1), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));

        // Mock db_schema for logging final owners (for the successful shard)
        let db_schema_for_logging_s2 = create_mock_db_schema_with_shards("db1", "table1", vec![shard_t2_s1.clone()]);
        mock_catalog.expect_db_schema().with(eq("db1")).returning(move |_| Ok(Some(db_schema_for_logging_s2.clone())));


        mock_catalog.expect_update_node_status().with(eq(new_node_id), eq(NodeStatus::Active)).times(1).returning(|_,_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, Arc::clone(&object_store));

        // Manually put dummy file for shard_t2_s1 (successful one)
        let dummy_content = Bytes::from_static(b"dummy parquet data");
        let s2_pf = &mock_parquet_files_s2[0];
        let source_path_s2 = ObjectPath::from(s2_pf.path.as_str());
        object_store.put(&source_path_s2, dummy_content.clone()).await.unwrap();


        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance should succeed overall even if one shard fails: {:?}", result.err());

        // Verify file for s2 was "copied" and then "deleted"
        let mut parts_s2 = s2_pf.path.splitn(2, '/');
        let _original_node_id_part_s2 = parts_s2.next().unwrap();
        let rest_of_path_s2 = parts_s2.next().unwrap();
        let target_path_s2_str = format!("{}/{}", new_node_id.get(), rest_of_path_s2);
        let target_path_s2 = ObjectPath::from(target_path_s2_str.as_str());

        object_store.get(&target_path_s2).await.expect("File for s2 not found at target path after copy");
        assert!(matches!(object_store.get(&source_path_s2).await, Err(object_store::Error::NotFound { .. })), "File for s2 not deleted from source path after finalize");
    }

    #[tokio::test]
    async fn test_orchestrator_add_new_node_list_files_failure_continues() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let new_node_def = create_test_node_definition(10, "newly_added_node", NodeStatus::Joining);
        let new_node_id = new_node_def.id;

        let shard_s1 = create_test_shard_definition(10, vec![1]); // Will fail at list_parquet_files_for_shard
        let shard_s2 = create_test_shard_definition(20, vec![2]); // Will succeed

        let db_schema = create_mock_db_schema_with_shards("db1", "table1", vec![shard_s1.clone(), shard_s2.clone()]);

        let mock_parquet_files_s2 = vec![TempParquetFilePlaceholder {
            id: 1, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_s2.id,
            path: format!("{}/db1/table1/{}/s2_file1.parquet", shard_s2.node_ids[0].get(), shard_s2.id.get()),
            size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
        }];
        let mock_parquet_files_s2_clone = mock_parquet_files_s2.clone();

        mock_catalog.expect_add_node().times(1).returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema().times(1).returning(move || Ok(vec![db_schema.clone()]));

        // Shard 1 - list_parquet_files_for_shard fails
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard()
            .with(eq("db1"), eq("table1"), eq(shard_s1.id))
            .times(1)
            .returning(|_,_,_| Err(influxdb3_catalog::Error::InternalError("Simulated list_files error".to_string())));
        mock_catalog.expect_mark_shard_migration_failed()
            .with(eq("db1"), eq("table1"), eq(shard_s1.id), always())
            .times(1)
            .returning(|_,_,_,_| Ok(()));

        // Shard 2 - all operations succeed
        let primary_source_s2 = shard_s2.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard().with(eq("db1"), eq("table1"), eq(shard_s2.id)).times(1).returning(move |_,_,_| Ok(mock_parquet_files_s2_clone.clone()));
        mock_catalog.expect_mark_target_bootstrapped().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(new_node_id), eq(primary_source_s2)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));

        let db_schema_for_logging_s2 = create_mock_db_schema_with_shards("db1", "table1", vec![shard_s2.clone()]);
        mock_catalog.expect_db_schema().with(eq("db1")).returning(move |_| Ok(Some(db_schema_for_logging_s2.clone())));

        mock_catalog.expect_update_node_status().with(eq(new_node_id), eq(NodeStatus::Active)).times(1).returning(|_,_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, Arc::clone(&object_store));

        let dummy_content = Bytes::from_static(b"dummy parquet data for s2");
        let s2_pf_path = &mock_parquet_files_s2[0].path;
        object_store.put(&ObjectPath::from(s2_pf_path.as_str()), dummy_content.clone()).await.unwrap();

        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance should succeed overall: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_orchestrator_add_new_node_object_store_copy_failure_continues() {
        let mut mock_catalog = MockCatalog::new();
        let mock_node_info_provider = Arc::new(MockNodeInfoProvider::new());

        let new_node_def = create_test_node_definition(10, "newly_added_node", NodeStatus::Joining);
        let new_node_id = new_node_def.id;

        let shard_s1 = create_test_shard_definition(10, vec![1]); // Will fail at object_store.copy
        let shard_s2 = create_test_shard_definition(20, vec![2]); // Will succeed

        let db_schema = create_mock_db_schema_with_shards("db1", "table1", vec![shard_s1.clone(), shard_s2.clone()]);

        let s1_file_path_str = format!("{}/db1/table1/{}/s1_file1.parquet", shard_s1.node_ids[0].get(), shard_s1.id.get());
        let mock_parquet_files_s1 = vec![TempParquetFilePlaceholder {
            id: 1, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_s1.id,
            path: s1_file_path_str.clone(),
            size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
        }];
         let mock_parquet_files_s1_clone = mock_parquet_files_s1.clone();

        let s2_file_path_str = format!("{}/db1/table1/{}/s2_file1.parquet", shard_s2.node_ids[0].get(), shard_s2.id.get());
        let mock_parquet_files_s2 = vec![TempParquetFilePlaceholder {
            id: 2, db_name: Arc::from("db1"), table_name: Arc::from("table1"), shard_id: shard_s2.id,
            path: s2_file_path_str.clone(),
            size_bytes: 100, row_count: 10, min_time: 0, max_time: 100,
        }];
        let mock_parquet_files_s2_clone = mock_parquet_files_s2.clone();

        // Use FaultyObjectStore
        let in_memory_store = Arc::new(InMemory::new());
        let faulty_store = Arc::new(FaultyObjectStore::new(Arc::clone(&in_memory_store), Some(s1_file_path_str.clone()), Some("copy".to_string())));


        mock_catalog.expect_add_node().times(1).returning(|_| Ok(()));
        mock_catalog.expect_list_db_schema().times(1).returning(move || Ok(vec![db_schema.clone()]));

        // Shard 1 - object_store.copy fails
        let primary_source_s1 = shard_s1.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_s1.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard().with(eq("db1"), eq("table1"), eq(shard_s1.id)).times(1).returning(move |_,_,_| Ok(mock_parquet_files_s1_clone.clone()));
        mock_catalog.expect_mark_shard_migration_failed().with(eq("db1"), eq("table1"), eq(shard_s1.id), always()).times(1).returning(|_,_,_,_| Ok(()));
        // No further catalog calls for shard_s1 after copy failure and marking as failed

        // Shard 2 - all operations succeed
        let primary_source_s2 = shard_s2.node_ids[0];
        mock_catalog.expect_begin_shard_migration_out().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_mark_source_data_flushed().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(vec![new_node_id])).times(1).returning(|_,_,_,_| Ok(()));
        mock_catalog.expect_list_parquet_files_for_shard().with(eq("db1"), eq("table1"), eq(shard_s2.id)).times(1).returning(move |_,_,_| Ok(mock_parquet_files_s2_clone.clone()));
        mock_catalog.expect_mark_target_bootstrapped().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_wal_syncing().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_mark_target_ready_for_cutover().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_commit_shard_migration_on_target().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(new_node_id), eq(primary_source_s2)).times(1).returning(|_,_,_,_,_| Ok(()));
        mock_catalog.expect_finalize_shard_migration_on_source().with(eq("db1"), eq("table1"), eq(shard_s2.id), eq(primary_source_s2), eq(new_node_id)).times(1).returning(|_,_,_,_,_| Ok(()));

        let db_schema_for_logging_s2 = create_mock_db_schema_with_shards("db1", "table1", vec![shard_s2.clone()]);
        mock_catalog.expect_db_schema().with(eq("db1")).returning(move |_| Ok(Some(db_schema_for_logging_s2.clone())));

        mock_catalog.expect_update_node_status().with(eq(new_node_id), eq(NodeStatus::Active)).times(1).returning(|_,_| Ok(()));

        let orchestrator = RebalanceOrchestrator::new(Arc::new(mock_catalog), mock_node_info_provider, faulty_store);

        let dummy_content = Bytes::from_static(b"dummy data");
        // Put files for both s1 (will fail copy) and s2 (will succeed) into the underlying InMemory store
        in_memory_store.put(&ObjectPath::from(s1_file_path_str.as_str()), dummy_content.clone()).await.unwrap();
        in_memory_store.put(&ObjectPath::from(s2_file_path_str.as_str()), dummy_content.clone()).await.unwrap();

        let strategy = RebalanceStrategy::AddNewNode(new_node_def);
        let result = orchestrator.initiate_rebalance(strategy).await;
        assert!(result.is_ok(), "Rebalance should succeed overall: {:?}", result.err());
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
    #[error("Failed to parse object store path: {0}")]
    ObjectStorePathError(String),
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
    #[allow(dead_code)]
    node_info_provider: Arc<dyn NodeInfoProvider>,
    object_store: Arc<dyn ObjectStore>,
}

impl RebalanceOrchestrator {
    pub fn new(
        catalog: Arc<Catalog>,
        node_info_provider: Arc<dyn NodeInfoProvider>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self { catalog, node_info_provider, object_store }
    }

    fn parse_source_node_from_path(path_str: &str) -> Result<String, ClusterManagerError> {
        path_str.split('/').next().map(String::from).ok_or_else(||
            ClusterManagerError::ObjectStorePathError(format!("Could not parse source node ID from path: {}", path_str))
        )
    }

    fn construct_target_path(source_path_str: &str, target_node_id: NodeId) -> Result<ObjectPath, ClusterManagerError> {
        let mut parts: Vec<&str> = source_path_str.split('/').collect();
        if parts.is_empty() {
            return Err(ClusterManagerError::ObjectStorePathError(format!("Path is empty: {}", source_path_str)));
        }
        parts[0] = Box::leak(target_node_id.get().to_string().into_boxed_str()); // Replace node_id part
        Ok(ObjectPath::from_iter(parts))
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

                for db_schema in all_dbs {
                    for table_def_arc in db_schema.tables() {
                        if let Some(shard_to_move_arc) = table_def_arc.shards.resource_iter().next() {
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
                    let primary_data_source_node_id = original_source_node_ids[0];

                    // 1. Begin Migration
                    if let Err(e) = self.catalog.begin_shard_migration_out(&db_name, &table_name, &shard_id, vec![new_node_id]).await {
                        error!("Catalog error during begin_shard_migration_out for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at begin_shard_migration_out: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                            error!("Failed to mark shard {} as failed after begin_shard_migration_out error: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Marked shard {:?} for migration to {:?}.", shard_id, new_node_id);

                    // 2. Source Flushes Data (Conceptual) -> Update Catalog State
                    info!("Rebalance: Source node {:?} conceptually flushing data for shard {:?}.", primary_data_source_node_id, shard_id);
                    if let Err(e) = self.catalog.mark_source_data_flushed(&db_name, &table_name, &shard_id, vec![new_node_id]).await {
                        error!("Catalog error during mark_source_data_flushed for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_source_data_flushed: {:?}", e);
                         if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                            error!("Failed to mark shard {} as failed after mark_source_data_flushed error: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    // 3. List Parquet files from Catalog
                    let parquet_files_to_copy = match self.catalog.list_parquet_files_for_shard(&db_name, &table_name, shard_id).await {
                        Ok(files) => files,
                        Err(e) => {
                            error!("Failed to list Parquet files for shard {}.{}.{}: {:?}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), e);
                            let err_msg = format!("Failed to list Parquet files: {:?}", e);
                            if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                               error!("Failed to mark shard {} as failed after list_parquet_files_for_shard error: {:?}", shard_id.get(), mark_err);
                            }
                            continue;
                        }
                    };
                    info!("Rebalance: Identified {} Parquet files for shard {:?} to copy.", parquet_files_to_copy.len(), shard_id);

                    // 4. Copy Parquet Files
                    let mut copy_failed = false;
                    for pf_placeholder in &parquet_files_to_copy {
                        let source_object_path = ObjectPath::from(pf_placeholder.path.as_str());
                        let target_object_path = match Self::construct_target_path(&pf_placeholder.path, new_node_id) {
                            Ok(p) => p,
                            Err(e) => {
                                error!("Failed to construct target path for shard {}.{}.{}: {:?}. Path: {}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), e, pf_placeholder.path);
                                let err_msg = format!("Failed to construct target path: {:?}", e);
                                if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                                    error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                                }
                                copy_failed = true; break;
                            }
                        };

                        info!("Rebalance: Copying Parquet file from '{}' to '{}'", source_object_path, target_object_path);
                        if let Err(e) = self.object_store.copy(&source_object_path, &target_object_path).await {
                            error!("ObjectStore copy failed for shard {}.{}.{} from {} to {}: {:?}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), source_object_path, target_object_path, e);
                            let err_msg = format!("ObjectStore copy failed: {:?}", e);
                            if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                                error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                            }
                            copy_failed = true; break;
                        }
                    }
                    if copy_failed { continue; }

                    // 5. Target Bootstraps (Conceptual) -> Update Catalog State
                    info!("Rebalance: Target node {:?} conceptually bootstrapped shard {:?} from Parquet files.", new_node_id, shard_id);
                    if let Err(e) = self.catalog.mark_target_bootstrapped(&db_name, &table_name, &shard_id, primary_data_source_node_id, new_node_id).await {
                        error!("Catalog error during mark_target_bootstrapped for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_bootstrapped: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                            error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    // 6. Target WAL Syncing (Conceptual) -> Update Catalog State
                    info!("Rebalance: Target node {:?} starting WAL sync for shard {:?} from source {:?}.", new_node_id, shard_id, primary_data_source_node_id);
                    if let Err(e) = self.catalog.mark_target_wal_syncing(&db_name, &table_name, &shard_id, primary_data_source_node_id, new_node_id).await {
                         error!("Catalog error during mark_target_wal_syncing for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_wal_syncing: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                            error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Target node {:?} finished WAL sync for shard {:?}.", new_node_id, shard_id);
                    if let Err(e) = self.catalog.mark_target_ready_for_cutover(&db_name, &table_name, &shard_id, primary_data_source_node_id, new_node_id).await {
                        error!("Catalog error during mark_target_ready_for_cutover for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_ready_for_cutover: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    // 7. Commit on Target (Updates shard ownership in catalog)
                    if let Err(e) = self.catalog.commit_shard_migration_on_target(&db_name, &table_name, &shard_id, new_node_id, primary_data_source_node_id).await {
                        error!("Catalog error during commit_shard_migration_on_target for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at commit_shard_migration_on_target: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                            error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Committed shard {:?} to target node {:?}. It is now an owner.", shard_id, new_node_id);

                    // 8. Finalize on Source (Removes source from owners, sets to Stable)
                    if let Err(e) = self.catalog.finalize_shard_migration_on_source(&db_name, &table_name, &shard_id, primary_data_source_node_id, new_node_id).await {
                        error!("Catalog error during finalize_shard_migration_on_source for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at finalize_shard_migration_on_source: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Finalized migration for shard {:?}. Source {:?} removed as owner. New primary owner is {:?}.", shard_id, primary_data_source_node_id, new_node_id);

                    // 9. Delete files from old source node
                    for pf_placeholder in &parquet_files_to_copy {
                        let old_source_object_path = ObjectPath::from(pf_placeholder.path.as_str());
                        match Self::parse_source_node_from_path(&pf_placeholder.path) {
                            Ok(parsed_source_node_str) if parsed_source_node_str == primary_data_source_node_id.get().to_string() => {
                                info!("Rebalance: Deleting Parquet file from old source location '{}'", old_source_object_path);
                                if let Err(e) = self.object_store.delete(&old_source_object_path).await {
                                     warn!("Rebalance: Failed to delete {}: {}. Manual cleanup may be required.", old_source_object_path, e);
                                }
                            }
                            Ok(parsed_source_node_str) => {
                                warn!("Rebalance: Skipping deletion of {} as its path source node {} does not match current primary data source node {}.",
                                    pf_placeholder.path, parsed_source_node_str, primary_data_source_node_id.get()
                                );
                            }
                            Err(e) => {
                                warn!("Rebalance: Failed to parse source node from path {} for deletion: {:?}. Skipping delete.", pf_placeholder.path, e);
                            }
                        }
                    }

                    info!("Migration complete for shard {:?} from node {:?} to new node ID {:?}. Final owners: {:?}.",
                        shard_id, primary_data_source_node_id, new_node_id,
                        self.catalog.db_schema(&db_name).await.map(|opt_db| opt_db.and_then(|db| db.table_definition(&table_name)?.shards.get_by_id(&shard_id).map(|s| s.node_ids.clone()))).unwrap_or_default().unwrap_or_default()
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
                                ));
                            }
                        }
                    }
                }

                if shards_on_decommissioning_node.is_empty() {
                    info!("Rebalance: DecommissionNode - Node ID {:?} has no shards. Marking as Down.", node_id_to_remove);
                    self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Down).await?;
                    return Ok(());
                }

                let active_nodes = self.catalog.list_nodes().await?
                    .into_iter()
                    .filter(|n| n.id != node_id_to_remove && n.status == NodeStatus::Active)
                    .map(|n| n.id)
                    .collect::<Vec<_>>();

                if active_nodes.is_empty() {
                    error!("Rebalance: DecommissionNode - No active nodes available to migrate shards from ID {:?}. Reverting status to Active.", node_id_to_remove);
                    if let Err(e) = self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Active).await {
                        error!("Failed to revert node {:?} status to Active: {}", node_id_to_remove, e);
                    }
                    return Err(ClusterManagerError::NoTargetNodes);
                }

                let mut node_picker = active_nodes.iter().cycle();

                for (db_name, table_name, shard_id) in shards_on_decommissioning_node {
                    // Re-fetch current shard definition in case it was modified by a previous iteration (e.g. if it was on multiple nodes including the decommissioning one)
                    let current_shard_def = match self.catalog.db_schema(&db_name).await {
                        Ok(Some(db)) => match db.table_definition(&table_name) {
                            Some(table) => match table.shards.get_by_id(&shard_id) {
                                Some(sd) => sd,
                                None => {
                                    warn!("Shard {:?} in table {}.{} not found during decommission loop. Skipping.", shard_id, db_name, table_name);
                                    continue;
                                }
                            },
                            None => {
                                warn!("Table {}.{} not found during decommission loop for shard {:?}. Skipping.", db_name, table_name, shard_id);
                                continue;
                            }
                        },
                        _ => {
                             warn!("Database {} not found during decommission loop for shard {:?}. Skipping.", db_name, shard_id);
                             continue;
                        }
                    };

                    if !current_shard_def.node_ids.contains(&node_id_to_remove) {
                        info!("Shard {:?} in table {}.{} no longer on decommissioning node {:?}. Skipping.", shard_id, db_name, table_name, node_id_to_remove);
                        continue;
                    }

                    let chosen_target_node_id = *node_picker.next().unwrap_or(&active_nodes[0]);

                    info!(
                        "Rebalance: Starting migration for shard {:?} from table {}.{} on decommissioning node ID {:?} to target node ID {:?}",
                        shard_id, db_name, table_name, node_id_to_remove, chosen_target_node_id
                    );

                    if let Err(e) = self.catalog.begin_shard_migration_out(&db_name, &table_name, &shard_id, vec![chosen_target_node_id]).await {
                        error!("Catalog error during begin_shard_migration_out for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at begin_shard_migration_out: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Marked shard {:?} for migration from {:?} to {:?}.", shard_id, node_id_to_remove, chosen_target_node_id);

                    if let Err(e) = self.catalog.mark_source_data_flushed(&db_name, &table_name, &shard_id, vec![chosen_target_node_id]).await {
                        error!("Catalog error during mark_source_data_flushed for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_source_data_flushed: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    let parquet_files_to_copy = match self.catalog.list_parquet_files_for_shard(&db_name, &table_name, shard_id).await {
                        Ok(files) => files,
                        Err(e) => {
                            error!("Failed to list Parquet files for shard {}.{}.{}: {:?}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), e);
                             let err_msg = format!("Failed to list Parquet files: {:?}", e);
                            if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                               error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                            }
                            continue;
                        }
                    };
                    info!("Rebalance: Identified {} Parquet files for shard {:?} to copy.", parquet_files_to_copy.len(), shard_id);

                    let mut copy_failed = false;
                    for pf_placeholder in &parquet_files_to_copy {
                        let source_object_path = ObjectPath::from(pf_placeholder.path.as_str());
                        let target_object_path = match Self::construct_target_path(&pf_placeholder.path, chosen_target_node_id) {
                             Ok(p) => p,
                             Err(e) => {
                                error!("Failed to construct target path for shard {}.{}.{}: {:?}. Path: {}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), e, pf_placeholder.path);
                                let err_msg = format!("Failed to construct target path: {:?}", e);
                                if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                                    error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                                }
                                copy_failed = true; break;
                            }
                        };

                        info!("Rebalance: Copying Parquet file from '{}' to '{}'", source_object_path, target_object_path);
                        if let Err(e) = self.object_store.copy(&source_object_path, &target_object_path).await {
                            error!("ObjectStore copy failed for shard {}.{}.{} from {} to {}: {:?}. Marking shard migration as failed.", db_name, table_name, shard_id.get(), source_object_path, target_object_path, e);
                            let err_msg = format!("ObjectStore copy failed: {:?}", e);
                            if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                                error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                            }
                            copy_failed = true; break;
                        }
                    }
                    if copy_failed { continue; }

                    if let Err(e) = self.catalog.mark_target_bootstrapped(&db_name, &table_name, &shard_id, node_id_to_remove, chosen_target_node_id).await {
                         error!("Catalog error during mark_target_bootstrapped for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_bootstrapped: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    if let Err(e) = self.catalog.mark_target_wal_syncing(&db_name, &table_name, &shard_id, node_id_to_remove, chosen_target_node_id).await {
                        error!("Catalog error during mark_target_wal_syncing for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_wal_syncing: {:?}", e);
                         if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    if let Err(e) = self.catalog.mark_target_ready_for_cutover(&db_name, &table_name, &shard_id, node_id_to_remove, chosen_target_node_id).await {
                        error!("Catalog error during mark_target_ready_for_cutover for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at mark_target_ready_for_cutover: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }

                    if let Err(e) = self.catalog.commit_shard_migration_on_target(&db_name, &table_name, &shard_id, chosen_target_node_id, node_id_to_remove).await {
                        error!("Catalog error during commit_shard_migration_on_target for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at commit_shard_migration_on_target: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Committed shard {:?} to target node {:?}.", shard_id, chosen_target_node_id);

                    if let Err(e) = self.catalog.finalize_shard_migration_on_source(&db_name, &table_name, &shard_id, node_id_to_remove, chosen_target_node_id).await {
                        error!("Catalog error during finalize_shard_migration_on_source for shard {}.{}.{}: {:?}. Halting migration for this shard.", db_name, table_name, shard_id.get(), e);
                        let err_msg = format!("Catalog error at finalize_shard_migration_on_source: {:?}", e);
                        if let Err(mark_err) = self.catalog.mark_shard_migration_failed(&db_name, &table_name, &shard_id, err_msg).await {
                           error!("Failed to mark shard {} as failed: {:?}", shard_id.get(), mark_err);
                        }
                        continue;
                    }
                    info!("Rebalance: Finalized migration for shard {:?}. Decommissioning source {:?} removed as owner.", shard_id, node_id_to_remove);

                    for pf_placeholder in &parquet_files_to_copy {
                        let old_source_object_path = ObjectPath::from(pf_placeholder.path.as_str());
                         match Self::parse_source_node_from_path(&pf_placeholder.path) {
                            Ok(parsed_source_node_str) if parsed_source_node_str == node_id_to_remove.get().to_string() => {
                                info!("Rebalance: Deleting Parquet file from decommissioning node's location '{}'", old_source_object_path);
                                if let Err(e) = self.object_store.delete(&old_source_object_path).await {
                                    warn!("Rebalance: Failed to delete {}: {}. Manual cleanup may be required.", old_source_object_path, e);
                                }
                            }
                            Ok(parsed_source_node_str) => {
                                 warn!("Rebalance: Skipping deletion of {} as its path source node {} does not match decommissioning node {}.",
                                    pf_placeholder.path, parsed_source_node_str, node_id_to_remove.get()
                                );
                            }
                            Err(e) => {
                                warn!("Rebalance: Failed to parse source node from path {} for deletion: {:?}. Skipping delete.", pf_placeholder.path, e);
                            }
                        }
                    }

                    info!("Migration complete for shard {:?} from ID {:?} to ID {:?}. Final owners: {:?}.",
                        shard_id, node_id_to_remove, chosen_target_node_id,
                        self.catalog.db_schema(&db_name).await.map(|opt_db| opt_db.and_then(|db| db.table_definition(&table_name)?.shards.get_by_id(&shard_id).map(|s| s.node_ids.clone()))).unwrap_or_default().unwrap_or_default()
                    );
                }

                info!("All shards migrated off node ID {:?}. Marking as Down.", node_id_to_remove);
                self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Down).await?;
            }
        }
        Ok(())
    }
}
