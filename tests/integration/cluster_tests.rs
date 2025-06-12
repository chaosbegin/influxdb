use std::sync::Arc;
use std::time::Duration;
use influxdb3_client::Client as InfluxDBClient;
use influxdb3_client::Error as ClientError;
use influxdb3_id::NodeId as InternalNodeId;
use influxdb3_server::management::generated_types::cluster_management_service_client::ClusterManagementServiceClient;
use influxdb3_server::management::generated_types::{AddNodeRequest, ListNodesRequest, NodeDefinition as ManagementNodeDefinition, CreateShardRequest, ShardDefinition as ManagementShardDefinition, ShardTimeRange as ManagementShardTimeRange, UpdateTableShardingStrategyRequest, ShardingStrategy as ManagementShardingStrategy};
use object_store::memory::InMemory;
use object_store::{DynObjectStore, ObjectStore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info, warn};
use uuid::Uuid;
use futures::FutureExt;
use futures::pin_mut;

// Actual Server Components
use influxdb3::commands::serve::Config as MainServeConfig; // Actual config from main crate
use influxdb3_server::CommonServerState;
use influxdb3_server::builder::ServerBuilder;
use influxdb3_server::query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::WalConfig;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
use influxdb3_write::sharding_utils::calculate_hash as server_calculate_hash;
use iox_query::exec::{Executor, ExecutorConfig, DedicatedExecutor, PerQueryMemoryPoolConfig};
use iox_time::SystemProvider;
use parquet_file::storage::{ParquetStorage, StorageId};
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use std::collections::HashMap;
use tokio::net::TcpListener;
use influxdb3_clap_blocks::object_store::ObjectStoreConfig;
use influxdb3_clap_blocks::socket_addr::SocketAddr as ClapSocketAddr;
use influxdb3_shutdown::ShutdownManager;


#[derive(Debug, Clone)]
pub struct ServerInstanceConfig {
    pub node_id_prefix: String,
    pub http_port: u16,
    pub grpc_port: u16,
    pub seed_nodes: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct ServerInstanceHandle {
    pub node_id_prefix: String,
    pub config: ServerInstanceConfig,
    pub shutdown_token: CancellationToken,
    pub task_handle: JoinHandle<()>,
    pub http_base_url: String,
    pub grpc_addr_str: String,
    pub cluster_mgmt_client: Option<ClusterManagementServiceClient<Channel>>,
    pub flight_client: Option<InfluxDBClient<Channel>>,
}

async fn run_server_logic_for_test(
    mut config: MainServeConfig,
    grpc_port: u16, // Pass gRPC port separately
    shared_data_store: Arc<DynObjectStore>,
    shared_catalog_store: Arc<DynObjectStore>,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    let startup_timer = tokio::time::Instant::now();
    let metrics = Arc::new(metric::Registry::default());
    let time_provider = Arc::new(SystemProvider::new());
    let shutdown_manager = ShutdownManager::new(shutdown_token.clone());

    config.object_store_config.data_path = format!("mem://data_{}/", config.node_identifier_prefix).into();
    config.object_store_config.catalog_path = format!("mem://catalog_{}/", config.node_identifier_prefix).into();

    let catalog = Catalog::new_with_shutdown(
        config.node_identifier_prefix.as_str(),
        Arc::clone(&shared_catalog_store),
        Arc::clone(&time_provider),
        Arc::clone(&metrics),
        shutdown_manager.register(),
        Default::default(),
    ).await?;
    info!(node_id = %config.node_identifier_prefix, catalog_uuid = %catalog.catalog_uuid(), "Catalog initialized for test instance {}", config.node_identifier_prefix);

    let _ = catalog.register_node(
        &config.node_identifier_prefix,
        num_cpus::get() as u64,
        vec![influxdb3_catalog::log::NodeMode::Core]
    ).await?;

    let persister = Arc::new(Persister::new(
        Arc::clone(&shared_data_store),
        config.node_identifier_prefix.as_str(),
        Arc::clone(&time_provider),
        config.parquet_row_group_write_size.unwrap_or(influxdb3_write::persister::DEFAULT_ROW_GROUP_WRITE_SIZE),
    ));

    let wal_config = WalConfig {
        gen1_duration: config.gen1_duration,
        max_write_buffer_size: config.wal_max_write_buffer_size,
        flush_interval: config.wal_flush_interval.into(),
        snapshot_size: config.wal_snapshot_size,
    };

    let tokio_datafusion_config = config.tokio_datafusion_config.clone();
    let write_path_executor_config = ExecutorConfig {
        target_query_partitions: tokio_datafusion_config.num_threads.unwrap_or_else(|| std::num::NonZeroUsize::new(1).unwrap()),
        object_stores: [(ParquetStorage::DEFAULT_ID, Arc::clone(&shared_data_store))].into_iter().collect(),
        metric_registry: Arc::clone(&metrics),
        mem_pool_size: usize::MAX,
        per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
        heap_memory_limit: None,
    };
    let write_path_executor = Arc::new(Executor::new_with_config_and_executor(
        write_path_executor_config,
        DedicatedExecutor::new(&format!("test_write_{}", config.node_identifier_prefix), tokio_datafusion_config.builder()?, Arc::clone(&metrics)),
    ));

    let write_buffer = Arc::new(WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache: Arc::new(influxdb3_cache::last_cache::LastCacheNop::new()),
        distinct_cache: Arc::new(influxdb3_cache::distinct_cache::DistinctCacheNop::new()),
        time_provider: Arc::clone(&time_provider),
        executor: write_path_executor,
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::clone(&metrics),
        snapshotted_wal_files_to_keep: config.snapshotted_wal_files_to_keep,
        query_file_limit: config.query_file_limit,
        shutdown: shutdown_manager.register(),
        wal_replay_concurrency_limit: config.wal_replay_concurrency_limit,
        current_node_id: Arc::from(config.node_identifier_prefix.as_str()),
        max_snapshots_to_load_on_start: config.max_snapshots_to_load_on_start,
        replication_client_factory: influxdb3_write::replication_client::default_replication_client_factory(),
    }).await?);

    let query_executor_exec_config = ExecutorConfig {
        target_query_partitions: config.tokio_datafusion_config.num_threads.unwrap_or_else(|| std::num::NonZeroUsize::new(1).unwrap()),
        object_stores: [(ParquetStorage::DEFAULT_ID, Arc::clone(&shared_data_store))].into_iter().collect(),
        metric_registry: Arc::clone(&metrics),
        mem_pool_size: config.exec_mem_pool_bytes.as_num_bytes(),
        per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
        heap_memory_limit: None,
    };
     let query_executor_exec = Arc::new(Executor::new_with_config_and_executor(
        query_executor_exec_config,
        DedicatedExecutor::new(&format!("test_query_{}", config.node_identifier_prefix), config.tokio_datafusion_config.builder()?, Arc::clone(&metrics)),
    ));

    let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: Arc::clone(&catalog),
        write_buffer: Arc::clone(&write_buffer),
        exec: query_executor_exec,
        metrics: Arc::clone(&metrics),
        datafusion_config: Arc::new(config.iox_query_datafusion_config.build()),
        query_log_size: config.query_log_size,
        telemetry_store: Arc::new(influxdb3_telemetry::store::TelemetryStore::disabled()),
        sys_events_store: Arc::new(influxdb3_sys_events::SysEventStore::new(Arc::clone(&time_provider))),
        started_with_auth: !config.without_auth,
        time_provider: Arc::clone(&time_provider),
        current_node_id: Arc::from(config.node_identifier_prefix.as_str()),
    }));

    let common_state = CommonServerState::new(
        Arc::clone(&metrics), None, TraceHeaderParser::new(), query_executor.telemetry_store(),
    );

    let http_listener = TcpListener::bind(config.http_bind_address.to_string()).await?;
    let grpc_listener = TcpListener::bind(format!("127.0.0.1:{}", grpc_port)).await?; // Use passed grpc_port

    let server = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(write_buffer)
        .query_executor(query_executor)
        .time_provider(Arc::clone(&time_provider))
        .persister(Arc::new(influxdb3_write::persister::NopPersister))
        .tcp_listener(http_listener)
        .grpc_listener(grpc_listener)
        .processing_engine(Arc::new(influxdb3_processing_engine::NoOpProcessingEngineManager::new()))
        .catalog(catalog)
        .build(config.cert_file.clone(), config.key_file.clone(), config.tls_minimum_version.into())
        .await;

    info!(node_id = %config.node_identifier_prefix, "Test server instance built. Starting serve loop.");
    let paths_without_authz: &'static Vec<&'static str> = Box::leak(Box::new(Vec::new()));

    let server_future = influxdb3_server::serve(
        server,
        shutdown_token.clone(),
        startup_timer,
        config.without_auth,
        paths_without_authz,
        config.tcp_listener_file_path.clone(),
    );

    pin_mut!(server_future);
    server_future.await?;

    info!(node_id = %config.node_identifier_prefix, "Test server instance shut down.");
    Ok(())
}


pub async fn start_server_instance(
    instance_config: ServerInstanceConfig,
    shared_data_store: Arc<DynObjectStore>,
    shared_catalog_store: Arc<DynObjectStore>,
) -> Result<ServerInstanceHandle, String> {

    let http_addr_str = format!("127.0.0.1:{}", instance_config.http_port);
    let grpc_addr_str = format!("127.0.0.1:{}", instance_config.grpc_port);

    let serve_config = MainServeConfig { // Use the actual ServeConfig from the main crate
        object_store_config: ObjectStoreConfig::new_mem(), // Default, paths will be overridden by run_server_logic_for_test
        logging_config: Default::default(),
        tracing_config: TracingConfig { traces_endpoint: None, ..Default::default() },
        tokio_datafusion_config: Default::default(),
        iox_query_datafusion_config: Default::default(),
        max_http_request_size: 10 * 1024 * 1024,
        http_bind_address: http_addr_str.parse::<ClapSocketAddr>().unwrap(),
        exec_mem_pool_bytes: influxdb3_clap_blocks::memory_size::MemorySizeMb::from_str("20%").unwrap(),
        without_auth: true,
        disable_authz: None,
        gen1_duration: influxdb3_wal::Gen1Duration::T10m,
        wal_flush_interval: humantime::parse_duration("1s").unwrap().into(),
        wal_snapshot_size: 600,
        wal_max_write_buffer_size: 100_000,
        snapshotted_wal_files_to_keep: 300,
        query_log_size: 1000,
        node_identifier_prefix: instance_config.node_id_prefix.clone(),
        parquet_mem_cache_size: influxdb3_clap_blocks::memory_size::MemorySizeMb::from_str("20%").unwrap(),
        parquet_mem_cache_prune_percentage: influxdb3_clap_blocks::object_store::ParquetCachePrunePercent::from_str("0.1").unwrap(),
        parquet_mem_cache_prune_interval: humantime::parse_duration("1s").unwrap().into(),
        disable_parquet_mem_cache: false,
        parquet_mem_cache_query_path_duration: humantime::parse_duration("5h").unwrap().into(),
        last_cache_eviction_interval: humantime::parse_duration("10s").unwrap().into(),
        distinct_cache_eviction_interval: humantime::parse_duration("10s").unwrap().into(),
        processing_engine_config: Default::default(),
        force_snapshot_mem_threshold: influxdb3_clap_blocks::memory_size::MemorySizeMb::from_str("50%").unwrap(),
        disable_telemetry_upload: true,
        telemetry_endpoint: "".to_string(),
        query_file_limit: Some(1000),
        key_file: None,
        cert_file: None,
        tls_minimum_version: Default::default(),
        tcp_listener_file_path: None,
        wal_replay_concurrency_limit: Some(4),
        hard_delete_default_duration: influxdb3_catalog::catalog::Catalog::DEFAULT_HARD_DELETE_DURATION.into(),
        catalog_max_dbs: Some(10),
        catalog_max_tables_total: Some(200),
        catalog_max_columns_per_table: Some(100),
        catalog_max_tag_columns_per_table: Some(50),
        max_snapshots_to_load_on_start: Some(10),
        parquet_row_group_write_size: Some(1024*1024),
        // grpc_bind_port_testing_only: Some(instance_config.grpc_port), // This field is not on the real ServeConfig
        // seed_nodes: instance_config.seed_nodes.clone(), // This field is not on the real ServeConfig
    };

    let shutdown_token = CancellationToken::new();
    let server_shutdown_token = shutdown_token.clone();
    let node_id_clone = instance_config.node_id_prefix.clone();
    // Pass the same token to ShutdownManager that server_future will use
    let shutdown_manager = ShutdownManager::new(server_shutdown_token.clone());


    let task_handle = tokio::spawn(async move {
        if let Err(e) = run_server_logic_for_test(
            serve_config,
            instance_config.grpc_port, // Pass grpc_port separately
            shared_data_store,
            shared_catalog_store,
            server_shutdown_token,
            shutdown_manager, // Pass shutdown_manager
        ).await {
            error!("Server instance {} failed: {:?}", node_id_clone, e);
        }
    });

    // Improved server startup synchronization
    let mut flight_client_opt = None;
    let mut mgmt_client_opt = None;
    let grpc_connect_addr_tonic = format!("http://{}", grpc_addr_str);

    for attempt in 0..20 { // Try for ~10 seconds (20 * 500ms)
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("Attempting to connect clients for node {} (attempt {})...", instance_config.node_id_prefix, attempt + 1);

        if flight_client_opt.is_none() {
            match InfluxDBClient::connect(grpc_connect_addr_tonic.clone()).await {
                Ok(c) => {
                    info!("Flight client connected for node {}", instance_config.node_id_prefix);
                    flight_client_opt = Some(c);
                }
                Err(_e) => { // warn!("Flight client connection attempt {} for node {} failed: {}", attempt + 1, instance_config.node_id_prefix, e);
                }
            }
        }

        if mgmt_client_opt.is_none() {
            match ClusterManagementServiceClient::connect(grpc_connect_addr_tonic.clone()).await {
                Ok(c) => {
                    info!("ClusterManagementService client connected for node {}", instance_config.node_id_prefix);
                    mgmt_client_opt = Some(c);
                }
                Err(_e) => { // warn!("ClusterManagementService client connection attempt {} for node {} failed: {}", attempt + 1, instance_config.node_id_prefix, e);
                }
            }
        }

        if flight_client_opt.is_some() && mgmt_client_opt.is_some() {
            info!("All clients connected for node {}", instance_config.node_id_prefix);
            break;
        }
    }

    if flight_client_opt.is_none() || mgmt_client_opt.is_none() {
        shutdown_token.cancel();
        let _ = task_handle.await;
        return Err(format!("Failed to connect one or more gRPC clients for node {} after multiple attempts.", instance_config.node_id_prefix));
    }

    Ok(ServerInstanceHandle {
        node_id_prefix: instance_config.node_id_prefix.clone(),
        http_base_url: format!("http://{}", http_addr_str),
        grpc_addr_str,
        config: instance_config,
        shutdown_token,
        task_handle,
        cluster_mgmt_client: mgmt_client_opt,
        flight_client: flight_client_opt,
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_client::Precision;
    use influxdb3_server::management::generated_types::{AddNodeRequest, ListNodesRequest, NodeDefinition as ManagementNodeDefinition, CreateShardRequest, ShardDefinition as ManagementShardDefinition, ShardTimeRange as ManagementShardTimeRange, UpdateTableShardingStrategyRequest, ShardingStrategy as ManagementShardingStrategy};
    // use std::collections::hash_map::DefaultHasher; // Not needed if using server_calculate_hash
    // use std::hash::{Hash, Hasher}; // Not needed

    fn management_node_def_from_handle(handle: &ServerInstanceHandle, catalog_node_id: u64) -> ManagementNodeDefinition {
        ManagementNodeDefinition {
            id: catalog_node_id,
            node_name: handle.node_id_prefix.clone(),
            instance_id: Uuid::new_v4().to_string(),
            rpc_address: handle.grpc_addr_str.clone(),
            http_address: handle.http_base_url.trim_start_matches("http://").to_string(),
            mode: vec!["Core".to_string()],
            core_count: num_cpus::get() as u64,
            status: "Unknown".to_string(),
            last_heartbeat: 0,
        }
    }

    async fn create_database(client: &mut InfluxDBClient<Channel>, db_name: &str) -> Result<(), ClientError> {
        info!("Creating database: {}", db_name);
        client.statement_post(format!("CREATE DATABASE \"{}\"", db_name)).await?;
        Ok(())
    }

    async fn create_table_with_timeandkey_sharding(
        flight_client: &mut InfluxDBClient<Channel>,
        mgmt_client: &mut ClusterManagementServiceClient<Channel>,
        db_name: &str,
        table_name: &str,
        shard_key_columns: Vec<String>
    ) -> Result<(), anyhow::Error> {
        info!("Creating table: {}.{} with TimeAndKey sharding on {:?}", db_name, table_name, shard_key_columns);

        let pk_statement = if !shard_key_columns.is_empty() {
             if shard_key_columns.is_empty() {
                 anyhow::bail!("Shard key columns cannot be empty for TimeAndKey sharding strategy during table creation helper.");
            }
            format!(", PRIMARY KEY ({})", shard_key_columns.join(","))
        } else {
            // This case should ideally not be hit if ShardingStrategy::TimeAndKey is chosen,
            // as it requires shard_key_columns. For a simple table, an empty PK might be okay,
            // but for sharding, this setup is incomplete.
            warn!("Creating table {} without explicit PRIMARY KEY for TimeAndKey sharding. This might not be intended.", table_name);
            "".to_string()
        };

        let tags_ddl = shard_key_columns.iter().map(|col| format!(", {} TAG", col)).collect::<String>();
        let ddl = format!("CREATE TABLE \"{}\" (time TIMESTAMP, val DOUBLE {}){}", table_name, tags_ddl, pk_statement);

        flight_client.statement_post_db(db_name.to_string(), ddl).await?;

        mgmt_client.update_table_sharding_strategy(UpdateTableShardingStrategyRequest {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            sharding_strategy: ManagementShardingStrategy::TimeAndKey as i32,
            shard_key_columns: shard_key_columns.clone(),
        }).await?;
        Ok(())
    }


    #[tokio::test]
    // #[ignore] // Temporarily re-enable to see if it passes with changes
    async fn test_2_node_basic_sharded_write_query() {
        let data_store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let catalog_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let config_node_a = ServerInstanceConfig {
            node_id_prefix: "node_a".to_string(),
            http_port: 18080,
            grpc_port: 18082,
            seed_nodes: None,
        };
        let grpc_addr_node_a = format!("127.0.0.1:{}", config_node_a.grpc_port);

        let config_node_b = ServerInstanceConfig {
            node_id_prefix: "node_b".to_string(),
            http_port: 18090,
            grpc_port: 18092,
            seed_nodes: Some(vec![grpc_addr_node_a.clone()]),
        };

        let mut handle_a = start_server_instance(config_node_a.clone(), Arc::clone(&data_store), Arc::clone(&catalog_store)).await.unwrap();
        let mut handle_b = start_server_instance(config_node_b.clone(), Arc::clone(&data_store), Arc::clone(&catalog_store)).await.unwrap();

        info!("Node A clients: mgmt={:?}, flight={:?}", handle_a.cluster_mgmt_client.is_some(), handle_a.flight_client.is_some());
        info!("Node B clients: mgmt={:?}, flight={:?}", handle_b.cluster_mgmt_client.is_some(), handle_b.flight_client.is_some());

        assert!(handle_a.cluster_mgmt_client.is_some(), "Node A mgmt client failed to connect");
        assert!(handle_a.flight_client.is_some(), "Node A flight client failed to connect");
        assert!(handle_b.cluster_mgmt_client.is_some(), "Node B mgmt client failed to connect");
        assert!(handle_b.flight_client.is_some(), "Node B flight client failed to connect");

        // --- Cluster Operations ---
        let nodes_on_a_initial = handle_a.cluster_mgmt_client.as_mut().unwrap().list_nodes(ListNodesRequest {}).await.unwrap().into_inner();
        let node_a_def_in_a = nodes_on_a_initial.nodes.iter().find(|n| n.node_name == handle_a.node_id_prefix).expect("Node A not found in its own list");
        let node_a_catalog_id = node_a_def_in_a.id;

        let nodes_on_b_initial = handle_b.cluster_mgmt_client.as_mut().unwrap().list_nodes(ListNodesRequest {}).await.unwrap().into_inner();
        let node_b_def_in_b = nodes_on_b_initial.nodes.iter().find(|n| n.node_name == handle_b.node_id_prefix).expect("Node B not found in its own list");
        let node_b_catalog_id = node_b_def_in_b.id;

        let node_a_for_b_mgmt_def = management_node_def_from_handle(&handle_a, node_a_catalog_id);
        let node_b_for_a_mgmt_def = management_node_def_from_handle(&handle_b, node_b_catalog_id);

        info!("Node A (id {}) adding Node B (reported id {})", node_a_catalog_id, node_b_catalog_id);
        handle_a.cluster_mgmt_client.as_mut().unwrap().add_node(AddNodeRequest { node_definition: Some(node_b_for_a_mgmt_def.clone()) }).await.expect("Node A failed to add Node B");
        info!("Node B (id {}) adding Node A (reported id {})", node_b_catalog_id, node_a_catalog_id);
        handle_b.cluster_mgmt_client.as_mut().unwrap().add_node(AddNodeRequest { node_definition: Some(node_a_for_b_mgmt_def.clone()) }).await.expect("Node B failed to add Node A");

        tokio::time::sleep(Duration::from_secs(1)).await;  // Allow for catalog sync

        let nodes_on_a_final = handle_a.cluster_mgmt_client.as_mut().unwrap().list_nodes(ListNodesRequest {}).await.unwrap().into_inner();
        let nodes_on_b_final = handle_b.cluster_mgmt_client.as_mut().unwrap().list_nodes(ListNodesRequest {}).await.unwrap().into_inner();

        assert!(nodes_on_a_final.nodes.iter().any(|n| n.node_name == handle_b.node_id_prefix), "Node A does not see Node B");
        assert!(nodes_on_b_final.nodes.iter().any(|n| n.node_name == handle_a.node_id_prefix), "Node B does not see Node A");

        let node_a_resolved_id = nodes_on_a_final.nodes.iter().find(|n| n.node_name == handle_a.node_id_prefix).unwrap().id;
        let node_b_resolved_id = nodes_on_a_final.nodes.iter().find(|n| n.node_name == handle_b.node_id_prefix).unwrap().id;

        // --- Schema & Sharding Setup (via Node A) ---
        let db_name = "testdb_sharded_2node";
        let table_name = "metrics2node";
        let shard_key_col = "host_tag".to_string();

        create_database(handle_a.flight_client.as_mut().unwrap(), db_name).await.unwrap();
        create_table_with_timeandkey_sharding(
            handle_a.flight_client.as_mut().unwrap(),
            handle_a.cluster_mgmt_client.as_mut().unwrap(),
            db_name,
            table_name,
            vec![shard_key_col.clone()]
        ).await.unwrap();

        let shard_s1 = ManagementShardDefinition {
            id: 1,
            time_range: Some(ManagementShardTimeRange { start_time: 0, end_time: 1_000_000_000_000_000 }),
            node_ids: vec![node_a_resolved_id],
            hash_key_range_start: 0,
            hash_key_range_end: u64::MAX / 2,
            migration_status: None,
        };
        let shard_s2 = ManagementShardDefinition {
            id: 2,
            time_range: Some(ManagementShardTimeRange { start_time: 0, end_time: 1_000_000_000_000_000 }),
            node_ids: vec![node_b_resolved_id],
            hash_key_range_start: u64::MAX / 2 + 1,
            hash_key_range_end: u64::MAX,
            migration_status: None,
        };

        handle_a.cluster_mgmt_client.as_mut().unwrap().create_shard(CreateShardRequest {
            db_name: db_name.to_string(), table_name: table_name.to_string(), shard_definition: Some(shard_s1)
        }).await.expect("Failed to create shard S1");
        handle_a.cluster_mgmt_client.as_mut().unwrap().create_shard(CreateShardRequest {
            db_name: db_name.to_string(), table_name: table_name.to_string(), shard_definition: Some(shard_s2)
        }).await.expect("Failed to create shard S2");

        // --- Write Data ---
        let hash_host_a = server_calculate_hash(&["host_a"]);
        let line_p1 = format!("{},host_tag=host_a val=1.0 500000000", table_name);

        let hash_host_z = server_calculate_hash(&["host_z"]);
        let line_p2 = format!("{},host_tag=host_z val=2.0 600000000", table_name);

        info!("Writing P1 ('host_a', hash: {}): {}", hash_host_a, line_p1);
        info!("Writing P2 ('host_z', hash: {}): {}", hash_host_z, line_p2);

        handle_a.flight_client.as_mut().unwrap().lp_write(db_name.to_string(), line_p1.clone(), Precision::Nanosecond).await.expect("Write P1 failed");
        handle_a.flight_client.as_mut().unwrap().lp_write(db_name.to_string(), line_p2.clone(), Precision::Nanosecond).await.expect("Write P2 failed");

        tokio::time::sleep(Duration::from_secs(1)).await;

        // --- Query Data ---
        info!("Querying all data from Node A");
        let mut results_all_on_a = handle_a.flight_client.as_mut().unwrap().sql(db_name.to_string(), format!("SELECT * FROM \"{}\" ORDER BY time", table_name)).await.expect("Query all on A failed");
        let mut total_rows = 0;
        let mut found_p1 = false;
        let mut found_p2 = false;

        while let Some(res) = results_all_on_a.next().await {
            let batch = res.expect("Error fetching batch");
            total_rows += batch.num_rows();
            let pretty_batches = arrow::util::pretty::pretty_format_batches(&[batch.clone()]).unwrap().to_string();
            info!("Received batch on Node A:\n{}", pretty_batches);
            if pretty_batches.contains("host_a") { found_p1 = true; }
            if pretty_batches.contains("host_z") { found_p2 = true; }
        }
        assert_eq!(total_rows, 2, "Both P1 and P2 should be returned from Node A query to all. Found {} rows.", total_rows);
        assert!(found_p1, "Point P1 (host_a) not found in query result from Node A");
        assert!(found_p2, "Point P2 (host_z) not found in query result from Node A");

        info!("Shutting down Node A");
        handle_a.shutdown_token.cancel();
        if let Err(e) = handle_a.task_handle.await {
            error!("Node A task panicked: {:?}", e);
        }
        info!("Shutting down Node B");
        handle_b.shutdown_token.cancel();
        if let Err(e) = handle_b.task_handle.await {
            error!("Node B task panicked: {:?}", e);
        }
    }
}
