use influxdb3_proto::influxdb3::internal::distributed_query::v1::{
    distributed_query_service_server::{DistributedQueryService, DistributedQueryServiceServer},
    ExecuteQueryFragmentRequest, ExecuteQueryFragmentResponse,
};
use influxdb3_internal_api::query_executor::{QueryExecutor as QueryExecutorTrait, QueryExecutorError}; // Renamed to avoid clash
use crate::query_executor::QueryExecutorImpl; // Specific implementation

use tonic::{Request, Response, Status, Streaming};
use arrow::ipc::writer::{StreamWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan; // For deserialized plan
use datafusion::execution::context::SessionContext; // For creating physical plan
use trace::ctx::SpanContext;
use influxdb3_distributed_query::exec::deserialize_logical_plan; // Import the helper

// Type alias for the stream returned by the RPC method
pub type ExecuteQueryFragmentStream = Pin<Box<dyn Stream<Item = Result<ExecuteQueryFragmentResponse, Status>> + Send>>;

#[derive(Debug)]
pub struct DistributedQueryServerImpl {
    // QueryExecutorImpl holds Arc<Catalog>, Arc<WriteBuffer>, Arc<Executor>, etc.
    query_executor: Arc<QueryExecutorImpl>,
}

impl DistributedQueryServerImpl {
    pub fn new(query_executor: Arc<QueryExecutorImpl>) -> Self {
        Self { query_executor }
    }
}

#[tonic::async_trait]
impl DistributedQueryService for DistributedQueryServerImpl {
    type ExecuteQueryFragmentStream = ExecuteQueryFragmentStream;

    async fn execute_query_fragment(
        &self,
        request: Request<ExecuteQueryFragmentRequest>,
    ) -> Result<Response<Self::ExecuteQueryFragmentStream>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.clone(); // Clone for use in async block
        let query_fragment_bytes = req.query_fragment.clone(); // Clone for use in async block
        let fragment_type = req.fragment_type.clone(); // Clone for use in async block

        tracing::info!(
            database_name = %db_name,
            fragment_type = %fragment_type,
            fragment_len = %query_fragment_bytes.len(),
            "Received ExecuteQueryFragment request"
        );

        // Removed the premature check for "RawSql" only.
        // The if/else if block below handles different fragment_types.

        let record_batch_stream_result = if fragment_type == "RawSql" {
            let sql_query = String::from_utf8(query_fragment_bytes)
                .map_err(|e| Status::invalid_argument(format!("Failed to decode RawSql fragment: {}", e)))?;

            let span_ctx = SpanContext::new_current_trace_id();
            self.query_executor
                .query_sql(&db_name, &sql_query, None, Some(span_ctx), None)
                .await
        } else if fragment_type == "SerializedDataFusionLogicalPlan" {
            let logical_plan: LogicalPlan = deserialize_logical_plan(&query_fragment_bytes)
                .map_err(|e| Status::internal(format!("Failed to deserialize LogicalPlan: {}", e)))?;

            // Obtain a new SessionContext configured for the target database.
            // This assumes QueryExecutorImpl can provide such a context.
            // The context needs the correct catalog (for the given db_name) registered.
            let session_ctx = self.query_executor.new_df_session_context(&db_name); // Conceptual method

            let physical_plan = session_ctx.create_physical_plan(&logical_plan).await
                .map_err(|e| Status::internal(format!("Failed to create physical plan: {}", e)))?;

            session_ctx.execute_stream(physical_plan).await
                .map_err(|e| Status::internal(format!("Failed to execute physical plan: {}", e)))
        } else {
            return Err(Status::unimplemented(format!(
                "Fragment type '{}' not supported.",
                fragment_type
            )));
        };

        let record_batch_stream = match record_batch_stream_result {
            Ok(stream) => stream,
            Err(QueryExecutorError::DatabaseNotFound { db_name: e_db_name }) => { // This error comes from query_sql
                tracing::warn!("Database '{}' not found for query fragment execution", e_db_name);
                return Err(Status::not_found(format!("Database '{}' not found", e_db_name)));
            }
            Err(QueryExecutorError::DataFusionError(e)) => { // This error comes from query_sql or execute_stream
                 tracing::error!("DataFusion error during query fragment execution for db '{}': {}", db_name, e);
                 return Err(Status::internal(format!("DataFusion execution error: {}", e)));
            }
            Err(e) => { // Other QueryExecutorErrors
                tracing::error!("Generic error during query fragment execution for db '{}': {}", db_name, e);
                return Err(Status::internal(format!("Fragment execution failed: {}", e)));
            }
        };

        let response_stream = record_batch_stream.map_err(|df_err: DataFusionError| {
            Status::internal(format!("Error processing RecordBatch stream: {}", df_err))
        }).and_then(|batch: RecordBatch| async move {
            // Serialize RecordBatch to Arrow IPC format
            let mut stream_writer = StreamWriter::try_new(Vec::new(), &batch.schema(), None)
                .map_err(|e| Status::internal(format!("Failed to create IPC StreamWriter: {}", e)))?;

            stream_writer.write(&batch)
                .map_err(|e| Status::internal(format!("Failed to write RecordBatch to IPC: {}", e)))?;

            stream_writer.finish()
                .map_err(|e| Status::internal(format!("Failed to finish IPC stream: {}", e)))?;

            let buffer = stream_writer.into_inner()
                .map_err(|e| Status::internal(format!("Failed to get buffer from IPC StreamWriter: {}", e)))?;

            Ok(ExecuteQueryFragmentResponse {
                record_batch_bytes: Bytes::from(buffer),
            })
        });

        Ok(Response::new(Box::pin(response_stream) as Self::ExecuteQueryFragmentStream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_executor::{QueryExecutorImpl, CreateQueryExecutorArgs}; // For actual QueryExecutorImpl
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::execution::context::SessionState;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionConfig;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_distributed_query::exec::serialize_logical_plan;
    use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
    use influxdb3_write::persister::Persister;
    use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use tokio_stream::wrappers::ReceiverStream;
    use datafusion::logical_expr::{LogicalPlanBuilder, table_scan, col};
    use datafusion::physical_plan::{PhysicalPlan, DisplayAs, DisplayFormatType, ExecutionPlanProperties, PlanProperties, Partitioning};
    use datafusion::common::Statistics as DFStatistics;
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_telemetry::store::TelemetryStore;
    use influxdb3_wal::WalConfig;
    use parquet_file::storage::{ParquetStorage, StorageId};


    // Helper to create a basic QueryExecutorImpl for tests
    async fn create_test_query_executor(catalog: Arc<Catalog>) -> Arc<QueryExecutorImpl> {
        let object_store = Arc::new(InMemory::new());
        let metrics = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let parquet_store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("test_qe_storage"));
        let exec = Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: std::num::NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store].into_iter().map(|store| (store.id(), Arc::clone(store.object_store()))).collect(),
                metric_registry: Arc::clone(&metrics),
                mem_pool_size: 1024 * 1024, // 1MB
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ));

        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host_qe", Arc::clone(&time_provider)));
        let shutdown_manager = ShutdownManager::new_testing();

        let write_buffer = Arc::new(WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).await.unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog)).await.unwrap(),
            time_provider: Arc::clone(&time_provider),
            executor: Arc::clone(&exec),
            wal_config: WalConfig::test_config(),
            parquet_cache: None,
            metric_registry: Arc::clone(&metrics),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown_manager.register(),
            wal_replay_concurrency_limit: Some(1),
            current_node_id: Arc::from("test_node_id_qe"),
            max_snapshots_to_load_on_start: Some(10),
        }).await.unwrap());

        let telemetry_store = TelemetryStore::new_without_background_runners(None, Arc::clone(&catalog));
        let sys_events_store = Arc::new(influxdb3_sys_events::SysEventStore::new(Arc::clone(&time_provider)));

        Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
            catalog,
            write_buffer,
            exec,
            metrics,
            datafusion_config: Arc::new(Default::default()),
            query_log_size: 100,
            telemetry_store,
            sys_events_store,
            started_with_auth: false,
            time_provider,
            current_node_id: Arc::from("test_node_id_qe_impl"),
        }))
    }

    #[tokio::test]
    async fn test_execute_serialized_logical_plan() {
        // 1. Setup
        let catalog = Arc::new(Catalog::new_in_memory("test_dist_query_cat_svc").await.unwrap());
        catalog.create_database("test_db").await.unwrap();

        // Create a dummy table in the catalog so the SessionContext can resolve it.
        // The actual table provider in the SessionContext for testing will be an EmptyTable,
        // so no actual data read will occur from this catalog table.
        catalog.create_table("test_db", "test_table_for_plan", &["tagA"], &[(String::from("id"), crate::FieldDataType::Integer)]).await.unwrap();

        let query_executor = create_test_query_executor(Arc::clone(&catalog)).await;
        let server_impl = DistributedQueryServerImpl::new(query_executor);

        // 2. Create a simple LogicalPlan and serialize it
        let test_arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new("id", ArrowDataType::Int64, false)]));
        let logical_plan = LogicalPlanBuilder::scan("test_table_for_plan", Arc::clone(&test_arrow_schema), None).unwrap()
            .project(vec![col("id")]).unwrap()
            .build().unwrap();
        let serialized_plan = serialize_logical_plan(&logical_plan).unwrap();

        // 3. Construct ExecuteQueryFragmentRequest
        let request_payload = ExecuteQueryFragmentRequest {
            db_name: "test_db".to_string(),
            query_fragment: Bytes::from(serialized_plan),
            fragment_type: "SerializedDataFusionLogicalPlan".to_string(),
        };
        let tonic_request = Request::new(request_payload);

        // 4. Call the server method
        let response_result = server_impl.execute_query_fragment(tonic_request).await;
        assert!(response_result.is_ok(), "execute_query_fragment failed: {:?}", response_result.err());
        let mut response_stream = response_result.unwrap().into_inner();

        // 5. Process the stream and verify (expecting empty stream from EmptyTable)
        let mut received_batches = Vec::new();
        while let Some(res) = response_stream.next().await {
            assert!(res.is_ok(), "Stream item is an error: {:?}", res.err());
            let query_response = res.unwrap();
            // assert!(query_response.error_message.is_none(), "Received error message in stream: {:?}", query_response.error_message);

            let cursor = Cursor::new(query_response.record_batch_bytes);
            let mut reader = StreamReader::try_new(cursor, None).expect("Failed to create IPC StreamReader from response bytes");
            if let Some(batch_res) = reader.next() {
                received_batches.push(batch_res.expect("Failed to read batch from IPC response"));
            }
            // EmptyTable might produce one empty batch or just end the stream.
            // If it produces an empty batch, reader.next() will be Some(Ok(empty_batch)).
            // If it ends the stream, reader.next() will be None.
        }

        // EmptyTable provider for "test_table_for_plan" will result in an empty stream of batches.
        assert!(received_batches.is_empty() || received_batches.iter().all(|rb| rb.num_rows() == 0), "Expected no data or only empty batches from EmptyTable, got: {:?}", received_batches);
    }
}
