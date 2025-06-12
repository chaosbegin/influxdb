use super::*;
use std::sync::Arc;
use arrow_schema::{Schema, Field, DataType as ArrowDataType};
use arrow::array::{Int64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use datafusion::logical_expr::{LogicalPlanBuilder, table_scan};
use datafusion::prelude::Expr;
use influxdb3_internal_api::query_executor::{QueryExecutor, QueryExecutorError};
use iox_query::exec::IOxSessionContext;
use iox_query::QueryDatabase; // For new_context_for_db mock
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use tokio_stream::wrappers::ReceiverStream;
use std::collections::HashMap;


// --- Mock QueryExecutor ---
#[derive(Debug)]
struct MockQueryExecutor {
    should_execution_error: bool,
    db_name_to_mock: String,
    table_name_to_mock: String,
    schema_to_mock: SchemaRef,
    // Store the extensions passed to new_context_for_db for verification
    captured_extensions: std::sync::Mutex<Option<Extensions>>,
}

impl MockQueryExecutor {
    fn new(db_name: &str, table_name: &str, schema: SchemaRef, should_execution_error: bool) -> Self {
        Self {
            should_execution_error,
            db_name_to_mock: db_name.to_string(),
            table_name_to_mock: table_name.to_string(),
            schema_to_mock: schema,
            captured_extensions: std::sync::Mutex::new(None),
        }
    }

    fn take_captured_extensions(&self) -> Option<Extensions> {
        self.captured_extensions.lock().unwrap().take()
    }
}

#[async_trait::async_trait]
impl QueryExecutor for MockQueryExecutor {
    async fn query_sql(
        &self,
        _database: &str,
        _query: &str,
        _params: Option<iox_query_params::StatementParams>,
        _span_ctx: Option<trace::ctx::SpanContext>,
        _external_span_ctx: Option<trace_http::ctx::RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        unimplemented!("MockQueryExecutor::query_sql")
    }

    async fn query_influxql(
        &self,
        _database: &str,
        _query: &str,
        _statement: influxdb_influxql_parser::statement::Statement,
        _params: Option<iox_query_params::StatementParams>,
        _span_ctx: Option<trace::ctx::SpanContext>,
        _external_span_ctx: Option<trace_http::ctx::RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        unimplemented!("MockQueryExecutor::query_influxql")
    }

    fn show_databases(&self, _include_deleted: bool) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        unimplemented!()
    }

    async fn show_retention_policies(
        &self, _database: Option<&str>, _span_ctx: Option<trace::ctx::SpanContext>
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        unimplemented!()
    }

    // This is the key method our service will call indirectly
    fn new_context_for_db(
        &self,
        db_name: &str,
        _span_ctx: Option<trace::ctx::SpanContext>,
        query_config: datafusion::execution::context::QueryConfig,
    ) -> Result<IOxSessionContext, QueryExecutorError> {
        if db_name != self.db_name_to_mock {
            return Err(QueryExecutorError::DatabaseNotFound { db_name: db_name.to_string() });
        }
        // Capture extensions from the QueryConfig
        *self.captured_extensions.lock().unwrap() = Some(query_config.extensions().clone());

        let session_ctx = IOxSessionContext::with_testing();
        session_ctx.inner().with_default_catalog_and_schema(db_name, "public").unwrap(); // Ensure db is registered
        session_ctx.inner().set_config_options(query_config.options().clone()); // Apply other options

        // Mock the table provider for this session
        let table_provider = Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::clone(&self.schema_to_mock)));
        session_ctx.inner().register_table(&self.table_name_to_mock, table_provider).unwrap();

        Ok(session_ctx)
    }


    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)> {
        // This upcast is tricky for mocks if the QueryDatabase part is complex.
        // For this test, new_context_for_db is on QueryExecutor.
        unimplemented!("MockQueryExecutor::upcast for QueryDatabase")
    }
}


#[tokio::test]
async fn test_execute_query_fragment_success() {
    let db_name = "test_db";
    let table_name = "test_table";
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("value", ArrowDataType::Int64, true)]));

    // Create a mock QueryExecutor that will return a specific RecordBatch stream
    let mock_qe = Arc::new(MockQueryExecutor::new(db_name, table_name, Arc::clone(&schema), false));

    // Override create_physical_plan and execute_stream for the test session context
    // This is hard without deep integration or modifying IOxSessionContext for testability.
    // Instead, we'll have the service call execute_stream on a plan that yields our desired batch.
    // The challenge is that the service itself calls create_physical_plan.
    // For this test, we'll assume the logical plan is simple enough that DataFusion's default
    // planning of an EmptyTable (if that's what the mock context provides) will work,
    // and we'll mock what `execute_stream` would return.
    //
    // A better approach would be a MockSessionContext that returns a MockExecutionPlan,
    // which in turn returns a MockStream. This is too much for this step.

    // Let's simplify: the service will execute a plan. We'll test if it correctly
    // serializes the output of that plan. We can't easily mock the plan execution itself
    // without a more invasive mock of IOxSessionContext.
    //
    // What we *can* test easily:
    // - Request deserialization (plan_json)
    // - Response serialization (IPC bytes)
    // - Error propagation for bad plan_json
    // - Error propagation if execute_stream fails (by making the mock QE setup a context that does this)

    let server = DistributedQueryServerImpl::new(mock_qe);

    // 1. Create a simple LogicalPlan (TableScan) and serialize it to JSON
    let logical_plan = LogicalPlanBuilder::scan(table_name, Arc::clone(&schema), None).unwrap().build().unwrap();
    let plan_json = logical_plan.to_json().unwrap();

    let request_msg = ExecuteQueryFragmentRequest {
        db_name: db_name.to_string(),
        shard_id: 123,
        logical_plan_fragment_json: plan_json,
        session_config: HashMap::new(),
    };
    let tonic_request = Request::new(request_msg);

    // To test success path with data, we'd need the mocked QE's session_ctx to return a plan
    // that, when executed, yields known batches. Default EmptyTable scan yields nothing.
    // This test will thus test the "empty successful result" path.
    let response_result = server.execute_query_fragment(tonic_request).await;
    assert!(response_result.is_ok(), "execute_query_fragment failed: {:?}", response_result.err());

    let mut stream = response_result.unwrap().into_inner();
    let mut received_batches = vec![];
    let mut final_error_msg = None;

    while let Some(res) = stream.next().await {
        match res {
            Ok(resp) => {
                if let Some(err_msg) = resp.error_message {
                    final_error_msg = Some(err_msg);
                    break;
                }
                if !resp.record_batch_ipc_bytes.is_empty() {
                    let mut reader = StreamReader::try_new(std::io::Cursor::new(resp.record_batch_ipc_bytes), None).unwrap();
                    if let Some(batch) = reader.next() {
                        received_batches.push(batch.unwrap());
                    }
                }
            }
            Err(status) => {
                final_error_msg = Some(format!("gRPC status error: {}", status.message()));
                break;
            }
        }
    }
    assert!(final_error_msg.is_none(), "Stream ended with error: {:?}", final_error_msg);
    // EmptyTable scan results in zero batches.
    assert!(received_batches.is_empty(), "Expected no batches for EmptyTable scan, got {}", received_batches.len());
}


#[tokio::test]
async fn test_execute_query_fragment_plan_deserialization_error() {
    let mock_qe = Arc::new(MockQueryExecutor::new("db", "tbl", Arc::new(ArrowSchema::empty()), false));
    let server = DistributedQueryServerImpl::new(mock_qe);

    let request_msg = ExecuteQueryFragmentRequest {
        db_name: "test_db".to_string(),
        shard_id: 1,
        logical_plan_fragment_json: "this is not valid json".to_string(),
        session_config: HashMap::new(),
    };
    let tonic_request = Request::new(request_msg);

    let response_status = server.execute_query_fragment(tonic_request).await.err().unwrap();
    assert_eq!(response_status.code(), tonic::Code::InvalidArgument);
    assert!(response_status.message().contains("Failed to deserialize LogicalPlan fragment"));
}

// TODO: Add test for when session_ctx.execute_stream() itself returns an error.
// This would require MockQueryExecutor's new_context_for_db to return a mock IOxSessionContext
// where execute_stream can be made to fail.

    #[tokio::test]
    async fn test_execute_query_fragment_with_shard_filter() {
        let db_name = "test_db_for_shard_filter";
        let table_name = "test_table_for_shard_filter";
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("value", ArrowDataType::Int64, true)]));

        let mock_qe = Arc::new(MockQueryExecutor::new(db_name, table_name, Arc::clone(&schema), false));
        let server = DistributedQueryServerImpl::new(Arc::clone(&mock_qe) as Arc<dyn QueryExecutor>);

        // --- Test with a specific shard_id ---
        let target_shard_id_val = 123u64;
        let logical_plan_s1 = LogicalPlanBuilder::scan(table_name, Arc::clone(&schema), None).unwrap().build().unwrap();
        let plan_json_s1 = logical_plan_s1.to_json().unwrap();
        let request_msg_s1 = ExecuteQueryFragmentRequest {
            db_name: db_name.to_string(),
            shard_id: Some(target_shard_id_val),
            logical_plan_fragment_json: plan_json_s1,
            session_config: HashMap::new(),
        };
        let tonic_request_s1 = Request::new(request_msg_s1);

        let _response_result_s1 = server.execute_query_fragment(tonic_request_s1).await;
        // We don't care about the actual data stream for this test, only the captured extensions.

        let captured_extensions_s1 = mock_qe.take_captured_extensions().expect("Extensions should have been captured");
        let shard_filter_ext_s1 = captured_extensions_s1.get::<ShardIdFilterExtension>().expect("ShardIdFilterExtension should be present");
        assert_eq!(shard_filter_ext_s1.0, Some(ShardId::new(target_shard_id_val)), "ShardIdFilter should be Some({})", target_shard_id_val);

        // --- Test with no shard_id (None in request) ---
        let logical_plan_s_none = LogicalPlanBuilder::scan(table_name, Arc::clone(&schema), None).unwrap().build().unwrap();
        let plan_json_s_none = logical_plan_s_none.to_json().unwrap();
        let request_msg_s_none = ExecuteQueryFragmentRequest {
            db_name: db_name.to_string(),
            shard_id: None, // Explicitly no shard_id
            logical_plan_fragment_json: plan_json_s_none,
            session_config: HashMap::new(),
        };
        let tonic_request_s_none = Request::new(request_msg_s_none);

        let _response_result_s_none = server.execute_query_fragment(tonic_request_s_none).await;

        let captured_extensions_s_none = mock_qe.take_captured_extensions().expect("Extensions should have been captured for None case");
        let shard_filter_ext_s_none = captured_extensions_s_none.get::<ShardIdFilterExtension>().expect("ShardIdFilterExtension should be present for None case");
        assert_eq!(shard_filter_ext_s_none.0, None, "ShardIdFilter should be None");
    }
}
