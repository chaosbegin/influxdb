// This module will house the gRPC server implementation for the DistributedQueryService.

use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::{
    distributed_query_service_server::{DistributedQueryService, DistributedQueryServiceServer},
    ExecuteQueryFragmentRequest,
    FlightData as GrpcFlightData,
};
pub use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::distributed_query_service_server::DistributedQueryServiceServer;

use std::pin::Pin;
use std::sync::Arc;
use futures::Stream;
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::FlightData;
use datafusion::{
    physical_plan::SendableRecordBatchStream,
    prelude::SessionContext,
    error::DataFusionError,
};
use influxdb3_internal_api::query_executor::QueryExecutor;
use datafusion_proto::physical_plan::from_physical_plan_bytes; // Added for deserialization
use datafusion::execution::context::SessionState; // For creating plan with state

// Define the server struct
#[derive(Debug)]
pub struct DistributedQueryServerImpl {
    query_executor: Arc<dyn QueryExecutor>,
    // Store a SessionContext or RuntimeEnv if QueryExecutor doesn't provide easy access.
    // For now, we'll create a new SessionContext per request for deserialization.
}

impl DistributedQueryServerImpl {
    pub fn new(query_executor: Arc<dyn QueryExecutor>) -> Self {
        Self { query_executor }
    }
}

type FlightDataStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

#[tonic::async_trait]
impl DistributedQueryService for DistributedQueryServerImpl {
    type ExecuteQueryFragmentStream = FlightDataStream;

    async fn execute_query_fragment(
        &self,
        request: Request<ExecuteQueryFragmentRequest>,
    ) -> Result<Response<Self::ExecuteQueryFragmentStream>, Status> {
        let req = request.into_inner();
        observability_deps::tracing::info!(
            database_name = %req.database_name,
            shard_id = ?req.shard_id, // This might be less relevant for SQL queries
            query_id = %req.query_id,
            fragment_type = %req.fragment_type, // Assuming field exists after regen
            payload_len = req.fragment_payload_bytes.len(),
            "Received ExecuteQueryFragment request"
        );

        // Define assumed enum values (as if from generated proto code)
        const SERIALIZED_PLAN: i32 = 0;
        const SQL_QUERY: i32 = 1;

        let record_batch_stream_result: Result<SendableRecordBatchStream, Status> = match req.fragment_type {
            typ if typ == SQL_QUERY => {
                let sql_query = String::from_utf8(req.fragment_payload_bytes.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Failed to decode SQL query from payload: {}", e)))?;

                observability_deps::tracing::info!(query_id = %req.query_id, sql = %sql_query, "Executing SQL query fragment");

                // TODO: Properly obtain span, external_span_ctx, statement_params
                // For now, using None or default. QueryExecutor trait might need adjustment
                // or a simplified execution path for these internal/distributed queries.
                self.query_executor.query_sql(
                    &req.database_name,
                    &sql_query,
                    None, // current_span
                    None, // external_span_ctx
                    Default::default(), // statement_params
                )
                .await
                .map_err(|e| {
                    observability_deps::tracing::error!("Error executing SQL query fragment (query_id: {}): {}", req.query_id, e);
                    Status::internal(format!("Failed to execute SQL query fragment: {}", e))
                })
            }
            typ if typ == SERIALIZED_PLAN => {
                observability_deps::tracing::info!(query_id = %req.query_id, "Executing serialized plan fragment");
                let session_ctx = SessionContext::new(); // Consider reusing or global context
                // TODO: Apply req.session_config to the session_ctx

                let physical_plan = from_physical_plan_bytes(&req.fragment_payload_bytes, &session_ctx, session_ctx.runtime_env().as_ref())
                    .map_err(|e| {
                        observability_deps::tracing::error!("Failed to deserialize plan fragment (query_id: {}): {}", req.query_id, e);
                        Status::invalid_argument(format!("Invalid plan fragment payload: {}", e))
                    })?;

                session_ctx.execute_physical_plan(physical_plan).await
                    .map_err(|e| {
                        observability_deps::tracing::error!("Error executing deserialized plan fragment (query_id: {}): {}", req.query_id, e);
                        Status::internal(format!("Failed to execute plan fragment: {}", e))
                    })
            }
            unknown_type => {
                observability_deps::tracing::error!("Unknown fragment type: {}", unknown_type);
                Err(Status::invalid_argument(format!("Unknown fragment type: {}", unknown_type)))
            }
        };

        let record_batch_stream = record_batch_stream_result?;

        // Convert SendableRecordBatchStream to a stream of FlightData.
        let schema = record_batch_stream.schema();
        let flight_data_stream = arrow_flight::utils::flight_data_from_arrow_stream(record_batch_stream, Default::default());

        let response_stream: FlightDataStream = Box::pin(flight_data_stream.map_err(|e| {
            observability_deps::tracing::error!("Error converting to FlightData stream (query_id: {}): {}", req.query_id, e);
            Status::internal(format!("Error streaming results: {}", e))
        }));

        Ok(Response::new(response_stream))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_internal_api::query_executor::MockQueryExecutor;
    use futures::{stream, TryStreamExt}; // Added TryStreamExt for map_err on stream
    use std::collections::HashMap;
    use arrow_schema::{Schema, Field, DataType,TimeUnit};
    use datafusion::physical_plan::common::RecordBatchStreamAdapter;
    use datafusion::arrow::record_batch::RecordBatch;
    use bytes::Bytes;


    // Helper to create ExecuteQueryFragmentRequest for testing
    fn create_test_request(
        db_name: String,
        fragment_type: i32,
        payload: Vec<u8>,
        query_id: String,
    ) -> ExecuteQueryFragmentRequest {
        ExecuteQueryFragmentRequest {
            database_name: db_name,
            fragment_payload_bytes: payload.into(),
            shard_id: None, // Not strictly needed for these tests
            session_config: HashMap::new(),
            query_id,
            expected_schema_bytes: vec![], // Not deeply checked by these specific handler tests
            fragment_type,
        }
    }

    fn create_empty_rb_stream() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()))
    }

    #[tokio::test]
    async fn test_execute_sql_query_fragment() {
        let mut mock_query_executor = MockQueryExecutor::new();
        let db_name = "test_db_sql".to_string();
        let sql_query = "SELECT * FROM my_table".to_string();
        let query_id = "test_sql_query_id".to_string();

        mock_query_executor.expect_query_sql()
            .withf(move |db, sql, _, _, _| db == db_name && sql == sql_query)
            .times(1)
            .returning(|_, _, _, _, _| Ok(create_empty_rb_stream()));

        let server = DistributedQueryServerImpl::new(Arc::new(mock_query_executor));

        let request = create_test_request(
            db_name.clone(),
            1, // SQL_QUERY type
            sql_query.into_bytes(),
            query_id.clone()
        );

        let result = server.execute_query_fragment(Request::new(request)).await;
        assert!(result.is_ok(), "Execute SQL query fragment failed: {:?}", result.err());

        // Check if the stream can be polled (even if empty)
        let mut response_stream = result.unwrap().into_inner();
        assert!(response_stream.next().await.is_none()); // Empty stream from mock
    }

    #[tokio::test]
    async fn test_execute_serialized_plan_fragment_invalid_bytes() {
        let mock_query_executor = MockQueryExecutor::new(); // Not actually called for this error path
        let server = DistributedQueryServerImpl::new(Arc::new(mock_query_executor));

        let request = create_test_request(
            "test_db_plan_invalid".to_string(),
            0, // SERIALIZED_PLAN type
            vec![1,2,3,4], // Invalid plan bytes
            "test_plan_invalid_id".to_string()
        );

        let result = server.execute_query_fragment(Request::new(request)).await;
        assert!(result.is_err());
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            assert!(status.message().contains("Invalid plan fragment payload"));
        } else {
            panic!("Expected InvalidArgument error for bad plan bytes");
        }
    }

    #[tokio::test]
    async fn test_unknown_fragment_type() {
        let mock_query_executor = MockQueryExecutor::new();
        let server = DistributedQueryServerImpl::new(Arc::new(mock_query_executor));

        let request = create_test_request(
            "test_db_unknown".to_string(),
            99, // Unknown type
            vec![],
            "test_unknown_type_id".to_string()
        );

        let result = server.execute_query_fragment(Request::new(request)).await;
        assert!(result.is_err());
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            assert!(status.message().contains("Unknown fragment type"));
        } else {
            panic!("Expected InvalidArgument error for unknown fragment type");
        }
    }

    // TODO: Add a test for successful serialized plan execution.
    // This would require creating valid serialized plan bytes and potentially mocking
    // how SessionContext executes it if we don't want to test full DataFusion execution.
    // For example, using `physical_plan_to_bytes` from a simple plan.
    // And then ensuring the FlightData stream conversion works.

    #[tokio::test]
    async fn test_flight_data_streaming() {
        let mut mock_query_executor = MockQueryExecutor::new();
        let db_name = "test_db_flight".to_string();
        let sql_query = "SELECT id FROM t".to_string();
        let query_id = "flight_stream_query".to_string();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))]
        ).unwrap();
        let live_stream: SendableRecordBatchStream = Box::pin(
            RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]))
        );

        mock_query_executor.expect_query_sql()
            .withf(move |db, sql, _, _, _| db == db_name && sql == sql_query)
            .times(1)
            .returning(move |_, _, _, _, _| Ok(live_stream.clone())); // Clone SendableRecordBatchStream if needed

        let server = DistributedQueryServerImpl::new(Arc::new(mock_query_executor));

        let request = create_test_request(
            db_name.clone(),
            1, // SQL_QUERY type
            sql_query.into_bytes(),
            query_id.clone()
        );

        let result = server.execute_query_fragment(Request::new(request)).await;
        assert!(result.is_ok(), "Flight data streaming test failed: {:?}", result.err());

        let mut flight_data_stream = result.unwrap().into_inner();

        // Expect schema message first (if flight_data_from_arrow_stream sends it)
        if let Some(flight_data_res) = flight_data_stream.try_next().await.transpose() {
            let flight_data = flight_data_res.expect("Expected FlightData message");
            assert!(flight_data.flight_descriptor.is_none()); // Schema usually has no descriptor
            // Further checks for schema bytes if needed
        } else {
           // panic!("Expected at least one FlightData message for schema");
           // flight_data_from_arrow_stream might not send schema as first message explicitly
           // if schema is part of FlightData's app_metadata with the first data batch.
           // Or it sends it as a separate FlightInfo. This test might need refinement based on exact util behavior.
        }

        // Expect data message (actual test would deserialize and check content)
        // For now, just check that some messages are received.
        // The exact number of messages depends on how FlightDataEncoder chunks batches.
        let mut data_message_received = false;
        while let Some(flight_data_res) = flight_data_stream.try_next().await.transpose() {
             data_message_received = true;
             let _flight_data = flight_data_res.expect("Expected FlightData message");
             // Here one could deserialize _flight_data to RecordBatch and verify content
        }
        assert!(data_message_received, "No data messages received in FlightData stream");
    }
}
