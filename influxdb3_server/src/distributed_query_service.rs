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
            shard_id = ?req.shard_id,
            query_id = %req.query_id,
            plan_bytes_len = req.plan_bytes.len(),
            "Received ExecuteQueryFragment request"
        );

        // 1. Create a new SessionContext for deserializing and executing the plan.
        // In a real system, this context might need to be configured based on req.session_config
        // or inherit from a pre-configured context associated with self.query_executor.
        let session_ctx = SessionContext::new();

        // TODO: Apply req.session_config to the session_ctx

        // 2. Deserialize plan_bytes into a DataFusion ExecutionPlan.
        let physical_plan = match from_physical_plan_bytes(&req.plan_bytes, &session_ctx, session_ctx.runtime_env().as_ref()) {
            Ok(plan) => plan,
            Err(e) => {
                observability_deps::tracing::error!("Failed to deserialize plan_bytes: {}", e);
                return Err(Status::invalid_argument(format!("Invalid plan_bytes: {}", e)));
            }
        };

        observability_deps::tracing::info!(query_id = %req.query_id, "Successfully deserialized execution plan.");

        // 3. Execute the deserialized physical plan.
        // The QueryExecutor might need a method to execute a pre-built physical plan,
        // or we use the session context directly if the plan is self-contained for the target node.
        // For now, using session_ctx.execute_physical_plan.
        // This assumes the plan is now targeting data accessible by this node.
        // The database_name from the request might be used to set the default catalog/schema in session_ctx if not already handled.

        // Ensure the session context has the correct database selected, if necessary for the plan.
        // This might be implicitly handled if the plan fully qualifies table names or if the QueryExecutor
        // sets this up. For this example, we assume the plan is executable within the default state of session_ctx
        // or that `from_physical_plan_bytes` handles context correctly.
        // A more robust solution might involve `session_ctx.use_database(&req.database_name);` if the plan
        // relies on the session's default database.

        let record_batch_stream: SendableRecordBatchStream = match session_ctx.execute_physical_plan(physical_plan).await {
            Ok(stream) => stream,
            Err(e) => {
                observability_deps::tracing::error!("Error executing deserialized query fragment (query_id: {}): {}", req.query_id, e);
                return Err(Status::internal(format!("Failed to execute query fragment: {}", e)));
            }
        };

        // 4. Convert SendableRecordBatchStream to a stream of FlightData.
        // This part remains conceptual / placeholder as full FlightData conversion is complex.
        // A real implementation would use arrow_flight::utils::flight_data_from_arrow_stream or similar.
        observability_deps::tracing::info!(query_id = %req.query_id, "Query fragment executed, attempting to stream FlightData.");

        let output_stream = futures::stream::once(async {
            Err(Status::unimplemented(format!("FlightData streaming for query_id {} not fully implemented", req.query_id)))
        });
        // Example of how it might look (very simplified):
        // let schema = record_batch_stream.schema();
        // let flight_data_producer = Arc::new(arrow_flight::flight_service_server::FlightDataEncoderBuilder::new().build(schema));
        // let mut flight_data_stream = flight_data_producer.encode(record_batch_stream);
        // let response_stream: FlightDataStream = Box::pin(flight_data_stream.map_err(|e| Status::internal(e.to_string())));

        let response_stream: FlightDataStream = Box::pin(output_stream);
        Ok(Response::new(response_stream))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_internal_api::query_executor::MockQueryExecutor;
    use futures::stream;
    use std::collections::HashMap;
    use arrow_schema::{Schema, Field, DataType};


    #[tokio::test]
    async fn test_execute_query_fragment_server_basic_flow() {
        let mut mock_query_executor = MockQueryExecutor::new();

        // Mock the query_sql behavior - this test will change as we are no longer sending SQL
        // but a serialized plan. For now, this test is less relevant to the new approach.
        // A new test for plan deserialization would be needed.
        mock_query_executor.expect_query_sql()
            .returning(|_db_name, _sql_query, _span, _external_span_ctx, _statement_params| {
                // This part of the mock is no longer directly hit if plan_bytes is a physical plan.
                // However, the SessionContext will execute the physical plan.
                let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
                let empty_stream: SendableRecordBatchStream = Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        schema,
                        stream::empty(),
                    )
                );
                Ok(empty_stream)
            });

        let server = DistributedQueryServerImpl::new(Arc::new(mock_query_executor));

        // This request is now for a serialized plan, not SQL.
        // Creating a valid serialized plan for a unit test is complex.
        // We'll test the error path for invalid plan_bytes instead for now.
        let request_invalid_plan = GrpcExecuteQueryFragmentRequest {
            database_name: "test_db".to_string(),
            plan_bytes: vec![1,2,3,4], // Invalid plan bytes
            shard_id: Some(1),
            session_config: HashMap::new(),
            query_id: "test_query_id_server_invalid_plan".to_string(),
            expected_schema_bytes: vec![],
        };

        let result_invalid = server.execute_query_fragment(Request::new(request_invalid_plan)).await;
        assert!(result_invalid.is_err());
        if let Err(status) = result_invalid {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            assert!(status.message().contains("Invalid plan_bytes"));
        } else {
            panic!("Expected InvalidArgument error for bad plan bytes");
        }
    }
}
