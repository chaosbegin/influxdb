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
use trace::ctx::SpanContext; // For SpanContext

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

        if fragment_type != "RawSql" {
            return Err(Status::invalid_argument(format!(
                "Unsupported fragment_type: {}. Only 'RawSql' is currently supported.",
                fragment_type
            )));
        }

        let sql_query = match String::from_utf8(query_fragment_bytes) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to decode query_fragment bytes to UTF-8 SQL string: {}", e);
                return Err(Status::invalid_argument(format!(
                    "Failed to decode query_fragment as UTF-8 SQL string: {}",
                    e
                )));
            }
        };

        // Create a SpanContext for the query execution
        // Assuming the query_executor's methods are updated to accept an optional SpanContext
        // For now, if it's not part of the signature, we might create one but not pass it,
        // or it might be implicitly handled by the query_executor.
        // Let's assume a SpanContext can be created and might be passed if the executor supports it.
        let span_ctx = SpanContext::new_current_trace_id(); // Example: create a new context

        let record_batch_stream = match self.query_executor
            .query_sql(&db_name, &sql_query, None, Some(span_ctx), None) // Pass None for StatementParams and query_options for now
            .await
        {
            Ok(stream) => stream,
            Err(QueryExecutorError::DatabaseNotFound { db_name: e_db_name }) => {
                tracing::warn!("Database '{}' not found for query fragment", e_db_name);
                return Err(Status::not_found(format!("Database '{}' not found", e_db_name)));
            }
            Err(QueryExecutorError::DataFusionError(e)) => {
                 tracing::error!("DataFusion error executing query fragment for db '{}': {}", db_name, e);
                 return Err(Status::internal(format!("Query execution error: {}", e)));
            }
            Err(e) => {
                tracing::error!("Generic error executing query fragment for db '{}': {}", db_name, e);
                return Err(Status::internal(format!("Failed to execute query fragment: {}", e)));
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
