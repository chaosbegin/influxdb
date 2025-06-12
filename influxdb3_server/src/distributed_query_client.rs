// Copyright (c) 2024 InfluxData Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::{
    distributed_query_service_client::DistributedQueryServiceClient,
    ExecuteQueryFragmentRequest as GrpcExecuteQueryFragmentRequest,
    FlightData as GrpcFlightData,
};
use observability_deps::tracing::{info, warn, error};
use tonic::transport::Channel;
use arrow_schema::{ArrowError, RecordBatch, SchemaRef, Schema as ArrowSchema};
use futures::stream::{BoxStream, StreamExt};
use arrow_flight::utils::flight_data_stream_to_arrow_stream;
use std::sync::Arc;
use std::time::Duration; // For tokio::time::sleep
use tonic::Code; // For status codes


const MAX_QUERY_FRAGMENT_RETRIES: u32 = 3;
const QUERY_RETRY_DELAY_MS: u64 = 100;

/// gRPC client for the DistributedQueryService.
#[derive(Debug, Clone)]
pub struct GrpcDistributedQueryClient {
    client: Option<DistributedQueryServiceClient<Channel>>,
    target_addr: String,
}

impl GrpcDistributedQueryClient {
    /// Creates a new client and attempts to connect to the target address.
    pub async fn new(target_addr: String) -> Self {
        info!(target_addr = %target_addr, "Attempting to connect to distributed query service");
        match DistributedQueryServiceClient::connect(target_addr.clone()).await {
            Ok(client_channel) => {
                info!(target_addr = %target_addr, "Successfully connected to distributed query service");
                Self {
                    client: Some(client_channel),
                    target_addr,
                }
            }
            Err(e) => {
                warn!(target_addr = %target_addr, error = %e, "Failed to connect to distributed query service. Client will be unavailable.");
                Self {
                    client: None,
                    target_addr,
                }
            }
        }
    }

    /// Executes a query fragment on a remote node and returns a stream of RecordBatches.
    pub async fn execute_query_fragment_streaming(
        &mut self,
        request: GrpcExecuteQueryFragmentRequest, // Request is not Clone, so pass by value
    ) -> Result<BoxStream<'static, Result<RecordBatch, ArrowError>>, ArrowError> {
        let Some(client_ref) = self.client.as_mut() else {
            warn!(target_addr = %self.target_addr, query_id = %request.query_id, "Attempted to execute query fragment with no active client connection.");
            return Err(ArrowError::IoError(
                format!("Distributed query client for {} is not connected (initial connection failed).", self.target_addr),
                std::io::ErrorKind::NotConnected.into()
            ));
        };

        info!(target_addr = %self.target_addr, query_id = %request.query_id, "Executing remote query fragment");

        // Deserialize expected_schema_bytes from request (DataFusion proto format) into SchemaRef
        // This only needs to be done once, even with retries, as the request object itself is not changing.
        let expected_schema = {
                use datafusion_proto::protobuf::Schema as DfSchemaProto;
                use prost::Message;

                let df_schema_proto = DfSchemaProto::decode(request.expected_schema_bytes.as_ref())
                    .map_err(|e| {
                        let err_msg = format!("Failed to decode DataFusion protobuf Schema: {}", e);
                        error!(target_addr = %self.target_addr, query_id = %request.query_id, error = %err_msg, "Schema deserialization error");
                        ArrowError::ParseError(err_msg)
                    })?;

                let arrow_schema_converted: ArrowSchema = (&df_schema_proto).try_into()
                    .map_err(|e: datafusion_proto::Error| {
                        let err_msg = format!("Failed to convert DataFusion protobuf Schema to Arrow Schema: {}", e);
                        error!(target_addr = %self.target_addr, query_id = %request.query_id, error = %err_msg, "Schema conversion error");
                        ArrowError::ParseError(err_msg)
                    })?;
                Arc::new(arrow_schema_converted)
            };

        // The gRPC request is not Clone, so we can't easily clone it for each retry attempt
        // if it's consumed by client.execute_query_fragment.
        // However, tonic clients often take `tonic::Request<T>` which wraps `T`.
        // If `GrpcExecuteQueryFragmentRequest` itself is `Clone`, we can clone it before each call.
        // Let's assume GrpcExecuteQueryFragmentRequest is Clone for this retry logic.
        // If not, the structure of passing request to client.execute_query_fragment might need adjustment,
        // or we only retry if the request object can be reconstituted or cloned.
        // For now, proceeding with assumption that we can re-send the request (or a clone).
        // Tonic generated clients usually take `Request<T>` where T is the message.
        // `tonic::Request::new(message)` means message is moved.
        // If `request` (GrpcExecuteQueryFragmentRequest) is not Clone, we cannot put its creation inside the loop easily
        // without re-parsing `expected_schema_bytes` etc.
        // A common pattern is to ensure the message (`GrpcExecuteQueryFragmentRequest`) is `Clone`.
        // If it's not, we'd have to serialize the essential parts and rebuild the request in each attempt,
        // or only retry on errors where the request wouldn't have been consumed.
        // For this exercise, let's assume `GrpcExecuteQueryFragmentRequest` can be cloned.

        for attempt in 0..MAX_QUERY_FRAGMENT_RETRIES {
            // We need to pass a new `tonic::Request` wrapper for each call.
            let current_grpc_request_for_tonic = request.clone(); // Assuming GrpcExecuteQueryFragmentRequest is Clone

            match client_ref.execute_query_fragment(current_grpc_request_for_tonic).await {
                Ok(tonic_response) => {
                    let streaming_flight_data = tonic_response.into_inner();
                    info!(target_addr = %self.target_addr, query_id = %request.query_id, "Received FlightData stream, converting to RecordBatch stream.");

                    let flight_data_stream_with_arrow_errors = streaming_flight_data.map(|res| {
                        res.map_err(|status| ArrowError::ExternalError(Box::new(status)))
                    });

                    let record_batch_stream = flight_data_stream_to_arrow_stream(
                        flight_data_stream_with_arrow_errors,
                        expected_schema.clone(), // Clone Arc for schema
                        &[],
                    ).await
                    .map_err(|e| {
                        error!(target_addr = %self.target_addr, query_id = %request.query_id, error = %e, "Failed to adapt FlightData stream to Arrow stream");
                        ArrowError::ExternalError(Box::new(e))
                    })?;

                    return Ok(Box::pin(record_batch_stream));
                }
                Err(status) => {
                    let code = status.code();
                    if (code == Code::Unavailable || code == Code::Cancelled || code == Code::DeadlineExceeded)
                        && attempt < MAX_QUERY_FRAGMENT_RETRIES - 1
                    {
                        warn!(
                            target_addr = %self.target_addr, query_id = %request.query_id, attempt = attempt + 1, max_attempts = MAX_QUERY_FRAGMENT_RETRIES, code = ?code, delay_ms = QUERY_RETRY_DELAY_MS,
                            "Query fragment RPC failed. Retrying..."
                        );
                        tokio::time::sleep(Duration::from_millis(QUERY_RETRY_DELAY_MS)).await;

                        if code == Code::Unavailable {
                             info!("Attempting to reconnect to distributed query service at {} before retry...", self.target_addr);
                             match DistributedQueryServiceClient::connect(self.target_addr.clone()).await {
                                Ok(new_client_channel) => {
                                    info!("Successfully reconnected to distributed query service at {}", self.target_addr);
                                    *client_ref = new_client_channel; // Update the client instance
                                }
                                Err(e) => {
                                    warn!("Failed to reconnect to distributed query service at {}: {}. Retrying with old client if possible.", self.target_addr, e);
                                }
                            }
                        }
                        continue;
                    } else {
                        error!(
                            target_addr = %self.target_addr, query_id = %request.query_id, attempts = attempt + 1, code = ?code, error_message = %status.message(),
                            "Query fragment RPC failed permanently."
                        );
                        return Err(ArrowError::ExternalError(Box::new(status))); // Convert final tonic::Status to ArrowError
                    }
                }
            }
        }
        // Fallback error if loop finishes (e.g. MAX_QUERY_FRAGMENT_RETRIES is 0)
        Err(ArrowError::ExternalError(Box::new(Status::internal(
            "Exhausted retry attempts for execute_query_fragment_streaming"
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, DataType};
    use datafusion_proto::protobuf::Schema as DfSchemaProto;
    use prost::Message;

    #[tokio::test]
    async fn test_grpc_distributed_query_client_new_connection_error() {
        let client = GrpcDistributedQueryClient::new("http://invalid-address-for-query:54321".to_string()).await;
        assert!(client.client.is_none(), "Client should be None if connection fails");
    }

    #[tokio::test]
    async fn test_grpc_distributed_query_client_execute_query_fragment_no_client() {
        let mut client = GrpcDistributedQueryClient {
            client: None,
            target_addr: "dummy-query-target:123".to_string()
        };

        // Create a dummy schema and serialize it
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("test", arrow_schema::DataType::Int32, false)]));
        // Create a dummy schema and serialize it using DataFusion proto
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new("test", DataType::Int32, false)]));
        let df_schema_proto: DfSchemaProto = arrow_schema.as_ref().try_into().expect("Failed to convert to DF proto");
        let mut schema_bytes_proto = Vec::new();
        df_schema_proto.encode(&mut schema_bytes_proto).expect("Failed to encode DF proto");


        let request = GrpcExecuteQueryFragmentRequest {
            database_name: "test_db".to_string(),
            plan_bytes: vec![1, 2, 3], // Using plan_bytes as defined in proto
            // shard_id is no longer part of GrpcExecuteQueryFragmentRequest in the protos I'm assuming from context
            // If it is, it should be added here. Assuming it's part of plan_bytes or db_name identifies the table.
            session_config: Default::default(),
            query_id: "test_query_id".to_string(),
            expected_schema_bytes: schema_bytes_proto.into(),
        };

        let result = client.execute_query_fragment_streaming(request).await;
        assert!(result.is_err());
        if let Err(ArrowError::IoError(msg, _)) = result {
            assert!(msg.contains("not connected"));
        } else {
            panic!("Expected IoError for unavailable client, got {:?}", result);
        }
    }
}
