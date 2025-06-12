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

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_schema::{ArrowError, RecordBatch, SchemaRef, Schema as ArrowSchema};
use bytes::Bytes;
use datafusion::physical_plan::RecordBatchStream;
use futures::{
    future::BoxFuture,
    stream::{BoxStream, Stream, StreamExt, try_stream}, // Added try_stream for error handling
};
use parking_lot::Mutex;
use observability_deps::tracing::{info, warn, error, debug};


use crate::distributed_query_client::GrpcDistributedQueryClient;
use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::{
    ExecuteQueryFragmentRequest as GrpcExecuteQueryFragmentRequest,
};

// Helper to serialize SchemaRef to DataFusion protobuf bytes.
fn serialize_schema_to_df_proto_bytes(schema: &SchemaRef) -> Result<Bytes, ArrowError> {
    use datafusion_proto::protobuf::Schema as DfSchemaProto;
    use prost::Message;

    let df_schema_proto: DfSchemaProto = schema.as_ref().try_into().map_err(|e| {
        ArrowError::ExternalError(Box::new(e)) // Convert ProstError or similar if try_into returns that
    })?;
    let mut buf = Vec::new();
    df_schema_proto.encode(&mut buf).map_err(|e| {
        ArrowError::ExternalError(Box::new(e)) // Convert ProstError
    })?;
    Ok(Bytes::from(buf))
}


/// State for the RemoteRecordBatchStream state machine.
#[derive(Debug)]
enum RemoteStreamState {
    /// Initial state: Client connection not yet initiated.
    Initial,
    /// Client is connecting.
    Connecting(BoxFuture<'static, Result<GrpcDistributedQueryClient, tonic::Status>>),
    /// Requesting the actual RecordBatch stream after connection.
    RequestingStream(BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch, ArrowError>>, ArrowError>>),
    /// Client has connected, and stream is being fetched.
    Fetching(BoxStream<'static, Result<RecordBatch, ArrowError>>),
    /// Stream has finished.
    Finished,
    /// An error occurred. Using Arc for shareable error.
    Error(Arc<ArrowError>),
}

/// A stream of RecordBatches received from a remote DataFusion node.
#[derive(Debug)]
pub struct RemoteRecordBatchStream {
    target_node_address: String,
    db_name: String,
    plan_bytes: Bytes,
    expected_schema: SchemaRef,
    session_config: HashMap<String, String>,
    query_id: String,
    state: Mutex<RemoteStreamState>,
}

impl RemoteRecordBatchStream {
    pub fn new(
        target_node_address: String,
        db_name: String,
        plan_bytes: Bytes,
        expected_schema: SchemaRef,
        session_config: HashMap<String, String>,
        query_id: String,
    ) -> Self {
        Self {
            target_node_address,
            db_name,
            plan_bytes,
            expected_schema,
            session_config,
            query_id,
            state: Mutex::new(RemoteStreamState::Initial),
        }
    }
}

impl Stream for RemoteRecordBatchStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state_guard = self.state.lock();

        loop {
            match &mut *state_guard {
                RemoteStreamState::Initial => {
                    let target_addr = self.target_node_address.clone();
                    debug!(target_addr = %target_addr, query_id = %self.query_id, "RemoteRecordBatchStream: Initializing connection");

                    let connecting_future = async move {
                        let client = GrpcDistributedQueryClient::new(target_addr).await;
                        // GrpcDistributedQueryClient::new doesn't return Result, it logs errors and sets client to None.
                        // We need to check if client.client is Some.
                        if client.is_connected() { // Assuming an is_connected method or similar check
                            Ok(client)
                        } else {
                            Err(tonic::Status::unavailable("Failed to connect to remote node"))
                        }
                    };
                    *state_guard = RemoteStreamState::Connecting(Box::pin(connecting_future));
                    // Continue to poll the new state immediately
                }
                RemoteStreamState::Connecting(connecting_fut) => {
                    debug!(query_id = %self.query_id, "RemoteRecordBatchStream: Connecting...");
                    match Pin::new(connecting_fut).poll(cx) {
                        Poll::Ready(Ok(mut query_client)) => { // query_client is GrpcDistributedQueryClient
                            info!(target_addr = %self.target_node_address, query_id = %self.query_id, "RemoteRecordBatchStream: Connected. Requesting fragment stream.");

                            let expected_schema_bytes_for_request = match serialize_schema_to_df_proto_bytes(&self.expected_schema) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    error!(query_id = %self.query_id, error = %e, "Failed to serialize expected schema to DF Proto for remote request");
                                    *state_guard = RemoteStreamState::Error(Arc::new(e));
                                    continue; // Re-poll in Error state
                                }
                            };

                            // Assuming FragmentType enum exists in generated code from proto
                            // and SQL_QUERY is 1.
                            // pub const SERIALIZED_PLAN: i32 = 0;
                            // pub const SQL_QUERY: i32 = 1;
                            const SQL_QUERY_FRAGMENT_TYPE: i32 = 1;

                            let request = GrpcExecuteQueryFragmentRequest {
                                database_name: self.db_name.clone(),
                                fragment_payload_bytes: self.plan_bytes.clone(), // Renamed from plan_bytes
                                // shard_id is not part of the GrpcExecuteQueryFragmentRequest proto,
                                // but if it were relevant for SQL, it might be part of the SQL string itself.
                                session_config: self.session_config.clone(),
                                query_id: self.query_id.clone(),
                                expected_schema_bytes: expected_schema_bytes_for_request,
                                fragment_type: SQL_QUERY_FRAGMENT_TYPE, // Set fragment type
                            };

                            let stream_request_future = async move {
                                query_client.execute_query_fragment_streaming(request).await
                            };
                            *state_guard = RemoteStreamState::RequestingStream(Box::pin(stream_request_future));
                            // Continue to poll the new state immediately
                        }
                        Poll::Ready(Err(status)) => {
                            error!(query_id = %self.query_id, error = %status, "RemoteRecordBatchStream: Connection future failed");
                            *state_guard = RemoteStreamState::Error(Arc::new(ArrowError::ExternalError(Box::new(status))));
                            // Continue to poll the Error state
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                RemoteStreamState::RequestingStream(stream_fut) => {
                    debug!(query_id = %self.query_id, "RemoteRecordBatchStream: Requesting stream...");
                    match Pin::new(stream_fut).poll(cx) {
                        Poll::Ready(Ok(batch_stream)) => {
                            info!(query_id = %self.query_id, "RemoteRecordBatchStream: Stream received, now fetching.");
                            *state_guard = RemoteStreamState::Fetching(batch_stream);
                            // Continue to poll the new Fetching state
                        }
                        Poll::Ready(Err(e)) => { // ArrowError from execute_query_fragment_streaming
                            error!(query_id = %self.query_id, error = %e, "Failed to obtain remote stream");
                            *state_guard = RemoteStreamState::Error(Arc::new(e));
                            // Continue to poll the Error state
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                RemoteStreamState::Fetching(stream) => {
                    match stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            error!(query_id = %self.query_id, error = %e, "RemoteRecordBatchStream: Error during fetching");
                            // The error `e` is already an ArrowError.
                            // Store its Arc in the Error state and return the original error.
                            *state_guard = RemoteStreamState::Error(Arc::new(e.clone()));
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            info!(query_id = %self.query_id, "RemoteRecordBatchStream: Finished fetching");
                            *state_guard = RemoteStreamState::Finished;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                RemoteStreamState::Error(e) => {
                    // Error already propagated, subsequent polls yield None.
                    *state_guard = RemoteStreamState::Finished;
                    return Poll::Ready(None);
                }
                RemoteStreamState::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for RemoteRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.expected_schema.clone()
    }
}

// Add a helper to GrpcDistributedQueryClient
impl GrpcDistributedQueryClient {
    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Schema, Field, DataType};
    use arrow_array::Int32Array;
    use std::sync::Arc;
    use futures::stream;
    use tokio::sync::Mutex as TokioMutex; // For mock client if it needs async mutex

    // Mock GrpcDistributedQueryClient for testing.
    // This is a simplified version. In a real scenario, you might use a mocking library
    // or more sophisticated trait-based DI.
    #[derive(Debug)]
    struct TestClientHandle {
        // Simulate connection error for a specific address
        connect_error_for_addr: Option<String>,
        // Simulate request error for a specific address
        request_error_for_addr: Option<String>,
        // Stream to return on successful request
        stream_to_provide: Option<BoxStream<'static, Result<RecordBatch, ArrowError>>>,
        // Use a tokio mutex if the client itself needs to be mutated across await points by tests
        // For this mock, fields are set once.
    }

    impl TestClientHandle {
        // Factory for the mock client that RemoteRecordBatchStream will try to create
        async fn create_mock_grpc_client(addr: String, handle: Arc<TokioMutex<Self>>) -> Result<GrpcDistributedQueryClient, tonic::Status> {
            let handle_guard = handle.lock().await;
            if let Some(err_addr) = &handle_guard.connect_error_for_addr {
                if *err_addr == addr {
                    return Err(tonic::Status::unavailable("Simulated connect error"));
                }
            }
            // Return a dummy GrpcDistributedQueryClient because its internal client isn't directly used by RemoteRecordBatchStream's RequestingStream state.
            // That state uses the *future* we return from mock_execute_query_fragment_streaming.
            // The actual tonic::Channel is not needed for these unit tests.
            Ok(GrpcDistributedQueryClient{ client: None, target_addr: addr })
        }

        // Mock of the method that RemoteRecordBatchStream calls
        async fn mock_execute_query_fragment_streaming(
            addr: String, // Pass addr to simulate per-address behavior
            _request: GrpcExecuteQueryFragmentRequest, // Can inspect request if needed
            handle: Arc<TokioMutex<Self>>
        ) -> Result<BoxStream<'static, Result<RecordBatch, ArrowError>>, ArrowError> {
            let mut handle_guard = handle.lock().await;
            if let Some(err_addr) = &handle_guard.request_error_for_addr {
                if *err_addr == addr {
                    return Err(ArrowError::ComputeError("Simulated request error".to_string()));
                }
            }
            handle_guard.stream_to_provide.take().ok_or_else(|| ArrowError::ComputeError("Mock stream exhausted or not provided".to_string()))
        }
    }

    fn test_arrow_schema() -> SchemaRef { // Renamed from test_schema to avoid conflict
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn create_record_batch(ids: Vec<i32>) -> Result<RecordBatch, ArrowError> {
        let schema = test_arrow_schema();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))])
    }

    // Helper to create a RemoteRecordBatchStream with a mock client setup
    // This is a bit involved due to the direct `GrpcDistributedQueryClient::new` call.
    // For these tests, we will manually set the state to simulate outcomes of these calls.

    #[tokio::test]
    async fn test_successful_flow() {
        let schema = test_arrow_schema();
        let batch1 = create_record_batch(vec![1, 2]).unwrap();
        let batch2 = create_record_batch(vec![3, 4]).unwrap();
        let expected_batches = vec![batch1.clone(), batch2.clone()];

        let mock_stream_data = vec![Ok(batch1), Ok(batch2)];
        let mock_upstream = stream::iter(mock_stream_data).boxed();

        let mut stream = RemoteRecordBatchStream::new(
            "success_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            schema.clone(),
            HashMap::new(),
            "query_success".to_string(),
        );

        // Manually set to Fetching state with the mock stream
        stream.state = Mutex::new(RemoteStreamState::Fetching(mock_upstream));

        let mut result_batches = Vec::new();
        while let Some(item) = stream.next().await {
            result_batches.push(item.unwrap());
        }

        assert_eq!(result_batches.len(), expected_batches.len());
        // Further checks on batch content can be added if necessary
    }

    #[tokio::test]
    async fn test_client_connection_error_via_state() {
        let schema = test_arrow_schema();
        let mut stream = RemoteRecordBatchStream::new(
            "connect_error_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            schema.clone(),
            HashMap::new(),
            "query_connect_err".to_string(),
        );

        // Simulate GrpcDistributedQueryClient::new resulting in an error
        let mock_connect_error_future = Box::pin(async {
            Err(tonic::Status::unavailable("Simulated connect error via future"))
        });

        stream.state = Mutex::new(RemoteStreamState::Connecting(mock_connect_error_future));

        let result = stream.next().await; // This will poll the Connecting future
        assert!(result.is_some(), "Stream should yield an item (error)");
        match result.unwrap() {
            Err(ArrowError::ExternalError(e)) => {
                assert!(e.to_string().contains("Simulated connect error via future"));
            },
            other => panic!("Expected ExternalError (from tonic::Status), got {:?}", other),
        }

        // After an error, the stream should be exhausted or continue to yield the same error
        // Current implementation: Error state makes subsequent polls yield None.
        let next_item = stream.next().await;
        assert!(next_item.is_none(), "Stream should be exhausted after error state propagation");
         // Check that the state is now Finished (or Error, then Finished)
        {
            let guard = stream.state.lock();
            assert!(matches!(*guard, RemoteStreamState::Finished));
        }
    }

    #[tokio::test]
    async fn test_request_stream_error_via_state() {
        let schema = test_arrow_schema();
        let mut stream = RemoteRecordBatchStream::new(
            "request_error_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            schema.clone(),
            HashMap::new(),
            "query_req_err".to_string(),
        );

        let mock_request_error_future = Box::pin(async {
            Err(ArrowError::ComputeError("Simulated stream request error".to_string()))
        });

        stream.state = Mutex::new(RemoteStreamState::RequestingStream(mock_request_error_future));

        let result = stream.next().await;
        assert!(result.is_some());
        match result.unwrap() {
            Err(ArrowError::ComputeError(msg)) => assert!(msg.contains("Simulated stream request error")),
            other => panic!("Expected ComputeError, got {:?}", other),
        }
         let next_item = stream.next().await;
        assert!(next_item.is_none(), "Stream should be exhausted after error state propagation");
    }

    #[tokio::test]
    async fn test_fetching_error_from_upstream_data() {
        let schema = test_arrow_schema();
        let batch1 = create_record_batch(vec![1]).unwrap();
        let error_stream_data = vec![
            Ok(batch1.clone()),
            Err(ArrowError::OutOfMemoryError("Simulated OOM during fetch".to_string()))
        ];
        let mock_error_upstream = stream::iter(error_stream_data).boxed();

        let mut stream = RemoteRecordBatchStream::new(
            "fetching_error_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            schema.clone(),
            HashMap::new(),
            "query_fetch_err".to_string(),
        );
        stream.state = Mutex::new(RemoteStreamState::Fetching(mock_error_upstream));

        let first_item = stream.next().await;
        assert!(first_item.is_some());
        assert!(first_item.unwrap().is_ok(), "First batch should be Ok");

        let second_item = stream.next().await;
        assert!(second_item.is_some(), "Stream should yield an item (error)");
        match second_item.unwrap() {
            Err(ArrowError::OutOfMemoryError(msg)) => { // Expecting the original error type
                 assert!(msg.contains("Simulated OOM during fetch"));
            }
            other => panic!("Expected OutOfMemoryError, got {:?}", other),
        }

        let next_item = stream.next().await;
        assert!(next_item.is_none(), "Stream should be exhausted after error state propagation");
    }

    #[tokio::test]
    async fn test_empty_successful_stream_via_state() {
        let schema = test_arrow_schema();
        let empty_upstream = stream::empty().boxed();

        let mut stream = RemoteRecordBatchStream::new(
            "empty_success_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            schema.clone(),
            HashMap::new(),
            "query_empty_ok".to_string(),
        );
        stream.state = Mutex::new(RemoteStreamState::Fetching(empty_upstream));

        let item = stream.next().await;
        assert!(item.is_none(), "Empty stream should yield None directly");

        {
            let guard = stream.state.lock();
            assert!(matches!(*guard, RemoteStreamState::Finished));
        }
    }

    #[tokio::test]
    async fn test_schema_serialization_error_direct() {
        // Test the serialize_schema_to_df_proto_bytes function directly if a schema can cause error
        // For example, if try_into() from ArrowSchema to DfSchemaProto can fail.
        // ArrowSchema itself doesn't make it easy to create an "invalid" schema that passes its own validation
        // but fails proto conversion, unless there are specific constraints in DfSchemaProto not in ArrowSchema.
        // Most common errors would be from I/O or external calls, not schema structure itself for protos.

        // This test is more about ensuring the error pathway in `poll_next` for `Connecting` state
        // if `serialize_schema_to_df_proto_bytes` fails.
        let mut stream = RemoteRecordBatchStream::new(
            "schema_err_addr".to_string(),
            "test_db".to_string(),
            Bytes::new(),
            // Provide a schema that might cause `try_into` to DfSchemaProto to fail, if such a case exists
            // and is testable. For now, using a normal schema. The error will be injected.
            test_arrow_schema(),
            HashMap::new(),
            "query_schema_err".to_string(),
        );

        // Simulate being in Connecting state, and `serialize_schema_to_df_proto_bytes` fails.
        // To do this, we need a client future that resolves successfully, so the code path
        // for serializing schema is reached.
        let dummy_client_future = Box::pin(async {
             Ok(GrpcDistributedQueryClient{ client: None, target_addr: "dummy".to_string() })
        });
        stream.state = Mutex::new(RemoteStreamState::Connecting(dummy_client_future));

        // To make `serialize_schema_to_df_proto_bytes` fail, we'd have to modify it or the schema.
        // Let's assume it does fail by altering its behavior for testing (conceptually).
        // If `serialize_schema_to_df_proto_bytes` returns an error, the state machine should transition to Error.
        // This is hard to test without either:
        // 1. A schema known to fail `try_into` to `DfSchemaProto`.
        // 2. Modifying `RemoteRecordBatchStream` to inject this error.

        // For now, this test case acknowledges the path but relies on other tests for error state transitions.
        // If a specific schema is found that causes `(&ArrowSchema).try_into DfSchemaProto` to fail,
        // it could be used here to make `serialize_schema_to_df_proto_bytes` return `Err`.
        // Then, polling `stream.next().await` should result in `Some(Err(...))`.

        // Example: If `serialize_schema_to_df_proto_bytes` was fallible in a way we can trigger:
        // stream.expected_schema = Arc::new(create_known_bad_schema_for_df_proto());
        // let result = stream.next().await;
        // assert!(result.is_some() && result.unwrap().is_err());
        println!("Note: Test for schema serialization error in poll_next relies on specific schema failure or mock injection not yet implemented.");
    }
}
