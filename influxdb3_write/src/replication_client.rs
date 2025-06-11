use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Response, Status};
use observability_deps::tracing::{info, warn}; // For logging

// Use the placeholder request/response types defined in influxdb3_write/src/lib.rs
// These are for the trait interface. The GrpcReplicationClient will convert to/from the generated gRPC types.
use crate::{ReplicateWalOpRequest as PlaceholderReplicateWalOpRequest, ReplicateWalOpResponse as PlaceholderReplicateWalOpResponse};

// Import generated gRPC client and request/response types from the new protos crate
use influxdb3_replication_protos::influxdb3::internal::replication::v1::{
    replication_service_client::ReplicationServiceClient,
    ReplicateWalOpRequest as GrpcReplicateWalOpRequest, // Alias to avoid name clash
    ReplicateWalOpResponse as GrpcReplicateWalOpResponse, // Alias
};


/// Trait defining the interface for a replication client.
#[async_trait]
pub trait ReplicationClient: std::fmt::Debug + Send + Sync {
    async fn replicate_wal_op(
        &mut self, // Changed to &mut self to allow client to update its state if needed (e.g. reconnect)
        request: PlaceholderReplicateWalOpRequest,
    ) -> Result<Response<PlaceholderReplicateWalOpResponse>, Status>;
}

/// gRPC implementation of the ReplicationClient.
#[derive(Debug, Clone)] // Clone is possible because Channel is cloneable
pub struct GrpcReplicationClient {
    // client is an Option to handle cases where connection might fail during creation or be deferred.
    // For simplicity in this iteration, we'll attempt connection in `new` and store Some(client) or None.
    // A more robust implementation might store the channel and create the client on each call, or use a connection pool.
    client: Option<ReplicationServiceClient<tonic::transport::Channel>>,
    target_addr: String,
}

impl GrpcReplicationClient {
    pub async fn new(target_addr: String) -> Self {
        info!("Attempting to connect to replication service at {}", target_addr);
        match ReplicationServiceClient::connect(target_addr.clone()).await {
            Ok(client_channel) => {
                info!("Successfully connected to replication service at {}", target_addr);
                Self {
                    client: Some(client_channel),
                    target_addr,
                }
            }
            Err(e) => {
                warn!("Failed to connect to replication service at {}: {}. Client will be unavailable.", target_addr, e);
                Self {
                    client: None,
                    target_addr,
                }
            }
        }
    }
}

#[async_trait]
impl ReplicationClient for GrpcReplicationClient {
    async fn replicate_wal_op(
        &mut self,
        request: PlaceholderReplicateWalOpRequest,
    ) -> Result<Response<PlaceholderReplicateWalOpResponse>, Status> {
        if let Some(mut client) = self.client.as_mut() { // Use as_mut() to get a mutable reference
            // Convert placeholder request to generated gRPC request
            let grpc_request = GrpcReplicateWalOpRequest {
                wal_op_bytes: request.wal_op_bytes, // Assuming field name is the same
                originating_node_id: request.originating_node_id,
                shard_id: request.shard_id, // Pass through Option<u64>
                database_name: request.database_name,
                table_name: request.table_name,
            };

            match client.replicate_wal_op(grpc_request).await {
                Ok(grpc_response_wrapper) => {
                    let grpc_response = grpc_response_wrapper.into_inner();
                    // Convert generated gRPC response back to placeholder response
                    let placeholder_response = PlaceholderReplicateWalOpResponse {
                        success: grpc_response.success,
                        error_message: grpc_response.error_message,
                    };
                    Ok(Response::new(placeholder_response))
                }
                Err(status) => {
                    warn!("gRPC call to {} for ReplicateWalOp failed: {}", self.target_addr, status);
                    Err(status)
                }
            }
        } else {
            Err(Status::unavailable(format!(
                "Replication client for {} is not connected.",
                self.target_addr
            )))
        }
    }
}


/// Mock implementation of the ReplicationClient for testing.
#[derive(Debug, Clone)]
pub struct MockReplicationClient {
    target_node_id: String,
    force_success: bool,
    force_rpc_error: Option<Status>,
    force_response_error: Option<String>,
    pub calls: Arc<Mutex<Vec<PlaceholderReplicateWalOpRequest>>>,
}

impl MockReplicationClient {
    pub fn new(target_node_id: String) -> Self {
        Self {
            target_node_id,
            force_success: true,
            force_rpc_error: None,
            force_response_error: None,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[cfg(test)]
    pub fn set_force_success(&mut self, success: bool) {
        self.force_success = success;
    }

    #[cfg(test)]
    pub fn set_force_rpc_error(&mut self, status: Option<Status>) {
        self.force_rpc_error = status;
    }

    #[cfg(test)]
    pub fn set_force_response_error(&mut self, error_msg: Option<String>) {
        self.force_response_error = error_msg;
        if self.force_response_error.is_some() {
            self.force_success = false;
        }
    }

    pub fn get_calls(&self) -> Vec<PlaceholderReplicateWalOpRequest> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ReplicationClient for MockReplicationClient {
    async fn replicate_wal_op(
        &mut self,
        request: PlaceholderReplicateWalOpRequest,
    ) -> Result<Response<PlaceholderReplicateWalOpResponse>, Status> {
        self.calls.lock().unwrap().push(request.clone());

        if let Some(status) = &self.force_rpc_error {
            return Err(status.clone());
        }

        if !self.force_success || self.force_response_error.is_some() {
            return Ok(Response::new(PlaceholderReplicateWalOpResponse {
                success: false,
                error_message: self.force_response_error.clone().or_else(|| Some(format!("Simulated failure from node {}", self.target_node_id))),
            }));
        }

        Ok(Response::new(PlaceholderReplicateWalOpResponse { success: true, error_message: None }))
    }
}

pub type ReplicationClientFactory = Arc<dyn Fn(String) -> Arc<dyn ReplicationClient> + Send + Sync>;

pub fn default_replication_client_factory() -> ReplicationClientFactory {
    Arc::new(|target_addr: String| -> Arc<dyn ReplicationClient> {
        // Using futures::executor::block_on for simplicity in factory initialization.
        // This assumes that the factory itself is not called in a deeply nested async context
        // where blocking would be problematic. A fully async factory pattern might involve
        // the factory returning a Future that resolves to the client.
        let client = futures::executor::block_on(GrpcReplicationClient::new(target_addr));
        Arc::new(client)
    })
}

#[cfg(test)]
pub fn mock_replication_client_factory(
    mock_behaviors: Arc<Mutex<HashMap<String, Result<bool, i32>>>>,
) -> ReplicationClientFactory {
    Arc::new(move |target_node_id: String| -> Arc<dyn ReplicationClient> {
        let behaviors = mock_behaviors.lock().unwrap();
        let behavior = behaviors.get(&target_node_id);

        let mut client = MockReplicationClient::new(target_node_id.clone());
        if let Some(beh) = behavior {
            match beh {
                Ok(success_flag) => {
                    client.set_force_success(*success_flag);
                    if !*success_flag {
                         client.set_force_response_error(Some("Simulated application error by test factory".to_string()));
                    }
                }
                Err(status_code_int) => {
                    let code = tonic::Code::from_i32(status_code_int.abs() % 17);
                    client.set_force_rpc_error(Some(Status::new(code, "Simulated RPC error by test factory")));
                }
            }
        }
        Arc::new(client)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReplicateWalOpRequest as PlaceholderRequest; // Use the placeholder for test data

    #[tokio::test]
    async fn test_grpc_replication_client_new_connection_error() {
        // Attempt to connect to an obviously invalid address to simulate connection error.
        // Assuming "invalid-address" will not resolve or connect.
        let client = GrpcReplicationClient::new("http://invalid-address:12345".to_string()).await;
        assert!(client.client.is_none(), "Client should be None if connection fails");
    }

    #[tokio::test]
    async fn test_grpc_replication_client_replicate_wal_op_no_client() {
        let mut client = GrpcReplicationClient { client: None, target_addr: "dummy:123".to_string() };
        let request = PlaceholderRequest {
            wal_op_bytes: vec![1, 2, 3],
            originating_node_id: "test_origin".to_string(),
            shard_id: Some(1),
            database_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
        };
        let result = client.replicate_wal_op(request).await;
        assert!(result.is_err());
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::Unavailable);
            assert!(status.message().contains("not connected"));
        }
    }

    // Further tests for successful replicate_wal_op calls would require a mock gRPC server
    // or a more complex setup with a real server, which is beyond the scope of easily
    // unit-testing this client part without significant test infrastructure.
    // The main things to test here are:
    // 1. Connection failure handling (done above).
    // 2. Correct conversion of request/response types (compile-time checked by types, runtime by field mapping).
    //    This could be unit-tested by manually creating a GrpcReplicationClient with a
    //    mocked channel if tonic makes that straightforward, but often it's not.
    //    For now, we assume the direct field mapping is correct if it compiles.
}
