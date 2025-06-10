use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Response, Status};

// Use the placeholder request/response types defined in influxdb3_write/src/lib.rs
use crate::{ReplicateWalOpRequest, ReplicateWalOpResponse};

/// Trait defining the interface for a replication client.
#[async_trait]
pub trait ReplicationClient: std::fmt::Debug + Send + Sync {
    async fn replicate_wal_op(
        &mut self,
        request: ReplicateWalOpRequest,
    ) -> Result<Response<ReplicateWalOpResponse>, Status>;
}

/// Mock implementation of the ReplicationClient for testing.
#[derive(Debug, Clone)]
pub struct MockReplicationClient {
    target_node_id: String,
    // Predefined responses for specific requests or a default response
    // For simplicity, just a success/failure switch for now.
    force_success: bool,
    force_rpc_error: Option<Status>,
    force_response_error: Option<String>,
    // To track if replicate_wal_op was called and with what.
    // This needs to be behind Arc<Mutex<>> to be shared and modified by the async fn.
    pub calls: Arc<Mutex<Vec<ReplicateWalOpRequest>>>,
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
            self.force_success = false; // If there's a response error, it's not a full success
        }
    }

    pub fn get_calls(&self) -> Vec<ReplicateWalOpRequest> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ReplicationClient for MockReplicationClient {
    async fn replicate_wal_op(
        &mut self,
        request: ReplicateWalOpRequest,
    ) -> Result<Response<ReplicateWalOpResponse>, Status> {
        self.calls.lock().unwrap().push(request.clone()); // Store the request for inspection

        if let Some(status) = &self.force_rpc_error {
            return Err(status.clone());
        }

        if !self.force_success || self.force_response_error.is_some() {
            return Ok(Response::new(ReplicateWalOpResponse {
                success: false,
                error_message: self.force_response_error.clone().or_else(|| Some(format!("Simulated failure from node {}", self.target_node_id))),
            }));
        }

        Ok(Response::new(ReplicateWalOpResponse { success: true, error_message: None }))
    }
}

// Type alias for the factory function that creates ReplicationClient instances.
pub type ReplicationClientFactory = Arc<dyn Fn(String) -> Arc<dyn ReplicationClient> + Send + Sync>;

// Default factory for production code (would create real gRPC clients)
// For now, this will also return a mock client as we can't implement real gRPC.
pub fn default_replication_client_factory() -> ReplicationClientFactory {
    Arc::new(|target_node_address: String| -> Arc<dyn ReplicationClient> {
        // In real code:
        // let actual_client = RealReplicationServiceClient::connect(target_node_address).await.unwrap();
        // Arc::new(actual_client)

        // For this subtask, return a mock that always succeeds by default.
        // The address here would be something like "http://node_id:port"
        Arc::new(MockReplicationClient::new(target_node_address))
    })
}

// Factory for creating mock clients, configurable for testing
#[cfg(test)]
pub fn mock_replication_client_factory(
    // Map of node_id to behavior: Ok(success_bool) for app-level success/fail, Err(status_code_int) for RPC error
    // This allows fine-grained control over each mock replica in a test.
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
                    // Map integer to tonic::Code (simplified)
                    let code = tonic::Code::from_i32(status_code_int.abs() % 17); // Ensure valid code
                    client.set_force_rpc_error(Some(Status::new(code, "Simulated RPC error by test factory")));
                }
            }
        }
        // Default behavior if not in map: success (as per MockReplicationClient::new)
        Arc::new(client)
    })
}
