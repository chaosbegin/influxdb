use influxdb3_proto::influxdb3::internal::replication::v1::{
    replication_service_client::ReplicationServiceClient,
    ReplicateWalOpRequest, ReplicateWalOpResponse,
};
use tonic::transport::Channel;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicationClientError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC service error: {0}")]
    Service(#[from] tonic::Status),

    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri), // tonic::transport::Error can wrap this
}

pub struct GrpcReplicationClient {
    client: ReplicationServiceClient<Channel>,
}

impl GrpcReplicationClient {
    pub async fn new(node_address: String) -> Result<Self, ReplicationClientError> {
        let formatted_address = if !node_address.starts_with("http://") && !node_address.starts_with("https://") {
            format!("http://{}", node_address)
        } else {
            node_address
        };

        let channel = Channel::from_shared(formatted_address)
            .map_err(|e| ReplicationClientError::InvalidUri(e.into()))? // Ensure InvalidUri is covered if from_shared returns http::Error directly
            .connect_timeout(std::time::Duration::from_secs(5))
            .connect()
            .await?; // This await can return tonic::transport::Error

        Ok(Self {
            client: ReplicationServiceClient::new(channel),
        })
    }

    pub async fn replicate_wal_op(
        &mut self,
        request: ReplicateWalOpRequest,
    ) -> Result<tonic::Response<ReplicateWalOpResponse>, tonic::Status> {
        let mut tonic_request = tonic::Request::new(request);
        tonic_request.set_timeout(std::time::Duration::from_secs(10));
        self.client.replicate_wal_op(tonic_request).await
    }
}

// Mock client for testing purposes
#[cfg(any(test, feature = "test_utils"))] // Keep mock available for integration tests via feature flag
pub mod mock {
    use super::*; // Imports GrpcReplicationClient's ReplicateWalOpRequest/Response from proto
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    // If MockReplicationClient needs to be used by WriteBufferImpl directly (not just in tests of WriteBufferImpl)
    // then its ReplicateWalOpRequest/Response types might need to be generic or also use the proto types.
    // The existing mock uses crate::ReplicateWalOpRequest which would mean they are defined in influxdb3_write/src/lib.rs
    // For now, let's assume they are the same as proto types or will be made compatible.

    #[derive(Debug, Clone)]
    pub struct MockReplicationClient {
        successful_nodes: Arc<Mutex<HashSet<String>>>,
        programmed_success_count: Arc<Mutex<usize>>,
        // For asserting requests if needed by tests
        // requests_tx: Arc<Mutex<tokio::sync::mpsc::Sender<ReplicateWalOpRequest>>>,
    }

    impl MockReplicationClient {
        pub fn new() -> Self {
            Self {
                successful_nodes: Arc::new(Mutex::new(HashSet::new())),
                programmed_success_count: Arc::new(Mutex::new(0)),
                // requests_tx: Arc::new(Mutex::new(requests_tx)), // Example if needed
            }
        }

        pub async fn prime_with_successes(&self, programmed_successes: usize) {
            let mut count = self.programmed_success_count.lock().await;
            *count = programmed_successes;
        }

        pub async fn prime_successful_nodes(&self, nodes_to_succeed: Vec<String>) {
            let mut successful_nodes = self.successful_nodes.lock().await;
            successful_nodes.clear();
            for node_id in nodes_to_succeed {
                successful_nodes.insert(node_id);
            }
        }

        // Updated to use proto types for request and response for consistency
        pub async fn replicate_wal_op(
            &self,
            target_node_address: &str,
            _request: &ReplicateWalOpRequest, // Request content still not deeply inspected by this mock
        ) -> Result<ReplicateWalOpResponse, String> { // Error type simplified to String for mock
            let successful_nodes_locked = self.successful_nodes.lock().await;
            if successful_nodes_locked.contains(target_node_address) {
                return Ok(ReplicateWalOpResponse { success: true, error_message: None });
            }

            if successful_nodes_locked.is_empty() {
                let mut programmed_s_count = self.programmed_success_count.lock().await;
                if *programmed_s_count > 0 {
                    *programmed_s_count -= 1;
                    return Ok(ReplicateWalOpResponse { success: true, error_message: None });
                } else {
                    return Ok(ReplicateWalOpResponse {
                        success: false,
                        error_message: Some(format!("Mock error: Node {} failed or no programmed successes left", target_node_address))
                    });
                }
            }

            Ok(ReplicateWalOpResponse {
                success: false,
                error_message: Some(format!("Mock error: Node {} not in successful_nodes list", target_node_address)),
            })
        }
    }

    impl Default for MockReplicationClient {
        fn default() -> Self {
            Self::new()
        }
    }
}
