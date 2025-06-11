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
    ExecuteQueryFragmentRequest as GrpcExecuteQueryFragmentRequest, // Alias to avoid confusion
    FlightData as GrpcFlightData, // Alias
};
use observability_deps::tracing::{info, warn};
use tonic::transport::Channel;

/// gRPC client for the DistributedQueryService.
#[derive(Debug, Clone)]
pub struct GrpcDistributedQueryClient {
    client: Option<DistributedQueryServiceClient<Channel>>,
    target_addr: String,
}

impl GrpcDistributedQueryClient {
    /// Creates a new client and attempts to connect to the target address.
    pub async fn new(target_addr: String) -> Self {
        info!("Attempting to connect to distributed query service at {}", target_addr);
        match DistributedQueryServiceClient::connect(target_addr.clone()).await {
            Ok(client_channel) => {
                info!("Successfully connected to distributed query service at {}", target_addr);
                Self {
                    client: Some(client_channel),
                    target_addr,
                }
            }
            Err(e) => {
                warn!("Failed to connect to distributed query service at {}: {}. Client will be unavailable.", target_addr, e);
                Self {
                    client: None,
                    target_addr,
                }
            }
        }
    }

    /// Executes a query fragment on a remote node.
    pub async fn execute_query_fragment(
        &mut self, // Takes &mut self to allow client to be updated (e.g. reconnected if necessary, though not implemented yet)
        request: GrpcExecuteQueryFragmentRequest,
    ) -> Result<tonic::Response<tonic::Streaming<GrpcFlightData>>, tonic::Status> {
        if let Some(client) = self.client.as_mut() {
            client.execute_query_fragment(request).await
        } else {
            Err(tonic::Status::unavailable(format!(
                "Distributed query client for {} is not connected.",
                self.target_addr
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let request = GrpcExecuteQueryFragmentRequest {
            database_name: "test_db".to_string(),
            plan_bytes: vec![1, 2, 3],
            shard_id: Some(1),
            session_config: Default::default(),
            query_id: "test_query_id".to_string(),
            expected_schema_bytes: vec![],
        };

        let result = client.execute_query_fragment(request).await;
        assert!(result.is_err());
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::Unavailable);
            assert!(status.message().contains("not connected"));
        }
    }
}
