// influxdb3_cluster_manager/src/grpc_node_data_client.rs
use influxdb3_proto::influxdb3::internal::node_data_management::v1::{
    node_data_management_service_client::NodeDataManagementServiceClient,
    ApplyShardSnapshotRequest, ApplyShardSnapshotResponse,
    DeleteShardDataRequest, DeleteShardDataResponse,
    LockShardWritesRequest, LockShardWritesResponse,
    PrepareShardSnapshotRequest, PrepareShardSnapshotResponse,
    SignalWalStreamProcessedRequest, SignalWalStreamProcessedResponse,
    UnlockShardWritesRequest, UnlockShardWritesResponse,
};
use tonic::transport::Channel;
use super::node_data_client::NodeDataManagementClient; // The trait
use async_trait::async_trait;
use tonic::{Response, Status, Request as TonicRequest}; // Added TonicRequest for client calls

#[derive(Debug, Clone)]
pub struct GrpcNodeDataManagementClient {
    client: NodeDataManagementServiceClient<Channel>,
}

impl GrpcNodeDataManagementClient {
    pub async fn new(node_address: String) -> Result<Self, tonic::transport::Error> {
        let formatted_address = if !node_address.starts_with("http://") && !node_address.starts_with("https://") {
            format!("http://{}", node_address)
        } else {
            node_address
        };
        // Consider making timeouts configurable
        let channel = Channel::from_shared(formatted_address)
            .map_err(|e| tonic::transport::Error::from(Box::new(e)))? // Convert InvalidUri to transport::Error
            .connect_timeout(std::time::Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self { client: NodeDataManagementServiceClient::new(channel) })
    }
}

#[async_trait]
impl NodeDataManagementClient for GrpcNodeDataManagementClient {
    async fn prepare_shard_snapshot(&mut self, req: PrepareShardSnapshotRequest) -> Result<Response<PrepareShardSnapshotResponse>, Status> {
        self.client.prepare_shard_snapshot(TonicRequest::new(req)).await
    }

    async fn apply_shard_snapshot(&mut self, req: ApplyShardSnapshotRequest) -> Result<Response<ApplyShardSnapshotResponse>, Status> {
        self.client.apply_shard_snapshot(TonicRequest::new(req)).await
    }

    async fn signal_wal_stream_processed(&mut self, req: SignalWalStreamProcessedRequest) -> Result<Response<SignalWalStreamProcessedResponse>, Status> {
        self.client.signal_wal_stream_processed(TonicRequest::new(req)).await
    }

    async fn lock_shard_writes(&mut self, req: LockShardWritesRequest) -> Result<Response<LockShardWritesResponse>, Status> {
        self.client.lock_shard_writes(TonicRequest::new(req)).await
    }

    async fn unlock_shard_writes(&mut self, req: UnlockShardWritesRequest) -> Result<Response<UnlockShardWritesResponse>, Status> {
        self.client.unlock_shard_writes(TonicRequest::new(req)).await
    }

    async fn delete_shard_data(&mut self, req: DeleteShardDataRequest) -> Result<Response<DeleteShardDataResponse>, Status> {
        self.client.delete_shard_data(TonicRequest::new(req)).await
    }
}
