// influxdb3_cluster_manager/src/node_data_client.rs
use async_trait::async_trait;
use influxdb3_proto::influxdb3::internal::node_data_management::v1::{
    ApplyShardSnapshotRequest, ApplyShardSnapshotResponse,
    DeleteShardDataRequest, DeleteShardDataResponse,
    LockShardWritesRequest, LockShardWritesResponse,
    PrepareShardSnapshotRequest, PrepareShardSnapshotResponse,
    SignalWalStreamProcessedRequest, SignalWalStreamProcessedResponse,
    UnlockShardWritesRequest, UnlockShardWritesResponse,
};
use tonic::{Response, Status};
use std::fmt::Debug;

#[async_trait]
pub trait NodeDataManagementClient: Send + Sync + Debug {
    async fn prepare_shard_snapshot(&mut self, req: PrepareShardSnapshotRequest) -> Result<Response<PrepareShardSnapshotResponse>, Status>;
    async fn apply_shard_snapshot(&mut self, req: ApplyShardSnapshotRequest) -> Result<Response<ApplyShardSnapshotResponse>, Status>;
    async fn signal_wal_stream_processed(&mut self, req: SignalWalStreamProcessedRequest) -> Result<Response<SignalWalStreamProcessedResponse>, Status>;
    async fn lock_shard_writes(&mut self, req: LockShardWritesRequest) -> Result<Response<LockShardWritesResponse>, Status>;
    async fn unlock_shard_writes(&mut self, req: UnlockShardWritesRequest) -> Result<Response<UnlockShardWritesResponse>, Status>;
    async fn delete_shard_data(&mut self, req: DeleteShardDataRequest) -> Result<Response<DeleteShardDataResponse>, Status>;
}
