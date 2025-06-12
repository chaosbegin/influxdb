// influxdb3_cluster_manager/src/node_data_client_mock.rs
#![cfg(any(test, feature = "test_utils"))]

use influxdb3_proto::influxdb3::internal::node_data_management::v1::{
    ApplyShardSnapshotRequest, ApplyShardSnapshotResponse,
    DeleteShardDataRequest, DeleteShardDataResponse,
    LockShardWritesRequest, LockShardWritesResponse,
    PrepareShardSnapshotRequest, PrepareShardSnapshotResponse,
    SignalWalStreamProcessedRequest, SignalWalStreamProcessedResponse,
    UnlockShardWritesRequest, UnlockShardWritesResponse,
    ShardIdentifier as ProtoShardIdentifier,
};
use std::sync::{Arc, Mutex};
use tonic::{Response, Status};
use async_trait::async_trait; // Added
use super::node_data_client::NodeDataManagementClient; // Added

#[derive(Debug, Clone, PartialEq)]
pub enum ExpectedNodeCall {
    PrepareShardSnapshot(PrepareShardSnapshotRequest),
    ApplyShardSnapshot(ApplyShardSnapshotRequest),
    SignalWalStreamProcessed(SignalWalStreamProcessedRequest),
    LockShardWrites(LockShardWritesRequest),
    UnlockShardWrites(UnlockShardWritesRequest),
    DeleteShardData(DeleteShardDataRequest),
}

#[derive(Clone, Debug, Default)]
pub struct MockNodeDataManagementClient {
    pub calls: Arc<Mutex<Vec<ExpectedNodeCall>>>,
    // Optional: Add fields to control responses for more advanced testing
    // pub prepare_snapshot_response: Result<Response<PrepareShardSnapshotResponse>, Status>,
    // ... other responses
}

impl MockNodeDataManagementClient {
    pub fn new() -> Self {
        Default::default()
    }

    fn record_call(&self, call: ExpectedNodeCall) {
        self.calls.lock().unwrap().push(call);
    }

    // Simulating the generated client's async methods
    pub async fn prepare_shard_snapshot(&mut self, req: PrepareShardSnapshotRequest) -> Result<Response<PrepareShardSnapshotResponse>, Status> {
        self.record_call(ExpectedNodeCall::PrepareShardSnapshot(req.clone()));
        // Example: Allow response customization if added to struct
        // if let Some(resp) = &self.prepare_snapshot_response { return resp.clone(); }
        Ok(Response::new(PrepareShardSnapshotResponse { success: true, error_message: "".to_string() }))
    }

    pub async fn apply_shard_snapshot(&mut self, req: ApplyShardSnapshotRequest) -> Result<Response<ApplyShardSnapshotResponse>, Status> {
        self.record_call(ExpectedNodeCall::ApplyShardSnapshot(req.clone()));
        Ok(Response::new(ApplyShardSnapshotResponse { success: true, error_message: "".to_string() }))
    }

    pub async fn signal_wal_stream_processed(&mut self, req: SignalWalStreamProcessedRequest) -> Result<Response<SignalWalStreamProcessedResponse>, Status> {
        self.record_call(ExpectedNodeCall::SignalWalStreamProcessed(req.clone()));
        Ok(Response::new(SignalWalStreamProcessedResponse { success: true, error_message: "".to_string() }))
    }

    pub async fn lock_shard_writes(&mut self, req: LockShardWritesRequest) -> Result<Response<LockShardWritesResponse>, Status> {
        self.record_call(ExpectedNodeCall::LockShardWrites(req.clone()));
        Ok(Response::new(LockShardWritesResponse { success: true, error_message: "".to_string() }))
    }

    pub async fn unlock_shard_writes(&mut self, req: UnlockShardWritesRequest) -> Result<Response<UnlockShardWritesResponse>, Status> {
        self.record_call(ExpectedNodeCall::UnlockShardWrites(req.clone()));
        Ok(Response::new(UnlockShardWritesResponse { success: true, error_message: "".to_string() }))
    }

    pub async fn delete_shard_data(&mut self, req: DeleteShardDataRequest) -> Result<Response<DeleteShardDataResponse>, Status> {
        self.record_call(ExpectedNodeCall::DeleteShardData(req.clone()));
        Ok(Response::new(DeleteShardDataResponse { success: true, error_message: "".to_string() }))
    }
}
