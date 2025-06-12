use influxdb3_proto::influxdb3::internal::node_data_management::v1::{
    node_data_management_service_server::NodeDataManagementService,
    ApplyShardSnapshotRequest, ApplyShardSnapshotResponse,
    DeleteShardDataRequest, DeleteShardDataResponse,
    LockShardWritesRequest, LockShardWritesResponse,
    PrepareShardSnapshotRequest, PrepareShardSnapshotResponse,
    SignalWalStreamProcessedRequest, SignalWalStreamProcessedResponse,
    UnlockShardWritesRequest, UnlockShardWritesResponse,
    ShardIdentifier, // Assuming ShardIdentifier is part of this proto module
};
use std::sync::Arc;
use influxdb3_catalog::catalog::Catalog;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Debug)]
pub struct NodeDataManagementServerImpl {
    #[allow(dead_code)] // May not be used in stub implementation but good for context
    catalog: Arc<Catalog>,
}

impl NodeDataManagementServerImpl {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl NodeDataManagementService for NodeDataManagementServerImpl {
    async fn prepare_shard_snapshot(
        &self,
        request: Request<PrepareShardSnapshotRequest>,
    ) -> Result<Response<PrepareShardSnapshotResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received PrepareShardSnapshot request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for preparing a shard snapshot
        Ok(Response::new(PrepareShardSnapshotResponse {
            success: true,
            error_message: "".to_string(),
            // snapshot_path_on_object_store: "conceptual/path/to/snapshot.tar.gz".to_string(),
        }))
    }

    async fn apply_shard_snapshot(
        &self,
        request: Request<ApplyShardSnapshotRequest>,
    ) -> Result<Response<ApplyShardSnapshotResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received ApplyShardSnapshot request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for applying a shard snapshot
        Ok(Response::new(ApplyShardSnapshotResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn signal_wal_stream_processed(
        &self,
        request: Request<SignalWalStreamProcessedRequest>,
    ) -> Result<Response<SignalWalStreamProcessedResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received SignalWalStreamProcessed request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for this signal
        Ok(Response::new(SignalWalStreamProcessedResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn lock_shard_writes(
        &self,
        request: Request<LockShardWritesRequest>,
    ) -> Result<Response<LockShardWritesResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received LockShardWrites request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for locking shard writes
        Ok(Response::new(LockShardWritesResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn unlock_shard_writes(
        &self,
        request: Request<UnlockShardWritesRequest>,
    ) -> Result<Response<UnlockShardWritesResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received UnlockShardWrites request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for unlocking shard writes
        Ok(Response::new(UnlockShardWritesResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn delete_shard_data(
        &self,
        request: Request<DeleteShardDataRequest>,
    ) -> Result<Response<DeleteShardDataResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received DeleteShardData request for shard_identifier: {:?}", req_ref.shard_identifier);
        // TODO: Implement actual logic for deleting shard data
        Ok(Response::new(DeleteShardDataResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::NodeId; // For creating dummy NodeId if needed for Catalog
    use object_store::memory::InMemory; // For dummy object store for Catalog
    use iox_time::{MockProvider, Time};
    use influxdb3_proto::influxdb3::internal::node_data_management::v1::ShardIdentifier;

    async fn setup_service() -> NodeDataManagementServerImpl {
        let object_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // Use a default CatalogArgs and CatalogLimits for test setup
        let catalog = Arc::new(
            Catalog::new_with_args(
                Arc::from(NodeId::new(0).to_string()), // Dummy node_id string
                object_store,
                time_provider,
                Default::default(), // metric registry
                Default::default(), // catalog args
                Default::default(), // catalog limits
            )
            .await
            .unwrap(),
        );
        NodeDataManagementServerImpl::new(catalog)
    }

    fn dummy_shard_identifier() -> Option<ShardIdentifier> {
        Some(ShardIdentifier {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            shard_id: 1,
        })
    }

    #[tokio::test]
    async fn test_prepare_shard_snapshot_stub() {
        let service = setup_service().await;
        let request = Request::new(PrepareShardSnapshotRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.prepare_shard_snapshot(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_apply_shard_snapshot_stub() {
        let service = setup_service().await;
        let request = Request::new(ApplyShardSnapshotRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.apply_shard_snapshot(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_signal_wal_stream_processed_stub() {
        let service = setup_service().await;
        let request = Request::new(SignalWalStreamProcessedRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.signal_wal_stream_processed(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_lock_shard_writes_stub() {
        let service = setup_service().await;
        let request = Request::new(LockShardWritesRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.lock_shard_writes(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_unlock_shard_writes_stub() {
        let service = setup_service().await;
        let request = Request::new(UnlockShardWritesRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.unlock_shard_writes(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_delete_shard_data_stub() {
        let service = setup_service().await;
        let request = Request::new(DeleteShardDataRequest { shard_identifier: dummy_shard_identifier() });
        let response = service.delete_shard_data(request).await.unwrap();
        assert!(response.get_ref().success);
    }
}
