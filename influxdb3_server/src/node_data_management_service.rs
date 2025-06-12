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
use std::collections::HashSet;
use std::sync::{Arc, Mutex}; // Use Mutex for interior mutability with Arc
use influxdb3_catalog::catalog::Catalog;
use tonic::{Request, Response, Status};
use tracing::info;

// Key type for HashSets
type ShardKey = (String, String, u64);

fn to_key(identifier: Option<&ShardIdentifier>) -> Option<ShardKey> {
    identifier.map(|id| (id.db_name.clone(), id.table_name.clone(), id.shard_id))
}

#[derive(Debug)]
pub struct NodeDataManagementServerImpl {
    #[allow(dead_code)]
    catalog: Arc<Catalog>,
    // In-memory state for simulation
    prepared_snapshots: Arc<Mutex<HashSet<ShardKey>>>,
    locked_shards: Arc<Mutex<HashSet<ShardKey>>>,
}

impl NodeDataManagementServerImpl {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            prepared_snapshots: Arc::new(Mutex::new(HashSet::new())),
            locked_shards: Arc::new(Mutex::new(HashSet::new())),
        }
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
        if let Some(key) = to_key(req_ref.shard_identifier.as_ref()) {
            self.prepared_snapshots.lock().unwrap().insert(key);
        } else {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
        Ok(Response::new(PrepareShardSnapshotResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn apply_shard_snapshot(
        &self,
        request: Request<ApplyShardSnapshotRequest>,
    ) -> Result<Response<ApplyShardSnapshotResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Received ApplyShardSnapshot request for shard_identifier: {:?}", req_ref.shard_identifier);
        // Optional check:
        if let Some(key) = to_key(req_ref.shard_identifier.as_ref()) {
            if !self.prepared_snapshots.lock().unwrap().contains(&key) {
                 info!("ApplyShardSnapshot called for a shard snapshot that was not prepared: {:?}", key);
                 // Depending on strictness, could return an error or just log
            }
        } else {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
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
        if req_ref.shard_identifier.is_none() {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
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
        if let Some(key) = to_key(req_ref.shard_identifier.as_ref()) {
            self.locked_shards.lock().unwrap().insert(key);
        } else {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
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
        if let Some(key) = to_key(req_ref.shard_identifier.as_ref()) {
            self.locked_shards.lock().unwrap().remove(&key);
        } else {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
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
        if let Some(key) = to_key(req_ref.shard_identifier.as_ref()) {
            self.prepared_snapshots.lock().unwrap().remove(&key);
            self.locked_shards.lock().unwrap().remove(&key);
        } else {
            return Err(Status::invalid_argument("ShardIdentifier is required"));
        }
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
    use influxdb3_id::NodeId;
    use object_store::memory::InMemory;
    use iox_time::{MockProvider, Time};
    // ShardIdentifier here is the proto message type
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

    // Creates a proto ShardIdentifier for test requests
    fn make_test_proto_shard_id(db: &str, table: &str, shard_id_val: u64) -> Option<ShardIdentifier> {
        Some(ShardIdentifier {
            db_name: db.to_string(),
            table_name: table.to_string(),
            shard_id: shard_id_val,
        })
    }

    // Creates a tuple key for direct HashSet access in tests
    fn make_test_shard_key(db: &str, table: &str, shard_id_val: u64) -> ShardKey {
        (db.to_string(), table.to_string(), shard_id_val)
    }

    #[tokio::test]
    async fn test_prepare_and_apply_snapshot_stubs() {
        let service = setup_service().await;
        let shard_id_proto = make_test_proto_shard_id("db1", "tbl1", 101);
        let shard_key_tuple = make_test_shard_key("db1", "tbl1", 101);

        // Prepare
        let request_prepare = Request::new(PrepareShardSnapshotRequest { shard_identifier: shard_id_proto.clone() });
        let response_prepare = service.prepare_shard_snapshot(request_prepare).await.unwrap();
        assert!(response_prepare.get_ref().success);
        assert!(service.prepared_snapshots.lock().unwrap().contains(&shard_key_tuple));

        // Apply (optional check: if it was prepared)
        let request_apply = Request::new(ApplyShardSnapshotRequest { shard_identifier: shard_id_proto.clone() });
        let response_apply = service.apply_shard_snapshot(request_apply).await.unwrap();
        assert!(response_apply.get_ref().success);
        // No error expected even if not "prepared" in this stub, just logs.

        // Apply for a non-prepared snapshot (should still succeed, but log if that check was added)
        let other_shard_id_proto = make_test_proto_shard_id("db1", "tbl1", 102);
        let request_apply_other = Request::new(ApplyShardSnapshotRequest { shard_identifier: other_shard_id_proto.clone() });
        let response_apply_other = service.apply_shard_snapshot(request_apply_other).await.unwrap();
        assert!(response_apply_other.get_ref().success);

    }

    #[tokio::test]
    async fn test_signal_wal_stream_processed_stub() { // Original test kept as is
        let service = setup_service().await;
        let request = Request::new(SignalWalStreamProcessedRequest { shard_identifier: make_test_proto_shard_id("db1", "tbl1", 1) });
        let response = service.signal_wal_stream_processed(request).await.unwrap();
        assert!(response.get_ref().success);
    }

    #[tokio::test]
    async fn test_lock_and_unlock_shard_writes_stubs() {
        let service = setup_service().await;
        let shard_id_proto = make_test_proto_shard_id("db2", "tbl2", 201);
        let shard_key_tuple = make_test_shard_key("db2", "tbl2", 201);

        // Lock
        let request_lock = Request::new(LockShardWritesRequest { shard_identifier: shard_id_proto.clone() });
        let response_lock = service.lock_shard_writes(request_lock).await.unwrap();
        assert!(response_lock.get_ref().success);
        assert!(service.locked_shards.lock().unwrap().contains(&shard_key_tuple));

        // Unlock
        let request_unlock = Request::new(UnlockShardWritesRequest { shard_identifier: shard_id_proto.clone() });
        let response_unlock = service.unlock_shard_writes(request_unlock).await.unwrap();
        assert!(response_unlock.get_ref().success);
        assert!(!service.locked_shards.lock().unwrap().contains(&shard_key_tuple));
    }

    #[tokio::test]
    async fn test_delete_shard_data_stub_clears_state() {
        let service = setup_service().await;
        let shard_id_proto = make_test_proto_shard_id("db3", "tbl3", 301);
        let shard_key_tuple = make_test_shard_key("db3", "tbl3", 301);

        // Set some initial state
        service.prepared_snapshots.lock().unwrap().insert(shard_key_tuple.clone());
        service.locked_shards.lock().unwrap().insert(shard_key_tuple.clone());
        assert!(service.prepared_snapshots.lock().unwrap().contains(&shard_key_tuple));
        assert!(service.locked_shards.lock().unwrap().contains(&shard_key_tuple));

        // Delete
        let request_delete = Request::new(DeleteShardDataRequest { shard_identifier: shard_id_proto.clone() });
        let response_delete = service.delete_shard_data(request_delete).await.unwrap();
        assert!(response_delete.get_ref().success);

        // Verify state is cleared
        assert!(!service.prepared_snapshots.lock().unwrap().contains(&shard_key_tuple));
        assert!(!service.locked_shards.lock().unwrap().contains(&shard_key_tuple));
    }
}
