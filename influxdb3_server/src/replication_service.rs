// This module will house the gRPC server implementation for the ReplicationService.

// Types are now sourced from the dedicated protos crate
use influxdb3_replication_protos::influxdb3::internal::replication::v1::{
    replication_service_server::{ReplicationService, ReplicationServiceServer},
    ReplicateWalOpRequest, ReplicateWalOpResponse,
};
// Make server trait available at the top level of this module for convenience, if needed elsewhere.
pub use influxdb3_replication_protos::influxdb3::internal::replication::v1::replication_service_server::ReplicationServiceServer;


use tonic::{Request, Response, Status};
use std::sync::Arc;
use influxdb3_write::WriteBuffer; // Trait for write buffer operations
use influxdb3_wal::WalOp; // Actual WalOp type

// Define the server struct
#[derive(Debug)]
pub struct ReplicationServerImpl {
    write_buffer: Arc<dyn WriteBuffer>,
}

impl ReplicationServerImpl {
    pub fn new(write_buffer: Arc<dyn WriteBuffer>) -> Self {
        Self { write_buffer }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServerImpl {
    async fn replicate_wal_op(
        &self,
        request: Request<ReplicateWalOpRequest>,
    ) -> Result<Response<ReplicateWalOpResponse>, Status> {
        let req = request.into_inner();

        // 1. Deserialize wal_op_bytes into a WalOp
        let wal_op: WalOp = match bitcode::deserialize(&req.wal_op_bytes) {
            Ok(op) => op,
            Err(e) => {
                // Log the error: error!("Failed to deserialize WalOp: {}", e);
                return Ok(Response::new(ReplicateWalOpResponse {
                    success: false,
                    error_message: Some(format!("Failed to deserialize WalOp: {}", e)),
                }));
            }
        };

        // 2. Call self.write_buffer.apply_replicated_wal_op
        //    The originating_node_id is passed directly from the request.
        match self.write_buffer
            .apply_replicated_wal_op(wal_op, Some(req.originating_node_id))
            .await
        {
            Ok(_) => Ok(Response::new(ReplicateWalOpResponse {
                success: true,
                error_message: None,
            })),
            Err(e) => {
                // Log the error: error!("Failed to apply replicated WalOp: {}", e);
                Ok(Response::new(ReplicateWalOpResponse {
                    success: false,
                    error_message: Some(format!("Failed to apply replicated WalOp: {}", e)),
                }))
            }
        }
    }
}

// To integrate this into the server (conceptual, actual code depends on server setup):
//
// In server main/setup:
//   let write_buffer_arc = ...; // Get Arc<dyn WriteBuffer>
//   let replication_service = ReplicationServerImpl::new(write_buffer_arc);
//   let replication_server = ReplicationServiceServer::new(replication_service);
//
//   tonic::transport::Server::builder()
//       .add_service(replication_server)
//       // ... add other services (Flight, etc.)
//       .serve(addr)
//       .await?;

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_wal::{WalOp, WriteBatch, Field, FieldData, Row, TableChunk, TableChunks};
    use influxdb3_id::{DbId, TableId, ColumnId, SerdeVecMap};
    use influxdb3_catalog::shard::ShardId;
    use std::sync::Arc;
    use influxdb3_write::write_buffer::WriteBufferImpl; // For test setup
    use influxdb3_write::write_buffer::tests::setup_with_metrics; // Test helper
    use influxdb3_wal::WalConfig;
    use iox_time::Time;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_replicate_wal_op_server_logic() {
        // 1. Setup a WriteBufferImpl instance (can use test helpers)
        let object_store = Arc::new(InMemory::new());
        let (write_buffer, _metrics) = setup_with_metrics(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig::test_config(),
        ).await;
        let write_buffer_arc = Arc::clone(&write_buffer);

        // Ensure the target database for the WriteBatch exists in the catalog for TableBuffer access
        let db_name_str = "test_db_for_replication";
        let _ = write_buffer.catalog().create_database(db_name_str).await.unwrap();


        // 2. Create a ReplicationServerImpl
        let replication_server = ReplicationServerImpl::new(write_buffer_arc);

        // 3. Construct a WalOp and serialize it
        let mut table_chunks_map = SerdeVecMap::new();
        let mut chunk_map_inner = std::collections::HashMap::new(); // Use std HashMap here as SerdeVecMap is for top-level
        chunk_map_inner.insert(0i64, TableChunk { rows: vec![
            Row { time: 12345, fields: vec![Field {id: ColumnId::new(1), value: FieldData::Integer(100)}]}
        ]});
        table_chunks_map.insert(TableId::new(1), TableChunks {
            min_time: 12345, max_time: 12345, chunk_time_to_chunk: chunk_map_inner
        });

        let wal_op_to_replicate = WalOp::Write(WriteBatch {
            catalog_sequence: 123,
            database_id: write_buffer.catalog().db_name_to_id(db_name_str).unwrap(), // Use actual DbId
            database_name: Arc::from(db_name_str),
            table_chunks: table_chunks_map,
            min_time_ns: 12345,
            max_time_ns: 12345,
            shard_id: Some(ShardId::new(7)),
        });
        let serialized_wal_op = bitcode::serialize(&wal_op_to_replicate).expect("Failed to serialize WalOp");

        // 4. Construct a ReplicateWalOpRequest
        let request_msg = ReplicateWalOpRequest {
            wal_op_bytes: serialized_wal_op,
            originating_node_id: "origin_node_test".to_string(),
            shard_id: Some(7),
            database_name: db_name_str.to_string(),
            table_name: "test_table_for_replication".to_string(), // Table name for context
        };
        let tonic_request = Request::new(request_msg);

        // 5. Call the server's replicate_wal_op method
        let response = replication_server.replicate_wal_op(tonic_request).await;

        // 6. Assert success
        assert!(response.is_ok(), "replicate_wal_op returned an error: {:?}", response.err());
        let inner_response = response.unwrap().into_inner();
        assert!(inner_response.success, "Replication reported as not successful: {:?}", inner_response.error_message);
        assert!(inner_response.error_message.is_none());

        // Further verification: Check if the data was actually written to the WAL/buffer.
        // This would involve flushing WAL and querying, or inspecting WAL directly.
        // For this test, we primarily check the RPC handler logic (deserialization, call to apply_replicated_wal_op).
        // The `test_apply_replicated_wal_op` in write_buffer tests the next step.
    }

    #[tokio::test]
    async fn test_replicate_wal_op_server_deserialization_error() {
        let (_object_store, write_buffer, _metrics) = crate::tests::TestServer::new().await; // Use existing test setup
        let replication_server = ReplicationServerImpl::new(write_buffer);

        let request_msg = ReplicateWalOpRequest {
            wal_op_bytes: vec![1, 2, 3, 4, 5], // Invalid bitcode
            originating_node_id: "origin_node_test_deser_error".to_string(),
            shard_id: Some(8),
            database_name: "test_db_deser_error".to_string(),
            table_name: "test_table_deser_error".to_string(),
        };
        let tonic_request = Request::new(request_msg);

        let response = replication_server.replicate_wal_op(tonic_request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_message.is_some());
        assert!(response.error_message.unwrap().contains("Failed to deserialize WalOp"));
    }

    // Mock WriteBuffer for apply_replicated_wal_op error testing
    #[derive(Debug)]
    struct MockWriteBufferError {}

    #[async_trait::async_trait]
    impl influxdb3_write::Bufferer for MockWriteBufferError {
        async fn write_lp(
            &self, _database: data_types::NamespaceName<'static>, _lp: &str, _ingest_time: iox_time::Time,
            _accept_partial: bool, _precision: influxdb3_write::Precision, _no_sync: bool,
        ) -> influxdb3_write::write_buffer::Result<influxdb3_write::BufferedWriteRequest> {
            unimplemented!()
        }
        fn catalog(&self) -> Arc<influxdb3_catalog::catalog::Catalog> { unimplemented!() }
        fn wal(&self) -> Arc<dyn influxdb3_wal::Wal> { unimplemented!() }
        fn parquet_files_filtered(&self, _db_id: influxdb3_id::DbId, _table_id: influxdb3_id::TableId, _filter: &influxdb3_write::ChunkFilter<'_>) -> Vec<influxdb3_write::ParquetFile> { unimplemented!() }
        fn watch_persisted_snapshots(&self) -> tokio::sync::watch::Receiver<Option<influxdb3_write::PersistedSnapshotVersion>> { unimplemented!() }
        async fn apply_replicated_wal_op(&self, _op: WalOp, _originating_node_id: Option<String>) -> Result<(), influxdb3_write::write_buffer::Error> {
            Err(influxdb3_write::write_buffer::Error::AnyhowError(anyhow::anyhow!("Mock apply error")))
        }
    }
    impl influxdb3_write::ChunkContainer for MockWriteBufferError {
        fn get_table_chunks(&self, _db_schema: Arc<influxdb3_catalog::catalog::DatabaseSchema>, _table_def: Arc<influxdb3_catalog::catalog::TableDefinition>, _filter: &influxdb3_write::ChunkFilter<'_>, _projection: Option<&Vec<usize>>, _ctx: &dyn datafusion::catalog::Session) -> influxdb3_write::Result<Vec<Arc<dyn iox_query::QueryChunk>>, datafusion::error::DataFusionError> { unimplemented!() }
    }
    impl influxdb3_write::LastCacheManager for MockWriteBufferError {
        fn last_cache_provider(&self) -> Arc<influxdb3_cache::last_cache::LastCacheProvider> { unimplemented!() }
    }
    impl influxdb3_write::DistinctCacheManager for MockWriteBufferError {
        fn distinct_cache_provider(&self) -> Arc<influxdb3_cache::distinct_cache::DistinctCacheProvider> { unimplemented!() }
    }
    impl influxdb3_write::WriteBuffer for MockWriteBufferError {}


    #[tokio::test]
    async fn test_replicate_wal_op_server_apply_error() {
        let mock_write_buffer = Arc::new(MockWriteBufferError {});
        let replication_server = ReplicationServerImpl::new(mock_write_buffer);

        let wal_op_to_replicate = WalOp::Noop(influxdb3_wal::NoopDetails::new_for_test(1)); // Simple WalOp
        let serialized_wal_op = bitcode::serialize(&wal_op_to_replicate).unwrap();

        let request_msg = ReplicateWalOpRequest {
            wal_op_bytes: serialized_wal_op,
            originating_node_id: "origin_node_test_apply_error".to_string(),
            shard_id: None,
            database_name: "test_db_apply_error".to_string(),
            table_name: "test_table_apply_error".to_string(),
        };
        let tonic_request = Request::new(request_msg);

        let response = replication_server.replicate_wal_op(tonic_request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_message.is_some());
        assert!(response.error_message.unwrap().contains("Failed to apply replicated WalOp"));
    }
}
