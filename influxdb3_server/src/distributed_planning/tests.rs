#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use influxdb3_catalog::shard::ShardId;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;

    fn create_test_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            // Add more fields if your test batches use them
        ]))
    }

    #[tokio::test]
    async fn test_remote_scan_execute_success() {
        let schema = create_test_schema();
        let exec_node = RemoteScanExec::new(
            Arc::clone(&schema),
            "remote_node_1".to_string(),
            "test_db".to_string(),
            "test_success_table".to_string(), // Triggers success path in execute
            ShardId::new(1),
            None,
            &[], // No filters for this basic test
        );

        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));

        let stream = exec_node.execute(0, task_ctx).unwrap();
        let batches = collect(stream).await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id_col.values(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn test_remote_scan_execute_service_error() {
        let schema = create_test_schema();
        let exec_node = RemoteScanExec::new(
            Arc::clone(&schema),
            "remote_node_2".to_string(),
            "test_db".to_string(),
            "test_error_service_stream".to_string(), // Triggers service error path
            ShardId::new(2),
            None,
            &[],
        );

        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));

        let stream = exec_node.execute(0, task_ctx).unwrap();
        let result = collect(stream).await;

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Simulated gRPC service error"));
        }
    }

    #[tokio::test]
    async fn test_remote_scan_execute_response_error_message() {
        let schema = create_test_schema();
        let exec_node = RemoteScanExec::new(
            Arc::clone(&schema),
            "remote_node_3".to_string(),
            "test_db".to_string(),
            "test_error_response_message".to_string(), // Triggers error message in response path
            ShardId::new(3),
            None,
            &[],
        );

        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));

        let stream = exec_node.execute(0, task_ctx).unwrap();
        let result = collect(stream).await;

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Error message in ExecuteQueryFragmentResponse"));
        }
    }

    // Test for plan serialization failure would be complex here as it depends on what makes a LogicalPlan fail serde_json.
    // Test for IPC deserialization failure would also require more setup to inject bad bytes into the stream,
    // which is not straightforward with the current test simulation in execute().
}
