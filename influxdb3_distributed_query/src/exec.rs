use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::error::DataFusionError; // Explicit import for mapping errors
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
};
use futures::stream::{self, StreamExt}; // For creating a SendableRecordBatchStream from empty
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use influxdb3_id::NodeId;
use influxdb3_proto::influxdb3::internal::distributed_query::v1::{
    distributed_query_service_client::DistributedQueryServiceClient, // For conceptual client
    ExecuteQueryFragmentRequest, ExecuteQueryFragmentResponse,
};
use arrow::ipc::reader::StreamReader; // For deserializing RecordBatch
use std::io::Cursor; // To read bytes as a stream
use bytes::Bytes; // For request bytes
use datafusion::logical_expr::LogicalPlan; // For helper function signatures

// Conceptual representation of what might be sent to a remote node for execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum QueryFragment {
    /// A serialized representation of a query plan segment.
    /// This could be, for example, a Substrait plan or a DataFusion LogicalPlan/PhysicalPlan
    /// serialized into bytes using a format like prost or serde_json.
    SerializedLogicalPlan(Vec<u8>), // Renamed for clarity

    // RawSql(String), // Commented out to focus on SerializedLogicalPlan for now
    // Potentially other variants like a specific list of filters or projections if the remote
    // node has a more structured API than just executing opaque plans/SQL.
}

// --- Serialization Helpers ---

/// Serializes a DataFusion LogicalPlan into a byte vector using JSON.
pub fn serialize_logical_plan(plan: &LogicalPlan) -> DFResult<Vec<u8>> {
    serde_json::to_vec(plan).map_err(|e| DataFusionError::Execution(format!("Failed to serialize LogicalPlan: {}", e)))
}

/// Deserializes a DataFusion LogicalPlan from a byte slice (JSON).
pub fn deserialize_logical_plan(bytes: &[u8]) -> DFResult<LogicalPlan> {
    serde_json::from_slice(bytes).map_err(|e| DataFusionError::Execution(format!("Failed to deserialize LogicalPlan: {}", e)))
}

// --- Execution Plan ---

/// `RemoteScanExec` is a conceptual physical operator responsible for executing a
/// query fragment on a remote InfluxDB 3 node and streaming the results back.
#[derive(Debug)]
pub struct RemoteScanExec {
    db_name: String, // Added database name
    target_node_id: NodeId,
    target_node_address: String,
    query_fragment: Arc<QueryFragment>,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl RemoteScanExec {
    /// Creates a new `RemoteScanExec` operator.
    pub fn new(
        db_name: String, // Added
        target_node_id: NodeId,
        target_node_address: String,
        query_fragment: QueryFragment,
        projected_schema: SchemaRef,
    ) -> Self {
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db_name, // Added
            target_node_id,
            target_node_address,
            query_fragment: Arc::new(query_fragment),
            projected_schema,
            cache,
        }
    }

    /// Computes the properties of this execution plan.
    /// This includes schema, partitioning, and execution mode.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema),
            // For a remote scan, each request to a remote node typically results in a single
            // stream of data, effectively one partition from the perspective of the calling node.
            // If the remote node itself performs further partitioning and this node could connect
            // to those, this might change. For now, UnknownPartitioning(1) is a safe default.
            Partitioning::UnknownPartitioning(1),
            // Assumes the remote execution is bounded (i.e., it will complete).
            // If it were a continuous streaming source, Unbounded might be appropriate.
            datafusion::physical_plan::ExecutionMode::Bounded,
        )
    }

    /// Returns the schema of the data produced by this operator.
    pub fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Returns the query fragment that will be sent to the remote node.
    pub fn query_fragment(&self) -> Arc<QueryFragment> {
        Arc::clone(&self.query_fragment)
    }

    /// Returns the ID of the target node.
    pub fn target_node_id(&self) -> NodeId {
        self.target_node_id
    }

    /// Returns the network address of the target node.
    pub fn target_node_address(&self) -> &str {
        &self.target_node_address
    }

    /// Returns the database name for the query.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }
}

#[async_trait]
impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str {
        "RemoteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // A scan operator is a leaf node in the execution plan, so it has no children.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Since this is a leaf node, it cannot have new children.
        // Returning self is appropriate.
        Ok(self)
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        tracing::debug!(
            "RemoteScanExec: Attempting to execute query fragment on remote node: {} for db: {}",
            self.target_node_address,
            self.db_name
        );

        // Ensure the partition index is valid (should be 0 for UnknownPartitioning(1))
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RemoteScanExec expected partition 0, got {}",
                partition
            )));
        }

        let formatted_address = if !self.target_node_address.starts_with("http://") && !self.target_node_address.starts_with("https://") {
            format!("http://{}", self.target_node_address)
        } else {
            self.target_node_address.clone()
        };

        let output_schema = self.projected_schema.clone();
        let db_name_cloned = self.db_name.clone();
        let query_fragment_cloned = self.query_fragment.clone(); // Arc clone
        let target_node_address_cloned = self.target_node_address.clone(); // For error messages and logging

        let response_stream = async_stream::try_stream! {
            // 1. Create gRPC Client
            let mut client = match influxdb3_proto::influxdb3::internal::distributed_query::v1::distributed_query_service_client::DistributedQueryServiceClient::connect(formatted_address).await {
                Ok(c) => c,
                Err(e) => {
                    let err_msg = format!("Failed to connect to remote query service at {}: {}", target_node_address_cloned, e);
                    tracing::error!("{}", err_msg);
                    Err(DataFusionError::Execution(err_msg))?;
                    unreachable!(); // Should be handled by `?`
                }
            };

            // 2. Prepare ExecuteQueryFragmentRequest
            let query_fragment_bytes = match &*query_fragment_cloned { // Deref Arc
                QueryFragment::SerializedLogicalPlan(plan_bytes) => plan_bytes.clone(), // plan_bytes is Vec<u8>
                // QueryFragment::RawSql(_) => { // Example if RawSql was still used
                //     let err_msg = "RawSql query fragment type is not supported in this path.".to_string();
                //     tracing::error!("{}", err_msg);
                //     Err(DataFusionError::NotImplemented(err_msg))?;
                //     unreachable!();
                // }
            };

            let request = tonic::Request::new(
                influxdb3_proto::influxdb3::internal::distributed_query::v1::ExecuteQueryFragmentRequest {
                    db_name: db_name_cloned.clone(),
                    query_fragment: query_fragment_bytes, // Already Vec<u8>
                    fragment_type: "SerializedDataFusionLogicalPlan".to_string(),
                },
            );

            // 3. Execute gRPC Call
            let mut inner_stream = match client.execute_query_fragment(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let err_msg = format!("Remote query fragment execution failed on {}: {}", target_node_address_cloned, status);
                    tracing::error!("{}", err_msg);
                    Err(DataFusionError::Execution(err_msg))?;
                    unreachable!();
                }
            };

            // 4. Adapt Stream of Responses
            fn deserialize_ipc_bytes_to_batch(bytes: bytes::Bytes, schema: SchemaRef) -> DFResult<arrow::record_batch::RecordBatch> {
                use arrow::ipc::reader::StreamReader;
                use std::io::Cursor;

                if bytes.is_empty() {
                    return Ok(arrow::record_batch::RecordBatch::new_empty(schema));
                }

                let mut cursor = Cursor::new(bytes);
                let mut reader = StreamReader::try_new(&mut cursor, None)
                    .map_err(|e| DataFusionError::ArrowError(e, Some(format!("IPC Read Error creating reader for schema {:?}", schema))))?;

                if let Some(batch_result) = reader.next() {
                    batch_result.map_err(|e| DataFusionError::ArrowError(e, None))
                } else {
                    Ok(arrow::record_batch::RecordBatch::new_empty(schema))
                }
            }

            while let Some(response_item_result) = inner_stream.next().await {
                match response_item_result {
                    Ok(response_item) => {
                        tracing::debug!("RemoteScanExec: Received batch of size {} bytes from {}", response_item.record_batch_bytes.len(), target_node_address_cloned);
                        let batch = deserialize_ipc_bytes_to_batch(response_item.record_batch_bytes, output_schema.clone())?;
                        yield batch;
                    }
                    Err(status) => {
                        tracing::error!("Error in remote query fragment stream from {}: {}", target_node_address_cloned, status);
                        Err(DataFusionError::Execution(format!("Stream error from remote node {}: {}", target_node_address_cloned, status)))?;
                    }
                }
            }
        };

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                output_schema,
                response_stream,
            ),
        ))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        // Statistics for a remote scan are generally unknown without fetching them
        // from the remote node or making estimations.
        // Returning unknown statistics is a safe default.
        Ok(Statistics::new_unknown(&self.projected_schema))
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let projection_formatted = self.projected_schema.fields().iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<&str>>()
                    .join(", ");

                write!(
                    f,
                    "RemoteScanExec: db_name={}, target_node_id={}, target_node_address={}, fragment_type={}, projection=[{}]",
                    self.db_name,
                    self.target_node_id.get(),
                    self.target_node_address,
                    match *self.query_fragment {
                        QueryFragment::SerializedPlan(_) => "SerializedPlan",
                        QueryFragment::RawSql(_) => "RawSql",
                    },
                    projection_formatted
                )
            }
        }
    }
}

// Basic empty stream for testing, implementing RecordBatchStream
struct EmptyRecordBatchStream {
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    #[allow(dead_code)] // May be useful for other tests later
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl futures::Stream for EmptyRecordBatchStream {
    type Item = DFResult<arrow::record_batch::RecordBatch>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(None) // No data, stream ends immediately
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema}; // Aliased to avoid clash with struct Schema from catalog
    use std::sync::Arc;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{col, sum, LogicalPlanBuilder, table_scan}; // For building test LogicalPlan
    use datafusion::prelude::SessionConfig;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_remote_scan_exec_new() {
        let schema = create_test_schema();
        let db_name = "test_db".to_string();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment.clone(),
            schema.clone(),
        );

        assert_eq!(exec_plan.db_name(), db_name);
        assert_eq!(exec_plan.target_node_id(), node_id);
        assert_eq!(exec_plan.target_node_address(), address);
        match &*exec_plan.query_fragment() { // Dereference Arc
            QueryFragment::RawSql(s) => assert_eq!(s, "SELECT * FROM test_table"),
            _ => panic!("Unexpected query fragment type"),
        }
        assert_eq!(exec_plan.schema(), schema);
        assert_eq!(exec_plan.name(), "RemoteScanExec");
        assert!(exec_plan.children().is_empty());
        assert_eq!(exec_plan.properties().output_partitioning().partition_count(), 1);
    }

    #[tokio::test]
    async fn test_remote_scan_exec_execute() {
        let schema = create_test_schema();
        let db_name = "test_db".to_string();
        let node_id = NodeId::new(1);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Test case 1: Invalid target_node_address (connection error)
        let invalid_address = "invalid-uri-that-will-not-connect"; // Tonic connect expects "http://" or "https://"
        let fragment_invalid_conn = QueryFragment::SerializedLogicalPlan(vec![1, 2, 3]); // Content doesn't matter for conn test

        let exec_plan_invalid_conn = Arc::new(RemoteScanExec::new(
            db_name.clone(),
            node_id,
            invalid_address.to_string(),
            fragment_invalid_conn,
            schema.clone(),
        ));

        let stream_result_invalid_conn = exec_plan_invalid_conn.execute(0, task_ctx.clone()).await;
        assert!(stream_result_invalid_conn.is_err(), "Expected error for invalid connection");
        match stream_result_invalid_conn.err().unwrap() {
            DataFusionError::Execution(msg) => {
                assert!(msg.contains("Failed to connect to remote query service"), "Error message mismatch: {}", msg);
            }
            other_err => panic!("Expected DataFusionError::Execution, got {:?}", other_err),
        }

        // Test case 2: Valid address but conceptual server (will likely try to connect and timeout or fail if not running)
        // This part is harder to test reliably without a mock server or network access control.
        // The current implementation of execute() will return an empty stream if the server is not found
        // after connection or if the stream is empty.
        // For now, we'll test the path that attempts connection and processes an empty stream (conceptual).
        // A real test would use a mock gRPC server.
        let valid_but_mock_address = "http://localhost:55555"; // Assume nothing running here for test
        let fragment_mock_server = QueryFragment::SerializedLogicalPlan(vec![1, 2, 3]);
        let exec_plan_mock_server = Arc::new(RemoteScanExec::new(
            db_name.clone(),
            node_id,
            valid_but_mock_address.to_string(),
            fragment_mock_server,
            schema.clone(),
        ));

        // This will try to connect. If it fails, it will return DataFusionError::Execution.
        // If it somehow succeeded (e.g. something IS listening), it would then try to process a stream.
        let stream_result_mock_server = exec_plan_mock_server.execute(0, task_ctx.clone()).await;
        // We expect this to fail connection, similar to invalid_address if server not up.
        assert!(stream_result_mock_server.is_err(), "Expected error for connection to non-existent server");
         match stream_result_mock_server.err().unwrap() {
            DataFusionError::Execution(msg) => {
                assert!(msg.contains("Failed to connect to remote query service") || msg.contains("transport error"));
            }
            other_err => panic!("Expected DataFusionError::Execution for mock server, got {:?}", other_err),
        }


        // Test case 3: Execution for an invalid partition index
        let exec_plan_for_partition_test = Arc::new(RemoteScanExec::new(
            db_name.clone(),
            node_id,
            valid_but_mock_address.to_string(), // Address doesn't matter as it errors before connection
            QueryFragment::SerializedLogicalPlan(vec![]),
            schema.clone(),
        ));
        let stream_result_invalid_partition = exec_plan_for_partition_test.execute(1, task_ctx).await;
        assert!(stream_result_invalid_partition.is_err());
        match stream_result_invalid_partition.err().unwrap() {
            DataFusionError::Internal(msg) => {
                assert!(msg.contains("RemoteScanExec expected partition 0, got 1"));
            }
            other_err => panic!("Expected internal error for invalid partition, got {:?}", other_err),
        }
    }

    #[test]
    fn test_remote_scan_exec_display_as() {
        let schema = create_test_schema();
        let db_name = "test_db_display".to_string();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment_sql = QueryFragment::RawSql("SELECT id, value FROM test_table".to_string());

        let exec_plan_sql = RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment_sql.clone(),
            schema.clone(),
        );

        let display_sql = format!("{}", exec_plan_sql.displayable());
        assert_eq!(
            display_sql,
            "RemoteScanExec: db_name=test_db_display, target_node_id=1, target_node_address=node1.example.com:8080, fragment_type=RawSql, projection=[id, value]"
        );

        let fragment_plan = QueryFragment::SerializedPlan(vec![0xDE, 0xAD, 0xBE, 0xEF]);
         let exec_plan_plan = RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment_plan.clone(),
            schema.clone(),
        );
        let display_plan = format!("{}", exec_plan_plan.displayable());
         assert_eq!(
            display_plan,
            "RemoteScanExec: db_name=test_db_display, target_node_id=1, target_node_address=node1.example.com:8080, fragment_type=SerializedPlan, projection=[id, value]"
        );
    }

     #[test]
    fn test_with_new_children() {
        let schema = create_test_schema();
        let db_name = "test_db_children".to_string();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = Arc::new(RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment.clone(),
            schema.clone(),
        ));

        // RemoteScanExec is a leaf, so it should return itself even if children are (incorrectly) provided.
        let result_with_children = exec_plan.clone().with_new_children(vec![]);
        assert!(result_with_children.is_ok());
        assert_eq!(Arc::as_ptr(&(result_with_children.unwrap())), Arc::as_ptr(&exec_plan));

        // Example of trying to provide children (though ExecutionPlan expects Arc<dyn ExecutionPlan>)
        // let dummy_child_schema = Arc::new(Schema::new(vec![Field::new("child_col", DataType::Int32, false)]));
        // let dummy_child_plan = Arc::new(RemoteScanExec::new(NodeId::new(2), "child:8080".to_string(), fragment, dummy_child_schema));
        // let result_with_actual_children = exec_plan.clone().with_new_children(vec![dummy_child_plan as Arc<dyn ExecutionPlan>]);
        // assert!(result_with_actual_children.is_ok());
        // This would still result in the original plan being returned because RemoteScanExec doesn't use children.
    }

    #[tokio::test]
    async fn test_statistics() {
        let schema = create_test_schema();
        let db_name = "test_db_stats".to_string();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment.clone(),
            schema.clone(),
        );

        let stats = exec_plan.statistics().unwrap();
        assert!(stats.is_unknown); // Expect statistics to be unknown
        assert!(stats.num_rows.is_none());
        assert!(stats.total_byte_size.is_none());
        assert!(stats.column_statistics.is_none());
    }

    #[test]
    fn test_logical_plan_serialization_deserialization() {
        // 1. Create a simple LogicalPlan
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Int32, false),
        ]));
        let plan = LogicalPlanBuilder::scan("test_table", schema, None).unwrap()
            .project(vec![col("c1"), col("c2")]).unwrap()
            // .filter(col("c2").gt(lit(10))).unwrap() // Example filter
            .build().unwrap();

        // 2. Serialize it
        let serialized_bytes = serialize_logical_plan(&plan).expect("Failed to serialize plan");
        assert!(!serialized_bytes.is_empty());

        // 3. Deserialize it
        let deserialized_plan = deserialize_logical_plan(&serialized_bytes).expect("Failed to deserialize plan");

        // 4. Assert equality (DataFusion's LogicalPlan implements PartialEq)
        // Note: Direct equality might fail due to subtle differences if not careful with types or context.
        // For robust comparison, often string representations or specific properties are checked.
        // However, for serde-derived equality, it should work if all components are serde-aware.
        assert_eq!(plan, deserialized_plan, "Original and deserialized plans do not match.");

        // Verify some properties to be sure
        match deserialized_plan {
            LogicalPlan::Projection(p) => {
                assert_eq!(p.expr.len(), 2);
                match p.input.as_ref() {
                    LogicalPlan::TableScan(ts) => {
                        assert_eq!(ts.table_name.table(), "test_table");
                    }
                    _ => panic!("Expected TableScan as input to projection"),
                }
            }
            _ => panic!("Expected Projection plan"),
        }
    }
}
