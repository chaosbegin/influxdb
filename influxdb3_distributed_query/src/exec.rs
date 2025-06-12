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
            "Attempting to execute RemoteScanExec: target_node_id={}, target_node_address={}, fragment={:?}, partition={}",
            self.target_node_id.get(), // Use .get() for NodeId's u64 value
            self.target_node_address,
            self.query_fragment,
            partition
        );

        // Ensure the partition index is valid (should be 0 for UnknownPartitioning(1))
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RemoteScanExec expected partition 0, got {}",
                partition
            )));
        }

        // ** CONCEPTUAL GRPC CLIENT CALL **
        tracing::info!("RemoteScanExec: Conceptually connecting to {} for node {}", self.target_node_address, self.target_node_id.get());

        // 1. Conceptual client creation
        // let mut client = DistributedQueryServiceClient::connect(self.target_node_address.clone()) // Requires "http://" prefix
        //     .await
        //     .map_err(|e| DataFusionError::Execution(format!("Failed to connect to remote node {}: {}", self.target_node_address, e)))?;

        // 2. Prepare request
        let (fragment_bytes, fragment_type_str) = match self.query_fragment.as_ref() {
            QueryFragment::RawSql(sql) => (Bytes::from(sql.clone()), "RawSql".to_string()),
            QueryFragment::SerializedPlan(plan_bytes) => (Bytes::from(plan_bytes.clone()), "SerializedDataFusionLogicalPlan".to_string()),
        };

        let _request_payload = ExecuteQueryFragmentRequest {
            db_name: self.db_name.clone(),
            query_fragment: fragment_bytes,
            fragment_type: fragment_type_str,
        };

        // let request = tonic::Request::new(request_payload);

        // 3. Call remote service (conceptual)
        // let grpc_response_stream = client.execute_query_fragment(request)
        //     .await
        //     .map_err(|e| DataFusionError::Execution(format!("Remote query execution failed: {}", e)))?
        //     .into_inner();

        // 4. Adapt gRPC stream to SendableRecordBatchStream (conceptual)
        // let output_stream = grpc_response_stream.map_err(|e: tonic::Status| DataFusionError::Execution(format!("gRPC stream error: {}", e)))
        //     .and_then(|res: ExecuteQueryFragmentResponse| async move {
        //         if let Some(err_msg) = res.error_message {
        //              return Err(DataFusionError::Execution(format!("Remote batch error: {}", err_msg)));
        //         }
        //         // Deserialize RecordBatch from res.record_batch_bytes (Arrow IPC format)
        //         let cursor = Cursor::new(res.record_batch_bytes);
        //         let mut reader = StreamReader::try_new(cursor, None) // Assuming stream format without schema, or schema known
        //             .map_err(|e| DataFusionError::Execution(format!("Failed to create IPC StreamReader: {}", e)))?;
        //         if let Some(batch) = reader.next() {
        //             batch.map_err(|e| DataFusionError::Execution(format!("Failed to read batch from IPC: {}", e)))
        //         } else {
        //             Err(DataFusionError::Execution("Received empty or invalid RecordBatch IPC message".to_string()))
        //         }
        //     });

        // For this conceptual implementation, return an empty stream with the correct schema.
        let schema = self.projected_schema.clone();
        let stream_adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(), // This produces BoxStream<'static, DFResult<RecordBatch>>
        );
        Ok(Box::pin(stream_adapter))
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
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::SerializedPlan(vec![1, 2, 3]);

        let exec_plan = Arc::new(RemoteScanExec::new(
            db_name.clone(),
            node_id,
            address.clone(),
            fragment.clone(),
            schema.clone(),
        ));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Test execution for partition 0
        let stream_result = exec_plan.execute(0, task_ctx.clone()).await;
        assert!(stream_result.is_ok());
        let mut stream = stream_result.unwrap();
        assert_eq!(stream.schema(), schema);
        assert!(stream.next().await.is_none()); // Expecting an empty stream

        // Test execution for an invalid partition
        let stream_result_invalid_partition = exec_plan.execute(1, task_ctx).await;
        assert!(stream_result_invalid_partition.is_err());
        if let Err(DataFusionError::Internal(msg)) = stream_result_invalid_partition {
            assert!(msg.contains("RemoteScanExec expected partition 0, got 1"));
        } else {
            panic!("Expected internal error for invalid partition");
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
