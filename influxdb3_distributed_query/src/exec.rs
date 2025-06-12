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
// use tokio::sync::Mutex; // Not needed for the conceptual client placeholder

use influxdb3_id::NodeId; // Using NodeId from influxdb3_id

// Conceptual representation of what might be sent to a remote node for execution.
#[derive(Debug, Clone)]
pub enum QueryFragment {
    /// A serialized representation of a query plan segment.
    /// This could be, for example, a Substrait plan or a DataFusion LogicalPlan/PhysicalPlan
    /// serialized into bytes using a format like prost or serde_json.
    SerializedPlan(Vec<u8>),

    /// A raw SQL query string specific to the data held by the target shard/node.
    RawSql(String),
    // Potentially other variants like a specific list of filters or projections if the remote
    // node has a more structured API than just executing opaque plans/SQL.
}

/// `RemoteScanExec` is a conceptual physical operator responsible for executing a
/// query fragment on a remote InfluxDB 3 node and streaming the results back.
#[derive(Debug)]
pub struct RemoteScanExec {
    target_node_id: NodeId, // Using the strong type NodeId
    target_node_address: String, // RPC address (e.g., "host:port")
    query_fragment: Arc<QueryFragment>, // Using Arc to allow cloning without deep copying QueryFragment
    projected_schema: SchemaRef, // The schema of the data this operator will produce
    cache: PlanProperties, // Cache for PlanProperties
}

impl RemoteScanExec {
    /// Creates a new `RemoteScanExec` operator.
    pub fn new(
        target_node_id: NodeId,
        target_node_address: String,
        query_fragment: QueryFragment,
        projected_schema: SchemaRef,
    ) -> Self {
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
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
        // The following section is a placeholder for the actual gRPC client implementation.
        // In a real scenario:
        // 1. A gRPC client for a `DistributedQueryService` would be instantiated,
        //    connecting to `self.target_node_address`. This client might be managed
        //    by the TaskContext or a query session state.
        //    Example:
        //    `let mut client = context.get_distributed_query_client(&self.target_node_address).await.map_err(|e| DataFusionError::Execution(format!("Failed to connect: {}", e)))?;`
        //
        // 2. A request message (e.g., `ExecuteFragmentRequest` from a .proto definition)
        //    would be constructed using `self.query_fragment` and other necessary context
        //    (like trace IDs, session parameters from `_context`).
        //    Example:
        //    `let proto_fragment = self.query_fragment.to_proto_enum(); // Assuming conversion to proto type`
        //    `let request = tonic::Request::new(ExecuteFragmentRequest { query_fragment: Some(proto_fragment), ... });`
        //
        // 3. The client's RPC method would be called:
        //    `let response_stream = client.execute_fragment(request).await.map_err(|e| DataFusionError::Execution(format!("Remote execution failed: {}", e)))?;`
        //
        // 4. The `response_stream` (a `tonic::Streaming<ExecuteFragmentResponse>`) would then be
        //    adapted into a `SendableRecordBatchStream`. Each `ExecuteFragmentResponse` would
        //    likely contain a serialized `RecordBatch` that needs deserialization.
        //    This adaptation requires careful handling of Arrow data over gRPC, often using
        //    Arrow Flight or a custom serialization format for RecordBatches.

        // For this conceptual implementation, we return an empty stream with the correct schema.
        // This allows the operator to be integrated into a plan and tested without
        // requiring a live gRPC service or client.
        let schema = self.projected_schema.clone();
        let empty_stream = futures::stream::empty();

        // Wrap the empty stream with RecordBatchStreamAdapter
        // This adapter requires the schema and the stream of RecordBatches.
        // Since our stream is `futures::stream::Empty<DFResult<RecordBatch>>`,
        // we need to ensure the types match. `Empty` produces `()`, not `DFResult<RecordBatch>`.
        // Correct way for an empty stream of RecordBatches:
        let stream_of_results: BoxStream<'static, DFResult<arrow::record_batch::RecordBatch>> = Box::pin(stream::empty());

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                stream_of_results,
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
                    "RemoteScanExec: target_node_id={}, target_node_address={}, fragment_type={}, projection=[{}]",
                    self.target_node_id.get(), // Use .get() for NodeId's u64 value
                    self.target_node_address,
                    match *self.query_fragment { // Dereference Arc then match
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
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use datafusion::execution::context::SessionContext; // For creating TaskContext

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_remote_scan_exec_new() {
        let schema = create_test_schema();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = RemoteScanExec::new(
            node_id,
            address.clone(),
            fragment.clone(),
            schema.clone(),
        );

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
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::SerializedPlan(vec![1, 2, 3]);

        let exec_plan = Arc::new(RemoteScanExec::new(
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
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment_sql = QueryFragment::RawSql("SELECT id, value FROM test_table".to_string());

        let exec_plan_sql = RemoteScanExec::new(
            node_id,
            address.clone(),
            fragment_sql.clone(),
            schema.clone(),
        );

        let display_sql = format!("{}", exec_plan_sql.displayable());
        assert_eq!(
            display_sql,
            "RemoteScanExec: target_node_id=1, target_node_address=node1.example.com:8080, fragment_type=RawSql, projection=[id, value]"
        );

        let fragment_plan = QueryFragment::SerializedPlan(vec![0xDE, 0xAD, 0xBE, 0xEF]);
         let exec_plan_plan = RemoteScanExec::new(
            node_id,
            address.clone(),
            fragment_plan.clone(),
            schema.clone(),
        );
        let display_plan = format!("{}", exec_plan_plan.displayable());
         assert_eq!(
            display_plan,
            "RemoteScanExec: target_node_id=1, target_node_address=node1.example.com:8080, fragment_type=SerializedPlan, projection=[id, value]"
        );
    }

     #[test]
    fn test_with_new_children() {
        let schema = create_test_schema();
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = Arc::new(RemoteScanExec::new(
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
        let node_id = NodeId::new(1);
        let address = "node1.example.com:8080".to_string();
        let fragment = QueryFragment::RawSql("SELECT * FROM test_table".to_string());

        let exec_plan = RemoteScanExec::new(
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
}
