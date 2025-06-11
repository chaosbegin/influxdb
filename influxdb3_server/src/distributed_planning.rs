use std::any::Any;
use std::sync::Arc;
use std::fmt;

use arrow_schema::{SchemaRef, ArrowError};
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        Partitioning, common,
    },
};
use futures::StreamExt;
use influxdb3_catalog::shard::ShardId; // Assuming ShardId is u64 based as in proto

#[derive(Debug)]
pub struct RemoteScanExec {
    schema: SchemaRef,
    target_node_id: String,
    db_name: String,
    table_name: String,
    shard_id: ShardId, // Using the actual ShardId type
    projection: Option<Vec<usize>>,
    filters_json: String, // Serialized filters (e.g., JSON of DataFusion Exprs)
    cache: PlanProperties,
}

impl RemoteScanExec {
    pub fn new(
        schema: SchemaRef,
        target_node_id: String,
        db_name: String,
        table_name: String,
        shard_id: ShardId,
        projection: Option<Vec<usize>>,
        filters: &[datafusion::prelude::Expr], // Take Exprs directly
    ) -> Self {
        // Serialize filters to JSON string for simplicity in this placeholder.
        // A more robust solution might use a binary format or DataFusion's own serialization.
        let filters_json = serde_json::to_string(
            &filters.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>() // Simplified serialization
        ).unwrap_or_else(|_| "[]".to_string());

        let cache = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1), // Each remote scan is one partition for now
            datafusion::physical_plan::ExecutionMode::Bounded,
        );
        Self {
            schema,
            target_node_id,
            db_name,
            table_name,
            shard_id,
            projection,
            filters_json,
            cache,
        }
    }

    pub fn target_node_id(&self) -> &str {
        &self.target_node_id
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RemoteScanExec: node={}, db={}, table={}, shard_id={}, projection={:?}, filters={}",
                    self.target_node_id, self.db_name, self.table_name, self.shard_id.get(), self.projection, self.filters_json
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str {
        "RemoteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !_children.is_empty() {
            Err(DataFusionError::Internal(
                "RemoteScanExec does not support children".to_string(),
            ))
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        observability_deps::tracing::warn!(
            "Executing placeholder RemoteScanExec for node {}, table {}.{}. Shard: {}. This should be replaced by actual RPC call.",
            self.target_node_id,
            self.db_name,
            self.table_name,
            self.shard_id.get()
        );
        // In a real implementation, this would:
        // 1. Get a gRPC client for the target_node_id.
        // 2. Construct a QueryFragmentRequest (with serialized plan/filters/projection for the specific shard).
        // 3. Make the RPC call (e.g., client.execute_query_fragment(request).await).
        // 4. Adapt the resulting stream of RecordBatches into a SendableRecordBatchStream.
        // For now, return an empty stream or an error.
        let schema = Arc::clone(&self.schema);
        // Return an empty stream with the correct schema.
        // Ok(common::empty_record_batch_stream(schema))
        // Or return an error to indicate it's not implemented.
        Err(DataFusionError::NotImplemented(format!(
            "RemoteScanExec to node {} for shard {} of table {}.{} is not yet implemented.",
            self.target_node_id, self.shard_id.get(), self.db_name, self.table_name
        )))
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
        // TODO: In the future, this could potentially fetch estimated statistics from the remote node.
        Ok(datafusion::physical_plan::Statistics::new_unknown(&self.schema()))
    }
}
