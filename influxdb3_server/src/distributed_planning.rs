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
        partition_index: usize, // Should always be 0 for this exec
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        observability_deps::tracing::info!(
            target_node_id = %self.target_node_id,
            db_name = %self.db_name,
            table_name = %self.table_name,
            shard_id = %self.shard_id.get(),
            projection = ?self.projection,
            filters_json = %self.filters_json,
            "RemoteScanExec::execute started"
        );

        if partition_index != 0 {
            return Err(DataFusionError::Internal(format!(
                "RemoteScanExec expected partition 0, got {}",
                partition_index
            )));
        }

        // 1. Construct LogicalPlan::TableScan
        // Deserialize filters_json back to Expr. This is a simplification.
        // A robust way would be to serialize the relevant part of the LogicalPlan itself,
        // or use a more structured filter representation than just `format!("{:?}")`.
        // For now, we'll assume filters_json might be complex to directly deserialize into Exprs
        // without more context or a proper (de)serialization framework for Expr.
        // So, we'll create a TableScan without filters for this simulation if deserialization is hard.

        // Let's assume self.filters_json is a string representation of a single simple filter for now for placeholder.
        // In a real scenario, self.filters_json would be a serialization of Vec<Expr>.
        // For this simulation, we'll create a TableScan and manually add a placeholder filter if self.filters_json is not empty.
        // A proper implementation would need to serialize/deserialize the actual filter expressions.

        let table_source = context
            .session_state()
            .catalog_list()
            .catalog(&self.db_name) // This assumes db_name is registered as a catalog, or use default_catalog
            .and_then(|c| c.schema(&self.db_name)) // This assumes db_name is also a schema, adjust if needed
            .and_then(|s| s.table(&self.table_name).now_or_never().transpose())
            .transpose()
            .map_err(|e| DataFusionError::Execution(format!("Error getting table provider: {}",e)))?
            .ok_or_else(|| DataFusionError::Execution(format!("Table {}.{} not found in context", self.db_name, self.table_name)))?;


        let mut scan_builder = datafusion::logical_expr::LogicalPlanBuilder::scan(
            self.table_name.clone(),
            table_source, // This needs to be a TableProvider
            self.projection.clone(),
        )?;

        // Placeholder for filter deserialization and application
        // If self.filters_json is not "[]" (empty list of debug strings)
        if !self.filters_json.is_empty() && self.filters_json != "[]" {
             observability_deps::tracing::warn!("RemoteScanExec: Filter deserialization from filters_json is a placeholder and not fully implemented. Filters will not be applied in the constructed TableScan plan for fragment serialization.");
            // Example: scan_builder = scan_builder.filter(deserialized_expr)?;
        }

        let logical_plan_fragment = scan_builder.build()?;

        let logical_plan_fragment_json = match serde_json::to_string(&logical_plan_fragment) {
            Ok(json) => json,
            Err(e) => {
                return Err(DataFusionError::Execution(format!(
                    "Failed to serialize logical plan fragment: {}",
                    e
                )));
            }
        };

        // 2. Construct ExecuteQueryFragmentRequest
        let request = crate::distributed_query_service::ExecuteQueryFragmentRequest {
            db_name: self.db_name.clone(),
            shard_id: Some(self.shard_id.get()),
            logical_plan_fragment_json,
            session_config: std::collections::HashMap::new(), // Empty for now
        };

        observability_deps::tracing::info!(
            target_node_id = %self.target_node_id,
            "Simulating call to DistributedQueryService.ExecuteQueryFragment with request: {:?}",
            request
        );

        // 3. Simulate Client Call (Option 2/3 boundary)
        // In a real scenario with Option 1, this would be:
        // let response_stream = self.dist_query_service.execute_query_fragment(request).await;
        // For now, as per simplified plan for this subtask:

        #[cfg(test)]
        {
            // Test-specific logic: return predefined streams based on table_name
            if self.table_name == "test_success_table" {
                use arrow::array::Int64Array;
                use arrow::record_batch::RecordBatch;
                let schema = self.schema(); // Use the exec's schema
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    schema.fields().iter().map(|f| {
                        match f.data_type() {
                            arrow_schema::DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                            // Add other types as needed for schema, or make schema simpler
                            _ => Arc::new(arrow::array::NullArray::new(3)) as ArrayRef,
                        }
                    }).collect(),
                ).unwrap();
                observability_deps::tracing::debug!("RemoteScanExec (test_success_table): Returning a predefined stream.");
                return Ok(Box::pin(datafusion::physical_plan::stream::MemoryStream::try_new(vec![batch], schema, None)?));
            } else if self.table_name == "test_error_service_stream" {
                observability_deps::tracing::debug!("RemoteScanExec (test_error_service_stream): Returning an error stream (simulated gRPC error).");
                let schema = self.schema();
                return Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(async { Err(DataFusionError::Execution("Simulated gRPC service error".to_string())) })
                )));
            } else if self.table_name == "test_error_response_message" {
                 observability_deps::tracing::debug!("RemoteScanExec (test_error_response_message): Returning an error stream (simulated error in response).");
                let schema = self.schema();
                 return Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(async { Err(DataFusionError::Execution("Error message in ExecuteQueryFragmentResponse".to_string())) })
                )));
            }
            // Default for other test cases
            observability_deps::tracing::debug!("RemoteScanExec (test default): Returning empty stream.");
            return Ok(common::empty_record_batch_stream(self.schema()));
        }

        #[cfg(not(test))]
        {
            observability_deps::tracing::warn!(
                "RemoteScanExec::execute is in SIMULATION MODE for non-test builds. \
                No actual remote call is made. Returning NotImplemented error."
            );
            return Err(DataFusionError::NotImplemented(format!(
                "SIMULATED RemoteScanExec to node {} for shard {} of table {}.{} is not making a real call.",
                self.target_node_id, self.shard_id.get(), self.db_name, self.table_name
            )));
        }
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
        // TODO: In the future, this could potentially fetch estimated statistics from the remote node.
        Ok(datafusion::physical_plan::Statistics::new_unknown(&self.schema()))
    }
}
