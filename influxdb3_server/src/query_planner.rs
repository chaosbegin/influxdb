use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{LogicalPlan, TableScan}, // Added TableScan for direct matching
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        union::UnionExec, // Added UnionExec
    },
    physical_planner::DefaultPhysicalPlanner, // Added DefaultPhysicalPlanner
};
use influxdb_influxql_parser::statement::Statement;
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;
use observability_deps::tracing; // For logging

use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::NodeId; // For parsing current_node_id
use influxdb3_distributed_query::{RemoteScanExec, QueryFragment}; // Import new types

type Result<T, E = DataFusionError> = std::result::Result<T, E>;

pub(crate) struct Planner {
    ctx: IOxSessionContext,
    catalog: Arc<Catalog>,
    current_node_id: Arc<str>,
}

impl Planner {
    pub(crate) fn new(
        ctx: &IOxSessionContext,
        catalog: Arc<Catalog>,
        current_node_id: Arc<str>,
    ) -> Self {
        Self {
            ctx: ctx.child_ctx("rest_api_query_planner"),
            catalog,
            current_node_id,
        }
    }

    pub(crate) async fn sql(
        &self,
        query: impl AsRef<str> + Send,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("rest_api_query_planner_sql");

        let logical_plan = planner.query_to_logical_plan(query, &ctx).await?;
        // Changed from ctx.create_physical_plan to the new distribution-aware method
        self.create_physical_plan_considering_distribution(logical_plan, &ctx).await
    }

    pub(crate) async fn influxql(
        &self,
        statement: Statement,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.ctx.child_ctx("rest_api_query_planner_influxql");

        let logical_plan = InfluxQLQueryPlanner::statement_to_plan(statement, params, &ctx).await?;
        // Changed from ctx.create_physical_plan to the new distribution-aware method
        let physical_plan = self.create_physical_plan_considering_distribution(logical_plan.clone(), &ctx).await?;

        // Preserve existing SchemaExec logic for InfluxQL if needed for metadata
        let input_schema = physical_plan.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone()); // Use original logical_plan for metadata
        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));
        Ok(Arc::new(SchemaExec::new(physical_plan, schema)))
    }

    async fn create_physical_plan_considering_distribution(
        &self,
        logical_plan: Arc<LogicalPlan>,
        session_ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        tracing::debug!(logical_plan = %logical_plan.display_indent(), "Original logical plan before distribution consideration");

        match logical_plan.as_ref() {
            LogicalPlan::TableScan(scan_node) => {
                let default_db_name = self.ctx.default_database_name(); // From planner's IOxSessionContext
                let table_name = scan_node.table_name.table();

                tracing::debug!(%default_db_name, %table_name, "Processing TableScan for potential distribution");

                if let Some(db_schema) = self.catalog.db_schema(&default_db_name) {
                    if let Some(table_def) = db_schema.table_definition(table_name) {
                        if !table_def.shards.is_empty() {
                            tracing::info!(
                                db_name = %default_db_name,
                                table_name = %table_name,
                                num_shards = %table_def.shards.len(),
                                current_node_id = %self.current_node_id,
                                "Table is sharded. Planning distributed execution."
                            );

                            let current_node_id_val: NodeId = self.current_node_id.as_ref().parse::<u64>()
                                .map(NodeId::new)
                                .map_err(|e| DataFusionError::Plan(format!("Failed to parse current_node_id: {}", e)))?;

                            let mut sub_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
                            let physical_planner = DefaultPhysicalPlanner::default();

                            for shard_def_arc in table_def.shards.resource_iter() {
                                let shard_def = shard_def_arc.as_ref();
                                if shard_def.node_ids.is_empty() {
                                    tracing::warn!(shard_id = %shard_def.id.get(), "Shard has no assigned nodes, skipping.");
                                    continue;
                                }

                                let is_local_shard = shard_def.node_ids.contains(&current_node_id_val);

                                if is_local_shard {
                                    tracing::debug!(shard_id = %shard_def.id.get(), "Planning local scan for shard.");
                                    // CONCEPTUAL: Create a logical plan for this specific shard.
                                    // This would typically involve creating a new TableProvider instance that is
                                    // aware of the shard ID and filters data accordingly (e.g., by Parquet file paths).
                                    // For now, we'll represent this by trying to create a physical plan for the
                                    // original scan_node, assuming QueryTable/TableProvider can be made shard-aware
                                    // implicitly or through session context (which is not done in this conceptual step).
                                    // A more robust way would be to create a new LogicalPlan::TableScan
                                    // with a source that *only* yields data for this shard.
                                    // Placeholder: use the original logical_plan for the local part.
                                    // This will scan ALL local data for the table, not just this shard's data,
                                    // unless QueryTable is updated to filter by shard based on some context.
                                    // For this conceptual step, we log and proceed. A real implementation needs
                                    // shard-specific data access for local scans.

                                    // Create a logical plan that represents scanning this specific shard.
                                    // This might involve creating a new TableScan with specific filters
                                    // or assuming the underlying TableProvider can be restricted.
                                    // For now, re-use the original scan node and let physical planner handle it.
                                    // This is a simplification; true shard-specific scan needs more.
                                    let local_scan_logical_plan = LogicalPlan::TableScan(scan_node.clone()); // Simplified

                                    // Add a comment to physical plan indicating it's for a specific shard
                                    // This is not standard DataFusion, just for conceptual clarity
                                    tracing::info!("Conceptual: Planning local physical scan for table {}, shard {}", table_name, shard_def.id.get());

                                    // Use DefaultPhysicalPlanner for the local part.
                                    // This assumes that the QueryTable used by the DefaultPhysicalPlanner
                                    // can be (or will be in future) made shard-aware.
                                    // If QueryTable's scan filters by Parquet paths containing shard_id, this could work.
                                    match physical_planner.create_physical_plan(Arc::new(local_scan_logical_plan), session_ctx).await {
                                        Ok(local_exec_plan) => sub_plans.push(local_exec_plan),
                                        Err(e) => {
                                            tracing::error!("Failed to create local physical plan for shard {}: {}", shard_def.id.get(), e);
                                            // Potentially skip this shard or return an error for the whole query
                                        }
                                    }
                                } else {
                                    // Remote shard: pick the first node_id as the target
                                    let target_node_id = shard_def.node_ids[0];
                                    tracing::debug!(shard_id = %shard_def.id.get(), %target_node_id, "Planning remote scan for shard.");

                                    if let Some(node_info) = self.catalog.get_cluster_node(target_node_id) {
                                        let target_rpc_address = node_info.rpc_address.clone();

                                        // Conceptual: Create a query fragment.
                                        // For this example, use a simple SQL string.
                                        // A real implementation might serialize parts of the logical plan or use Substrait.
                                        // Projection should be taken from scan_node.projection
                                        let projection_cols = scan_node.projection.as_ref().map(|indices| {
                                            indices.iter().map(|i| scan_node.source.schema().field(*i).name().as_str()).collect::<Vec<&str>>().join(", ")
                                        }).unwrap_or_else(|| "*".to_string());

                                        let query_fragment_sql = format!(
                                            "SELECT {} FROM \"{}\" /* CONCEPTUAL_SHARD_FILTER shard_id={} */",
                                            projection_cols,
                                            scan_node.table_name.table(), // Use only table part of TableReference
                                            shard_def.id.get()
                                        );
                                        let query_fragment = QueryFragment::RawSql(query_fragment_sql);

                                        let remote_exec = RemoteScanExec::new(
                                            target_node_id,
                                            target_rpc_address,
                                            query_fragment,
                                            scan_node.schema().clone(), // Output schema is the projected schema of the scan
                                        );
                                        sub_plans.push(Arc::new(remote_exec));
                                    } else {
                                        tracing::warn!("No node definition found for remote shard {} target node_id {}. Skipping shard.", shard_def.id.get(), target_node_id.get());
                                        // Decide if this should be an error or if the shard is skipped.
                                    }
                                }
                            }

                            if sub_plans.is_empty() {
                                tracing::warn!("No sub-plans created for sharded table {}. This might indicate an issue or no relevant shards.", table_name);
                                // Fallback to default planner or return error/empty plan
                                return physical_planner.create_physical_plan(logical_plan.clone(), session_ctx).await;
                            } else if sub_plans.len() == 1 {
                                return Ok(sub_plans.remove(0));
                            } else {
                                return Ok(Arc::new(UnionExec::new(sub_plans)));
                            }
                        }
                    }
                }
                // Table not found in catalog or not sharded, or DB not found: delegate to default planner
                tracing::debug!("Table {} not sharded or not found in catalog under DB {}, using default planner.", table_name, default_db_name);
                let physical_planner = DefaultPhysicalPlanner::default();
                physical_planner.create_physical_plan(logical_plan.clone(), session_ctx).await
            }
            _ => {
                // For other logical plan nodes (Projection, Filter, Join, etc.):
                // 1. Recursively transform children.
                // 2. Reconstruct the current node with transformed children.
                // 3. Convert the new logical node to a physical node using DefaultPhysicalPlanner.
                // This is a conceptual sketch of how a full planner would work.
                tracing::debug!("Non-TableScan node encountered, attempting recursive distribution for children.");
                let physical_planner = DefaultPhysicalPlanner::default();
                let current_inputs = logical_plan.inputs();
                let mut new_physical_inputs = Vec::with_capacity(current_inputs.len());

                for input_logical_plan in current_inputs {
                    let physical_input = self.create_physical_plan_considering_distribution(input_logical_plan.clone(), session_ctx).await?;
                    new_physical_inputs.push(physical_input);
                }

                // Create new logical plan with potentially distributed children's physical plans as inputs
                // This step is complex as it requires converting physical plans back to logical table providers or similar.
                // A more standard DataFusion approach uses optimizer rules or physical planner extensions.
                // For this conceptual change, we'll simplify: if children were transformed,
                // the DefaultPhysicalPlanner will be applied to the original logical_plan,
                // assuming it can correctly use any UnionExec/RemoteScanExec produced by children.
                // This is a major simplification. A real implementation would reconstruct the logical plan
                // with new sources (if children were TableScans) or use a PhysicalPlanner instance
                // that knows how to delegate parts of the plan.

                // Simplified: If children were TableScans that got distributed, they'd be replaced.
                // For other ops, the default physical planner is used on the original logical plan,
                // assuming it will correctly incorporate any UnionExecs from transformed children.
                // This part of the logic is highly conceptual and simplified.
                if !new_physical_inputs.is_empty() && new_physical_inputs.iter().any(|p| p.name() == "UnionExec" || p.name() == "RemoteScanExec") {
                     // If any child became distributed, the default planner might not know how to build the parent correctly
                     // without more sophisticated rules. For now, let's assume the default planner can handle it
                     // if the logical plan structure itself isn't changed beyond what it expects for its inputs.
                     // This is where a custom PhysicalPlanner would handle creating physical ops like HashJoinExec etc.
                     // using the (potentially distributed) children.
                    tracing::warn!("Recursive distribution for non-TableScan operator's children is highly conceptual here.");
                }

                // Fallback to default physical planner for the current logical_plan node.
                // If its children were TableScans that returned UnionExec or RemoteScanExec,
                // the DefaultPhysicalPlanner might not correctly create the parent physical operator
                // without further customization (e.g. custom PhysicalPlanner instance with rules).
                // For now, this is a placeholder for that more complex logic.
                physical_planner.create_physical_plan(logical_plan.clone(), session_ctx).await
            }
        }
    }
}

// SchemaExec and its impls (copied from original file, ensure it's correctly placed and used)
// ... (SchemaExec struct and impl ExecutionPlan for SchemaExec) ...
// ... (impl DisplayAs for SchemaExec) ...
// Ensure this is correctly defined as in the original. For brevity, I'll assume it's present.
// It was used by influxql method.

/// A physical operator that overrides the `schema` API,
/// to return an amended version owned by `SchemaExec`. The
/// principal use case is to add additional metadata to the schema.
struct SchemaExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl SchemaExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));
        Self {
            input,
            schema,
            cache,
        }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = match input.properties().output_ordering() {
            None => EquivalenceProperties::new(schema),
            Some(output_odering) => {
                EquivalenceProperties::new_with_orderings(schema, &[output_odering.clone()])
            }
        };
        let output_partitioning = input.output_partitioning().clone();
        PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.properties().execution_mode.clone(), // Use .execution_mode() if available, else properties().execution_mode
        )
    }
}

impl std::fmt::Debug for SchemaExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlan for SchemaExec {
    fn name(&self) -> &str {
        Self::static_name()
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SchemaExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]), Arc::clone(&self.schema))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        self.input.statistics() // Delegate statistics to input
    }
}

impl DisplayAs for SchemaExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaExec")
            }
        }
    }
}
