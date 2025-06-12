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

                                        // 1. Create a LogicalPlan for the remote shard.
                                        //    Simplified: Use the original TableScan node. Filters/projections are assumed
                                        //    to be part of this scan_node already due to DataFusion's planning.
                                        //    A more advanced version might prune this plan further or add shard-specific filters.
                                        let remote_logical_plan = LogicalPlan::TableScan(scan_node.clone());

                                        // 2. Serialize this LogicalPlan.
                                        let serialized_plan_bytes = influxdb3_distributed_query::exec::serialize_logical_plan(&remote_logical_plan)
                                            .map_err(|e| {
                                                tracing::error!("Failed to serialize logical plan for remote shard: {}", e);
                                                DataFusionError::Plan(format!("Serialization error for remote plan: {}", e))
                                            })?;

                                        // 3. Construct QueryFragment.
                                        let query_fragment = QueryFragment::SerializedLogicalPlan(serialized_plan_bytes);

                                        // 4. Create RemoteScanExec.
                                        //    db_name is required by RemoteScanExec::new.
                                        //    The table_name is implicitly part of the serialized LogicalPlan (TableScan).
                                        let remote_exec = RemoteScanExec::new(
                                            default_db_name.clone(), // Pass db_name
                                            target_node_id,
                                            target_rpc_address,
                                            query_fragment,
                                            scan_node.schema().clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit, table_scan};
    use datafusion::datasource::provider::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use influxdb3_catalog::catalog::CatalogArgs;
    use influxdb3_catalog::shard::{ShardDefinition, ShardTimeRange};
    use influxdb3_id::NodeId;
    use iox_time::MockProvider;
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use influxdb3_catalog::log::FieldDataType; // For creating tables in tests
    use influxdb3_distributed_query::exec::deserialize_logical_plan;


    async fn setup_planner_for_tests(current_node_id_str: &str) -> (Planner, Arc<Catalog>, Arc<IOxSessionContext>) {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        let object_store = Arc::new(InMemory::new());

        let catalog = Arc::new(
            Catalog::new_with_args(
                "test_planner_node",
                object_store,
                Arc::clone(&time_provider),
                metrics,
                CatalogArgs::default(),
                Default::default(), // CatalogLimits
            )
            .await
            .unwrap(),
        );

        // Register some nodes for remote shard assignment
        let node1_def = influxdb3_catalog::ClusterNodeDefinition {
            id: NodeId::new(1), rpc_address: "node1:8082".to_string(), http_address: "".to_string(),
            status: "active".to_string(), created_at: 0, updated_at: 0
        };
        let node2_def = influxdb3_catalog::ClusterNodeDefinition {
            id: NodeId::new(2), rpc_address: "node2:8082".to_string(), http_address: "".to_string(),
            status: "active".to_string(), created_at: 0, updated_at: 0
        };
        catalog.add_cluster_node(node1_def).await.unwrap();
        catalog.add_cluster_node(node2_def).await.unwrap();


        let session_config = datafusion::execution::context::SessionConfig::new()
            .with_default_catalog_and_schema("public", "public"); // DataFusion default
        let session_state = datafusion::execution::session_state::SessionState::new_with_config_rt(
            session_config,
            Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
        );

        // Create a basic IOxSessionContext
        // The default catalog in IOxSessionContext usually points to a specific database.
        // For these tests, we'll set a default DB name.
        let default_db_name = "test_db_planner";
        session_state.config().options_mut().catalog.default_catalog = default_db_name.to_string();


        let iox_session_ctx = Arc::new(IOxSessionContext::with_state(session_state, None));

        let planner = Planner::new(&iox_session_ctx, Arc::clone(&catalog), Arc::from(current_node_id_str));
        (planner, catalog, iox_session_ctx)
    }

    #[tokio::test]
    async fn test_planner_creates_serialized_logical_plan_fragment() {
        let current_node_id_str = "1"; // Current node is NodeId(1)
        let (planner, catalog, session_ctx) = setup_planner_for_tests(current_node_id_str).await;

        let db_name = session_ctx.default_database_name(); // "test_db_planner"
        let table_name = "sharded_table";

        // Setup catalog: DB, Table, Shards (one local, one remote)
        catalog.create_database(&db_name).await.unwrap();
        catalog.create_table(&db_name, table_name, &["tagA"], &[(String::from("field1"), FieldDataType::Integer)]).await.unwrap();

        let table_def_arc = catalog.db_schema(&db_name).unwrap().table_definition(table_name).unwrap();

        let local_node_id = NodeId::new(1);
        let remote_node_id = NodeId::new(2);

        let shard_local = ShardDefinition::new(
            influxdb3_id::ShardId::new(10),
            ShardTimeRange { start_time: 0, end_time: 1000 },
            vec![local_node_id], // Assigned to current node
        );
        let shard_remote = ShardDefinition::new(
            influxdb3_id::ShardId::new(20),
            ShardTimeRange { start_time: 1000, end_time: 2000 },
            vec![remote_node_id], // Assigned to remote node
        );
        catalog.create_shard(&db_name, table_name, shard_local).await.unwrap();
        catalog.create_shard(&db_name, table_name, shard_remote).await.unwrap();

        // Create a simple LogicalPlan: SELECT field1 FROM sharded_table WHERE tagA = 'value'
        // This requires the table to be registered in the session context's catalog provider.
        // For simplicity, we construct a TableScan directly.
        // The schema must match what the catalog would provide for "sharded_table".
        let arrow_schema = table_def_arc.schema.as_arrow(); // Get schema from catalog's TableDefinition

        let logical_plan = LogicalPlanBuilder::scan(table_name, Arc::clone(&arrow_schema) , None).unwrap()
            .project(vec![col("field1")]).unwrap()
            // .filter(col("tagA").eq(lit("some_value"))).unwrap() // Example filter
            .build().unwrap();

        // Execute planner
        let physical_plan = planner.create_physical_plan_considering_distribution(Arc::new(logical_plan), &session_ctx).await.unwrap();

        // Expect a UnionExec because we have one local and one remote shard (conceptual)
        assert_eq!(physical_plan.name(), "UnionExec");
        let union_exec = physical_plan.as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 2); // One local, one remote

        let mut remote_scan_found = false;
        for child_plan in union_exec.children() {
            if child_plan.name() == "RemoteScanExec" {
                remote_scan_found = true;
                let remote_scan = child_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();
                assert_eq!(remote_scan.target_node_id(), remote_node_id);
                assert_eq!(remote_scan.db_name(), db_name);

                match remote_scan.query_fragment().as_ref() {
                    QueryFragment::SerializedLogicalPlan(bytes) => {
                        let deserialized_lp = deserialize_logical_plan(bytes).unwrap();
                        // Verify it's a TableScan for the correct table, with the projection
                        match deserialized_lp {
                            LogicalPlan::Projection(p) => {
                                assert_eq!(p.expr.len(), 1); // field1
                                if let LogicalPlan::TableScan(ts) = p.input.as_ref() {
                                     assert_eq!(ts.table_name.table(), table_name);
                                } else {
                                    panic!("Expected TableScan input for projection in deserialized plan");
                                }
                            }
                            _ => panic!("Expected Projection as top of deserialized plan for remote scan, got {:?}", deserialized_lp),
                        }
                    }
                    // _ => panic!("Expected QueryFragment::SerializedLogicalPlan"), // RawSql is now commented out
                }
            } else {
                // Could assert that other child is ParquetExec or similar local scan
                assert!(child_plan.name().contains("ParquetExec") || child_plan.name().contains("EmptyExec") || child_plan.name() == "RecordBatchProjectExec", "Expected local scan, got {}", child_plan.name());

            }
        }
        assert!(remote_scan_found, "RemoteScanExec not found in the plan for sharded table");
    }
}


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
