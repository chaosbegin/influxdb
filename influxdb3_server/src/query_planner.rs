use std::{any::Any, sync::Arc, collections::HashMap, fmt::Debug};

use arrow_schema::{SchemaRef, Schema as ArrowSchema, ArrowError};
use bytes::Bytes;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        coalesce_batches::CoalesceBatchesExec, union::UnionExec, PhysicalPlanner, common,
    },
    logical_expr::LogicalPlan as DFLogicalPlan,
    datasource::physical_plan::TableScan,
    physical_optimizer::PhysicalOptimizerRule, // Added for rewriter
    config::ConfigOptions, // Added for rewriter
    common::tree_node::{TreeNode, TreeNodeRewriter, Transformed}, // Added for rewriter
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use influxdb_influxql_parser::statement::Statement;
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner, physical_optimizer::PhysicalOptimizer, Optimizer};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::NodeId as InfluxNodeId;
use crate::distributed_query_client::GrpcDistributedQueryClient;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use crate::expr_to_sql;
use chrono::{DateTime, Utc};


type Result<T, E = DataFusionError> = std::result::Result<T, E>;

/// Provides information about other nodes in the cluster.
pub trait NodeInfoProvider: Send + Sync + Debug {
    fn get_node_query_rpc_address(&self, node_id: &InfluxNodeId) -> Option<String>;
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MockNodeInfoProvider {
    addresses: HashMap<InfluxNodeId, String>,
}

#[cfg(test)]
impl MockNodeInfoProvider {
    pub fn new() -> Self { Self { addresses: HashMap::new() } }
    pub fn add_node(&mut self, node_id: InfluxNodeId, address: String) {
        self.addresses.insert(node_id, address);
    }
}

#[cfg(test)]
impl NodeInfoProvider for MockNodeInfoProvider {
    fn get_node_query_rpc_address(&self, node_id: &InfluxNodeId) -> Option<String> {
        self.addresses.get(node_id).cloned()
    }
}

// --- DistributedQueryRewriter ---
#[derive(Debug)]
pub struct DistributedQueryRewriter {
    catalog: Arc<Catalog>,
    current_node_id: Arc<str>,
    node_info_provider: Arc<dyn NodeInfoProvider>,
    session_ctx: Arc<IOxSessionContext>, // Changed to Arc<IOxSessionContext> for create_physical_plan
    db_name: Arc<str>, // Database context for the current query
}

impl DistributedQueryRewriter {
    pub fn new(
        catalog: Arc<Catalog>,
        current_node_id: Arc<str>,
        node_info_provider: Arc<dyn NodeInfoProvider>,
        session_ctx: Arc<IOxSessionContext>,
        db_name: Arc<str>,
    ) -> Self {
        Self {
            catalog,
            current_node_id,
            node_info_provider,
            session_ctx,
            db_name,
        }
    }

    // This method will contain the core logic previously in Planner::generate_distributed_physical_plan
    // but focused on a single TableScan physical node.
    // It now takes scan_db_name and scan_table_name_str, derived from the TableScan's TableReference.
    async fn generate_distributed_plan_for_scan(
        &self,
        original_phys_table_scan: &TableScan,
        scan_db_name: Arc<str>, // Specific DB name for this scan
        scan_table_name_str: Arc<str>, // Specific table name for this scan
    ) -> Result<Arc<dyn ExecutionPlan>> {
        observability_deps::tracing::debug!(db_name = %scan_db_name, table_name = %scan_table_name_str, "Rewriter: Checking table for sharding");

        if let Some(db_schema) = self.catalog.db_schema(&scan_db_name) {
            if let Some(table_def) = db_schema.table_definition(&scan_table_name_str) {
                if !table_def.shards.is_empty() {
                    observability_deps::tracing::info!(
                        db_name = %scan_db_name, table_name = %scan_table_name_str, num_shards=%table_def.shards.len(),
                        "Rewriter: Table is sharded, generating distributed plan components."
                    );

                    let mut sub_plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
                    let current_node_id_parsed = self.current_node_id.parse::<u64>().ok();

                    for shard_def_arc in table_def.shards.resource_iter() {
                        let shard_def = shard_def_arc.as_ref();

                        // Check for shard pruning based on shard key predicates
                        if table_def.sharding_strategy == influxdb3_catalog::shard::ShardingStrategy::TimeAndKey {
                            if let (Some(key_cols), Some(hash_range)) = (&table_def.shard_key_columns, shard_def.hash_key_range) {
                                let mut shard_key_values = Vec::new();
                                let mut all_keys_found = true;
                                for key_col_name in key_cols {
                                    let mut found_key_predicate = false;
                                    for filter in original_phys_table_scan.filters() {
                                        if let datafusion::logical_expr::Expr::BinaryExpr(be) = filter {
                                            if be.op == datafusion::logical_expr::Operator::Eq {
                                                if let datafusion::logical_expr::Expr::Column(c) = be.left.as_ref() {
                                                    if c.name == *key_col_name {
                                                        if let datafusion::logical_expr::Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(val))) = be.right.as_ref() {
                                                            shard_key_values.push(val.clone());
                                                            found_key_predicate = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if !found_key_predicate {
                                        all_keys_found = false;
                                        break;
                                    }
                                }

                                if all_keys_found {
                                    let hash = influxdb3_write::sharding_utils::calculate_hash(
                                        &shard_key_values.iter().map(|s|s.as_str()).collect::<Vec<&str>>()
                                    );
                                    if !(hash >= hash_range.0 && hash <= hash_range.1) {
                                        observability_deps::tracing::info!(
                                            shard_id = %shard_def.id.get(), table=%scan_table_name_str, calculated_hash=%hash, shard_hash_range=?hash_range,
                                            "Rewriter: Pruning remote shard due to hash key mismatch."
                                        );
                                        continue; // Skip this shard
                                    }
                                }
                            }
                        }

                        let generate_sql_for_shard_inner = |sd: &influxdb3_catalog::shard::ShardDefinition|
                            -> Result<Bytes, DataFusionError> {
                            let projected_columns_str = match original_phys_table_scan.projection() {
                                Some(indices) => indices.iter().map(|i| original_phys_table_scan.table_schema().field(*i).name().as_str()).collect::<Vec<&str>>().join(", "),
                                None => "*".to_string(),
                            };
                            // Use scan_db_name and scan_table_name_str for qualified name
                            let qualified_table_name_str = format!("\"{}\".\"{}\"", scan_db_name, scan_table_name_str);
                            let mut sql_filters: Vec<String> = Vec::new();
                            let shard_time_range = sd.time_range;
                            let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(shard_time_range.start_time / 1_000_000_000, (shard_time_range.start_time % 1_000_000_000) as u32).unwrap_or_default(), Utc).to_rfc3339();
                            let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(shard_time_range.end_time / 1_000_000_000, (shard_time_range.end_time % 1_000_000_000) as u32).unwrap_or_default(), Utc).to_rfc3339();
                            sql_filters.push(format!("\"time\" >= TIMESTAMP '{}'", start_time_str));
                            sql_filters.push(format!("\"time\" <= TIMESTAMP '{}'", end_time_str));
                            for filter_expr in original_phys_table_scan.filters() {
                                match expr_to_sql::expr_to_sql(filter_expr) {
                                    Ok(sql_pred) => sql_filters.push(sql_pred),
                                    Err(e) => { observability_deps::tracing::warn!("Failed to convert filter to SQL: {}", e); }
                                }
                            }
                            let sql_query = if sql_filters.is_empty() {
                                format!("SELECT {} FROM {}", projected_columns_str, qualified_table_name_str)
                            } else {
                                format!("SELECT {} FROM {} WHERE {}", projected_columns_str, qualified_table_name_str, sql_filters.join(" AND "))
                            };
                            Ok(Bytes::from(sql_query.into_bytes()))
                        };

                        let mut plans_for_this_shard_instance: Vec<Arc<dyn ExecutionPlan>> = vec![];

                        for owner_node_id in &shard_def.node_ids {
                            if Some(owner_node_id.get()) == current_node_id_parsed {
                                // Pass scan_db_name and scan_table_name_str to local plan creation
                                let local_plan = self.create_local_shard_physical_plan(original_phys_table_scan, shard_def, &scan_db_name, &scan_table_name_str).await?;
                                plans_for_this_shard_instance.push(local_plan);
                            } else {
                                if let Some(addr) = self.node_info_provider.get_node_query_rpc_address(owner_node_id) {
                                    let sql_bytes = generate_sql_for_shard_inner(shard_def)?;
                                    // Pass scan_db_name to RemoteScanExec if it needs to know its specific db context, or rely on SQL qualification
                                    plans_for_this_shard_instance.push(Arc::new(RemoteScanExec::new(addr, scan_db_name.to_string(), sql_bytes, original_phys_table_scan.schema())));
                                } else { observability_deps::tracing::warn!("No addr for owner {}", owner_node_id.get()); }
                            }
                        }

                        if let Some(influxdb3_catalog::shard::ShardMigrationStatus::MigratingOutTo(targets)) = &shard_def.migration_status {
                            for target_node_id in targets {
                                if !shard_def.node_ids.contains(target_node_id) { // Only add if not already an owner
                                    if let Some(addr) = self.node_info_provider.get_node_query_rpc_address(target_node_id) {
                                        observability_deps::tracing::info!("Shard {} migrating to {}. Adding scan for target.", shard_def.id.get(), target_node_id.get());
                                        let sql_bytes = generate_sql_for_shard_inner(shard_def)?;
                                        plans_for_this_shard_instance.push(Arc::new(RemoteScanExec::new(addr, scan_db_name.to_string(), sql_bytes, original_phys_table_scan.schema())));
                                    } else { observability_deps::tracing::warn!("No addr for target {}", target_node_id.get());}
                                }
                            }
                        }
                        sub_plans.extend(plans_for_this_shard_instance);
                    }

                    return if sub_plans.is_empty() {
                        observability_deps::tracing::warn!(%scan_db_name, %scan_table_name_str, "Sharded table resulted in no scannable shards after pruning.");
                        Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(original_phys_table_scan.schema())))
                    } else if sub_plans.len() == 1 {
                        Ok(sub_plans.remove(0))
                    } else {
                        Ok(Arc::new(UnionExec::new(sub_plans)))
                    };
                }
            }
        }
        // If not sharded, or no shards defined, or table/db not found in catalog for sharding info, return original plan
        Ok(Arc::clone(original_phys_table_scan))
    }

    async fn create_local_shard_physical_plan(
        &self,
        original_table_scan_physical: &TableScan,
        shard_def: &influxdb3_catalog::shard::ShardDefinition,
        db_name: &str, // Now passed in
        table_name: &str, // Now passed in
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut local_filters: Vec<datafusion::logical_expr::Expr> = Vec::new();
        let time_col = datafusion::logical_expr::col("time");

        let start_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.start_time), None));
        let end_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.end_time), None));

        local_filters.push(time_col.clone().gt_eq(start_lit));
        local_filters.push(time_col.lt_eq(end_lit));
        local_filters.extend(original_table_scan_physical.filters().to_vec());

        let combined_filter_opt = local_filters.into_iter().reduce(datafusion::logical_expr::Expr::and);
        let final_filters = combined_filter_opt.map_or_else(Vec::new, |f| vec![f]);

        let new_logical_scan = DFLogicalPlan::TableScan(datafusion::logical_expr::TableScan::try_new(
            datafusion::logical_expr::TableReference::partial(db_name, table_name), // Use passed db_name and table_name
            Arc::clone(original_table_scan_physical.source()),
            original_table_scan_physical.projection().cloned(),
            final_filters,
            original_table_scan_physical.fetch(),
        )?);

        self.session_ctx.create_physical_plan(&new_logical_scan).await
    }
}

impl PhysicalOptimizerRule for DistributedQueryRewriter {
    fn name(&self) -> &str { "DistributedQueryRewriter" }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()
            .map_err(|e| DataFusionError::Execution(format!("Failed to create Tokio runtime for rewriter: {}", e)))?;

        plan.transform_up(&|sub_plan| {
            // TODO: Future: For complex operators like Joins or Aggregates involving sharded tables,
            // this rewriter would need more sophisticated logic to potentially distribute these
            // operations themselves (e.g., broadcast join, repartition join, distributed aggregation).
            // Currently, such operators are passed through, and distribution only happens at the TableScan level.

            if let Some(phys_table_scan) = sub_plan.as_any().downcast_ref::<TableScan>() {
                let table_ref = phys_table_scan.table_name();
                let (scan_db_name, scan_table_name_str) = match table_ref {
                    datafusion::logical_expr::TableReference::Bare { table } => {
                        // For a bare table reference, assume it belongs to the rewriter's default db_name context.
                        (self.db_name.clone(), table.to_string())
                    }
                    datafusion::logical_expr::TableReference::Partial { schema, table } => {
                        (Arc::from(schema.as_ref()), table.to_string())
                    }
                    datafusion::logical_expr::TableReference::Full { catalog: _, schema, table } => {
                        // Assuming schema is the database name in a fully qualified name
                        (Arc::from(schema.as_ref()), table.to_string())
                    }
                };

                // Run the async method on the temporary runtime
                // Pass the correctly determined scan_db_name to generate_distributed_plan_for_scan
                let new_distributed_plan_for_this_scan = rt.block_on(
                    self.generate_distributed_plan_for_scan(phys_table_scan, scan_db_name, Arc::from(scan_table_name_str))
                )?;
                Ok(Transformed::yes(new_distributed_plan_for_this_scan))
            } else {
                Ok(Transformed::no(sub_plan))
            }
        })
    }
}


pub(crate) struct Planner {
    ctx: IOxSessionContext, // This is Arc<IOxSessionContextInner>
    catalog: Arc<Catalog>,
    current_node_id: Arc<str>,
    node_info_provider: Arc<dyn NodeInfoProvider>,
}

impl Planner {
    pub(crate) fn new(
        ctx: &IOxSessionContext, // Actually Arc<IOxSessionContextInner>
        catalog: Arc<Catalog>,
        current_node_id: Arc<str>,
        node_info_provider: Arc<dyn NodeInfoProvider>,
    ) -> Self {
        Self {
            ctx: ctx.child_ctx("planner_for_rewrite"), // Create a child context for the planner instance
            catalog,
            current_node_id,
            node_info_provider,
        }
    }

    pub(crate) async fn sql(
        &self,
        query: impl AsRef<str> + Send,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.as_ref();
        // Use self.ctx which is already a child context

        let logical_plan = planner.query_to_logical_plan(query, &self.ctx).await?;
        let initial_physical_plan = self.ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.current_node_id),
            Arc::clone(&self.node_info_provider),
            Arc::clone(&self.ctx.inner()), // Pass Arc<IOxSessionContextInner>
            Arc::from(self.ctx.default_database_name().as_str()),
        );
        let distributed_physical_plan = rewriter.optimize(initial_physical_plan, self.ctx.inner().state().config())?;

        Ok(distributed_physical_plan)
    }

    pub(crate) async fn influxql(
        &self,
        statement: Statement,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use self.ctx which is already a child context

        let logical_plan = InfluxQLQueryPlanner::statement_to_plan(statement, params, &self.ctx).await?;
        let initial_physical_plan = self.ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.current_node_id),
            Arc::clone(&self.node_info_provider),
            Arc::clone(&self.ctx.inner()),
            Arc::from(self.ctx.default_database_name().as_str()),
        );
        let distributed_physical_plan = rewriter.optimize(initial_physical_plan, self.ctx.inner().state().config())?;

        let input_schema = distributed_physical_plan.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone());
        let final_schema = Arc::new(ArrowSchema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));

        Ok(Arc::new(SchemaExec::new(distributed_physical_plan, final_schema)))
    }

    // The old generate_distributed_physical_plan is now refactored into DistributedQueryRewriter::generate_distributed_plan_for_scan
    // and Planner::create_local_shard_logical_plan (which is also called by the rewriter's method)
    // This method is kept for now to make create_local_shard_logical_plan belong to Planner
    // but it's not directly called by sql/influxql anymore.
    #[allow(dead_code)]
    async fn generate_distributed_physical_plan(
        &self,
        _physical_plan: Arc<dyn ExecutionPlan>, // Parameter not used anymore due to refactor
        _db_name: &str, // Parameter not used anymore
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // This entire method's core logic has been moved to DistributedQueryRewriter::generate_distributed_plan_for_scan
        // and Planner::create_local_shard_logical_plan.
        // The Planner's sql() and influxql() methods now use the rewriter.
        // This method could be removed if create_local_shard_logical_plan is moved to the rewriter or a shared location.
        unimplemented!("This method is deprecated; logic moved to DistributedQueryRewriter");
    }

    // Helper to create a shard-specific logical plan for local execution
    // This is called by DistributedQueryRewriter::generate_distributed_plan_for_scan
    // It needs access to self.ctx for create_physical_plan if we were to return physical plan from here.
    // But it returns a logical plan.
    pub(crate) fn create_local_shard_logical_plan(
        &self, // Does not need &self if ctx is passed or not used for logical plan part
        original_table_scan_physical: &TableScan,
        shard_def: &influxdb3_catalog::shard::ShardDefinition,
        db_name: &str,
        table_name: &str,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        observability_deps::tracing::debug!(shard_id = %shard_def.id.get(), "Planner: Creating logical plan for local shard.");

        let mut local_filters: Vec<datafusion::logical_expr::Expr> = Vec::new();
        let time_col = datafusion::logical_expr::col("time");

        let start_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.start_time), None));
        let end_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.end_time), None));

        local_filters.push(time_col.clone().gt_eq(start_lit));
        local_filters.push(time_col.lt_eq(end_lit));
        local_filters.extend(original_table_scan_physical.filters().to_vec());

        let combined_filter_opt = local_filters.into_iter().reduce(datafusion::logical_expr::Expr::and);
        let final_filters = combined_filter_opt.map_or_else(Vec::new, |f| vec![f]);

        let new_logical_scan = DFLogicalPlan::TableScan(datafusion::logical_expr::TableScan::try_new(
            datafusion::logical_expr::TableReference::partial(db_name, table_name),
            Arc::clone(original_table_scan_physical.source()),
            original_table_scan_physical.projection().cloned(),
            final_filters,
            original_table_scan_physical.fetch(),
        )?);

        Ok(new_logical_scan)
    }
}

// --- RemoteScanExec: For distributed query fragment execution ---
use crate::remote_stream::RemoteRecordBatchStream;

#[derive(Debug)]
struct RemoteScanExec {
    target_node_address: String,
    db_name: String,
    plan_or_query_bytes: Bytes,
    expected_schema: SchemaRef,
    cache: PlanProperties,
}

impl RemoteScanExec {
    #[allow(dead_code)]
    fn new(
        target_node_address: String,
        db_name: String,
        plan_or_query_bytes: Bytes,
        expected_schema: SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&expected_schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::ExecutionMode::Bounded,
        );
        Self {
            target_node_address,
            db_name,
            plan_or_query_bytes,
            expected_schema,
            cache,
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str { "RemoteScanExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { Arc::clone(&self.expected_schema) }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("RemoteScanExec does not support children".to_string()))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        observability_deps::tracing::debug!(
            target_node = %self.target_node_address,
            db_name = %self.db_name,
            plan_bytes_len = %self.plan_or_query_bytes.len(),
            session_id = %context.session_id(),
            "Creating RemoteRecordBatchStream for RemoteScanExec"
        );
        let session_config_map = HashMap::new();

        let stream = RemoteRecordBatchStream::new(
            self.target_node_address.clone(),
            self.db_name.clone(),
            self.plan_or_query_bytes.clone(),
            self.expected_schema.clone(),
            session_config_map,
        );

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
        Ok(datafusion::physical_plan::Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RemoteScanExec: target_node={}, db={}, schema_fields=[{}]",
                    self.target_node_address,
                    self.db_name,
                    self.expected_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>().join(", ")
                )
            }
        }
    }
}

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
            input.pipeline_behavior(),
            input.boundedness(),
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
        Ok(datafusion::physical_plan::Statistics::new_unknown(
            &self.schema(),
        ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::prelude::{CsvReadOptions, SessionContext, col as df_col};
    use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use arrow_schema::{DataType, Field, Schema as ArrowSchemaOriginal, SortOptions, TimeUnit};
    use std::path::Path;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use influxdb3_catalog::shard::{ShardDefinition, ShardId, ShardTimeRange, ShardingStrategy as CatalogShardingStrategy};
    use influxdb3_catalog::log::FieldDataType;
    use datafusion::logical_expr::Operator;


    #[tokio::test]
    async fn test_physical_plan_serialization_roundtrip() -> Result<()> {
        let ctx = SessionContext::new();

        let mut tmp_file = NamedTempFile::new().unwrap();
        writeln!(tmp_file, "c1,c2,c3").unwrap();
        writeln!(tmp_file, "1,2,3").unwrap();
        writeln!(tmp_file, "4,5,6").unwrap();
        let csv_path = tmp_file.path().to_str().unwrap().to_string();
        let table_path = ListingTableUrl::parse(format!("file://{}", csv_path))?;


        ctx.register_csv("test_table", table_path.as_ref(), CsvReadOptions::new(), None).await?;

        let physical_plan_original = ctx
            .table("test_table")
            .await?
            .create_physical_plan()
            .await?;

        let original_schema_str = format!("{:?}", physical_plan_original.schema());
        let original_plan_str = format!("{:?}", physical_plan_original);

        let plan_bytes = datafusion_proto::physical_plan::physical_plan_to_bytes(physical_plan_original.clone())
            .map_err(|e| DataFusionError::Execution(format!("Failed to serialize plan: {}", e)))?;

        let physical_plan_deserialized = datafusion_proto::physical_plan::from_physical_plan_bytes(&plan_bytes, &ctx, ctx.runtime_env().as_ref())
            .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize plan: {}", e)))?;

        let deserialized_schema_str = format!("{:?}", physical_plan_deserialized.schema());
        let deserialized_plan_str = format!("{:?}", physical_plan_deserialized);

        assert_eq!(original_schema_str, deserialized_schema_str, "Schemas do not match after roundtrip");
        assert_eq!(original_plan_str, deserialized_plan_str, "Plan string representations do not match");

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = physical_plan_deserialized.execute(0, task_ctx)?;
        let mut batch_count = 0;
        while let Some(batch_result) = stream.next().await {
            assert!(batch_result.is_ok(), "Execution of deserialized plan failed");
            batch_count += 1;
        }
        assert_eq!(batch_count, 1, "Expected one batch from executing the deserialized plan");

        Ok(())
    }

    // Updated helper to also return IOxSessionContext (which is Arc<IOxSessionContextInner>)
    // and use MockNodeInfoProvider by default.
    async fn create_test_planner_and_catalog_with_node_id_provider(
        current_node_id_str: &str,
        node_info_provider: Arc<dyn NodeInfoProvider>,
    ) -> (Planner, Arc<Catalog>, Arc<IOxSessionContext>) {
        let catalog = Arc::new(Catalog::new_in_memory("test_planner_host").await.unwrap());
        let iox_session_ctx = IOxSessionContext::new_default();
        let current_node_id_arc = Arc::from(current_node_id_str);
        let planner = Planner::new(&iox_session_ctx, Arc::clone(&catalog), current_node_id_arc, node_info_provider);
        (planner, catalog, iox_session_ctx)
    }

    // Simplified helper for tests that don't need custom NodeInfoProvider or specific node_id
    async fn create_test_planner_and_catalog() -> (Planner, Arc<Catalog>, Arc<IOxSessionContext>) {
        create_test_planner_and_catalog_with_node_id_provider("0", Arc::new(MockNodeInfoProvider::new())).await
    }


    #[tokio::test]
    async fn test_distributed_plan_non_sharded_table() -> Result<()> {
        let (planner, catalog, iox_session_ctx) = create_test_planner_and_catalog().await;
        let db_name = "test_db_non_sharded";
        let table_name = "my_table";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag_a"], &[("val", FieldDataType::Integer)]).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(&format!("SELECT * FROM {}", table_name)).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        let distributed_plan_str = format!("{:?}", distributed_plan);
        assert!(!distributed_plan_str.contains("UnionExec") && !distributed_plan_str.contains("RemoteScanExec"),
                "Plan should not be UnionExec or RemoteScanExec for non-sharded table, but was: {}", distributed_plan_str);

        assert!(distributed_plan.as_any().downcast_ref::<TableScan>().is_some() ||
                distributed_plan.as_any().downcast_ref::<ParquetExec>().is_some() ||
                (distributed_plan.children().len() == 1 && (distributed_plan.children()[0].as_any().downcast_ref::<TableScan>().is_some() || distributed_plan.children()[0].as_any().downcast_ref::<ParquetExec>().is_some())),
                "Plan for non-sharded table should remain a direct scan or simple wrapper, but was: {}", distributed_plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_all_remote_shards() -> Result<()> {
        let mut mock_node_info_provider = MockNodeInfoProvider::new();
        mock_node_info_provider.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());
        mock_node_info_provider.add_node(InfluxNodeId::new(2), "http://node-2:8082".to_string());
        let (planner, catalog, iox_session_ctx) =
            create_test_planner_and_catalog_with_node_id_provider("0", Arc::new(mock_node_info_provider)).await;
        let db_name = "test_db_remote";
        let table_name = "metrics_remote";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(2)], None)).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(&format!("SELECT * FROM {}", table_name)).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert_ne!(format!("{:?}", distributed_plan), format!("{:?}", initial_physical_plan), "Plan should change for remote sharded table");
        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (remote shards)");

        for (i, child) in distributed_plan.children().iter().enumerate() {
            assert!(child.as_any().is::<RemoteScanExec>(), "Child {} should be RemoteScanExec", i);
            let remote_exec = child.as_any().downcast_ref::<RemoteScanExec>().unwrap();

            let expected_node_id = i + 1;
            assert_eq!(remote_exec.target_node_address, format!("http://node-{}:8082", expected_node_id), "Child {} address mismatch", i);

            let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).expect("SQL query bytes are not valid UTF-8");
            observability_deps::tracing::info!(%sql_query, "Generated SQL for remote shard");

            assert!(sql_query.contains(&format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name)), "SQL query does not contain correct SELECT and FROM clauses");

            if remote_exec.target_node_address.contains("node-1") {
                let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(0).unwrap_or_default(), Utc).to_rfc3339();
                let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(1000).unwrap_or_default(), Utc).to_rfc3339();
                assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str)), "Missing start time filter for shard 1: actual {}", sql_query);
                assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str)), "Missing end time filter for shard 1: actual {}", sql_query);
            } else if remote_exec.target_node_address.contains("node-2") {
                let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(1001).unwrap_or_default(), Utc).to_rfc3339();
                let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(2000).unwrap_or_default(), Utc).to_rfc3339();
                assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str)), "Missing start time filter for shard 2: actual {}", sql_query);
                assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str)), "Missing end time filter for shard 2: actual {}", sql_query);
            } else {
                panic!("Unexpected target node address: {}", remote_exec.target_node_address);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_all_local_shards() -> Result<()> {
        let (planner, catalog, iox_session_ctx) = create_test_planner_and_catalog().await;
        let db_name = "test_db_local";
        let table_name = "metrics_local";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(
            &format!("SELECT * FROM {} WHERE host = 'server1'", table_name)
        ).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for multiple local shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local shards)");

        for child_plan in distributed_plan.children() {
            assert!(!child_plan.as_any().is::<RemoteScanExec>(), "Local shard plan should not be RemoteScanExec");
            let plan_str = format!("{:?}", child_plan);
            assert!(plan_str.contains("time >= TimestampNanosecond(0"), "Missing start time filter for local shard (or incorrect format)");
            assert!(plan_str.contains("host = Utf8(\"server1\")"), "Missing original predicate 'host = server1' in local shard plan");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_mixed_shards() -> Result<()> {
        let mut mock_node_info_provider = MockNodeInfoProvider::new();
        mock_node_info_provider.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());
        let (planner, catalog, iox_session_ctx) =
            create_test_planner_and_catalog_with_node_id_provider("0", Arc::new(mock_node_info_provider)).await;
        let db_name = "test_db_mixed";
        let table_name = "metrics_mixed";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(&format!("SELECT * FROM {}", table_name)).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for mixed shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local + remote)");

        let mut found_local_plan = false;
        let mut found_remote_scan = false;

        for child_plan in distributed_plan.children() {
            if child_plan.as_any().is::<RemoteScanExec>() {
                found_remote_scan = true;
                let remote_exec = child_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();
                assert_eq!(remote_exec.target_node_address, "http://node-1:8082");
            } else {
                found_local_plan = true;
                let plan_str = format!("{:?}", child_plan);
                assert!(plan_str.contains("time >= TimestampNanosecond(0"), "Missing start time filter for local shard (or incorrect format)");
                assert!(plan_str.contains("time <= TimestampNanosecond(1000000000000"), "Missing end time filter for local shard (or incorrect format): {}", plan_str);

            }
        }
        assert!(found_local_plan, "Did not find a plan for the local shard");
        assert!(found_remote_scan, "Did not find RemoteScanExec for the remote shard");

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_remote_shard_with_predicates() -> Result<()> {
        let mut mock_node_info_provider = MockNodeInfoProvider::new();
        mock_node_info_provider.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());
        let (planner, catalog, iox_session_ctx) =
            create_test_planner_and_catalog_with_node_id_provider("0", Arc::new(mock_node_info_provider)).await;
        let db_name = "test_db_remote_pred";
        let table_name = "metrics_remote_pred";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        let shard_start_time_ns = 0;
        let shard_end_time_ns = 1_000_000_000;
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: shard_start_time_ns, end_time: shard_end_time_ns}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(
            &format!("SELECT * FROM {} WHERE host = 'serverA' AND cpu_usage > 0.5", table_name)
        ).await?;

        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<RemoteScanExec>(), "Plan should be a RemoteScanExec");
        let remote_exec = distributed_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();

        let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).expect("SQL query bytes are not valid UTF-8");
        observability_deps::tracing::info!(%sql_query, "Generated SQL for remote shard with predicates");

        assert!(sql_query.contains(&format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name)));

        let start_time_str_expected = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(shard_start_time_ns).unwrap_or_default(), Utc).to_rfc3339();
        let end_time_str_expected = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_nanos(shard_end_time_ns).unwrap_or_default(), Utc).to_rfc3339();

        assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str_expected)), "SQL missing start time: {}", sql_query);
        assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str_expected)), "SQL missing end time: {}", sql_query);
        assert!(sql_query.contains("(\"host\") = ('serverA')"), "SQL missing host predicate: {}", sql_query);
        assert!(sql_query.contains("(\"cpu_usage\") > (0.5)"), "SQL missing cpu_usage predicate: {}", sql_query);
        assert!(sql_query.matches(" AND ").count() >= 3, "Expected at least 3 ANDs for 4 conditions. SQL: {}", sql_query);

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_query_migrating_shard() -> Result<()> {
        let mut mock_node_info_provider = MockNodeInfoProvider::new();
        mock_node_info_provider.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());
        mock_node_info_provider.add_node(InfluxNodeId::new(2), "http://node-2:8082".to_string());
        mock_node_info_provider.add_node(InfluxNodeId::new(3), "http://node-3:8082".to_string());
        let (planner, catalog, iox_session_ctx) =
            create_test_planner_and_catalog_with_node_id_provider("0", Arc::new(mock_node_info_provider)).await;
        let db_name = "test_db_migrating";
        let table_name = "metrics_migrating";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        let source_node_id = InfluxNodeId::new(1);
        let target_node_id_a = InfluxNodeId::new(2);
        let target_node_id_b = InfluxNodeId::new(3);

        let mut shard1_def = ShardDefinition::new(
            ShardId::new(1),
            ShardTimeRange{start_time: 0, end_time: 1000},
            vec![source_node_id],
            None
        );
        shard1_def.migration_status = Some(influxdb3_catalog::shard::ShardMigrationStatus::MigratingOutTo(vec![target_node_id_a, target_node_id_b]));
        catalog.create_shard(db_name, table_name, shard1_def).await.unwrap();

        let logical_plan = iox_session_ctx.state().create_logical_plan(&format!("SELECT * FROM {}", table_name)).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            planner.node_info_provider.clone(),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec");
        let union_exec = distributed_plan.as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 3, "UnionExec should have 3 children (1 original owner + 2 targets)");

        let mut found_source_scan_addr = None;
        let mut found_target_a_scan_addr = None;
        let mut found_target_b_scan_addr = None;

        let expected_sql_query_part = format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name);

        for child_plan in union_exec.children() {
            assert!(child_plan.as_any().is::<RemoteScanExec>(), "Child plan should be RemoteScanExec");
            let remote_exec = child_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();
            let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
            assert!(sql_query.contains(&expected_sql_query_part), "SQL query {} does not contain expected part {}", sql_query, expected_sql_query_part);

            if remote_exec.target_node_address == "http://node-1:8082" {
                found_source_scan_addr = Some(remote_exec.target_node_address.clone());
            } else if remote_exec.target_node_address == "http://node-2:8082" {
                found_target_a_scan_addr = Some(remote_exec.target_node_address.clone());
            } else if remote_exec.target_node_address == "http://node-3:8082" {
                found_target_b_scan_addr = Some(remote_exec.target_node_address.clone());
            }
        }

        assert!(found_source_scan_addr.is_some(), "Did not find RemoteScanExec for source node");
        assert!(found_target_a_scan_addr.is_some(), "Did not find RemoteScanExec for target node A");
        assert!(found_target_b_scan_addr.is_some(), "Did not find RemoteScanExec for target node B");

        Ok(())
    }

    #[test]
    fn test_create_local_shard_logical_plan_filters() -> Result<()> {
        let (_planner, _catalog, iox_session_ctx) = futures::executor::block_on(create_test_planner_and_catalog());
        let db_name = "test_db_local_filters";
        let table_name = "metrics_local_filters";

        let schema = Arc::new(ArrowSchemaOriginal::new(vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let dummy_table_source = Arc::new(influxdb3_catalog::catalog::NoopTableSource::new(Arc::clone(&schema)));

        let shard_time_range = ShardTimeRange { start_time: 100_000_000_000, end_time: 200_000_000_000 };
        let shard_def = ShardDefinition::new(ShardId::new(1), shard_time_range, vec![InfluxNodeId::new(0)], None);

        let physical_scan = TableScan::try_new(
            FileScanConfig {
                object_store_url: datafusion::datasource::object_store::ObjectStoreUrl::parse("file:///")?,
                file_schema: schema.clone(),
                file_groups: vec![],
                statistics: datafusion::physical_plan::Statistics::new_unknown(&schema),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![None],
                infinite_source: false,
            },
            dummy_table_source,
            vec![df_col("host").eq(datafusion::logical_expr::lit("serverX"))],
        )?;

        let current_node_id = Arc::from("0");
        // For this specific test, we need a Planner instance.
        // The planner's own catalog and node_info_provider are not strictly needed for create_local_shard_logical_plan's logic
        // but the constructor requires them.
        let mock_catalog_for_planner = Arc::new(Catalog::new_in_memory("mock_catalog_for_local_plan_test").await.unwrap());
        let mock_node_info_for_planner = Arc::new(MockNodeInfoProvider::new());
        let planner_instance = Planner::new(&iox_session_ctx, mock_catalog_for_planner, current_node_id, mock_node_info_for_planner);


        let local_shard_logical_plan = planner_instance.create_local_shard_logical_plan(
            &physical_scan,
            &shard_def,
            db_name,
            table_name
        )?;

        if let DFLogicalPlan::TableScan(local_scan_details) = local_shard_logical_plan {
            assert_eq!(local_scan_details.filters.len(), 1, "Expected one combined filter expression, got: {:?}", local_scan_details.filters);
            let combined_filter = &local_scan_details.filters[0];

            let filter_str = format!("{:?}", combined_filter);
            assert!(filter_str.contains("time >= TimestampNanosecond(100000000000, None)"), "Filter string does not contain start time: {}", filter_str);
            assert!(filter_str.contains("time <= TimestampNanosecond(200000000000, None)"), "Filter string does not contain end time: {}", filter_str);
            assert!(filter_str.contains("host = Utf8(\"serverX\")"), "Filter string does not contain original predicate: {}", filter_str);
            assert!(filter_str.matches("AND").count() == 2, "Expected 2 ANDs for 3 conditions. Filter string: {}", filter_str);

        } else {
            panic!("Expected a TableScan logical plan, got {:?}", local_shard_logical_plan);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rewriter_projection_on_sharded_table() -> Result<()> {
        let (planner, catalog, iox_session_ctx) = create_test_planner_and_catalog().await;
        let db_name = "test_proj_sharded_db";
        let table_name = "metrics_proj_sharded";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu", FieldDataType::Float), ("mem", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let mut mock_node_info = MockNodeInfoProvider::new();
        mock_node_info.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());

        let iox_session_ctx_inner = planner.ctx.inner(); // Get Arc<IOxSessionContextInner>

        let logical_plan = iox_session_ctx_inner.state().create_logical_plan(
            &format!("SELECT host, cpu FROM \"{}\".\"{}\"", db_name, table_name) // Fully qualify to be safe
        ).await?;
        let initial_physical_plan = iox_session_ctx_inner.create_physical_plan(&logical_plan).await?;

        assert!(initial_physical_plan.as_any().is::<datafusion::physical_plan::projection::ProjectionExec>(), "Initial plan should be ProjectionExec but was {:?}", initial_physical_plan);

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            Arc::new(mock_node_info),
            planner.ctx.clone(), // Pass the IOxSessionContext (Arc wrapper)
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<datafusion::physical_plan::projection::ProjectionExec>(), "Distributed plan should still be ProjectionExec at root, but was {:?}", distributed_plan);
        let proj_exec = distributed_plan.as_any().downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>().unwrap();

        assert!(proj_exec.input().as_any().is::<UnionExec>(), "Child of ProjectionExec should now be UnionExec, but was {:?}", proj_exec.input());
        let union_exec = proj_exec.input().as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 2, "UnionExec should have 2 children (local + remote)");

        for child in union_exec.children() {
            if let Some(remote_exec) = child.as_any().downcast_ref::<RemoteScanExec>() {
                let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
                assert!(sql_query.to_lowercase().contains("select \"host\", \"cpu\" from") || sql_query.to_lowercase().contains("select host, cpu from"),
                        "Remote SQL query does not reflect projection: {}", sql_query);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rewriter_filter_on_sharded_table() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_filter_sharded_db";
        let table_name = "metrics_filter_sharded";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let mut mock_node_info = MockNodeInfoProvider::new();
        mock_node_info.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());

        let iox_session_ctx_inner = planner.ctx.inner();

        let logical_plan = iox_session_ctx_inner.state().create_logical_plan(
            &format!("SELECT * FROM \"{}\".\"{}\" WHERE host = 'serverA'", db_name, table_name)
        ).await?;
        let initial_physical_plan = iox_session_ctx_inner.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            Arc::new(mock_node_info),
            planner.ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        let mut current_plan_for_check = Arc::clone(&distributed_plan);
        if let Some(filter_exec) = distributed_plan.as_any().downcast_ref::<datafusion::physical_plan::filter::FilterExec>() {
            current_plan_for_check = Arc::clone(filter_exec.input());
        }

        assert!(current_plan_for_check.as_any().is::<UnionExec>(), "Underlying plan after potential FilterExec should be UnionExec, but was {:?}", current_plan_for_check);
        let union_exec = current_plan_for_check.as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 2, "UnionExec should have 2 children");

        for child in union_exec.children() {
            if let Some(remote_exec) = child.as_any().downcast_ref::<RemoteScanExec>() {
                let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
                assert!(sql_query.contains("(\"host\") = ('serverA')"), "Remote SQL query does not reflect filter: {}", sql_query);
            } else {
                let local_plan_str = format!("{:?}", child);
                assert!(local_plan_str.contains("host = Utf8(\"serverA\")"), "Local plan string does not reflect filter: {}", local_plan_str);
            }
        }
        Ok(())
    }

    // TODO: Add tests for ProjectionExec(TableScan) and FilterExec(TableScan)
    // These tests will ensure that the rewriter correctly finds and replaces
    // the TableScan node even when it's wrapped by other operators.

    #[tokio::test]
    async fn test_rewriter_projection_on_sharded_table() -> Result<()> {
        let (planner, catalog, iox_session_ctx) = create_test_planner_and_catalog().await; // Uses default current_node_id "0"
        let db_name = "test_proj_sharded_db";
        let table_name = "metrics_proj_sharded";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu", FieldDataType::Float), ("mem", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap(); // Local
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap(); // Remote

        let mut mock_node_info = MockNodeInfoProvider::new();
        mock_node_info.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());

        let logical_plan = iox_session_ctx.state().create_logical_plan(
            &format!("SELECT host, cpu FROM \"{}\".\"{}\"", db_name, table_name)
        ).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        assert!(initial_physical_plan.as_any().is::<ProjectionExec>(), "Initial plan should be ProjectionExec but was {:?}", initial_physical_plan);

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            Arc::new(mock_node_info),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        assert!(distributed_plan.as_any().is::<ProjectionExec>(), "Distributed plan should still be ProjectionExec at root, but was {:?}", distributed_plan);
        let proj_exec = distributed_plan.as_any().downcast_ref::<ProjectionExec>().unwrap();

        assert!(proj_exec.input().as_any().is::<UnionExec>(), "Child of ProjectionExec should now be UnionExec, but was {:?}", proj_exec.input());
        let union_exec = proj_exec.input().as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 2, "UnionExec should have 2 children (local + remote)");

        for child in union_exec.children() {
            if let Some(remote_exec) = child.as_any().downcast_ref::<RemoteScanExec>() {
                let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
                assert!(sql_query.to_lowercase().contains("select \"host\", \"cpu\" from") || sql_query.to_lowercase().contains("select host, cpu from"),
                        "Remote SQL query does not reflect projection: {}", sql_query);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rewriter_filter_on_sharded_table() -> Result<()> {
        let (planner, catalog, iox_session_ctx) = create_test_planner_and_catalog().await;
        let db_name = "test_filter_sharded_db";
        let table_name = "metrics_filter_sharded";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let mut mock_node_info = MockNodeInfoProvider::new();
        mock_node_info.add_node(InfluxNodeId::new(1), "http://node-1:8082".to_string());

        let logical_plan = iox_session_ctx.state().create_logical_plan(
            &format!("SELECT * FROM \"{}\".\"{}\" WHERE host = 'serverA'", db_name, table_name)
        ).await?;
        let initial_physical_plan = iox_session_ctx.create_physical_plan(&logical_plan).await?;

        let rewriter = DistributedQueryRewriter::new(
            Arc::clone(&catalog),
            planner.current_node_id.clone(),
            Arc::new(mock_node_info),
            iox_session_ctx.clone(),
            Arc::from(db_name)
        );
        let distributed_plan = rewriter.optimize(initial_physical_plan.clone(), planner.ctx.state().config())?;

        let mut current_plan_for_check = Arc::clone(&distributed_plan);
        if let Some(filter_exec) = distributed_plan.as_any().downcast_ref::<FilterExec>() {
            current_plan_for_check = Arc::clone(filter_exec.input());
        }

        assert!(current_plan_for_check.as_any().is::<UnionExec>(), "Underlying plan after potential FilterExec should be UnionExec, but was {:?}", current_plan_for_check);
        let union_exec = current_plan_for_check.as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 2, "UnionExec should have 2 children");

        for child in union_exec.children() {
            if let Some(remote_exec) = child.as_any().downcast_ref::<RemoteScanExec>() {
                let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
                assert!(sql_query.contains("(\"host\") = ('serverA')"), "Remote SQL query does not reflect filter: {}", sql_query);
            } else {
                let local_plan_str = format!("{:?}", child);
                assert!(local_plan_str.contains("host = Utf8(\"serverA\")"), "Local plan string does not reflect filter: {}", local_plan_str);
            }
        }
        Ok(())
    }
}
