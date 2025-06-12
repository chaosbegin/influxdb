use std::{any::Any, sync::Arc, collections::HashMap, fmt::Debug}; // Added HashMap, Debug

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
    logical_expr::LogicalPlan as DFLogicalPlan, // Aliased to avoid clash
    datasource::physical_plan::TableScan, // Assuming we might construct this
};
use datafusion::prelude::SessionContext; // For creating plans
use futures::StreamExt;
use influxdb_influxql_parser::statement::Statement;
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner, physical_optimizer::PhysicalOptimizer, Optimizer};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::NodeId as InfluxNodeId; // Using the ID type from influxdb3_id
use crate::distributed_query_client::GrpcDistributedQueryClient;
// GrpcExecuteQueryFragmentRequest is used by RemoteRecordBatchStream, not directly here anymore for SQL.
// use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::{
// ExecuteQueryFragmentRequest as GrpcExecuteQueryFragmentRequest,
// };
use arrow_flight::utils::flight_data_stream_to_arrow_stream;
// physical_plan_to_bytes is not used for SQL fragments.
// use datafusion_proto::physical_plan::{physical_plan_to_bytes, from_physical_plan_bytes};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use crate::expr_to_sql; // For converting DataFusion Expr to SQL
use chrono::{DateTime, Utc}; // For formatting timestamps


type Result<T, E = DataFusionError> = std::result::Result<T, E>;

/// Provides information about other nodes in the cluster.
pub trait NodeInfoProvider: Send + Sync + Debug {
    /// Returns the gRPC address for distributed query execution for a given node ID.
    fn get_node_query_rpc_address(&self, node_id: &InfluxNodeId) -> Option<String>;
}

// A mock implementation for testing
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


pub(crate) struct Planner {
    ctx: IOxSessionContext,
    catalog: Arc<Catalog>,
    current_node_id: Arc<str>,
    node_info_provider: Arc<dyn NodeInfoProvider>, // Added
}

impl Planner {
    pub(crate) fn new(
        ctx: &IOxSessionContext,
        catalog: Arc<Catalog>,
        current_node_id: Arc<str>,
        node_info_provider: Arc<dyn NodeInfoProvider>, // Added
    ) -> Self {
        Self {
            ctx: ctx.child_ctx("rest_api_query_planner"),
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
        let ctx = self.ctx.child_ctx("rest_api_query_planner_sql");

        let logical_plan = planner.query_to_logical_plan(query, &ctx).await?;
        let initial_physical_plan = ctx.create_physical_plan(&logical_plan).await?;
        self.generate_distributed_physical_plan(initial_physical_plan, ctx.default_database_name().as_str()).await
    }

    pub(crate) async fn influxql(
        &self,
        statement: Statement,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.ctx.child_ctx("rest_api_query_planner_influxql");

        let logical_plan = InfluxQLQueryPlanner::statement_to_plan(statement, params, &ctx).await?;
        let initial_physical_plan = ctx.create_physical_plan(&logical_plan).await?;
        let distributed_physical_plan = self.generate_distributed_physical_plan(initial_physical_plan, ctx.default_database_name().as_str()).await?;

        // Preserve metadata from logical plan if needed (SchemaExec wrapper)
        let input_schema = distributed_physical_plan.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone());
        let final_schema = Arc::new(ArrowSchema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));

        Ok(Arc::new(SchemaExec::new(distributed_physical_plan, final_schema)))
    }

    async fn generate_distributed_physical_plan(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        db_name: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(table_scan) = physical_plan.as_any().downcast_ref::<TableScan>() {
            let table_name = table_scan.table_name();
            observability_deps::tracing::debug!(%db_name, %table_name, "Checking table for sharding in physical plan");

            if let Some(db_schema) = self.catalog.db_schema(db_name) {
                if let Some(table_def) = db_schema.table_definition(table_name) {
                    if !table_def.shards.is_empty() {
                        observability_deps::tracing::info!(
                            %db_name, %table_name, num_shards=%table_def.shards.len(),
                            "Table is sharded, generating distributed plan components."
                        );

                        let mut sub_plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
                        let current_node_id_parsed = self.current_node_id.parse::<u64>().ok();

                        for shard_def_arc in table_def.shards.resource_iter() {
                            let shard_def = shard_def_arc.as_ref();

                            let generate_sql_for_shard = |sd: &influxdb3_catalog::shard::ShardDefinition|
                                -> Result<Bytes, DataFusionError> {
                                let projected_columns_str = match table_scan.projection() {
                                    Some(indices) => indices
                                        .iter()
                                        .map(|i| table_scan.table_schema().field(*i).name().as_str())
                                        .collect::<Vec<&str>>()
                                        .join(", "),
                                    None => "*".to_string(),
                                };
                                let qualified_table_name_str = format!("\"{}\".\"{}\"", db_name, table_name);
                                let mut sql_filters: Vec<String> = Vec::new();
                                let shard_time_range = sd.time_range;
                                let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(
                                    chrono::NaiveDateTime::from_timestamp_opt(shard_time_range.start_time / 1_000_000_000, (shard_time_range.start_time % 1_000_000_000) as u32).unwrap_or_default(), Utc
                                ).to_rfc3339();
                                let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(
                                    chrono::NaiveDateTime::from_timestamp_opt(shard_time_range.end_time / 1_000_000_000, (shard_time_range.end_time % 1_000_000_000) as u32).unwrap_or_default(), Utc
                                ).to_rfc3339();
                                sql_filters.push(format!("\"time\" >= TIMESTAMP '{}'", start_time_str));
                                sql_filters.push(format!("\"time\" <= TIMESTAMP '{}'", end_time_str));
                                for filter_expr in table_scan.filters() {
                                    match expr_to_sql::expr_to_sql(filter_expr) {
                                        Ok(sql_pred) => sql_filters.push(sql_pred),
                                        Err(e) => {
                                            observability_deps::tracing::warn!(
                                                "Failed to convert filter expression to SQL, skipping filter: {:?}. Error: {}",
                                                filter_expr, e
                                            );
                                        }
                                    }
                                }
                                let sql_query = if sql_filters.is_empty() {
                                    format!("SELECT {} FROM {}", projected_columns_str, qualified_table_name_str)
                                } else {
                                    format!("SELECT {} FROM {} WHERE {}", projected_columns_str, qualified_table_name_str, sql_filters.join(" AND "))
                                };
                                observability_deps::tracing::debug!(%sql_query, shard_id = %sd.id.get(), "Generated SQL query for shard fragment");
                                Ok(Bytes::from(sql_query.into_bytes()))
                            };

                            let mut plans_for_this_shard_instance: Vec<Arc<dyn ExecutionPlan>> = vec![];

                            for owner_node_id in &shard_def.node_ids {
                                let is_owner_local = Some(owner_node_id.get()) == current_node_id_parsed;
                                if is_owner_local {
                                    let local_shard_logical_plan = self.create_local_shard_logical_plan(
                                        table_scan,
                                        shard_def,
                                        db_name,
                                        table_name,
                                    )?;
                                    let local_shard_physical_plan = self.ctx.create_physical_plan(&local_shard_logical_plan).await?;
                                    plans_for_this_shard_instance.push(local_shard_physical_plan);
                                } else {
                                    if let Some(target_node_address) = self.node_info_provider.get_node_query_rpc_address(owner_node_id) {
                                        observability_deps::tracing::info!(shard_id = %shard_def.id.get(), target_node=%target_node_address, "Planning remote scan for current owner of shard.");
                                        let sql_query_bytes = generate_sql_for_shard(shard_def)?;
                                        let remote_scan = RemoteScanExec::new(
                                            target_node_address,
                                            db_name.to_string(),
                                            sql_query_bytes,
                                            table_scan.schema(),
                                        );
                                        plans_for_this_shard_instance.push(Arc::new(remote_scan));
                                    } else {
                                        observability_deps::tracing::warn!("No RPC address for current owner {} of shard {}", owner_node_id.get(), shard_def.id.get());
                                    }
                                }
                            }

                            if let Some(influxdb3_catalog::shard::ShardMigrationStatus::MigratingOutTo(targets)) = &shard_def.migration_status {
                                for target_node_id in targets {
                                    if !shard_def.node_ids.contains(target_node_id) {
                                        if let Some(target_node_address) = self.node_info_provider.get_node_query_rpc_address(target_node_id) {
                                            observability_deps::tracing::info!(
                                                shard_id = %shard_def.id.get(),
                                                target_node=%target_node_address,
                                                migrating_to_node=%target_node_id.get(),
                                                "Shard is migrating. Adding scan for target node to query plan."
                                            );
                                            let sql_query_bytes = generate_sql_for_shard(shard_def)?;
                                            let remote_scan = RemoteScanExec::new(
                                                target_node_address,
                                                db_name.to_string(),
                                                sql_query_bytes,
                                                table_scan.schema(),
                                            );
                                            plans_for_this_shard_instance.push(Arc::new(remote_scan));
                                        } else {
                                            observability_deps::tracing::warn!("No RPC address for migration target {} of shard {}", target_node_id.get(), shard_def.id.get());
                                        }
                                    }
                                }
                            }
                            sub_plans.extend(plans_for_this_shard_instance);
                        }

                        if sub_plans.is_empty() {
                            observability_deps::tracing::warn!(%db_name, %table_name, "Sharded table resulted in no scannable shards.");
                            return Ok(physical_plan);
                        } else if sub_plans.len() == 1 {
                            return Ok(sub_plans.remove(0));
                        } else {
                            return Ok(Arc::new(UnionExec::new(sub_plans)));
                        }
                    }
                }
            }
        }
        Ok(physical_plan)
    }

    // Helper to create a shard-specific logical plan for local execution
    fn create_local_shard_logical_plan(
        &self,
        original_table_scan_physical: &TableScan, // The original physical TableScan node
        shard_def: &influxdb3_catalog::shard::ShardDefinition,
        db_name: &str,
        table_name: &str,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        observability_deps::tracing::debug!(shard_id = %shard_def.id.get(), "Creating logical plan for local shard.");

        let mut local_filters: Vec<datafusion::logical_expr::Expr> = Vec::new();
        let time_col = datafusion::logical_expr::col("time"); // Assuming "time" is the standard time column name

        // Time Range Filter from ShardDefinition
        let start_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.start_time), None));
        let end_lit = datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(shard_def.time_range.end_time), None));

        local_filters.push(time_col.clone().gt_eq(start_lit));
        local_filters.push(time_col.lt_eq(end_lit));

        // Original Predicates from the physical TableScan (which holds logical Exprs)
        local_filters.extend(original_table_scan_physical.filters().to_vec());

        // Combine all filters with AND
        let combined_filter_opt = local_filters.into_iter().reduce(datafusion::logical_expr::Expr::and);
        let final_filters = combined_filter_opt.map_or_else(Vec::new, |f| vec![f]);

        // Create New Filtered Logical TableScan for Local Shard
        // We need to use the original table schema, not the projected schema from the physical plan,
        // as filters apply to the base table schema.
        let new_logical_scan = DFLogicalPlan::TableScan(datafusion::logical_expr::TableScan::try_new(
            datafusion::logical_expr::TableReference::partial(db_name, table_name), // Use partial for db.table
            Arc::clone(original_table_scan_physical.source()),
            original_table_scan_physical.projection().cloned(), // Projection from original physical scan
            final_filters, // Use combined filter
            original_table_scan_physical.fetch(),
        )?);

        Ok(new_logical_scan)
    }
}

// --- RemoteScanExec: For distributed query fragment execution ---
use crate::remote_stream::RemoteRecordBatchStream; // Added for RemoteScanExec

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
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    use std::path::Path;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use influxdb3_catalog::shard::{ShardDefinition, ShardId, ShardTimeRange, ShardingStrategy as CatalogShardingStrategy};
    use influxdb3_catalog::log::FieldDataType;
    use datafusion::logical_expr::Operator; // For test_create_local_shard_logical_plan_filters


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

    impl NodeInfoProvider for Arc<Catalog> {
        fn get_node_query_rpc_address(&self, node_id: &InfluxNodeId) -> Option<String> {
            Some(format!("http://node-{}:8082", node_id.get()))
        }
    }

    async fn create_test_planner_and_catalog() -> (Planner, Arc<Catalog>) {
        let catalog = Arc::new(Catalog::new_in_memory("test_planner_host").await.unwrap());
        let session_ctx = IOxSessionContext::new_default();
        let current_node_id = Arc::from("0");
        let planner = Planner::new(&session_ctx, Arc::clone(&catalog), current_node_id, Arc::clone(&catalog) as Arc<dyn NodeInfoProvider>);
        (planner, catalog)
    }

    #[tokio::test]
    async fn test_distributed_plan_non_sharded_table() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_db_non_sharded";
        let table_name = "my_table";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag_a"], &[("val", FieldDataType::Integer)]).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert_eq!(format!("{:?}", distributed_plan), format!("{:?}", initial_physical_plan), "Plan should not change for non-sharded table");
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_all_remote_shards() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_db_remote";
        let table_name = "metrics_remote";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(2)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert_ne!(format!("{:?}", distributed_plan), format!("{:?}", initial_physical_plan), "Plan should change for remote sharded table");
        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (remote shards)");

        for (i, child) in distributed_plan.children().iter().enumerate() {
            assert!(child.as_any().is::<RemoteScanExec>(), "Child should be RemoteScanExec");
            let remote_exec = child.as_any().downcast_ref::<RemoteScanExec>().unwrap();

            let expected_node_id = i + 1;
            assert_eq!(remote_exec.target_node_address, format!("http://node-{}:8082", expected_node_id));

            let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).expect("SQL query bytes are not valid UTF-8");
            observability_deps::tracing::info!(%sql_query, "Generated SQL for remote shard");

            assert!(sql_query.contains(&format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name)), "SQL query does not contain correct SELECT and FROM clauses");

            if expected_node_id == 1 {
                let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0,0).unwrap_or_default(), Utc).to_rfc3339();
                let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0,1000).unwrap_or_default(), Utc).to_rfc3339();
                assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str)), "Missing start time filter for shard 1");
                assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str)), "Missing end time filter for shard 1");
            } else {
                let start_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0,1001).unwrap_or_default(), Utc).to_rfc3339();
                let end_time_str = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0,2000).unwrap_or_default(), Utc).to_rfc3339();
                assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str)), "Missing start time filter for shard 2");
                assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str)), "Missing end time filter for shard 2");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_all_local_shards() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_db_local";
        let table_name = "metrics_local";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        let shard1_time_range = ShardTimeRange{start_time: 0, end_time: 1000};
        let shard2_time_range = ShardTimeRange{start_time: 1001, end_time: 2000};
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), shard1_time_range, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), shard2_time_range, vec![InfluxNodeId::new(0)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?
            .filter(datafusion::logical_expr::col("host").eq(datafusion::logical_expr::lit("server1")))?
            .to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for multiple local shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local shards)");

        for child_plan in distributed_plan.children() {
            assert_ne!(format!("{:?}", child_plan), format!("{:?}", initial_physical_plan), "Local shard plan should not be identical to original plan");
            let plan_str = format!("{:?}", child_plan);
            assert!(plan_str.contains("FilterExec") || plan_str.contains("CoalesceBatchesExec") || plan_str.contains("ProjectionExec"),
                     "Local shard plan expected to contain FilterExec, CoalesceBatchesExec or ProjectionExec, but was: {}", plan_str);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_mixed_shards() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_db_mixed";
        let table_name = "metrics_mixed";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        let local_shard_time_range = ShardTimeRange{start_time: 0, end_time: 1000};
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), local_shard_time_range, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for mixed shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local + remote)");

        let mut found_local_filtered_scan = false;
        let mut found_remote_scan = false;

        for child in distributed_plan.children() {
            if child.as_any().is::<RemoteScanExec>() {
                found_remote_scan = true;
            } else {
                found_local_filtered_scan = true;
                assert_ne!(format!("{:?}", child), format!("{:?}", initial_physical_plan), "Local shard plan should not be identical to original plan");
                 let plan_str = format!("{:?}", child);
                 assert!(plan_str.contains("FilterExec") || plan_str.contains("CoalesceBatchesExec") || plan_str.contains("ProjectionExec"),
                         "Local shard plan expected to contain FilterExec, CoalesceBatchesExec or ProjectionExec, but was: {}", plan_str);
            }
        }
        assert!(found_local_filtered_scan, "Did not find filtered plan for local shard scan");
        assert!(found_remote_scan, "Did not find RemoteScanExec for remote shard");

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_remote_shard_with_predicates() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
        let db_name = "test_db_remote_pred";
        let table_name = "metrics_remote_pred";
        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["host", "region"], &[("cpu_usage", FieldDataType::Float)]).await.unwrap();
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap();

        let shard_start_time_ns = 0;
        let shard_end_time_ns = 1000;
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: shard_start_time_ns, end_time: shard_end_time_ns}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?
            .filter(datafusion::logical_expr::col("host").eq(datafusion::logical_expr::lit("serverA")))?
            .filter(datafusion::logical_expr::col("cpu_usage").gt(datafusion::logical_expr::lit(0.5)))?
            .to_logical_plan()?;

        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert!(distributed_plan.as_any().is::<RemoteScanExec>(), "Plan should be a RemoteScanExec");
        let remote_exec = distributed_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();

        let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).expect("SQL query bytes are not valid UTF-8");
        observability_deps::tracing::info!(%sql_query, "Generated SQL for remote shard with predicates");

        assert!(sql_query.contains(&format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name)));

        let start_time_str_expected = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(shard_start_time_ns / 1_000_000_000, (shard_start_time_ns % 1_000_000_000) as u32).unwrap_or_default(), Utc).to_rfc3339();
        let end_time_str_expected = DateTime::<Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(shard_end_time_ns / 1_000_000_000, (shard_end_time_ns % 1_000_000_000) as u32).unwrap_or_default(), Utc).to_rfc3339();

        assert!(sql_query.contains(&format!("\"time\" >= TIMESTAMP '{}'", start_time_str_expected)));
        assert!(sql_query.contains(&format!("\"time\" <= TIMESTAMP '{}'", end_time_str_expected)));
        assert!(sql_query.contains("(\"host\") = ('serverA')"));
        assert!(sql_query.contains("(\"cpu_usage\") > (0.5)"));
        assert!(sql_query.contains(" AND "));

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_plan_query_migrating_shard() -> Result<()> {
        let (planner, catalog) = create_test_planner_and_catalog().await;
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

        let mut mock_node_info = MockNodeInfoProvider::new();
        mock_node_info.add_node(source_node_id, "http://node-1:8082".to_string());
        mock_node_info.add_node(target_node_id_a, "http://node-2:8082".to_string());
        mock_node_info.add_node(target_node_id_b, "http://node-3:8082".to_string());

        let planner_with_mock_nodes = Planner::new(
            &planner.ctx,
            Arc::clone(&catalog),
            Arc::from("0"),
            Arc::new(mock_node_info)
        );

        let logical_plan = planner_with_mock_nodes.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner_with_mock_nodes.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner_with_mock_nodes.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec");
        let union_exec = distributed_plan.as_any().downcast_ref::<UnionExec>().unwrap();
        assert_eq!(union_exec.children().len(), 3, "UnionExec should have 3 children (1 original owner + 2 targets)");

        let mut found_source_scan = false;
        let mut found_target_a_scan = false;
        let mut found_target_b_scan = false;

        let expected_sql_query_part = format!("SELECT * FROM \"{}\".\"{}\"", db_name, table_name);

        for child_plan in union_exec.children() {
            assert!(child_plan.as_any().is::<RemoteScanExec>(), "Child plan should be RemoteScanExec");
            let remote_exec = child_plan.as_any().downcast_ref::<RemoteScanExec>().unwrap();
            let sql_query = String::from_utf8(remote_exec.plan_or_query_bytes.to_vec()).unwrap();
            assert!(sql_query.contains(&expected_sql_query_part));

            if remote_exec.target_node_address == "http://node-1:8082" {
                found_source_scan = true;
            } else if remote_exec.target_node_address == "http://node-2:8082" {
                found_target_a_scan = true;
            } else if remote_exec.target_node_address == "http://node-3:8082" {
                found_target_b_scan = true;
            }
        }

        assert!(found_source_scan, "Did not find RemoteScanExec for source node");
        assert!(found_target_a_scan, "Did not find RemoteScanExec for target node A");
        assert!(found_target_b_scan, "Did not find RemoteScanExec for target node B");

        Ok(())
    }

    #[test]
    fn test_create_local_shard_logical_plan_filters() -> Result<()> {
        let (planner, catalog) = futures::executor::block_on(create_test_planner_and_catalog());
        let db_name = "test_db_local_filters";
        let table_name = "metrics_local_filters";

        futures::executor::block_on(catalog.create_database(db_name)).unwrap();
        futures::executor::block_on(catalog.create_table(db_name, table_name, &["host", "region"], &[("value", FieldDataType::Float)] )).unwrap();

        let shard_time_range = ShardTimeRange { start_time: 100_000_000_000, end_time: 200_000_000_000 };
        let shard_def = ShardDefinition::new(ShardId::new(1), shard_time_range, vec![InfluxNodeId::new(0)], None);

        let session_ctx = SessionContext::new();
        let dummy_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("time", DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None), false),
            Field::new("host", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let dummy_table_source = Arc::new(influxdb3_catalog::catalog::NoopTableSource::new(dummy_schema));

        let logical_plan_orig = DFLogicalPlan::TableScan(datafusion::logical_expr::TableScan::try_new(
            datafusion::logical_expr::TableReference::bare(table_name),
            dummy_table_source,
            None,
            vec![datafusion::logical_expr::col("host").eq(datafusion::logical_expr::lit("serverX"))],
            None,
        )?);
        let physical_plan_orig = futures::executor::block_on(session_ctx.create_physical_plan(&logical_plan_orig))?;
        let table_scan_orig = physical_plan_orig.as_any().downcast_ref::<TableScan>().unwrap();

        let local_shard_logical_plan = planner.create_local_shard_logical_plan(
            table_scan_orig,
            &shard_def,
            db_name,
            table_name
        )?;

        if let DFLogicalPlan::TableScan(local_scan_details) = local_shard_logical_plan {
            assert_eq!(local_scan_details.filters.len(), 1, "Expected one combined filter expression, got: {:?}", local_scan_details.filters);
            let combined_filter = &local_scan_details.filters[0];

            let filter_str = format!("{:?}", combined_filter);
            assert!(filter_str.contains("time >= TimestampNanosecond(100000000000, None)"), "Filter string: {}", filter_str);
            assert!(filter_str.contains("time <= TimestampNanosecond(200000000000, None)"), "Filter string: {}", filter_str);
            assert!(filter_str.contains("host = Utf8(\"serverX\")"), "Filter string: {}", filter_str);
            assert!(filter_str.matches("AND").count() == 2, "Expected 2 ANDs for 3 conditions. Filter string: {}", filter_str);

        } else {
            panic!("Expected a TableScan logical plan");
        }
        Ok(())
    }
}
