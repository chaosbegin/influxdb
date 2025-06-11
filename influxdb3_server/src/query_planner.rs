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
use influxdb3_distributed_query_protos::influxdb3::internal::query::v1::{
    ExecuteQueryFragmentRequest as GrpcExecuteQueryFragmentRequest,
};
use arrow_flight::utils::flight_data_stream_to_arrow_stream;
use datafusion_proto::physical_plan::{physical_plan_to_bytes, from_physical_plan_bytes};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;


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
        // This is a simplified traversal and replacement.
        // A full implementation would use a PhysicalPlanRewriter.
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
                        let current_node_id_parsed = self.current_node_id.parse::<u64>().ok(); // Assuming NodeId is u64

                        for shard_def in table_def.shards.resource_iter() {
                            let is_local = shard_def.node_ids.iter().any(|id| Some(id.get()) == current_node_id_parsed);

                            if is_local {
                                // CONCEPTUAL: Create a local scan for this specific shard.
                                // This requires TableProvider to accept shard_id or specific file lists.
                                // For now, if any shard is local, we might execute the original plan locally,
                                // or a more refined local plan for that shard.
                                // Simplified: Re-use original plan for local portion if it covers the local shard.
                                // This is not ideal as it doesn't isolate the scan to *only* the local shard's data.
                                // A true implementation needs to filter the scan to the specific shard's data.
                                // For this conceptual step, let's assume the original plan can be used if one shard is local.
                                // If multiple local shards, this simple model breaks down without proper filtering.
                                observability_deps::tracing::info!(shard_id = %shard_def.id.get(), "Planning local scan for shard.");
                                // Placeholder: create a scan for *this* shard.
                                // This might involve creating a new LogicalPlan::TableScan with filters for the shard,
                                // then planning it. For now, we'll clone the input plan as a placeholder for a local shard scan.
                                // This is NOT CORRECT for filtering to a single shard, but demonstrates structure.
                                sub_plans.push(physical_plan.clone()); // Placeholder for actual local shard scan
                            } else {
                                // Remote shard: pick a target node
                                if let Some(target_node_id_obj) = shard_def.node_ids.first() {
                                    if let Some(target_node_address) = self.node_info_provider.get_node_query_rpc_address(target_node_id_obj) {
                                        observability_deps::tracing::info!(shard_id = %shard_def.id.get(), target_node=%target_node_address, "Planning remote scan for shard.");

                                        // Create a simplified logical plan for the remote scan (scan the whole table)
                                        // A real implementation would push down filters relevant to this shard.
                                        let remote_logical_plan = DFLogicalPlan::TableScan(datafusion::logical_expr::TableScan {
                                            table_name: datafusion::logical_expr::TableReference::bare(table_name.to_string()),
                                            source: provider_as_source(Arc::clone(table_scan.source())), // Simplified
                                            projection: table_scan.projection().cloned(),
                                            filters: table_scan.filters().to_vec(), // Send original filters
                                            fetch: table_scan.fetch(),
                                            table_schema: Arc::clone(table_scan.table_schema()),
                                            projected_schema: table_scan.schema(), // Schema of this scan node
                                        });

                                        // Serialize this logical plan (or a physical plan derived from it)
                                        // For simplicity, let's try to serialize the logical plan if datafusion-proto supports it easily,
                                        // otherwise, create a physical plan from it first.
                                        // datafusion_proto focuses on physical plans, so we'll make a dummy physical plan.
                                        // This is highly conceptual as the remote node needs to be able to execute it.
                                        // A more robust way is to define a specific "remote shard scan" message.

                                        // Create a minimal physical plan for serialization.
                                        // This is a placeholder for what would be a meaningful sub-plan.
                                        // Here, we use the original table_scan's schema.
                                        let scan_for_remote = TableScan::try_new(
                                            table_scan.table_name(),
                                            table_scan.table_schema(),
                                            table_scan.projection().cloned(),
                                        )?.with_filters(table_scan.filters())?.with_fetch(table_scan.fetch())?;

                                        let physical_plan_for_remote = Arc::new(scan_for_remote);

                                        let plan_bytes = physical_plan_to_bytes(physical_plan_for_remote)
                                            .map_err(|e| DataFusionError::Plan(format!("Failed to serialize remote plan fragment: {}", e)))?;

                                        let remote_scan = RemoteScanExec::new(
                                            target_node_address,
                                            db_name.to_string(),
                                            plan_bytes,
                                            table_scan.schema(), // Expected schema from this remote operation
                                        );
                                        sub_plans.push(Arc::new(remote_scan));
                                    } else {
                                        observability_deps::tracing::warn!(shard_id = %shard_def.id.get(), node_id = %target_node_id_obj.get(), "No RPC address found for remote node.");
                                    }
                                }
                            }
                        }

                        if sub_plans.is_empty() {
                            // No shards could be planned (e.g., no node addresses)
                            observability_deps::tracing::warn!(%db_name, %table_name, "Sharded table resulted in no scannable shards.");
                            return Ok(physical_plan); // Return original plan or an empty plan
                        } else if sub_plans.len() == 1 {
                            return Ok(sub_plans.remove(0));
                        } else {
                            // Combine with UnionExec. Ensure schemas are compatible (should be, as they originate from the same TableScan)
                            return Ok(Arc::new(UnionExec::new(sub_plans)));
                        }
                    }
                }
            }
        }
        // If not a TableScan we can handle, or not sharded, return original plan
        Ok(physical_plan)
    }
}

// --- RemoteScanExec: For distributed query fragment execution ---
#[derive(Debug)]
struct RemoteScanExec {
    target_node_address: String,
    db_name: String,
    plan_or_query_bytes: Vec<u8>,
    expected_schema: SchemaRef,
    cache: PlanProperties,
}

impl RemoteScanExec {
    #[allow(dead_code)]
    fn new(
        target_node_address: String,
        db_name: String,
        plan_or_query_bytes: Vec<u8>,
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

fn serialize_schema(schema: &SchemaRef) -> Result<Vec<u8>, ArrowError> {
    let mut writer = arrow_ipc::writer::FileWriter::try_new(vec![], schema.as_ref())?;
    writer.finish()?;
    Ok(writer.into_inner()?)
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
        observability_deps::tracing::info!(
            target_node = %self.target_node_address,
            db_name = %self.db_name,
            plan_bytes_len = %self.plan_or_query_bytes.len(),
            "Executing RemoteScanExec"
        );

        let expected_schema_bytes = serialize_schema(&self.expected_schema)
            .map_err(|e| DataFusionError::Execution(format!("Failed to serialize expected schema: {}", e)))?;

        let request = GrpcExecuteQueryFragmentRequest {
            database_name: self.db_name.clone(),
            plan_bytes: self.plan_or_query_bytes.clone(),
            shard_id: None,
            session_config: HashMap::new(),
            query_id: context.task_id().unwrap_or_else(|| format!("remote_scan_{}", uuid::Uuid::new_v4())).to_string(),
            expected_schema_bytes,
        };

        let target_address = self.target_node_address.clone();
        let expected_schema_cloned = Arc::clone(&self.expected_schema);

        let stream_fut = async move {
            let mut query_client = GrpcDistributedQueryClient::new(target_address).await;

            match query_client.execute_query_fragment(request).await {
                Ok(response) => {
                    let flight_data_stream = response.into_inner();
                    let record_batch_stream = flight_data_stream_to_arrow_stream(
                        flight_data_stream,
                        expected_schema_cloned,
                        &[],
                    ).await.map_err(|e| DataFusionError::Execution(format!("Failed to adapt FlightData stream: {}", e)));

                    record_batch_stream
                }
                Err(status) => {
                    Err(DataFusionError::Execution(format!("Remote query fragment execution failed: {}", status)))
                }
            }
        };

        let record_batch_stream = futures::executor::block_on(stream_fut)?;
        Ok(Box::pin(record_batch_stream))
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
    // use datafusion::datasource::provider_as_source; // Already imported
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    // use datafusion_proto::physical_plan::{from_physical_plan_bytes, physical_plan_to_bytes}; // Already imported
    use std::path::Path;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use influxdb3_catalog::shard::{ShardDefinition, ShardId, ShardTimeRange, ShardingStrategy as CatalogShardingStrategy};
    use influxdb3_catalog::log::FieldDataType;


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

        let plan_bytes = physical_plan_to_bytes(physical_plan_original.clone())
            .map_err(|e| DataFusionError::Execution(format!("Failed to serialize plan: {}", e)))?;

        let physical_plan_deserialized = from_physical_plan_bytes(&plan_bytes, &ctx, ctx.runtime_env().as_ref())
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

    // Mock NodeInfoProvider for planner tests
    impl NodeInfoProvider for Arc<Catalog> {
        fn get_node_query_rpc_address(&self, node_id: &InfluxNodeId) -> Option<String> {
            // In a real scenario, catalog would store node RPC addresses.
            // For testing, we can hardcode or make catalog's NodeDefinition store this.
            // For now, let's assume node_id.get() gives a u64, and we map it.
            Some(format!("http://node-{}:8082", node_id.get()))
        }
    }

    async fn create_test_planner_and_catalog() -> (Planner, Arc<Catalog>) {
        let catalog = Arc::new(Catalog::new_in_memory("test_planner_host").await.unwrap());
        let session_ctx = IOxSessionContext::new_default();
        let current_node_id = Arc::from("0"); // Assume current node_id is "0" (as u64) for tests

        // The catalog itself can act as a NodeInfoProvider for this test.
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
        catalog.update_table_sharding_strategy(db_name, table_name, CatalogShardingStrategy::Time, None).await.unwrap(); // Simple time based

        // Add two remote shards
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(2)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert_ne!(format!("{:?}", distributed_plan), format!("{:?}", initial_physical_plan), "Plan should change for remote sharded table");
        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (remote shards)");
        for child in distributed_plan.children() {
            assert!(child.as_any().is::<RemoteScanExec>(), "Child should be RemoteScanExec");
            let remote_exec = child.as_any().downcast_ref::<RemoteScanExec>().unwrap();
            assert!(remote_exec.target_node_address.starts_with("http://node-"));
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

        // Current node_id for planner is "0"
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        // Current simplified logic for local shards re-uses the original plan.
        // If multiple local shards, it will be a Union of these cloned original plans.
        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for multiple local shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local shards)");
        for child in distributed_plan.children() {
            // In this simplified version, children are clones of the original plan.
            // A more accurate test would check if they are specific local shard scans.
            assert_eq!(format!("{:?}", child), format!("{:?}", initial_physical_plan));
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

        // Current node_id for planner is "0"
        // Local Shard
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(1), ShardTimeRange{start_time: 0, end_time: 1000}, vec![InfluxNodeId::new(0)], None)).await.unwrap();
        // Remote Shard
        catalog.create_shard(db_name, table_name, ShardDefinition::new(ShardId::new(2), ShardTimeRange{start_time: 1001, end_time: 2000}, vec![InfluxNodeId::new(1)], None)).await.unwrap();

        let logical_plan = planner.ctx.table(table_name).await?.to_logical_plan()?;
        let initial_physical_plan = planner.ctx.create_physical_plan(&logical_plan).await?;

        let distributed_plan = planner.generate_distributed_physical_plan(initial_physical_plan.clone(), db_name).await?;

        assert!(distributed_plan.as_any().is::<UnionExec>(), "Plan should be a UnionExec for mixed shards");
        assert_eq!(distributed_plan.children().len(), 2, "UnionExec should have 2 children (local + remote)");

        let mut found_local_placeholder = false;
        let mut found_remote_scan = false;

        for child in distributed_plan.children() {
            if child.as_any().is::<RemoteScanExec>() {
                found_remote_scan = true;
                let remote_exec = child.as_any().downcast_ref::<RemoteScanExec>().unwrap();
            assert!(remote_exec.target_node_address.starts_with("http://node-1"));
            } else if format!("{:?}", child) == format!("{:?}", initial_physical_plan) {
                // This identifies our placeholder local scan
                found_local_placeholder = true;
            }
        }
        assert!(found_local_placeholder, "Did not find placeholder for local shard scan");
        assert!(found_remote_scan, "Did not find RemoteScanExec for remote shard");

        Ok(())
    }
}
