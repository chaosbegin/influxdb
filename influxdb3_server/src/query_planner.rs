use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    },
};
use influxdb_influxql_parser::statement::Statement;
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

type Result<T, E = DataFusionError> = std::result::Result<T, E>;

/// A query planner for creating physical query plans for SQL/InfluxQL queries made through the REST
/// API using a separate threadpool.
///
/// This is based on the similar implementation for the planner in the flight service [here][ref].
///
use influxdb3_catalog::catalog::Catalog; // Added for Catalog access

/// [ref]: https://github.com/influxdata/influxdb3_core/blob/6fcbb004232738d55655f32f4ad2385523d10696/service_grpc_flight/src/planner.rs#L24-L33
pub(crate) struct Planner {
    ctx: IOxSessionContext,
    catalog: Arc<Catalog>,        // Added for sharding info
    current_node_id: Arc<str>, // Added for local/remote distinction
}

impl Planner {
    /// Create a new `Planner`
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

    /// Plan a SQL query and return a DataFusion physical plan
    pub(crate) async fn sql(
        &self,
        query: impl AsRef<str> + Send,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("rest_api_query_planner_sql");

        let logical_plan = planner.query_to_logical_plan(query, &ctx).await?;
        self.distribute_plan_if_sharded(&logical_plan, "SQL").await?; // Conceptual sharding check
        ctx.create_physical_plan(&logical_plan).await
    }

    /// Plan an InfluxQL query and return a DataFusion physical plan
    pub(crate) async fn influxql(
        &self,
        statement: Statement,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.ctx.child_ctx("rest_api_query_planner_influxql");

        let logical_plan = InfluxQLQueryPlanner::statement_to_plan(statement, params, &ctx).await?;
        self.distribute_plan_if_sharded(&logical_plan, "InfluxQL").await?; // Conceptual sharding check
        let input = ctx.create_physical_plan(&logical_plan).await?;
        let input_schema = input.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone());
        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));

        Ok(Arc::new(SchemaExec::new(input, schema)))
    }

    async fn distribute_plan_if_sharded(
        &self,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
        query_type: &str,
    ) -> Result<()> {
        use datafusion::logical_expr::LogicalPlan;
        use observability_deps::tracing::debug;

        // Collect table names from the logical plan
        let mut table_names = std::collections::HashSet::new();
        logical_plan.collect_table_scan_table_names(&mut table_names);

        for table_name_ref in table_names {
            let table_name = table_name_ref.table();
            // Assume tables are in the default database context of the session for simplicity.
            // IOxSessionContext holds a default catalog and schema.
            // The catalog name is usually "public" or "datafusion", schema name "public".
            // The actual database name for our catalog lookup comes from the session context.
            // This part might need more robust database name resolution if queries can span DBs.
            let default_db_name = self.ctx.default_database_name();

            if let Some(db_schema) = self.catalog.db_schema(&default_db_name) {
                if let Some(table_def) = db_schema.table_definition(table_name) {
                    if !table_def.shards.is_empty() {
                        debug!(
                            db_name = %default_db_name,
                            table_name = %table_name,
                            query_type = %query_type,
                            num_shards = %table_def.shards.len(),
                            current_node_id = %self.current_node_id,
                            "Query targets a sharded table. Conceptual distributed planning would occur here."
                        );
                        // --- Conceptual Distributed Planning Logic ---
                        // 1. Identify relevant shards based on query predicates (e.g., time range filters).
                        //    - This requires predicate pushdown analysis or extracting filters from `logical_plan`.
                        //
                        // 2. For each relevant shard in `table_def.shards`:
                        //    - let shard_node_ids = &shard_def.node_ids;
                        //    - let is_local = shard_node_ids.iter().any(|id| id.to_string() == self.current_node_id.as_ref());
                        //    - if is_local:
                        //        - Plan a local scan for this shard. This means the `QueryTable::scan` or underlying
                        //          `WriteBuffer::get_table_chunks` would need to accept a `shard_id` filter.
                        //          The `ExecutionPlan` for this local part would be generated.
                        //    - else (remote shard):
                        //        - Create a "remote query operator" / `ExecutionPlan` node.
                        //        - This operator would serialize the relevant part of the query (or a specific fragment plan).
                        //        - It would make a gRPC call (e.g., to `ExecuteQueryFragment`) to the target node(s) in `shard_node_ids`.
                        //        - It would deserialize the stream of RecordBatches received from the remote node.
                        //
                        // 3. Aggregate Results:
                        //    - Add an aggregation operator (e.g., `UnionExec`, `SortPreservingMergeExec`, custom shuffle/exchange)
                        //      to combine results from all local and remote shard plans.
                        //
                        // For this subtask, we are only logging and not modifying the plan.
                        // The original physical plan created by DataFusion (local scan) will be used.
                    }
                }
            }
        }
        Ok(())
    }
}

// NOTE: the below code is currently copied from IOx and needs to be made pub so we can
//       re-use it.

/// A physical operator that overrides the `schema` API,
/// to return an amended version owned by `SchemaExec`. The
/// principal use case is to add additional metadata to the schema.
struct SchemaExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
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

    /// This function creates the cache object that stores the plan properties such as equivalence
    /// properties, partitioning, ordering, etc.
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
