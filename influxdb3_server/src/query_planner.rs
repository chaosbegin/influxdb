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
                        // Conceptual: If a remote shard is identified:
                        // let remote_node_addr = format!("http://{}:{}", remote_node_id, crate::REPLICATION_PORT); // Placeholder for query port
                        // let plan_for_remote_bytes = ...; // Serialized sub-plan for the remote shard
                        // let schema_for_remote = ...; // Expected schema from this remote scan
                        // let remote_scan_exec = Arc::new(RemoteScanExec::new(
                        //     remote_node_addr,
                        //     db_schema.name.to_string(),
                        //     plan_for_remote_bytes,
                        //     schema_for_remote,
                        //     // self.distributed_query_client_factory.clone(), // Would need a factory
                        // ));
                        // The plan would then be reconstructed with `remote_scan_exec` nodes,
                        // potentially wrapped in `UnionExec` or other merge operators.
                    }
                }
            }
        }
        Ok(())
    }
}

// --- RemoteScanExec: Placeholder for distributed query fragment execution ---
// This new struct would typically be in its own file e.g. physical_plan/remote_scan.rs
#[derive(Debug)]
struct RemoteScanExec {
    target_node_address: String,
    db_name: String,
    plan_or_query_bytes: Vec<u8>,
    expected_schema: SchemaRef,
    // In a real implementation, this might hold an Arc<dyn DistributedQueryClient>
    // or a factory to create one. For now, it's omitted for simplicity as the
    // execute method will be a placeholder.
    cache: PlanProperties,
}

impl RemoteScanExec {
    #[allow(dead_code)] // Will be used when Planner is fully implemented
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        observability_deps::tracing::info!(
            target_node = %self.target_node_address,
            db_name = %self.db_name,
            plan_bytes_len = %self.plan_or_query_bytes.len(),
            "Executing RemoteScanExec (conceptual placeholder)"
        );

        // --- Conceptual gRPC Client Call ---
        // 1. Obtain a DistributedQueryServiceClient instance.
        //    (e.g., from a client factory or connection pool)
        //    let mut client = ...;
        //
        // 2. Construct ExecuteQueryFragmentRequest
        //    use crate::distributed_query_service::ExecuteQueryFragmentRequest; // Assuming this path
        //    let request = ExecuteQueryFragmentRequest {
        //        database_name: self.db_name.clone(),
        //        plan_bytes: self.plan_or_query_bytes.clone(),
        //        shard_id: None, // Or extract if part of plan_or_query_bytes or a separate field
        //        session_config: std::collections::HashMap::new(), // Populate from TaskContext/Session
        //        query_id: _context.task_id().unwrap_or("unknown_query_id").to_string(), // Example query_id
        //        expected_schema_bytes: vec![], // Placeholder for serialized expected schema
        //    };
        //
        // 3. Make the RPC call (conceptual)
        //    // let tonic_request = tonic::Request::new(request);
        //    // let stream_of_flight_data = client.execute_query_fragment(tonic_request).await
        //    //     .map_err(|e| DataFusionError::Execution(format!("Remote query fragment execution failed: {}", e)))?
        //    //     .into_inner();
        //
        // 4. Adapt FlightData stream to SendableRecordBatchStream. This is complex.
        //    // return Ok(Box::pin(FlightDataStreamAdapter::new(stream_of_flight_data, self.expected_schema.clone())));
        // --- End Conceptual gRPC Client Call ---

        observability_deps::tracing::error!("RemoteScanExec::execute is a placeholder and not implemented for actual remote execution.");
        Err(DataFusionError::NotImplemented("RemoteScanExec::execute actual remote call not implemented".to_string()))
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
