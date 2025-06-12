use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        UnionExec,
    },
};
use datafusion::logical_expr::LogicalPlan; // For plan inspection
use influxdb_influxql_parser::statement::Statement;
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner};
use crate::distributed_planning::RemoteScanExec;
use crate::distributed_query_service::DistributedQueryServerImpl; // Added
use observability_deps::tracing::debug;
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
    catalog: Arc<Catalog>,
    current_node_id: Arc<str>,
    dist_query_service: Arc<DistributedQueryServerImpl>, // Added
}

impl Planner {
    /// Create a new `Planner`
    pub(crate) fn new(
        ctx: &IOxSessionContext,
        catalog: Arc<Catalog>,
        current_node_id: Arc<str>,
        dist_query_service: Arc<DistributedQueryServerImpl>, // Added
    ) -> Self {
        Self {
            ctx: ctx.child_ctx("rest_api_query_planner"),
            catalog,
            current_node_id,
            dist_query_service, // Stored
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
        let initial_physical_plan = ctx.create_physical_plan(&logical_plan).await?;
        self.generate_distributed_physical_plan(&logical_plan, initial_physical_plan, "SQL").await
    }

    /// Plan an InfluxQL query and return a DataFusion physical plan
    pub(crate) async fn influxql(
        &self,
        statement: Statement,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.ctx.child_ctx("rest_api_query_planner_influxql");

        let logical_plan = InfluxQLQueryPlanner::statement_to_plan(statement, params, &ctx).await?;
        let initial_physical_plan = ctx.create_physical_plan(&logical_plan).await?;
        let distributed_physical_plan = self.generate_distributed_physical_plan(&logical_plan, initial_physical_plan, "InfluxQL").await?;

        // The SchemaExec wraps the final plan (which might be a UnionExec or the original plan)
        let input_schema = distributed_physical_plan.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone());
        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));

        Ok(Arc::new(SchemaExec::new(input, schema)))
    }

    async fn generate_distributed_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        initial_physical_plan: Arc<dyn ExecutionPlan>,
        query_type: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut table_names = std::collections::HashSet::new();
        logical_plan.collect_table_scan_table_names(&mut table_names);

        let mut overall_is_sharded = false;
        let mut plans_for_union: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let default_db_name = self.ctx.default_database_name(); // Used for catalog lookups

        for table_name_ref in &table_names {
            let table_name = table_name_ref.table();
            if let Some(db_schema) = self.catalog.db_schema(&default_db_name) {
                if let Some(table_def) = db_schema.table_definition(table_name) {
                    if !table_def.shards.is_empty() {
                        overall_is_sharded = true;
                        debug!(
                            db_name = %default_db_name,
                            table_name = %table_name,
                            query_type = %query_type,
                            num_shards = %table_def.shards.len(),
                            current_node_id = %self.current_node_id,
                            "Query targets a sharded table. Generating distributed plan components."
                        );

                        // Extract filters and projection relevant to this table from the logical_plan
                        // This is a simplification; a real implementation needs to map logical plan
                        // expressions to physical plan expressions carefully, especially for projections.
                        let (table_projection, table_filters) =
                            extract_projection_and_filters(logical_plan, table_name_ref.clone())?;

                        for shard_def_arc in table_def.shards.resource_iter() {
                            let shard_is_local = shard_def_arc.node_ids.iter().any(|id| {
                                // Assuming NodeId can be converted to string for comparison
                                id.to_string() == self.current_node_id.as_ref()
                            });

                            if shard_is_local {
                                debug!("Planning for local shard: {:?}", shard_def_arc.id);
                                // SIMPLIFICATION: For now, if any shard is local, we assume the initial_physical_plan
                                // (which scans all local data for the table) covers all local shards for this table.
                                // A more precise implementation would create a scan specifically for this shard_def_arc.id,
                                // potentially by adding shard_id filters to the TableProvider's scan method or
                                // by having one TableProvider instance per local shard.
                                // Adding the initial_physical_plan once if any local shards are found for this table.
                                // This needs to be smarter to avoid adding it multiple times if multiple tables are sharded.
                                // For now, if we add it, we only add it once per invocation of this function IF local shards exist.
                                // This part is tricky and the current approach is a HEAVY simplification.
                                // A proper solution would be to generate a new local physical plan for *each* local shard.
                                // For now, we'll just add the initial_physical_plan to represent all local shards.
                                // This is added outside this loop to avoid duplicates if multiple local shards.
                            } else {
                                // Assume first node in node_ids is the primary for remote shard for now
                                if let Some(remote_node_id) = shard_def_arc.node_ids.first() {
                                    debug!("Planning for remote shard: {:?} on node {}", shard_def_arc.id, remote_node_id.to_string());
                                    let remote_scan = Arc::new(RemoteScanExec::new(
                                        initial_physical_plan.schema(), // Use schema from initial plan
                                        remote_node_id.to_string(),
                                        default_db_name.clone(),
                                        table_name.to_string(),
                                        shard_def_arc.id,
                                        table_projection.clone(),
                                        &table_filters,
                                        Arc::clone(&self.dist_query_service), // Pass service
                                    ));
                                    plans_for_union.push(remote_scan);
                                }
                            }
                        }
                    } else {
                        // Table is not sharded, use the initial physical plan for this table's part.
                        // This is complex if the query involves joins between sharded and non-sharded tables.
                        // For now, if ANY table is sharded and has remote parts, we try to build a Union.
                        // If no tables are sharded, or all sharded tables are 100% local, we use original plan.
                        // This logic will be simplified for now: if overall_is_sharded, we try to build a union.
                        // If a non-sharded table is part of a query with other sharded tables,
                        // its initial_physical_plan (or relevant part) would need to be one of the inputs to UnionExec.
                    }
                }
            }
        }

        if overall_is_sharded && !plans_for_union.is_empty() { // Only create UnionExec if there are remote scans
            // Add the initial_physical_plan as the representation of all local shards' data.
            // This is a major simplification. A real system would have separate plans for each local shard,
            // or the initial_physical_plan would need to be a scan of *only* the relevant local shards.
            plans_for_union.insert(0, Arc::clone(&initial_physical_plan));

            debug!("Creating UnionExec with {} inputs for distributed query.", plans_for_union.len());
            Ok(Arc::new(UnionExec::new(plans_for_union)))
        } else {
            // No remote shards detected, or no sharded tables at all. Use original plan.
            debug!("No remote shards detected or no sharded tables. Using original physical plan.");
            Ok(initial_physical_plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::{table_scan, logical_plan_builder::LogicalPlanBuilder};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use influxdb3_catalog::catalog::{Catalog, CatalogLimits, TableDefinition, DatabaseSchema, RetentionPeriod};
    use influxdb3_catalog::shard::{ShardDefinition, ShardId, ShardTimeRange};
    use influxdb3_id::{DatabaseId, NodeId, TableId, ColumnId};
    use iox_query::exec::IOxSessionContext;
    use object_store::memory::InMemory;
    use iox_time::MockProvider;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::union::UnionExec;
    use crate::distributed_planning::RemoteScanExec;
    use influxdb3_catalog::log::FieldDataType as CatalogFieldDataType;


    async fn setup_planner_with_catalog_state(
        db_name: &str,
        table_name: &str,
        is_sharded: bool,
        current_node_id_str: &str,
        remote_node_id_str: &str, // Used if sharded
    ) -> (Planner, Arc<Catalog>) {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        let object_store = Arc::new(InMemory::new());

        let catalog = Arc::new(
            Catalog::new_with_args(
                "test_catalog_id".into(),
                object_store,
                time_provider,
                metrics,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap(),
        );

        // Create database
        let _ = catalog.create_database(db_name).await.unwrap();
        let db_schema_arc = catalog.db_schema(db_name).unwrap();

        // Create table and columns (simplified schema)
        let mut txn = catalog.begin(db_name).unwrap();
        let _ = txn.create_table(table_name, &["tagA"], &[("fieldB", CatalogFieldDataType::Integer)]).unwrap();
        catalog.commit(txn).await.unwrap();

        let db_schema_arc = catalog.db_schema(db_name).unwrap(); // Re-fetch schema after commit
        let table_def_arc = db_schema_arc.table_definition(table_name).unwrap();


        if is_sharded {
            let shard_def_local = ShardDefinition::new(
                ShardId::new(1),
                ShardTimeRange { start_time: 0, end_time: 1000 },
                vec![NodeId::new_from_str(current_node_id_str).unwrap()],
            );
            let shard_def_remote = ShardDefinition::new(
                ShardId::new(2),
                ShardTimeRange { start_time: 1001, end_time: 2000 },
                vec![NodeId::new_from_str(remote_node_id_str).unwrap()],
            );
            catalog.create_shard(db_name, table_name, shard_def_local).await.unwrap();
            catalog.create_shard(db_name, table_name, shard_def_remote).await.unwrap();
        }

        let session_ctx = IOxSessionContext::with_testing();
        // Register the database as a catalog provider in DataFusion context
        session_ctx.inner().register_catalog(db_name, Arc::new(crate::query_executor::Database::new(
            crate::query_executor::CreateDatabaseArgs {
                db_schema: db_schema_arc.clone(),
                write_buffer: Arc::new(influxdb3_write::test_helpers::MockWriteBuffer::new()), // Using a mock write buffer
                exec: Arc::new(iox_query::exec::Executor::new_testing()),
                datafusion_config: Arc::new(Default::default()),
                query_log: Arc::new(iox_query::query_log::QueryLog::new_testing()),
                system_schema_provider: Arc::new(crate::system_tables::SystemSchemaProvider::new_all_tables_provider(
                    db_schema_arc.clone(),
                    Arc::new(iox_query::query_log::QueryLog::new_testing()),
                    Arc::new(influxdb3_write::test_helpers::MockWriteBuffer::new()),
                    Arc::new(influxdb3_sys_events::SysEventStore::new_testing()),
                    Arc::clone(&catalog),
                    false,
                )),
            }
        )));
        session_ctx.inner().use_catalog(db_name);


        let planner = Planner::new(&session_ctx, Arc::clone(&catalog), Arc::from(current_node_id_str));
        (planner, catalog)
    }

    #[tokio::test]
    async fn test_non_sharded_table_plan() {
        let db_name = "testdb_nonsharded";
        let table_name = "table_A";
        let current_node_id = "node1";
        let (planner, _catalog) = setup_planner_with_catalog_state(db_name, table_name, false, current_node_id, "node2").await;

        let sql = format!("SELECT * FROM {}", table_name);
        let df_logical_plan = planner.ctx.state().create_logical_plan(&sql).await.unwrap();
        let initial_physical_plan = planner.ctx.create_physical_plan(&df_logical_plan).await.unwrap();

        let final_plan = planner.generate_distributed_physical_plan(&df_logical_plan, initial_physical_plan, "SQL").await.unwrap();

        assert_eq!(final_plan.name(), "QueryableParquetChunkExec", "Plan for non-sharded table should be a local scan (QueryableParquetChunkExec or similar)");
        assert!(final_plan.children().is_empty() || final_plan.children().iter().all(|c| c.name() != "UnionExec" && c.name() != "RemoteScanExec"));
    }

    #[tokio::test]
    async fn test_sharded_table_remote_only_plan() {
        let db_name = "testdb_sharded_remote";
        let table_name = "table_B";
        let current_node_id = "node1"; // Current node
        let remote_node_id = "node2";   // All shards are on this node

        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(metric::Registry::new());
        let object_store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_args("test_cat_remote".into(), object_store, time_provider, metrics, Default::default(), Default::default()).await.unwrap()
        );
        catalog.create_database(db_name).await.unwrap();
        let mut txn = catalog.begin(db_name).unwrap();
        txn.create_table(table_name, &["tagR"], &[("fieldR", CatalogFieldDataType::Integer)]).unwrap();
        catalog.commit(txn).await.unwrap();

        let shard_def_remote1 = ShardDefinition::new(
            ShardId::new(100), ShardTimeRange { start_time: 0, end_time: 100 },
            vec![NodeId::new_from_str(remote_node_id).unwrap()]
        );
        let shard_def_remote2 = ShardDefinition::new(
            ShardId::new(101), ShardTimeRange { start_time: 101, end_time: 200 },
            vec![NodeId::new_from_str(remote_node_id).unwrap()]
        );
        catalog.create_shard(db_name, table_name, shard_def_remote1).await.unwrap();
        catalog.create_shard(db_name, table_name, shard_def_remote2).await.unwrap();

        let session_ctx = IOxSessionContext::with_testing();
        let db_schema_arc = catalog.db_schema(db_name).unwrap();
        session_ctx.inner().register_catalog(db_name, Arc::new(crate::query_executor::Database::new(
             crate::query_executor::CreateDatabaseArgs {
                db_schema: db_schema_arc.clone(),
                write_buffer: Arc::new(influxdb3_write::test_helpers::MockWriteBuffer::new()),
                exec: Arc::new(iox_query::exec::Executor::new_testing()),
                datafusion_config: Arc::new(Default::default()),
                query_log: Arc::new(iox_query::query_log::QueryLog::new_testing()),
                system_schema_provider: Arc::new(crate::system_tables::SystemSchemaProvider::new_all_tables_provider(
                    db_schema_arc.clone(), Arc::new(iox_query::query_log::QueryLog::new_testing()),
                    Arc::new(influxdb3_write::test_helpers::MockWriteBuffer::new()),
                    Arc::new(influxdb3_sys_events::SysEventStore::new_testing()), Arc::clone(&catalog), false,
                )),
            }
        )));
        session_ctx.inner().use_catalog(db_name);

        let planner = Planner::new(&session_ctx, Arc::clone(&catalog), Arc::from(current_node_id));
        let sql = format!("SELECT * FROM {}", table_name);

        let df_logical_plan = planner.ctx.state().create_logical_plan(&sql).await.unwrap();
        let initial_physical_plan = planner.ctx.create_physical_plan(&df_logical_plan).await.unwrap();
        let final_plan = planner.generate_distributed_physical_plan(&df_logical_plan, initial_physical_plan, "SQL").await.unwrap();

        assert_eq!(final_plan.name(), "UnionExec", "Plan for remote-only sharded table should be UnionExec");
        assert_eq!(final_plan.children().len(), 2, "Should be two RemoteScanExec children for the two remote shards");
        for child in final_plan.children() {
            assert_eq!(child.name(), "RemoteScanExec");
            let remote_exec = child.as_any().downcast_ref::<RemoteScanExec>().unwrap();
            assert_eq!(remote_exec.target_node_id(), remote_node_id);
        }
    }
}

// Helper to extract projection and filters for a specific table from a LogicalPlan.
// This is a placeholder and needs a robust implementation.
fn extract_projection_and_filters(
    logical_plan: &LogicalPlan,
    table_name: datafusion::datasource::listing::ListingTableUrl,
) -> Result<(Option<Vec<usize>>, Vec<Expr>)> {
    // Placeholder: In a real scenario, traverse the plan (TableScan -> Filter -> Project)
    // to get the actual filters and projection applied to this specific table.
    // For now, returning no specific projection (None means all columns) and empty filters.
    // This is a significant simplification.
    let mut filters = Vec::new();
    if let LogicalPlan::Filter(filter_node) = logical_plan {
        // This is too simple, doesn't ensure filter applies to *this* table
        // filters.push(filter_node.predicate().clone());
    }
    // Projection also needs to be extracted by finding the Project node related to this TableScan.
    Ok((None, filters))
}


// NOTE: the below code is currently copied from IOx and needs to be made pub so we can
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
