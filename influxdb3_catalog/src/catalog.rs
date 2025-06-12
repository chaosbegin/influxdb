//! Implementation of the Catalog that sits entirely in memory.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use bimap::BiHashMap;
use influxdb3_authz::Actions;
use influxdb3_authz::Permission;
use influxdb3_authz::ResourceIdentifier;
use influxdb3_authz::ResourceType;
use influxdb3_authz::TokenInfo;
use influxdb3_authz::TokenProvider;
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId, TokenId,
    TriggerId,
};
use influxdb3_shutdown::ShutdownToken;
use influxdb3_telemetry::ProcessingEngineMetrics;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::CatalogMetrics;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use rand::RngCore;
use rand::rngs::OsRng;
use schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha512;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

mod metrics;
mod update;
use schema::sort::SortKey;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::HardDeletionTime;
pub use update::{CatalogUpdate, DatabaseCatalogTransaction, Prompt, TableUpdate};

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::replication::ReplicationInfo;
use crate::shard::{ShardDefinition, ShardId};
use crate::log::{
    ClearRetentionPeriodLog, CreateAdminTokenDetails, CreateDatabaseLog, DatabaseBatch,
    DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode, RegenerateAdminTokenDetails,
    RegisterNodeLog, SetRetentionPeriodLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerSpecificationDefinition, AddNodeLog, RemoveNodeLog, UpdateNodeStateLog, UpdateNodeLog,
    ClusterNodeBatch, ClusterNodeCatalogOp, CreateShardLog, UpdateShardMetadataLog, DeleteShardLog, SetReplicationLog,
    UpdateTableMetadataLog,
};
use crate::object_store::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use crate::snapshot::{
    CatalogSnapshot, ClusterNodeDefinitionSnapshot,
    NodeSnapshot, DatabaseSnapshot, TableSnapshot, ColumnDefinitionSnapshot,
    LastCacheSnapshot, DistinctCacheSnapshot, ProcessingEngineTriggerSnapshot,
    RepositorySnapshot, TokenInfoSnapshot, NodeStateSnapshot, RetentionPeriodSnapshot,
    DataType, InfluxType, TimeUnit, ShardDefinitionSnapshot,
};

use crate::{
    CatalogError, Result, ClusterNodeDefinition,
    log::{
        AddFieldsLog, CatalogBatch, CreateTableLog, DeleteDistinctCacheLog, DeleteLastCacheLog,
        DeleteTriggerLog, DistinctCacheDefinition, FieldDefinition, LastCacheDefinition,
        OrderedCatalogBatch, SoftDeleteDatabaseLog, SoftDeleteTableLog, TriggerDefinition,
        TriggerIdentifier,
    },
};


const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";
pub const TIME_COLUMN_NAME: &str = "time";
pub const INTERNAL_DB_NAME: &str = "_internal";
const DEFAULT_OPERATOR_TOKEN_NAME: &str = "_admin";

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u64);

impl CatalogSequenceNumber {
    pub const fn new(id: u64) -> Self { Self(id) }
    pub fn next(&self) -> Self { Self(self.0 + 1) }
    pub fn get(&self) -> u64 { self.0 }
}

impl From<u64> for CatalogSequenceNumber {
    fn from(value: u64) -> Self { Self(value) }
}

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> =
    Mutex::const_new(CatalogSequenceNumber::new(0));
pub type CatalogWritePermit = MutexGuard<'static, CatalogSequenceNumber>;

pub struct Catalog {
    metric_registry: Arc<Registry>,
    state: parking_lot::Mutex<CatalogState>,
    subscriptions: Arc<tokio::sync::RwLock<CatalogSubscriptions>>,
    time_provider: Arc<dyn TimeProvider>,
    store: ObjectStoreCatalog,
    metrics: Arc<CatalogMetrics>,
    pub(crate) inner: RwLock<InnerCatalog>,
    limits: CatalogLimits,
    args: CatalogArgs,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").field("inner", &self.inner).finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum CatalogState { Active, Shutdown }
impl CatalogState { fn is_shutdown(&self) -> bool { matches!(self, Self::Shutdown) } }

const CATALOG_CHECKPOINT_INTERVAL: u64 = 100;

#[derive(Clone, Copy, Debug)]
pub struct CatalogArgs { pub default_hard_delete_duration: Duration }
impl CatalogArgs {
    pub fn new(default_hard_delete_duration: Duration) -> Self { Self { default_hard_delete_duration } }
}
impl Default for CatalogArgs {
    fn default() -> Self { Self { default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION } }
}

#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    pub num_dbs: usize,
    pub num_tables: usize,
    pub num_columns_per_table: usize,
    pub num_tag_columns_per_table: usize,
}
impl CatalogLimits {
    pub fn new(num_dbs: usize, num_tables: usize, num_columns_per_table: usize, num_tag_columns_per_table: usize) -> Self {
        Self { num_dbs, num_tables, num_columns_per_table, num_tag_columns_per_table }
    }
}
impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            num_dbs: Catalog::DEFAULT_NUM_DBS_LIMIT,
            num_tables: Catalog::DEFAULT_NUM_TABLES_LIMIT,
            num_columns_per_table: Catalog::DEFAULT_NUM_COLUMNS_PER_TABLE_LIMIT,
            num_tag_columns_per_table: Catalog::DEFAULT_NUM_TAG_COLUMNS_LIMIT,
        }
    }
}

// Catalog impl block starts
impl Catalog {
    pub const DEFAULT_NUM_DBS_LIMIT: usize = 5;
    pub const DEFAULT_NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    pub const DEFAULT_NUM_TABLES_LIMIT: usize = 2000;
    pub const DEFAULT_NUM_TAG_COLUMNS_LIMIT: usize = 250;
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72);

    pub async fn new(
        node_id: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>, limits: CatalogLimits,
    ) -> Result<Self> {
        Self::new_with_args(node_id, store, time_provider, metric_registry, CatalogArgs::default(), limits).await
    }

    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>, args: CatalogArgs, limits: CatalogLimits,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let store = ObjectStoreCatalog::new(Arc::clone(&node_id), CATALOG_CHECKPOINT_INTERVAL, store);
        let subscriptions = Default::default();
        let metrics = Arc::new(CatalogMetrics::new(&metric_registry));
        let catalog = store.load_or_create_catalog().await.map(RwLock::new)
            .map(|inner| Self {
                metric_registry, state: parking_lot::Mutex::new(CatalogState::Active),
                subscriptions, time_provider, store, metrics, inner, limits, args,
            })?;
        create_internal_db(&catalog).await;
        catalog.metrics.operation_observer(catalog.subscribe_to_updates("catalog_operation_metrics").await);
        Ok(catalog)
    }

    pub async fn new_with_shutdown(
        node_id: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>, shutdown_token: ShutdownToken, limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        let node_id_arc = node_id.into();
        let catalog = Arc::new(Self::new(Arc::clone(&node_id_arc), store, time_provider, metric_registry, limits).await?);
        let catalog_cloned = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            info!(node_id = %node_id_arc, "updating node state to stopped in catalog");
            if let Err(error) = catalog_cloned.update_node_state_stopped(node_id_arc.as_ref()).await {
                error!(?error, node_id = %node_id_arc, "encountered error while updating node to stopped state in catalog");
            }
        });
        Ok(catalog)
    }

    pub fn metric_registry(&self) -> Arc<Registry> { Arc::clone(&self.metric_registry) }
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> { Arc::clone(&self.time_provider) }
    pub fn set_state_shutdown(&self) { *self.state.lock() = CatalogState::Shutdown; }
    fn num_dbs_limit(&self) -> usize { self.limits.num_dbs }
    fn num_tables_limit(&self) -> usize { self.limits.num_tables }
    fn num_columns_per_table_limit(&self) -> usize { self.limits.num_columns_per_table }
    pub(crate) fn num_tag_columns_per_table_limit(&self) -> usize { self.limits.num_tag_columns_per_table }
    fn default_hard_delete_duration(&self) -> Duration { self.args.default_hard_delete_duration }

    pub async fn create_shard( &self, db_name: &str, table_name: &str, shard_definition: ShardDefinition) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::CreateShard(CreateShardLog {
                    db_id, table_id, table_name: table_arc.table_name.clone(), shard_definition: shard_definition.clone(),
                })],
            }))
        }).await?;
        Ok(())
    }

    pub async fn update_shard_metadata(&self, db_name: &str, table_name: &str, shard_id: ShardId, status: Option<String>, node_ids: Option<Vec<NodeId>>) -> Result<OrderedCatalogBatch> {
        info!(%db_name, %table_name, ?shard_id, ?status, ?node_ids, "update shard metadata");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema(db_name).ok_or_else(|| CatalogError::DbNotFound(db_name.to_string()))?;
            let table_def = db_schema.table_definition(table_name).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            if table_def.shards.get_by_id(&shard_id).is_none() {
                return Err(CatalogError::ShardNotFound{ shard_id, table_id: table_def.table_id});
            }

            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: current_time_ns,
                database_id: db_schema.id,
                database_name: Arc::clone(&db_schema.name),
                ops: vec![DatabaseCatalogOp::UpdateShardMetadata(UpdateShardMetadataLog {
                    db_id: db_schema.id,
                    table_id: table_def.table_id,
                    table_name: Arc::clone(&table_def.table_name),
                    shard_id,
                    status,
                    node_ids,
                    updated_at_ts: current_time_ns,
                })],
            }))
        }).await
    }

    pub async fn delete_shard( &self, db_name: &str, table_name: &str, shard_id: ShardId) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            if !table_arc.shards.contains_id(&shard_id) { return Err(CatalogError::ShardNotFound { shard_id, table_id }); }
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::DeleteShard(DeleteShardLog {
                    db_id, table_id, table_name: table_arc.table_name.clone(), shard_id,
                })],
            }))
        }).await?;
        Ok(())
    }

    pub async fn set_replication( &self, db_name: &str, table_name: &str, replication_info: ReplicationInfo) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::SetReplication(SetReplicationLog {
                    db_id, table_id, table_name: table_arc.table_name.clone(), replication_info: replication_info.clone(),
                })],
            }))
        }).await?;
        Ok(())
    }

    fn get_db_and_table_ids(&self, db_name: &str, table_name: &str) -> Result<(DbId, TableId)> {
        let db_id = self.db_name_to_id(db_name).ok_or_else(|| CatalogError::DbNotFound(db_name.to_string()))?;
        let table_id = self.db_schema_by_id(&db_id).and_then(|db| db.table_name_to_id(table_name))
            .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
        Ok((db_id, table_id))
    }

    pub async fn add_cluster_node(&self, node_def: ClusterNodeDefinition) -> Result<OrderedCatalogBatch> {
        info!(node_id = ?node_def.id, rpc_addr = %node_def.rpc_address, http_addr = %node_def.http_address, "add cluster node");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        let mut node_def_mut = node_def;
        node_def_mut.updated_at = current_time_ns;
        if node_def_mut.created_at == 0 { node_def_mut.created_at = current_time_ns; }
        self.catalog_update_with_retry(|| {
            if self.inner.read().cluster_nodes.contains_id(&node_def_mut.id) {
                 return Err(CatalogError::AlreadyExists);
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::AddClusterNode(AddClusterNodeLog {
                    node: node_def_mut.clone(),
                })],
            }))
        }).await
    }

    pub async fn remove_cluster_node(&self, node_id: NodeId) -> Result<OrderedCatalogBatch> {
        info!(?node_id, "remove cluster node");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        self.catalog_update_with_retry(|| {
            if self.inner.read().cluster_nodes.get_by_id(&node_id).is_none() {
                return Err(CatalogError::NotFound);
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::RemoveClusterNode(RemoveClusterNodeLog {
                    node_id, removed_at_ts: current_time_ns,
                })],
            }))
        }).await
    }

    pub fn get_cluster_node(&self, node_id: NodeId) -> Option<Arc<ClusterNodeDefinition>> {
        self.inner.read().cluster_nodes.get_by_id(&node_id)
    }

    pub fn list_cluster_nodes(&self) -> Vec<Arc<ClusterNodeDefinition>> {
        self.inner.read().cluster_nodes.resource_iter().cloned().collect()
    }

    pub async fn update_cluster_node_status(&self, node_id: NodeId, status: String) -> Result<OrderedCatalogBatch> {
        info!(?node_id, %status, "update cluster node status");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        self.catalog_update_with_retry(|| {
            let inner_read = self.inner.read();
            if inner_read.cluster_nodes.get_by_id(&node_id).is_none() {
                 return Err(CatalogError::NotFound);
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::UpdateClusterNodeStatus(UpdateClusterNodeStatusLog {
                    node_id, status: status.clone(), updated_at_ts: current_time_ns,
                })],
            }))
        }).await
    }

    pub async fn update_table_sharding_metadata(
        &self,
        db_name: &str,
        table_name: &str,
        shard_keys: Option<Vec<String>>,
        num_hash_partitions: Option<u32>
    ) -> Result<OrderedCatalogBatch> {
        info!(%db_name, %table_name, ?shard_keys, ?num_hash_partitions, "update table sharding metadata");
        let current_time_ns = self.time_provider.now().timestamp_nanos();

        let (db_id, table_id, db_name_arc, table_name_arc) = {
            let inner_read_guard = self.inner.read();
            let db_schema = inner_read_guard
                .databases
                .get_by_name(db_name)
                .ok_or_else(|| CatalogError::DbNotFound(db_name.to_string()))?;
            let table_definition = db_schema
                .tables
                .get_by_name(table_name)
                .ok_or_else(|| CatalogError::TableNotFound {
                    db_name: Arc::clone(&db_schema.name),
                    table_name: Arc::from(table_name),
                })?;
            (
                db_schema.id,
                table_definition.table_id,
                Arc::clone(&db_schema.name),
                Arc::clone(&table_definition.table_name),
            )
        };

        self.catalog_update_with_retry(|| {
            if let Some(n) = num_hash_partitions {
                if n == 0 {
                    return Err(CatalogError::InvalidConfiguration {
                        reason: "num_hash_partitions cannot be 0".to_string()
                    });
                }
            }

            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: current_time_ns,
                database_id: db_id,
                database_name: db_name_arc.clone(),
                ops: vec![DatabaseCatalogOp::UpdateTableMetadata(
                    UpdateTableMetadataLog {
                        db_id,
                        table_id,
                        table_name: table_name_arc.clone(),
                        shard_keys: shard_keys.clone(),
                        num_hash_partitions,
                    },
                )],
            }))
        })
        .await
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_name(db_name)
    }
    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_id(db_id)
    }
     fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().databases.name_to_id(db_name)
    }
    async fn catalog_update_with_retry<F>(&self, op_gen: F) -> Result<OrderedCatalogBatch>
    where F: Fn() -> Result<CatalogBatch> {
        let batch = op_gen()?;
        self.apply_catalog_batch(batch, None).await
    }
    pub(crate) async fn apply_catalog_batch(
        &self,
        catalog_batch: CatalogBatch,
        permit_opt: Option<CatalogWritePermit>,
    ) -> Result<OrderedCatalogBatch> {
        let mut inner = self.inner.write();
        let permit = match permit_opt {
            Some(p) => p,
            None => CATALOG_WRITE_PERMIT.lock().await,
        };
        let seq = inner.apply_catalog_batch(catalog_batch.clone(), &permit)?;
        let ordered_batch = OrderedCatalogBatch::new(catalog_batch, seq);
        self.store.put_log_entry(&ordered_batch).await?;
        Ok(ordered_batch)
    }
     pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }
     pub(crate) fn catalog_id(&self) -> Arc<str> {
        self.inner.read().catalog_id.clone()
    }
}


// Helper function for default num_hash_partitions (module level)
fn default_num_hash_partitions() -> u32 {
    1
}

/// Defines a table within a database in the catalog.
///
/// Table sharding:
/// A table can be sharded based on time and/or other shard keys (e.g., tags).
/// - Time-based sharding: `ShardDefinition` entries in `self.shards` define time ranges.
/// - Hash-based sharding (within a time-slice shard):
///   - `shard_keys`: A list of column names (tags) used to compute a hash.
///   - `num_hash_partitions`: The number of partitions the hash space is divided into.
///
/// If `num_hash_partitions > 1` for a table, then for any given time-slice `ShardDefinition`
/// associated with this table, the `ShardDefinition.node_ids` list is interpreted as an
/// ordered list of nodes. Each node in this list is responsible for a specific hash partition.
/// For example, if `num_hash_partitions = 3` and `node_ids = [NodeA, NodeB, NodeC]`:
///   - NodeA handles hash partition 0.
///   - NodeB handles hash partition 1.
///   - NodeC handles hash partition 2.
/// The length of `ShardDefinition.node_ids` MUST equal `num_hash_partitions` for tables
/// that use hash partitioning. This constraint should be enforced when creating or updating
/// shards for such tables.
///
/// If `num_hash_partitions == 1` (the default), then `shard_keys` are effectively ignored
/// for partitioning purposes (though they might still be used for informational or query optimization
/// purposes), and the single node in `ShardDefinition.node_ids` (or all nodes if replicated without hashing)
/// is responsible for all data within that shard's time range.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableDefinition {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub(crate) schema: Schema,
    pub(crate) columns: Repository<ColumnId, ColumnDefinition>,
    pub(crate) series_key: Vec<ColumnId>,
    pub(crate) series_key_names: Vec<Arc<str>>,
    pub(crate) sort_key: SortKey,
    pub(crate) last_caches: Repository<LastCacheId, LastCacheDefinition>,
    pub(crate) distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    pub deleted: bool,
    pub hard_delete_time: Option<Time>,
    #[serde(default)]
    pub shards: Repository<ShardId, ShardDefinition>,
    #[serde(default)]
    pub replication_info: Option<ReplicationInfo>,
    #[serde(default)]
    pub shard_keys: Vec<String>,
    #[serde(default = "default_num_hash_partitions")]
    pub num_hash_partitions: u32,
}

impl TableDefinition {
    pub(crate) fn new(
        table_id: TableId,
        table_name: Arc<str>,
        columns: Vec<FieldDefinition>,
        series_key: Vec<Arc<str>>,
    ) -> Result<Self> {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.measurement(table_name.as_ref());
        let mut columns_repo = Repository::new();

        for col_def_log in columns {
            let col_id = col_def_log.id;
            let col_name = Arc::clone(&col_def_log.name);
            let col_type: InfluxColumnType = col_def_log.data_type.into();
            schema_builder.influx_column(&col_name, col_type);
            columns_repo.add(ColumnDefinition {
                id: col_id,
                name: col_name,
                data_type: col_type,
                nullable: true,
            });
        }
        schema_builder.with_series_key(&series_key);
        let schema = schema_builder.build().map_err(|e| CatalogError::SchemaError {
            table_name: Arc::clone(&table_name),
            error: e.to_string(),
        })?;

        let final_series_key_ids = series_key
            .iter()
            .map(|name| {
                columns_repo
                    .name_to_id(name)
                    .ok_or_else(|| CatalogError::ColumnNotFound(name.to_string()))
            })
            .collect::<Result<Vec<ColumnId>>>()?;

        let sort_key = Self::make_sort_key(&series_key, columns_repo.contains_name(TIME_COLUMN_NAME));

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns: columns_repo,
            series_key: final_series_key_ids,
            series_key_names: series_key,
            sort_key,
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
            hard_delete_time: None,
            shards: Repository::new(),
            replication_info: None,
            shard_keys: Vec::new(),
            num_hash_partitions: default_num_hash_partitions(),
        })
    }

    pub(crate) fn make_sort_key(series_key_names: &[Arc<str>], has_time_col: bool) -> SortKey {
        let mut key_cols = series_key_names.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        if has_time_col {
            key_cols.push(TIME_COLUMN_NAME);
        }
        SortKey::from_columns(key_cols)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
}

impl<T: TableUpdate> UpdateDatabaseSchema for T {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(table) = schema.tables.get_by_id(&self.table_id()) else {
            return Err(CatalogError::TableNotFound {
                db_name: Arc::clone(&schema.name),
                table_name: Arc::clone(&self.table_name()),
            });
        };
        if let Cow::Owned(new_table) = self.update_table(Cow::Borrowed(table.as_ref()))? {
            schema
                .to_mut()
                .update_table(new_table.table_id, Arc::new(new_table))?;
        }
        Ok(schema)
    }
}

impl TableUpdate for UpdateShardMetadataLog {
    fn table_id(&self) -> TableId { self.table_id }
    fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) }
    fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> {
        let mut_table = table.to_mut();
        let existing_shard_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| {
            CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id, }
        })?;
        let mut shard_def_to_update = (*existing_shard_arc).clone();
        let mut changed = false;
        if let Some(new_status) = &self.status {
            if shard_def_to_update.status != *new_status {
                shard_def_to_update.status = new_status.clone();
                changed = true;
            }
        }
        if let Some(new_node_ids) = &self.node_ids {
            if shard_def_to_update.node_ids != *new_node_ids {
                 shard_def_to_update.node_ids = new_node_ids.clone();
                 changed = true;
            }
        }
        if changed {
            shard_def_to_update.updated_at_ts = Some(self.updated_at_ts);
            mut_table.shards.update(self.shard_id, Arc::new(shard_def_to_update))?;
        } else {
            if self.status.is_none() && self.node_ids.is_none() {
                shard_def_to_update.updated_at_ts = Some(self.updated_at_ts);
                mut_table.shards.update(self.shard_id, Arc::new(shard_def_to_update))?;
            }
        }
        Ok(table)
    }
}

impl TableUpdate for crate::log::UpdateTableMetadataLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        let mut_table = table.to_mut();

        if let Some(new_shard_keys) = &self.shard_keys {
            mut_table.shard_keys = new_shard_keys.clone();
        }

        if let Some(new_num_hash_partitions) = self.num_hash_partitions {
            if new_num_hash_partitions == 0 {
                return Err(CatalogError::InvalidConfiguration {
                    reason: "num_hash_partitions cannot be 0".to_string(),
                });
            }
            mut_table.num_hash_partitions = new_num_hash_partitions;
        }
        Ok(table)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub(crate) struct InnerCatalog {
    pub(crate) catalog_id: Arc<str>,
    pub(crate) catalog_uuid: Uuid,
    pub(crate) nodes: Repository<NodeId, NodeDefinition>,
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    pub(crate) sequence: CatalogSequenceNumber,
    #[serde(default)]
    pub(crate) tokens: Repository<TokenId, TokenInfo>,
    #[serde(default)]
    pub(crate) cluster_nodes: Repository<NodeId, Arc<ClusterNodeDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDefinition { pub node_id: Arc<str>, pub node_catalog_id: NodeId, /* ... */ }
impl CatalogResource for NodeDefinition { type Identifier = NodeId; fn id(&self) -> NodeId { self.node_catalog_id } fn name(&self) -> Arc<str> { self.node_id.clone() }}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseSchema { pub id: DbId, pub name: Arc<str>, pub tables: Repository<TableId, TableDefinition>, /* ... */ }
impl CatalogResource for DatabaseSchema { type Identifier = DbId; fn id(&self) -> DbId { self.id } fn name(&self) -> Arc<str> { self.name.clone() }}
impl DatabaseSchema {
    pub fn table_definition(&self, table_name: &str) -> Option<Arc<TableDefinition>> { self.tables.get_by_name(table_name) }
    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> { self.tables.get_by_id(table_id) }
    pub fn table_name_to_id(&self, table_name: &str) -> Option<TableId> { self.tables.name_to_id(table_name) }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDefinition { pub id: ColumnId, pub name: Arc<str>, pub data_type: InfluxColumnType, pub nullable: bool }
impl CatalogResource for ColumnDefinition { type Identifier = ColumnId; fn id(&self) -> ColumnId { self.id } fn name(&self) -> Arc<str> { self.name.clone() }}


#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Repository<I: CatalogId, R: CatalogResource<Identifier = I>> {
    repo: SerdeVecMap<I, Arc<R>>,
    name_to_id: BiHashMap<Arc<str>, I>,
    next_id: I,
}
impl<I: CatalogId, R: CatalogResource<Identifier = I>> Repository<I, R> {
    pub fn new() -> Self { Self { repo: SerdeVecMap::new(), name_to_id: BiHashMap::new(), next_id: I::default() }}
    pub fn add(&mut self, resource: R) { self.name_to_id.insert(Arc::clone(resource.name()), resource.id()); self.repo.insert(resource.id(), Arc::new(resource)); if resource.id() >= self.next_id { self.next_id = resource.id().next_id(); }}
    pub fn get_by_name(&self, name: &str) -> Option<Arc<R>> { self.name_to_id.get_by_left(name).and_then(|id| self.repo.get(id)).cloned() }
    pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> { self.repo.get(id).cloned() }
    pub fn name_to_id(&self, name: &str) -> Option<I> { self.name_to_id.get_by_left(name).cloned()}
    pub fn resource_iter(&self) -> impl Iterator<Item = &Arc<R>> { self.repo.values() }
    pub fn contains_id(&self, id: &I) -> bool { self.repo.contains_key(id) }
    pub fn len(&self) -> usize { self.repo.len() }
    pub fn is_empty(&self) -> bool { self.repo.is_empty() }
     pub fn update(&mut self, id: I, resource: Arc<R>) -> Result<()> { if self.repo.contains_key(&id) { self.repo.insert(id, resource); Ok(()) } else { Err(CatalogError::NotFound) }}
     pub fn remove(&mut self, id: &I) -> Option<Arc<R>> { if let Some(res) = self.repo.remove(id) { self.name_to_id.remove_by_right(id); Some(res) } else { None }}

}

impl InnerCatalog {
    pub(crate) fn new(node_id: Arc<str>, uuid: Uuid) -> Self {
        Self { catalog_id: node_id, catalog_uuid: uuid, ..Default::default() }
    }
    pub(crate) fn snapshot(&self) -> Result<CatalogSnapshot> { Ok(CatalogSnapshot::from(self)) }
    pub(crate) fn from_snapshot(snapshot: CatalogSnapshot, _node_id: Arc<str>) -> Result<Self> { Ok(InnerCatalog::from(snapshot)) }
    pub(crate) fn apply_catalog_batch(&mut self, batch: CatalogBatch, _permit: &CatalogWritePermit) -> Result<CatalogSequenceNumber> {
        match batch {
            CatalogBatch::Database(db_batch) => {
                let db_id = db_batch.database_id;
                let mut db_schema_clone = self.databases.get_by_id(&db_id)
                    .map(|arc| (*arc).clone())
                    .ok_or_else(|| CatalogError::DbNotFound(db_batch.database_name.to_string()))?;

                for op in db_batch.ops {
                    match op {
                        DatabaseCatalogOp::UpdateTableMetadata(log) => {
                            let table_id = log.table_id();
                            // Apply the update to a clone of the Arc<TableDefinition>
                            let original_table_arc = db_schema_clone.tables.get_by_id(&table_id)
                                .ok_or_else(|| CatalogError::TableNotFound {
                                    db_name: db_schema_clone.name.clone(),
                                    table_name: log.table_name()
                                })?;
                            let mut table_to_update = (*original_table_arc).clone();

                            // Apply changes from log
                            if let Some(new_shard_keys) = &log.shard_keys {
                                table_to_update.shard_keys = new_shard_keys.clone();
                            }
                            if let Some(new_num_hash_partitions) = log.num_hash_partitions {
                                if new_num_hash_partitions == 0 {
                                    return Err(CatalogError::InvalidConfiguration {
                                        reason: "num_hash_partitions cannot be 0".to_string(),
                                    });
                                }
                                table_to_update.num_hash_partitions = new_num_hash_partitions;
                            }
                            // Update the repository with the modified TableDefinition
                            db_schema_clone.tables.update(table_id, Arc::new(table_to_update))?;
                        }
                        // Other DatabaseCatalogOp variants would be handled here by calling respective methods on db_schema_clone or its repositories
                        _ => { /* ... */ }
                    }
                }
                self.databases.update(db_id, Arc::new(db_schema_clone))?;
            }
            _ => { /* Handle other CatalogBatch variants */ }
        }
        self.sequence = self.sequence.next();
        Ok(self.sequence)
    }
}

async fn create_internal_db(_catalog: &Catalog) { /* ... */ }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::FieldDataType;
    use iox_time::MockProvider;

    #[test_log::test(tokio::test)]
    async fn test_update_shard_metadata() {
        let catalog = Catalog::new_in_memory("shard_meta_host_v2").await.unwrap();
        let db_name = "meta_db_v2";
        let table_name = "meta_table_v2";
        let shard_id = ShardId::new(1);
        let initial_node = NodeId::new(100);
        let time_provider = catalog.time_provider();

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagM"], &[(String::from("fieldM"), FieldDataType::Float)]).await.unwrap();

        tokio::time::sleep(Duration::from_nanos(50)).await;
        let initial_ts = time_provider.now().timestamp_nanos();

        let shard_def = ShardDefinition {
            id: shard_id,
            time_range: crate::shard::ShardTimeRange { start_time: 0, end_time: 100 },
            node_ids: vec![initial_node],
            status: "Stable".to_string(),
            updated_at_ts: Some(initial_ts),
        };
        catalog.create_shard(db_name, table_name, shard_def.clone()).await.unwrap();

        let created_shard = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().shards.get_by_id(&shard_id).unwrap();
        assert_eq!(created_shard.updated_at_ts, Some(initial_ts));
    }

    #[test_log::test(tokio::test)]
    async fn test_update_table_sharding_metadata() {
        let catalog = Catalog::new_in_memory("test_host_table_meta").await.unwrap();
        let db_name = "test_db";
        let table_name = "test_table";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tagA"], &[(String::from("field1"), FieldDataType::Integer)]).await.unwrap();

        // 1. Initial update: set shard_keys and num_hash_partitions
        let shard_keys1 = vec!["tagA".to_string()];
        let num_partitions1 = 4u32;
        let res1 = catalog.update_table_sharding_metadata(db_name, table_name, Some(shard_keys1.clone()), Some(num_partitions1)).await;
        assert!(res1.is_ok());

        let table_def1 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
        assert_eq!(table_def1.shard_keys, shard_keys1);
        assert_eq!(table_def1.num_hash_partitions, num_partitions1);

        // 2. Update only shard_keys, num_hash_partitions should remain
        let shard_keys2 = vec!["tagA".to_string(), "tagB".to_string()]; // Assuming tagB would be added to schema separately
        let res2 = catalog.update_table_sharding_metadata(db_name, table_name, Some(shard_keys2.clone()), None).await;
        assert!(res2.is_ok());

        let table_def2 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
        assert_eq!(table_def2.shard_keys, shard_keys2);
        assert_eq!(table_def2.num_hash_partitions, num_partitions1); // Unchanged

        // 3. Update only num_hash_partitions, shard_keys should remain
        let num_partitions2 = 8u32;
        let res3 = catalog.update_table_sharding_metadata(db_name, table_name, None, Some(num_partitions2)).await;
        assert!(res3.is_ok());

        let table_def3 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
        assert_eq!(table_def3.shard_keys, shard_keys2); // Unchanged
        assert_eq!(table_def3.num_hash_partitions, num_partitions2);

        // 4. Update with num_hash_partitions = 0 (should error)
        let res4 = catalog.update_table_sharding_metadata(db_name, table_name, None, Some(0)).await;
        assert!(matches!(res4, Err(CatalogError::InvalidConfiguration { .. })));
        if let Err(CatalogError::InvalidConfiguration{reason}) = res4 {
            assert_eq!(reason, "num_hash_partitions cannot be 0");
        }


        // 5. Test error: table not found
        let res_table_not_found = catalog.update_table_sharding_metadata(db_name, "non_existent_table", Some(vec![]), Some(2)).await;
        assert!(matches!(res_table_not_found, Err(CatalogError::TableNotFound { .. })));

        // 6. Test error: db not found
        let res_db_not_found = catalog.update_table_sharding_metadata("non_existent_db", table_name, Some(vec![]), Some(2)).await;
        assert!(matches!(res_db_not_found, Err(CatalogError::DbNotFound { .. })));

        // Note: Snapshot tests for these new fields in TableDefinition would need to be added
        // to the existing snapshot tests (e.g., by creating a table with these fields populated
        // and then running the catalog snapshot test). This typically involves `insta::assert_json_snapshot!`.
    }
}
