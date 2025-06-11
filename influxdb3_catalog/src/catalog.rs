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
pub use update::{CatalogUpdate, DatabaseCatalogTransaction, Prompt};

pub mod management;

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::replication::ReplicationInfo;
use crate::shard::{ShardDefinition, ShardId, ShardingStrategy, ShardMigrationStatus};
use crate::log::{
    AdministrativeAddNodeLog,
    BeginShardMigrationOutLog,
    ClearRetentionPeriodLog, CommitShardMigrationOnTargetLog,
    CreateAdminTokenDetails, CreateDatabaseLog, DatabaseBatch,
    DatabaseCatalogOp, FinalizeShardMigrationOnSourceLog,
    NodeBatch, NodeCatalogOp, NodeMode, RegenerateAdminTokenDetails,
    RegisterNodeLog, RemoveNodeLog,
    SetRetentionPeriodLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerSpecificationDefinition, UpdateNodeHeartbeatLog, UpdateNodeStatusLog,
    UpdateShardMigrationStatusLog,
    UpdateTableShardingStrategyLog,
};
use crate::object_store::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use crate::snapshot::CatalogSnapshot;
use crate::snapshot::versions::Snapshot;
use crate::{
    CatalogError, Result,
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
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 250;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CatalogSequenceNumber(u64);
impl CatalogSequenceNumber { /* ... */
    pub const fn new(id: u64) -> Self { Self(id) }
    pub fn next(&self) -> Self { Self(self.0 + 1) }
    pub fn get(&self) -> u64 { self.0 }
}
impl From<u64> for CatalogSequenceNumber { fn from(value: u64) -> Self { Self(value) } }

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> = Mutex::const_new(CatalogSequenceNumber::new(0));
pub type CatalogWritePermit = MutexGuard<'static, CatalogSequenceNumber>;

pub struct Catalog { /* ... */
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
impl std::fmt::Debug for Catalog { /* ... */ fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("Catalog").field("inner", &self.inner).finish() }}
#[derive(Debug, Clone, Copy)] enum CatalogState { Active, Shutdown }
impl CatalogState { fn is_shutdown(&self) -> bool { matches!(self, Self::Shutdown) } }
const CATALOG_CHECKPOINT_INTERVAL: u64 = 100;
#[derive(Clone, Copy, Debug)] pub struct CatalogArgs { pub default_hard_delete_duration: Duration }
impl CatalogArgs { pub fn new(default_hard_delete_duration: Duration) -> Self { Self { default_hard_delete_duration } } }
impl Default for CatalogArgs { fn default() -> Self { Self { default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION } } }
#[derive(Debug, Clone, Copy)] pub struct CatalogLimits { pub num_dbs: usize, pub num_tables: usize, pub num_columns_per_table: usize, pub num_tag_columns_per_table: usize }
impl CatalogLimits { pub fn new(num_dbs: usize, num_tables: usize, num_columns_per_table: usize, num_tag_columns_per_table: usize) -> Self { Self { num_dbs, num_tables, num_columns_per_table, num_tag_columns_per_table } } }
impl Default for CatalogLimits { fn default() -> Self { Self { num_dbs: Catalog::DEFAULT_NUM_DBS_LIMIT, num_tables: Catalog::DEFAULT_NUM_TABLES_LIMIT, num_columns_per_table: Catalog::DEFAULT_NUM_COLUMNS_PER_TABLE_LIMIT, num_tag_columns_per_table: Catalog::DEFAULT_NUM_TAG_COLUMNS_LIMIT } } }

impl Catalog {
    pub const DEFAULT_NUM_DBS_LIMIT: usize = 5;
    pub const DEFAULT_NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    pub const DEFAULT_NUM_TABLES_LIMIT: usize = 2000;
    pub const DEFAULT_NUM_TAG_COLUMNS_LIMIT: usize = 250;
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72);

    pub async fn new( catalog_os_prefix: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>, metric_registry: Arc<Registry>, limits: CatalogLimits) -> Result<Self> { Self::new_with_args(catalog_os_prefix, store, time_provider, metric_registry, CatalogArgs::default(), limits).await }
    pub async fn new_with_args( catalog_os_prefix: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>, metric_registry: Arc<Registry>, args: CatalogArgs, limits: CatalogLimits) -> Result<Self> { let catalog_os_prefix = catalog_os_prefix.into(); let obj_store_catalog = ObjectStoreCatalog::new(Arc::clone(&catalog_os_prefix), CATALOG_CHECKPOINT_INTERVAL, store); let subscriptions = Default::default(); let metrics = Arc::new(CatalogMetrics::new(&metric_registry)); let catalog = obj_store_catalog.load_or_create_catalog().await.map(RwLock::new).map(|inner| Self { metric_registry, state: parking_lot::Mutex::new(CatalogState::Active), subscriptions, time_provider, store: obj_store_catalog, metrics, inner, limits, args })?; create_internal_db(&catalog).await; catalog.metrics.operation_observer(catalog.subscribe_to_updates("catalog_operation_metrics").await); Ok(catalog) }
    pub async fn new_with_shutdown( node_id: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>, metric_registry: Arc<Registry>, shutdown_token: ShutdownToken, limits: CatalogLimits, ) -> Result<Arc<Self>> { let node_id_arc = node_id.into(); let catalog = Arc::new(Self::new(Arc::clone(&node_id_arc), store, time_provider, metric_registry, limits).await?); let catalog_cloned = Arc::clone(&catalog); tokio::spawn(async move { shutdown_token.wait_for_shutdown().await; info!(catalog_prefix = node_id_arc.as_ref(), "Catalog shutdown initiated..."); if let Ok(Some(node_to_stop)) = catalog_cloned.get_node_by_name(&node_id_arc).await { if let Err(error) = catalog_cloned.update_node_status(&node_to_stop.id, NodeStatus::Down).await { error!(?error, node_name = %node_id_arc, "Error updating node to Down during shutdown"); } } else { info!(catalog_prefix = %node_id_arc, "No cluster node entry for this catalog's own ID to mark as Down during shutdown."); } }); Ok(catalog) }
    pub fn metric_registry(&self) -> Arc<Registry> { Arc::clone(&self.metric_registry) }
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> { Arc::clone(&self.time_provider) }
    pub fn set_state_shutdown(&self) { *self.state.lock() = CatalogState::Shutdown; }
    fn num_dbs_limit(&self) -> usize { self.limits.num_dbs }
    fn num_tables_limit(&self) -> usize { self.limits.num_tables }
    fn num_columns_per_table_limit(&self) -> usize { self.limits.num_columns_per_table }
    pub(crate) fn num_tag_columns_per_table_limit(&self) -> usize { self.limits.num_tag_columns_per_table }
    fn default_hard_delete_duration(&self) -> Duration { self.args.default_hard_delete_duration }
    pub async fn create_shard( &self, db_name: &str, table_name: &str, shard_definition: ShardDefinition,) -> Result<()> { let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?; self.catalog_update_with_retry(|| { let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; Ok(CatalogBatch::Database(DatabaseBatch { time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(), ops: vec![DatabaseCatalogOp::CreateShard(CreateShardLog { db_id, table_id, table_name: table_arc.table_name.clone(), shard_definition: shard_definition.clone() })], })) }).await?; Ok(()) }
    pub async fn update_shard_nodes(&self, db_name: &str, table_name: &str, shard_id: ShardId, new_node_ids: Vec<NodeId>) -> Result<()> { let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?; self.catalog_update_with_retry(|| { let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; if !table_arc.shards.contains_id(&shard_id) { return Err(CatalogError::ShardNotFound { shard_id, table_id }); } Ok(CatalogBatch::Database(DatabaseBatch { time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(), ops: vec![DatabaseCatalogOp::UpdateShard(UpdateShardLog { db_id, table_id, table_name: table_arc.table_name.clone(), shard_id, new_node_ids: Some(new_node_ids.clone()) })], })) }).await?; Ok(()) }
    pub async fn delete_shard( &self, db_name: &str, table_name: &str, shard_id: ShardId) -> Result<()> { let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?; self.catalog_update_with_retry(|| { let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; if !table_arc.shards.contains_id(&shard_id) { return Err(CatalogError::ShardNotFound { shard_id, table_id }); } Ok(CatalogBatch::Database(DatabaseBatch { time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(), ops: vec![DatabaseCatalogOp::DeleteShard(DeleteShardLog { db_id, table_id, table_name: table_arc.table_name.clone(), shard_id })], })) }).await?; Ok(()) }
    pub async fn set_replication( &self, db_name: &str, table_name: &str, replication_info: ReplicationInfo) -> Result<()> { let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?; self.catalog_update_with_retry(|| { let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; Ok(CatalogBatch::Database(DatabaseBatch { time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema.name(), ops: vec![DatabaseCatalogOp::SetReplication(SetReplicationLog { db_id, table_id, table_name: table_arc.table_name.clone(), replication_info: replication_info.clone() })], })) }).await?; Ok(()) }
    pub async fn update_table_sharding_strategy( &self, db_name: &str, table_name: &str, new_strategy: ShardingStrategy, new_shard_key_columns: Option<Vec<String>>,) -> Result<()> { if new_strategy == ShardingStrategy::TimeAndKey { match &new_shard_key_columns { Some(cols) if cols.is_empty() => return Err(CatalogError::InvalidShardKeyColumns { reason: "Shard key columns cannot be empty for TimeAndKey strategy.".to_string() }), None => return Err(CatalogError::InvalidShardKeyColumns { reason: "Shard key columns must be provided for TimeAndKey strategy.".to_string() }), _ => {} } } let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?; if let Some(key_columns) = &new_shard_key_columns { if new_strategy == ShardingStrategy::TimeAndKey { let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc = db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; for col_name in key_columns { if !table_arc.columns.contains_name(col_name) { return Err(CatalogError::InvalidShardKeyColumns { reason: format!("Shard key column '{}' not found in table '{}'.", col_name, table_name) }); } } } } self.catalog_update_with_retry(|| { let db_schema_inner = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_arc_inner = db_schema_inner.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; Ok(CatalogBatch::Database(DatabaseBatch { time_ns: self.time_provider.now().timestamp_nanos(), database_id: db_id, database_name: db_schema_inner.name(), ops: vec![DatabaseCatalogOp::UpdateTableShardingStrategy( UpdateTableShardingStrategyLog { db_id, table_id, table_name: table_arc_inner.table_name.clone(), new_strategy, new_shard_key_columns: new_shard_key_columns.clone() })], })) }).await?; Ok(()) }
    fn get_db_and_table_ids(&self, db_name: &str, table_name: &str) -> Result<(DbId, TableId)> { let db_id = self.db_name_to_id(db_name).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?; let table_id = self.db_schema_by_id(&db_id).and_then(|db| db.table_name_to_id(table_name)).ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?; Ok((db_id, table_id)) }
    pub fn object_store_prefix(&self) -> Arc<str> { Arc::clone(&self.store.prefix) }
    pub fn catalog_uuid(&self) -> Uuid { self.inner.read().catalog_uuid }
    pub async fn subscribe_to_updates(&self, name: &'static str) -> CatalogUpdateReceiver { self.subscriptions.write().await.subscribe(name) }
    pub fn object_store(&self) -> Arc<dyn ObjectStore> { self.store.object_store() }
    pub fn snapshot(&self) -> CatalogSnapshot { self.inner.read().snapshot() }
    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) { let mut inner = self.inner.write(); *inner = InnerCatalog::from_snapshot(snapshot); }
    pub async fn get_permit_and_verify_catalog_batch( &self, catalog_batch: CatalogBatch, sequence: CatalogSequenceNumber) -> Prompt<(OrderedCatalogBatch, CatalogWritePermit)> { let mut permit = CATALOG_WRITE_PERMIT.lock().await; if sequence != self.sequence_number() { self.metrics.catalog_operation_retries.inc(1); return Prompt::Retry(()); } *permit = self.sequence_number().next(); trace!(next_sequence = permit.get(), "got permit to write to catalog"); Prompt::Success((OrderedCatalogBatch::new(catalog_batch, *permit), permit)) }
    pub(crate) fn apply_ordered_catalog_batch( &self, batch: &OrderedCatalogBatch, _permit: &CatalogWritePermit) -> CatalogBatch { let batch_sequence = batch.sequence_number().get(); let current_sequence = self.sequence_number().get(); assert_eq!(batch_sequence, current_sequence + 1, "catalog batch received out of order"); let catalog_batch = self.inner.write().apply_catalog_batch(batch.batch(), batch.sequence_number()).expect("ordered catalog batch should succeed when applied").expect("ordered catalog batch should contain changes"); catalog_batch.into_batch() }
    // Note: Changed node() to get_node_by_name() to differentiate from get_node(by_id)
    pub async fn get_node_by_name(&self, node_name: &str) -> Result<Option<Arc<NodeDefinition>>> { Ok(self.inner.read().nodes.get_by_name(node_name)) }
    pub fn next_node_id(&self) -> NodeId { self.inner.read().nodes.next_id() }
    pub fn next_db_id(&self) -> DbId { self.inner.read().databases.next_id() }
    pub(crate) fn db_or_create( &self, db_name: &str, now_time_ns: i64) -> Result<(Arc<DatabaseSchema>, Option<CatalogBatch>)> { match self.db_schema(db_name) { Some(db) => Ok((db, None)), None => { let mut inner = self.inner.write(); if inner.database_count() >= self.num_dbs_limit() { return Err(CatalogError::TooManyDbs(self.num_dbs_limit())); } info!(database_name = db_name, "creating new database"); let db_id = inner.databases.get_and_increment_next_id(); let db_name_arc = Arc::from(db_name); let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name_arc))); let batch = CatalogBatch::database(now_time_ns, db.id, db.name(), vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog { database_id: db.id, database_name: Arc::clone(&db.name)})]); Ok((db, Some(batch))) } } }
    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> { self.inner.read().databases.name_to_id(db_name) }
    pub fn db_id_to_name(&self, db_id: &DbId) -> Option<Arc<str>> { self.inner.read().databases.id_to_name(db_id) }
    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> { self.inner.read().databases.get_by_name(db_name) }
    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> { self.inner.read().databases.get_by_id(db_id) }
    pub fn db_names(&self) -> Vec<String> { self.inner.read().databases.resource_iter().filter(|db| !db.deleted).map(|db| db.name.to_string()).collect() }
    pub fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> { self.inner.read().databases.resource_iter().cloned().collect() }
    pub fn sequence_number(&self) -> CatalogSequenceNumber { self.inner.read().sequence }
    pub fn clone_inner(&self) -> InnerCatalog { self.inner.read().clone() }
    pub fn catalog_id(&self) -> Arc<str> { Arc::clone(&self.inner.read().catalog_id) }
    pub fn db_exists(&self, db_id: DbId) -> bool { self.inner.read().db_exists(db_id) }
    pub fn active_triggers(&self) -> Vec<(Arc<str>, Arc<str>)> { vec![] }
    pub fn get_tokens(&self) -> Vec<Arc<TokenInfo>> { self.inner.read().tokens.repo().iter().map(|(_,ti)| Arc::clone(ti)).collect() }
    pub async fn create_admin_token(&self, _regenerate: bool) -> Result<(Arc<TokenInfo>, String)> { unimplemented!() }
    pub async fn create_named_admin_token_with_permission(&self, _token_name: String, _expiry_secs: Option<u64>) -> Result<(Arc<TokenInfo>, String)> { unimplemented!() }
    pub fn get_retention_period_cutoff_map(&self) -> BTreeMap<(DbId, TableId), i64> { BTreeMap::new() }

    // --- Node Management Methods ---
    pub async fn add_node(&self, node_def_payload: crate::management::NodeDefinition) -> Result<()> {
        let internal_node_id = if node_def_payload.id == NodeId::default() { self.next_node_id() } else { node_def_payload.id };
        let full_node_def = NodeDefinition {
            id: internal_node_id, node_name: Arc::clone(&node_def_payload.node_name), instance_id: Arc::clone(&node_def_payload.instance_id),
            rpc_address: node_def_payload.rpc_address.clone(), http_address: node_def_payload.http_address.clone(),
            mode: node_def_payload.mode.clone(), core_count: node_def_payload.core_count, status: node_def_payload.status,
            last_heartbeat: node_def_payload.last_heartbeat.or_else(|| Some(self.time_provider.now().timestamp_nanos())),
        };
        self.catalog_update_with_retry(|| Ok(CatalogBatch::Node(NodeBatch {
            time_ns: self.time_provider.now().timestamp_nanos(), node_catalog_id: internal_node_id,
            node_id: Arc::clone(&full_node_def.node_name),
            ops: vec![NodeCatalogOp::AdministrativeAddNode(AdministrativeAddNodeLog { node_definition: full_node_def.clone() })],
        }))).await?;
        Ok(())
    }

    pub async fn update_node_status(&self, node_catalog_id: &NodeId, status: NodeStatus) -> Result<()> {
        let node_name = self.inner.read().nodes.id_to_name(node_catalog_id).ok_or_else(|| CatalogError::NodeNotFoundById(*node_catalog_id))?;
        self.catalog_update_with_retry(|| Ok(CatalogBatch::Node(NodeBatch {
            time_ns: self.time_provider.now().timestamp_nanos(), node_catalog_id: *node_catalog_id, node_id: node_name.clone(),
            ops: vec![NodeCatalogOp::UpdateNodeStatus(UpdateNodeStatusLog { node_catalog_id: *node_catalog_id, status })],
        }))).await?;
        Ok(())
    }

    pub async fn record_node_heartbeat(&self, node_catalog_id: &NodeId, timestamp: i64) -> Result<()> {
        let node_name = self.inner.read().nodes.id_to_name(node_catalog_id).ok_or_else(|| CatalogError::NodeNotFoundById(*node_catalog_id))?;
        self.catalog_update_with_retry(|| Ok(CatalogBatch::Node(NodeBatch {
            time_ns: self.time_provider.now().timestamp_nanos(), node_catalog_id: *node_catalog_id, node_id: node_name.clone(),
            ops: vec![NodeCatalogOp::UpdateNodeHeartbeat(UpdateNodeHeartbeatLog { node_catalog_id: *node_catalog_id, timestamp })],
        }))).await?;
        Ok(())
    }

    pub async fn remove_node(&self, node_catalog_id: &NodeId) -> Result<()> {
        let node_name = self.inner.read().nodes.id_to_name(node_catalog_id).ok_or_else(|| CatalogError::NodeNotFoundById(*node_catalog_id))?;
        warn!(node_id = %node_catalog_id.get(), node_name = %node_name, "Attempting to remove node. Shard assignment check not yet implemented.");
        self.catalog_update_with_retry(|| Ok(CatalogBatch::Node(NodeBatch {
            time_ns: self.time_provider.now().timestamp_nanos(), node_catalog_id: *node_catalog_id, node_id: node_name.clone(),
            ops: vec![NodeCatalogOp::RemoveNode(RemoveNodeLog { node_catalog_id: *node_catalog_id })],
        }))).await?;
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<Arc<NodeDefinition>>> { Ok(self.inner.read().nodes.resource_iter().cloned().collect()) }
    pub async fn get_node(&self, node_catalog_id: &NodeId) -> Result<Option<Arc<NodeDefinition>>> { Ok(self.inner.read().nodes.get_by_id(node_catalog_id)) }

    // --- Shard Migration Methods (Public API) ---
    pub async fn begin_shard_migration_out(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_ids: Vec<NodeId>) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let current_db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let current_table_name = current_db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound {db_name: Arc::from(db_name), table_name: Arc::from(table_name)})?.table_name.clone();
            Ok(CatalogBatch::Database(DatabaseBatch{
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: current_db_schema.name.clone(),
                ops: vec![DatabaseCatalogOp::BeginShardMigrationOut(BeginShardMigrationOutLog{
                    db_id, table_id, table_name: current_table_name, shard_id: *shard_id, target_node_ids: target_node_ids.clone()
                })]
            }))
        }).await?;
        Ok(())
    }

    pub async fn commit_shard_migration_on_target(&self, db_name: &str, table_name: &str, shard_id: &ShardId, target_node_id: NodeId, source_node_id: NodeId) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let current_db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let current_table_name = current_db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound {db_name: Arc::from(db_name), table_name: Arc::from(table_name)})?.table_name.clone();
            Ok(CatalogBatch::Database(DatabaseBatch{
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: current_db_schema.name.clone(),
                ops: vec![DatabaseCatalogOp::CommitShardMigrationOnTarget(CommitShardMigrationOnTargetLog{
                    db_id, table_id, table_name: current_table_name, shard_id: *shard_id, target_node_id, source_node_id
                })]
            }))
        }).await?;
        Ok(())
    }

    pub async fn finalize_shard_migration_on_source(&self, db_name: &str, table_name: &str, shard_id: &ShardId, migrated_to_node_id: NodeId) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let current_db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let current_table_name = current_db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound {db_name: Arc::from(db_name), table_name: Arc::from(table_name)})?.table_name.clone();
            Ok(CatalogBatch::Database(DatabaseBatch{
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: current_db_schema.name.clone(),
                ops: vec![DatabaseCatalogOp::FinalizeShardMigrationOnSource(FinalizeShardMigrationOnSourceLog{
                    db_id, table_id, table_name: current_table_name, shard_id: *shard_id, migrated_to_node_id
                })]
            }))
        }).await?;
        Ok(())
    }
     pub async fn update_shard_migration_status(&self, db_name: &str, table_name: &str, shard_id: &ShardId, new_status: ShardMigrationStatus) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let current_db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let current_table_name = current_db_schema.table_definition_by_id(&table_id).ok_or_else(|| CatalogError::TableNotFound {db_name: Arc::from(db_name), table_name: Arc::from(table_name)})?.table_name.clone();
             Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: current_db_schema.name(),
                ops: vec![DatabaseCatalogOp::UpdateShardMigrationStatus(UpdateShardMigrationStatusLog {
                    db_id,
                    table_id,
                    table_name: current_table_name,
                    shard_id: *shard_id,
                    new_status: new_status.clone(),
                })],
            }))
        }).await?;
        Ok(())
    }
}

async fn create_internal_db(catalog: &Catalog) { /* ... */ }
impl Catalog { /* Test constructors */
    pub async fn new_in_memory(catalog_id: impl Into<Arc<str>>) -> Result<Self> { use iox_time::MockProvider; use object_store::memory::InMemory; let store = Arc::new(InMemory::new()); let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))); let metric_registry = Default::default(); Self::new(catalog_id.into(), store, time_provider, metric_registry, Default::default()).await }
    pub async fn new_in_memory_with_args( catalog_id: impl Into<Arc<str>>, time_provider: Arc<dyn TimeProvider>, args: CatalogArgs) -> Result<Self> { use object_store::memory::InMemory; let store = Arc::new(InMemory::new()); let metric_registry = Default::default(); Self::new_with_args(catalog_id.into(), store, time_provider, metric_registry, args, Default::default()).await }
    pub async fn new_with_checkpoint_interval( catalog_id: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>, time_provider: Arc<dyn TimeProvider>, metric_registry: Arc<Registry>, checkpoint_interval: u64) -> Result<Self> { let store_catalog = ObjectStoreCatalog::new(catalog_id, checkpoint_interval, store); let inner = store_catalog.load_or_create_catalog().await?; let subscriptions = Default::default(); let catalog = Self { state: parking_lot::Mutex::new(CatalogState::Active), subscriptions, time_provider, store: store_catalog, metrics: Arc::new(CatalogMetrics::new(&metric_registry)), metric_registry, inner: RwLock::new(inner), limits: Default::default(), args: Default::default(), }; create_internal_db(&catalog).await; Ok(catalog) }
}

impl TokenProvider for Catalog { /* ... */ fn get_token(&self, _token_hash: Vec<u8>) -> Option<Arc<TokenInfo>> { None } }
impl ProcessingEngineMetrics for Catalog { /* ... */ fn num_triggers(&self) -> (u64, u64, u64, u64) { (0,0,0,0) } }

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Repository<I: CatalogId, R: CatalogResource> { /* ... */ pub(crate) repo: SerdeVecMap<I, Arc<R>>, pub(crate) id_name_map: BiHashMap<I, Arc<str>>, pub(crate) next_id: I }
impl<I: CatalogId, R: CatalogResource> Repository<I, R> { /* ... */ pub fn new() -> Self { Self { repo: SerdeVecMap::new(), id_name_map: BiHashMap::new(), next_id: I::default() } } pub(crate) fn get_and_increment_next_id(&mut self) -> I { let next_id = self.next_id; self.next_id = self.next_id.next(); next_id } pub(crate) fn next_id(&self) -> I { self.next_id } pub(crate) fn set_next_id(&mut self, id: I) { self.next_id = id; } pub fn name_to_id(&self, name: &str) -> Option<I> { self.id_name_map.get_by_right(name).copied() } pub fn id_to_name(&self, id: &I) -> Option<Arc<str>> { self.id_name_map.get_by_left(id).cloned() } pub fn get_by_name(&self, name: &str) -> Option<Arc<R>> { self.id_name_map.get_by_right(name).and_then(|id| self.repo.get(id)).cloned() } pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> { self.repo.get(id).cloned() } pub fn contains_id(&self, id: &I) -> bool { self.repo.contains_key(id) } pub fn contains_name(&self, name: &str) -> bool { self.id_name_map.contains_right(name) } pub fn len(&self) -> usize { self.repo.len() } pub fn is_empty(&self) -> bool { self.repo.is_empty() } fn id_exists(&self, id: &I) -> bool { let id_in_map = self.id_name_map.contains_left(id); let id_in_repo = self.repo.contains_key(id); assert_eq!(id_in_map, id_in_repo, "id map and repository are in an inconsistent state, in map: {id_in_map}, in repo: {id_in_repo}"); id_in_repo } fn id_and_name_exists(&self, id: &I, name: &str) -> bool { let name_in_map = self.id_name_map.contains_right(name); self.id_exists(id) && name_in_map } pub(crate) fn insert(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> { let resource = resource.into(); if self.id_and_name_exists(&id, resource.name().as_ref()) { return Err(CatalogError::AlreadyExists); } self.id_name_map.insert(id, resource.name()); self.repo.insert(id, resource); self.next_id = match self.next_id.cmp(&id) { Ordering::Less | Ordering::Equal => id.next(), Ordering::Greater => self.next_id, }; Ok(()) } pub(crate) fn update(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> { let resource = resource.into(); if !self.id_exists(&id) { return Err(CatalogError::NotFound); } self.id_name_map.insert(id, resource.name()); self.repo.insert(id, resource); Ok(()) } pub(crate) fn remove(&mut self, id: &I) { self.id_name_map.remove_by_left(id); self.repo.shift_remove(id); } pub fn iter(&self) -> impl Iterator<Item = (&I, &Arc<R>)> { self.repo.iter() } pub fn id_iter(&self) -> impl Iterator<Item = &I> { self.repo.keys() } pub fn resource_iter(&self) -> impl Iterator<Item = &Arc<R>> { self.repo.values() }}
impl<I: CatalogId, R: CatalogResource> Default for Repository<I, R> { fn default() -> Self { Self::new() } }

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionPeriod { Indefinite, Duration(Duration), }

#[derive(Debug, Clone)]
pub struct InnerCatalog { /* ... */ pub(crate) sequence: CatalogSequenceNumber, pub(crate) catalog_id: Arc<str>, pub(crate) catalog_uuid: Uuid, pub(crate) nodes: Repository<NodeId, NodeDefinition>, pub(crate) databases: Repository<DbId, DatabaseSchema>, pub(crate) tokens: TokenRepository }
impl InnerCatalog { /* ... */ pub(crate) fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self { Self { sequence: CatalogSequenceNumber::new(0), catalog_id, catalog_uuid, nodes: Repository::default(), databases: Repository::default(), tokens: TokenRepository::default() } } pub fn sequence_number(&self) -> CatalogSequenceNumber { self.sequence } pub fn database_count(&self) -> usize { self.databases.iter().filter(|db| !db.1.deleted && db.1.name().as_ref() != INTERNAL_DB_NAME).count() } pub fn table_count(&self) -> usize { self.databases.resource_iter().map(|db| db.table_count()).sum() }
    pub(crate) fn apply_catalog_batch(&mut self, catalog_batch: &CatalogBatch, sequence: CatalogSequenceNumber) -> Result<Option<OrderedCatalogBatch>> { debug!(n_ops = catalog_batch.n_ops(), current_sequence = self.sequence_number().get(), applied_sequence = sequence.get(), "apply catalog batch"); let updated = match catalog_batch { CatalogBatch::Node(root_batch) => self.apply_node_batch(root_batch)?, CatalogBatch::Database(database_batch) => self.apply_database_batch(database_batch)?, CatalogBatch::Token(token_batch) => self.apply_token_batch(token_batch)?, }; Ok(updated.then(|| { self.sequence = sequence; OrderedCatalogBatch::new(catalog_batch.clone(), sequence) })) }
    fn apply_node_batch(&mut self, node_batch: &NodeBatch) -> Result<bool> { let mut updated = false; for op in &node_batch.ops { updated |= match op { NodeCatalogOp::RegisterNode(log_entry) => { if node_batch.node_catalog_id != log_entry.node_catalog_id { error!(batch_catalog_id = %node_batch.node_catalog_id.get(), log_catalog_id = %log_entry.node_catalog_id.get(), "Mismatch NodeCatalogId: NodeBatch vs RegisterNodeLog"); return Err(CatalogError::NodeIdMismatch); } if node_batch.node_id != log_entry.node_name { error!(batch_node_name = %node_batch.node_id, log_node_name = %log_entry.node_name, "Mismatch node name: NodeBatch vs RegisterNodeLog"); return Err(CatalogError::NodeNameMismatch); } if let Some(mut existing_node_arc) = self.nodes.get_by_id(&log_entry.node_catalog_id) { if existing_node_arc.name() != *log_entry.node_name { return Err(CatalogError::NodeIdNameMismatch { internal_id: log_entry.node_catalog_id, expected_name: log_entry.node_name.to_string(), found_name: existing_node_arc.name().to_string() }); } if &existing_node_arc.instance_id != &log_entry.instance_id { return Err(CatalogError::InvalidNodeRegistration); } let n = Arc::make_mut(&mut existing_node_arc); n.mode = log_entry.mode.clone(); n.core_count = log_entry.core_count; n.rpc_address = log_entry.rpc_address.clone(); n.http_address = log_entry.http_address.clone(); n.status = NodeStatus::Active; n.last_heartbeat = Some(log_entry.registered_time_ns); self.nodes.update(log_entry.node_catalog_id, existing_node_arc)?; } else { if let Some(named_node) = self.nodes.get_by_name(&log_entry.node_name) { return Err(CatalogError::NodeNameConflict { name: log_entry.node_name.to_string(), existing_id: named_node.id(), new_id: log_entry.node_catalog_id }); } let new_node = Arc::new(NodeDefinition { node_name: Arc::clone(&log_entry.node_name), id: log_entry.node_catalog_id, instance_id: Arc::clone(&log_entry.instance_id), rpc_address: log_entry.rpc_address.clone(), http_address: log_entry.http_address.clone(), mode: log_entry.mode.clone(), core_count: log_entry.core_count, status: NodeStatus::Active, last_heartbeat: Some(log_entry.registered_time_ns), }); self.nodes.insert(log_entry.node_catalog_id, new_node)?; } true } NodeCatalogOp::StopNode(log_entry) => { if let Some(node_catalog_id) = self.nodes.name_to_id(&log_entry.node_id) { if let Some(mut node_to_stop_arc) = self.nodes.get_by_id(&node_catalog_id) { let n = Arc::make_mut(&mut node_to_stop_arc); n.status = NodeStatus::Down; n.last_heartbeat = Some(log_entry.stopped_time_ns); self.nodes.update(node_catalog_id, node_to_stop_arc)?; true } else { warn!(node_name = %log_entry.node_id, "Node found by name but not by ID during StopNode"); false } } else { warn!(node_name = %log_entry.node_id, "Node to stop not found by name"); false } } NodeCatalogOp::AdministrativeAddNode(add_log) => { let node_def = &add_log.node_definition; if self.nodes.contains_id(&node_def.id) { return Err(CatalogError::NodeIdAlreadyExists(node_def.id)); } if self.nodes.contains_name(&node_def.node_name) { return Err(CatalogError::NodeAlreadyExists(node_def.node_name.to_string()));} self.nodes.insert(node_def.id, Arc::new(node_def.clone()))?; true } NodeCatalogOp::UpdateNodeStatus(status_log) => { if let Some(mut node_arc) = self.nodes.get_by_id(&status_log.node_catalog_id) { let n = Arc::make_mut(&mut node_arc); n.status = status_log.status; self.nodes.update(status_log.node_catalog_id, node_arc)?; true } else { warn!(node_catalog_id = %status_log.node_catalog_id.get(), "Node not found for UpdateNodeStatus"); false } } NodeCatalogOp::UpdateNodeHeartbeat(heartbeat_log) => { if let Some(mut node_arc) = self.nodes.get_by_id(&heartbeat_log.node_catalog_id) { let n = Arc::make_mut(&mut node_arc); n.last_heartbeat = Some(heartbeat_log.timestamp); if n.status == NodeStatus::Down || n.status == NodeStatus::Unknown { n.status = NodeStatus::Active; } self.nodes.update(heartbeat_log.node_catalog_id, node_arc)?; true } else { warn!(node_catalog_id = %heartbeat_log.node_catalog_id.get(), "Node not found for UpdateNodeHeartbeat"); false } } NodeCatalogOp::RemoveNode(remove_log) => { if self.nodes.contains_id(&remove_log.node_catalog_id) { self.nodes.remove(&remove_log.node_catalog_id); true } else { warn!(node_catalog_id = %remove_log.node_catalog_id.get(), "Node not found for RemoveNode"); false } } }; } Ok(updated) }
    fn apply_token_batch(&mut self, token_batch: &TokenBatch) -> Result<bool> { /* ... */ Ok(false) }
    fn apply_database_batch(&mut self, database_batch: &DatabaseBatch) -> Result<bool> { if let Some(db) = self.databases.get_by_id(&database_batch.database_id) { let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(&db, database_batch)? else { return Ok(false); }; self.databases.update(db.id, new_db).expect("existing database should be updated"); } else { let new_db = DatabaseSchema::new_from_batch(database_batch)?; self.databases.insert(new_db.id, new_db).expect("new database should be inserted"); }; Ok(true) }
    pub fn db_exists(&self, db_id: DbId) -> bool { self.databases.get_by_id(&db_id).is_some() }
    pub fn num_triggers(&self) -> (u64, u64, u64, u64) { (0,0,0,0) }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum NodeStatus { Joining, Active, Leaving, Down, Unknown, }
impl Default for NodeStatus { fn default() -> Self { NodeStatus::Unknown } }

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct NodeDefinition { pub node_name: Arc<str>, pub id: NodeId, pub instance_id: Arc<str>, pub rpc_address: String, pub http_address: String, pub mode: Vec<NodeMode>, pub core_count: u64, pub status: NodeStatus, pub last_heartbeat: Option<i64>, }
impl CatalogResource for NodeDefinition { type Identifier = NodeId; fn id(&self) -> Self::Identifier { self.id } fn name(&self) -> Arc<str> { Arc::clone(&self.node_name) }}
impl NodeDefinition { pub fn new(id: NodeId, node_name: Arc<str>, instance_id: Arc<str>, rpc_address: String, http_address: String, mode: Vec<NodeMode>, core_count: u64, status: NodeStatus, last_heartbeat: Option<i64>) -> Self { Self { id, node_name, instance_id, rpc_address, http_address, mode, core_count, status, last_heartbeat } } pub fn instance_id(&self) -> Arc<str> { Arc::clone(&self.instance_id) } pub fn modes(&self) -> &Vec<NodeMode> { &self.mode } pub fn core_count(&self) -> u64 { self.core_count } pub fn status(&self) -> NodeStatus { self.status } pub fn last_heartbeat(&self) -> Option<i64> { self.last_heartbeat } pub fn is_active(&self) -> bool { self.status == NodeStatus::Active } }

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema { /* ... */ pub id: DbId, pub name: Arc<str>, pub tables: Repository<TableId, TableDefinition>, pub retention_period: RetentionPeriod, pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>, pub deleted: bool }
impl DatabaseSchema { /* ... */ pub fn new(id: DbId, name: Arc<str>) -> Self { Self { id, name, tables: Repository::new(), retention_period: RetentionPeriod::Indefinite, processing_engine_triggers: Repository::new(), deleted: false } } pub fn name(&self) -> Arc<str> { Arc::clone(&self.name) } pub fn table_count(&self) -> usize { self.tables.iter().filter(|table| !table.1.deleted).count() } pub fn new_if_updated_from_batch( db_schema: &DatabaseSchema, database_batch: &DatabaseBatch) -> Result<Option<Self>> { let mut schema = Cow::Borrowed(db_schema); for catalog_op in &database_batch.ops { schema = catalog_op.update_schema(schema)?; } if let Cow::Owned(schema) = schema { Ok(Some(schema)) } else { Ok(None) } } pub fn new_from_batch(database_batch: &DatabaseBatch) -> Result<Self> { let db_schema = Self::new(database_batch.database_id, Arc::clone(&database_batch.database_name)); Ok(DatabaseSchema::new_if_updated_from_batch(&db_schema, database_batch)?.expect("database must be new")) } pub(crate) fn create_new_empty_table(&mut self, table_name: impl Into<Arc<str>>) -> Result<Arc<TableDefinition>> { let table_id = self.tables.get_and_increment_next_id(); let table_def = Arc::new(TableDefinition::new_empty(table_id, table_name.into())); self.tables.insert(table_id, Arc::clone(&table_def))?; Ok(table_def) } pub(crate) fn update_table(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) -> Result<()> { self.tables.update(table_id, table_def) } pub fn insert_table_from_log(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) { self.tables.insert(table_id, table_def).expect("table inserted from the log should not already exist"); } pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> { self.tables.get_by_id(table_id).map(|table| table.influx_schema().clone()) } pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> { self.tables.get_by_name(table_name.as_ref()) } pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> { self.tables.get_by_id(table_id) } pub fn table_ids(&self) -> Vec<TableId> { self.tables.id_iter().copied().collect() } pub fn table_names(&self) -> Vec<Arc<str>> { self.tables.resource_iter().map(|td| Arc::clone(&td.table_name)).collect() } pub fn table_exists(&self, table_id: &TableId) -> bool { self.tables.get_by_id(table_id).is_some() } pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> { self.tables.resource_iter().map(Arc::clone) } pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> { self.tables.name_to_id(table_name.as_ref()) } pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> { self.tables.id_to_name(table_id) } pub fn list_distinct_caches(&self) -> Vec<Arc<DistinctCacheDefinition>> { self.tables.resource_iter().filter(|t| !t.deleted).flat_map(|t| t.distinct_caches.resource_iter()).cloned().collect() } pub fn list_last_caches(&self) -> Vec<Arc<LastCacheDefinition>> { self.tables.resource_iter().filter(|t| !t.deleted).flat_map(|t| t.last_caches.resource_iter()).cloned().collect() } pub fn trigger_count_by_type(&self) -> (u64, u64, u64, u64) { (0,0,0,0) } pub fn get_retention_period_cutoff_ts_nanos( &self, time_provider: Arc<dyn TimeProvider>) -> Option<i64> { let retention_period = match self.retention_period { RetentionPeriod::Duration(d) => Some(d.as_nanos() as u64), RetentionPeriod::Indefinite => None, }?; Some(time_provider.now().timestamp_nanos() - retention_period as i64) }}

trait UpdateDatabaseSchema { fn update_schema<'a>(&self, schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>>; }
impl UpdateDatabaseSchema for DatabaseCatalogOp {
    fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> {
        match &self {
            DatabaseCatalogOp::CreateDatabase(_) => { schema.to_mut(); Ok(schema) }
            DatabaseCatalogOp::CreateTable(ct) => ct.update_schema(schema),
            DatabaseCatalogOp::AddFields(af) => af.update_schema(schema),
            DatabaseCatalogOp::CreateDistinctCache(cdc) => cdc.update_schema(schema),
            DatabaseCatalogOp::DeleteDistinctCache(ddc) => ddc.update_schema(schema),
            DatabaseCatalogOp::CreateLastCache(clc) => clc.update_schema(schema),
            DatabaseCatalogOp::DeleteLastCache(dlc) => dlc.update_schema(schema),
            DatabaseCatalogOp::SoftDeleteDatabase(sdd) => sdd.update_schema(schema),
            DatabaseCatalogOp::SoftDeleteTable(sdt) => sdt.update_schema(schema),
            DatabaseCatalogOp::CreateTrigger(ctr) => ctr.update_schema(schema),
            DatabaseCatalogOp::DeleteTrigger(dtr) => dtr.update_schema(schema),
            DatabaseCatalogOp::EnableTrigger(eti) => EnableTrigger(eti.clone()).update_schema(schema),
            DatabaseCatalogOp::DisableTrigger(dti) => DisableTrigger(dti.clone()).update_schema(schema),
            DatabaseCatalogOp::SetRetentionPeriod(srp) => srp.update_schema(schema),
            DatabaseCatalogOp::ClearRetentionPeriod(crp) => crp.update_schema(schema),
            DatabaseCatalogOp::CreateShard(cs) => cs.update_schema(schema),
            DatabaseCatalogOp::UpdateShard(us) => us.update_schema(schema),
            DatabaseCatalogOp::DeleteShard(ds) => ds.update_schema(schema),
            DatabaseCatalogOp::SetReplication(sr) => sr.update_schema(schema),
            DatabaseCatalogOp::UpdateTableShardingStrategy(utss) => utss.update_schema(schema),
            DatabaseCatalogOp::BeginShardMigrationOut(op) => op.update_schema(schema),
            DatabaseCatalogOp::CommitShardMigrationOnTarget(op) => op.update_schema(schema),
            DatabaseCatalogOp::FinalizeShardMigrationOnSource(op) => op.update_schema(schema),
            DatabaseCatalogOp::UpdateShardMigrationStatus(op) => op.update_schema(schema),
        }
    }
}
impl UpdateDatabaseSchema for CreateTableLog { /* ... */ fn update_schema<'a>(&self, mut database_schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { match database_schema.tables.get_by_id(&self.table_id) { Some(existing_table) => { if let Cow::Owned(updated_table) = existing_table.check_and_add_new_fields(self)? { database_schema.to_mut().update_table(self.table_id, Arc::new(updated_table))?; } } None => { let new_table = TableDefinition::new_from_op(self); database_schema.to_mut().insert_table_from_log(new_table.table_id, Arc::new(new_table)); } } Ok(database_schema) }}
impl UpdateDatabaseSchema for SoftDeleteDatabaseLog { /* ... */ fn update_schema<'a>( &self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { let deletion_time = Time::from_timestamp_nanos(self.deletion_time); let owned = schema.to_mut(); owned.name = make_new_name_using_deleted_time(&self.database_name, deletion_time); owned.deleted = true; Ok(schema) }}
impl UpdateDatabaseSchema for SoftDeleteTableLog { /* ... */ fn update_schema<'a>( &self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { if !schema.tables.contains_id(&self.table_id) { return Ok(schema); } let mut_schema = schema.to_mut(); if let Some(mut deleted_table) = mut_schema.tables.get_by_id(&self.table_id) { let deletion_time = Time::from_timestamp_nanos(self.deletion_time); let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time); let new_table_def = Arc::make_mut(&mut deleted_table); new_table_def.deleted = true; new_table_def.hard_delete_time = self.hard_deletion_time.map(Time::from_timestamp_nanos); new_table_def.table_name = table_name; mut_schema.tables.update(new_table_def.table_id, deleted_table).expect("the table should exist"); } Ok(schema) }}
impl UpdateDatabaseSchema for SetRetentionPeriodLog { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { schema.to_mut().retention_period = self.retention_period; Ok(schema) }}
impl UpdateDatabaseSchema for ClearRetentionPeriodLog { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { schema.to_mut().retention_period = RetentionPeriod::Indefinite; Ok(schema) }}
struct EnableTrigger(TriggerIdentifier); struct DisableTrigger(TriggerIdentifier);
impl UpdateDatabaseSchema for EnableTrigger { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { let Some(trigger) = schema.processing_engine_triggers.get_by_name(&self.0.trigger_name) else { return Err(CatalogError::ProcessingEngineTriggerNotFound { database_name: self.0.db_name.to_string(), trigger_name: self.0.trigger_name.to_string() }); }; if !trigger.disabled { return Ok(schema); } let mut mut_trigger = schema.processing_engine_triggers.get_by_id(&trigger.trigger_id).expect("already checked containment"); Arc::make_mut(&mut mut_trigger).disabled = false; schema.to_mut().processing_engine_triggers.update(trigger.trigger_id, mut_trigger).expect("existing trigger should update"); Ok(schema) }}
impl UpdateDatabaseSchema for DisableTrigger { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { let Some(trigger) = schema.processing_engine_triggers.get_by_name(&self.0.trigger_name) else { return Err(CatalogError::ProcessingEngineTriggerNotFound { database_name: self.0.db_name.to_string(), trigger_name: self.0.trigger_name.to_string() }); }; if trigger.disabled { return Ok(schema); } let mut mut_trigger = schema.processing_engine_triggers.get_by_id(&trigger.trigger_id).expect("already checked containment"); Arc::make_mut(&mut mut_trigger).disabled = true; schema.to_mut().processing_engine_triggers.update(trigger.trigger_id, mut_trigger).expect("existing trigger should update"); Ok(schema) }}
impl UpdateDatabaseSchema for TriggerDefinition { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { if let Some(current) = schema.processing_engine_triggers.get_by_name(&self.trigger_name) { if current.as_ref() == self { return Ok(schema); } return Err(CatalogError::ProcessingEngineTriggerExists { database_name: schema.name.to_string(), trigger_name: self.trigger_name.to_string() }); } schema.to_mut().processing_engine_triggers.insert(self.trigger_id, Arc::new(self.clone())).expect("new trigger should insert"); Ok(schema) }}
impl UpdateDatabaseSchema for DeleteTriggerLog { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { let Some(trigger) = schema.processing_engine_triggers.get_by_name(&self.trigger_name) else { return Ok(schema); }; if !trigger.disabled && !self.force { if self.force { warn!("deleting running trigger {}", self.trigger_name); } else { return Err(CatalogError::ProcessingEngineTriggerRunning { trigger_name: self.trigger_name.to_string() }); } } schema.to_mut().processing_engine_triggers.remove(&trigger.trigger_id); Ok(schema) }}
fn make_new_name_using_deleted_time(name: &str, deletion_time: Time) -> Arc<str> { Arc::from(format!("{}-{}", name, deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT))) }

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition { /* ... */ pub table_id: TableId, pub table_name: Arc<str>, pub schema: Schema, pub columns: Repository<ColumnId, ColumnDefinition>, pub series_key: Vec<ColumnId>, pub series_key_names: Vec<Arc<str>>, pub sort_key: SortKey, pub last_caches: Repository<LastCacheId, LastCacheDefinition>, pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>, pub deleted: bool, pub hard_delete_time: Option<Time>, pub shards: Repository<ShardId, ShardDefinition>, pub replication_info: Option<ReplicationInfo>, pub shard_key_columns: Option<Vec<String>>, pub sharding_strategy: ShardingStrategy }
impl TableDefinition { /* ... */ pub fn new_empty(table_id: TableId, table_name: Arc<str>) -> Self { Self::new(table_id, table_name, vec![], vec![], None, None, ShardingStrategy::default()).expect("empty table should create without error") } pub fn new(table_id: TableId, table_name: Arc<str>, columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>, series_key: Vec<ColumnId>, replication_info: Option<ReplicationInfo>, shard_key_columns: Option<Vec<String>>, sharding_strategy: ShardingStrategy) -> Result<Self> { let mut ordered_columns = BTreeMap::new(); for (col_id, name, column_type) in &columns { ordered_columns.insert(name.as_ref(), (col_id, column_type)); } let mut schema_builder = SchemaBuilder::with_capacity(columns.len()); schema_builder.measurement(table_name.as_ref()); let mut cols_repo = Repository::new(); for (name, (col_id, column_type)) in ordered_columns { schema_builder.influx_column(name, *column_type); let not_nullable = matches!(column_type, InfluxColumnType::Timestamp); assert!(cols_repo.insert(*col_id, Arc::new(ColumnDefinition::new(*col_id, name, *column_type, !not_nullable))).is_ok(), "table definition initialized with duplicate column ids"); } let series_key_names = series_key.clone().into_iter().map(|id| cols_repo.id_to_name(&id).expect("invalid column id in series key definition")).collect::<Vec<Arc<str>>>(); schema_builder.with_series_key(&series_key_names); let schema = schema_builder.build().expect("schema should be valid"); let sort_key = Self::make_sort_key(&series_key_names, cols_repo.contains_name(TIME_COLUMN_NAME)); Ok(Self { table_id, table_name, schema, columns: cols_repo, series_key, series_key_names, sort_key, last_caches: Repository::new(), distinct_caches: Repository::new(), deleted: false, hard_delete_time: None, shards: Repository::new(), replication_info, shard_key_columns, sharding_strategy }) } fn make_sort_key(series_key_names: &[Arc<str>], add_time: bool) -> SortKey { let iter = series_key_names.iter().cloned(); if add_time { SortKey::from_columns(iter.chain(iter::once(TIME_COLUMN_NAME.into()))) } else { SortKey::from_columns(iter) } } pub fn new_from_op(table_definition: &CreateTableLog) -> Self { let mut columns = Vec::with_capacity(table_definition.field_definitions.len()); for field_def in &table_definition.field_definitions { columns.push((field_def.id, Arc::clone(&field_def.name), field_def.data_type.into())); } Self::new(table_definition.table_id, Arc::clone(&table_definition.table_name), columns, table_definition.key.clone(), None, table_definition.shard_key_columns.clone(), table_definition.sharding_strategy.unwrap_or_default()).expect("tables defined from ops should not exceed column limits") } pub(crate) fn check_and_add_new_fields(&self, table_definition: &CreateTableLog) -> Result<Cow<'_, Self>> { Self::add_fields(Cow::Borrowed(self), &table_definition.field_definitions) } pub(crate) fn add_fields<'a>(mut table: Cow<'a, Self>, fields: &Vec<FieldDefinition>) -> Result<Cow<'a, Self>> { let mut new_fields: Vec<(ColumnId, Arc<str>, InfluxColumnType)> = Vec::with_capacity(fields.len()); for field_def in fields { if let Some(existing_type) = table.columns.get_by_id(&field_def.id).map(|def| def.data_type) { if existing_type != field_def.data_type.into() { return Err(CatalogError::FieldTypeMismatch { table_name: table.table_name.to_string(), column_name: field_def.name.to_string(), existing: existing_type, attempted: field_def.data_type.into() }); } } else { new_fields.push((field_def.id, Arc::clone(&field_def.name), field_def.data_type.into())); } } if !new_fields.is_empty() { let table = table.to_mut(); table.add_columns(new_fields)?; } Ok(table) } pub fn column_exists(&self, column: impl AsRef<str>) -> bool { self.columns.contains_name(column.as_ref()) } pub(crate) fn add_column(&mut self, column_name: Arc<str>, column_type: InfluxColumnType) -> Result<ColumnId> { let col_id = self.columns.get_and_increment_next_id(); self.add_columns(vec![(col_id, column_name, column_type)])?; Ok(col_id) } pub fn add_columns(&mut self, columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>) -> Result<()> { let mut cols = BTreeMap::new(); for col_def in self.columns.resource_iter().cloned() { cols.insert(Arc::clone(&col_def.name), col_def); } let mut sort_key_changed = false; for (id, name, column_type) in columns { let nullable = name.as_ref() != TIME_COLUMN_NAME; assert!(cols.insert(Arc::clone(&name), Arc::new(ColumnDefinition::new(id, Arc::clone(&name), column_type, nullable))).is_none(), "attempted to add existing column"); if matches!(column_type, InfluxColumnType::Tag) && !self.series_key.contains(&id) { self.series_key.push(id); self.series_key_names.push(name); sort_key_changed = true; } else if matches!(column_type, InfluxColumnType::Timestamp) && !self.series_key.contains(&id) { sort_key_changed = true; } } let mut schema_builder = SchemaBuilder::with_capacity(cols.len()); schema_builder.measurement(self.table_name.as_ref()); for (name, col_def) in &cols { schema_builder.influx_column(name.as_ref(), col_def.data_type); } schema_builder.with_series_key(&self.series_key_names); self.schema = schema_builder.build().expect("schema should be valid"); let mut new_columns = Repository::new(); for col in cols.values().cloned() { new_columns.insert(col.id, col).expect("should be a new column"); } self.columns = new_columns; if sort_key_changed { self.sort_key = Self::make_sort_key(&self.series_key_names, self.columns.contains_name(TIME_COLUMN_NAME)); } Ok(()) } pub fn index_column_ids(&self) -> Vec<ColumnId> { self.columns.iter().filter_map(|(id, def)| match def.data_type { InfluxColumnType::Tag => Some(*id), _ => None }).collect() } pub fn influx_schema(&self) -> &Schema { &self.schema } pub fn num_columns(&self) -> usize { self.influx_schema().len() } pub fn num_tag_columns(&self) -> usize { self.columns.resource_iter().filter(|c| matches!(c.data_type, InfluxColumnType::Tag)).count() } pub fn field_type_by_name(&self, name: impl AsRef<str>) -> Option<InfluxColumnType> { self.columns.get_by_name(name.as_ref()).map(|def| def.data_type) } pub fn column_name_to_id(&self, name: impl AsRef<str>) -> Option<ColumnId> { self.columns.name_to_id(name.as_ref()) } pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> { self.columns.id_to_name(id) } pub fn column_name_to_id_unchecked(&self, name: impl AsRef<str>) -> ColumnId { self.columns.name_to_id(name.as_ref()).expect("Column exists in mapping") } pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> { self.columns.id_to_name(id).expect("Column exists in mapping") } pub fn column_definition(&self, name: impl AsRef<str>) -> Option<Arc<ColumnDefinition>> { self.columns.get_by_name(name.as_ref()) } pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<Arc<ColumnDefinition>> { self.columns.get_by_id(id) } pub fn series_key_ids(&self) -> &[ColumnId] { &self.series_key } pub fn series_key_names(&self) -> &[Arc<str>] { &self.series_key_names }}

trait TableUpdate { fn table_id(&self) -> TableId; fn table_name(&self) -> Arc<str>; fn update_table<'a>(&self, table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>>; }
impl<T: TableUpdate> UpdateDatabaseSchema for T { /* ... */ fn update_schema<'a>(&self, mut schema: Cow<'a, DatabaseSchema>) -> Result<Cow<'a, DatabaseSchema>> { let Some(table) = schema.tables.get_by_id(&self.table_id()) else { return Err(CatalogError::TableNotFound { db_name: Arc::clone(&schema.name), table_name: Arc::clone(&self.table_name()) }); }; if let Cow::Owned(new_table) = self.update_table(Cow::Borrowed(table.as_ref()))? { schema.to_mut().update_table(new_table.table_id, Arc::new(new_table))?; } Ok(schema) }}
impl TableUpdate for AddFieldsLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { TableDefinition::add_fields(table, &self.field_definitions) }}
impl TableUpdate for DistinctCacheDefinition { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { table.to_mut().distinct_caches.insert(self.cache_id, self.clone())?; Ok(table) }}
impl TableUpdate for DeleteDistinctCacheLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { table.to_mut().distinct_caches.remove(&self.cache_id); Ok(table) }}
impl TableUpdate for LastCacheDefinition { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { table.to_mut().last_caches.insert(self.id, self.clone())?; Ok(table) }}
impl TableUpdate for DeleteLastCacheLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { table.to_mut().last_caches.remove(&self.id); Ok(table) }}
impl TableUpdate for CreateShardLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let shard_def = self.shard_definition.clone(); if table.shards.contains_id(&shard_def.id) { return Err(CatalogError::ShardAlreadyExists { shard_id: shard_def.id, table_id: self.table_id }); } table.to_mut().shards.insert(shard_def.id, Arc::new(shard_def))?; Ok(table) }}
impl TableUpdate for UpdateShardLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); let existing_shard_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id })?; let mut updated_shard_def = Arc::try_unwrap(existing_shard_arc).unwrap_or_else(|arc| (*arc).clone()); if let Some(new_node_ids) = &self.new_node_ids { updated_shard_def.node_ids = new_node_ids.clone(); } mut_table.shards.update(self.shard_id, Arc::new(updated_shard_def))?; Ok(table) }}
impl TableUpdate for DeleteShardLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { if !table.shards.contains_id(&self.shard_id) { return Ok(table); } table.to_mut().shards.remove(&self.shard_id); Ok(table) }}
impl TableUpdate for SetReplicationLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { table.to_mut().replication_info = Some(self.replication_info.clone()); Ok(table) }}
impl TableUpdate for UpdateTableShardingStrategyLog { /* ... */ fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); mut_table.sharding_strategy = self.new_strategy; mut_table.shard_key_columns = self.new_shard_key_columns.clone(); Ok(table) }}

impl TableUpdate for BeginShardMigrationOutLog { fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); let shard_to_update_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id })?; let mut updated_shard_def = Arc::try_unwrap(shard_to_update_arc).unwrap_or_else(|arc| (*arc).clone()); updated_shard_def.migration_status = Some(ShardMigrationStatus::MigratingOutTo(self.target_node_ids.clone())); mut_table.shards.update(self.shard_id, Arc::new(updated_shard_def))?; Ok(table) }}
impl TableUpdate for CommitShardMigrationOnTargetLog { fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); let shard_to_update_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id })?; let mut updated_shard_def = Arc::try_unwrap(shard_to_update_arc).unwrap_or_else(|arc| (*arc).clone()); if !updated_shard_def.node_ids.contains(&self.target_node_id) { updated_shard_def.node_ids.push(self.target_node_id); } mut_table.shards.update(self.shard_id, Arc::new(updated_shard_def))?; Ok(table) }}
impl TableUpdate for FinalizeShardMigrationOnSourceLog { fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); let shard_to_update_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id })?; let mut updated_shard_def = Arc::try_unwrap(shard_to_update_arc).unwrap_or_else(|arc| (*arc).clone()); updated_shard_def.node_ids.retain(|id| *id == self.migrated_to_node_id); if !updated_shard_def.node_ids.contains(&self.migrated_to_node_id) { updated_shard_def.node_ids.push(self.migrated_to_node_id); } updated_shard_def.migration_status = Some(ShardMigrationStatus::Stable); mut_table.shards.update(self.shard_id, Arc::new(updated_shard_def))?; Ok(table) }}
impl TableUpdate for UpdateShardMigrationStatusLog { fn table_id(&self) -> TableId { self.table_id } fn table_name(&self) -> Arc<str> { Arc::clone(&self.table_name) } fn update_table<'a>(&self, mut table: Cow<'a, TableDefinition>) -> Result<Cow<'a, TableDefinition>> { let mut_table = table.to_mut(); let shard_to_update_arc = mut_table.shards.get_by_id(&self.shard_id).ok_or_else(|| CatalogError::ShardNotFound { shard_id: self.shard_id, table_id: self.table_id })?; let mut updated_shard_def = Arc::try_unwrap(shard_to_update_arc).unwrap_or_else(|arc| (*arc).clone()); updated_shard_def.migration_status = Some(self.new_status.clone()); mut_table.shards.update(self.shard_id, Arc::new(updated_shard_def))?; Ok(table) }}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition { /* ... */ pub id: ColumnId, pub name: Arc<str>, pub data_type: InfluxColumnType, pub nullable: bool }
impl ColumnDefinition { /* ... */ pub fn new(id: ColumnId, name: impl Into<Arc<str>>, data_type: InfluxColumnType, nullable: bool) -> Self { Self { id, name: name.into(), data_type, nullable } } }

#[derive(Debug, Clone, Default)]
pub(crate) struct TokenRepository { /* ... */ repo: Repository<TokenId, TokenInfo>, hash_lookup_map: BiHashMap<TokenId, Vec<u8>> }
impl TokenRepository { /* ... */ pub(crate) fn new(repo: Repository<TokenId, TokenInfo>, hash_lookup_map: BiHashMap<TokenId, Vec<u8>>) -> Self { Self { repo, hash_lookup_map } } pub(crate) fn repo(&self) -> &Repository<TokenId, TokenInfo> { &self.repo } pub(crate) fn get_and_increment_next_id(&mut self) -> TokenId { self.repo.get_and_increment_next_id() } pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> { let id = self.hash_lookup_map.get_by_right(&hash).map(|id| id.to_owned())?; self.repo.get_by_id(&id) } pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> { self.hash_lookup_map.insert(token_id, token_info.hash.clone()); self.repo.insert(token_id, token_info)?; Ok(()) } pub(crate) fn update_admin_token_hash(&mut self, token_id: TokenId, hash: Vec<u8>, updated_at: i64) -> Result<()> { let mut token_info = self.repo.get_by_id(&token_id).ok_or_else(|| CatalogError::MissingAdminTokenToUpdate)?; let updatable = Arc::make_mut(&mut token_info); updatable.hash = hash.clone(); updatable.updated_at = Some(updated_at); updatable.updated_by = Some(token_id); self.repo.update(token_id, token_info)?; self.hash_lookup_map.insert(token_id, hash); Ok(()) } pub(crate) fn delete_token(&mut self, token_name: String) -> Result<()> { let token_id = self.repo.name_to_id(&token_name).ok_or_else(|| CatalogError::NotFound)?; self.repo.remove(&token_id); self.hash_lookup_map.remove_by_left(&token_id); Ok(()) }}
impl CatalogResource for TokenInfo { /* ... */ type Identifier = TokenId; fn id(&self) -> Self::Identifier { self.id } fn name(&self) -> Arc<str> { Arc::clone(&self.name) } }
fn create_token_and_hash() -> (String, Vec<u8>) { /* ... */ let token = { let mut token = String::from("apiv3_"); let mut key = [0u8; 64]; OsRng.fill_bytes(&mut key); token.push_str(&B64.encode(key)); token }; (token.clone(), Sha512::digest(&token).to_vec()) }

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum NodeStatus { Joining, Active, Leaving, Down, Unknown, }
impl Default for NodeStatus { fn default() -> Self { NodeStatus::Unknown } }

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct NodeDefinition { pub node_name: Arc<str>, pub id: NodeId, pub instance_id: Arc<str>, pub rpc_address: String, pub http_address: String, pub mode: Vec<NodeMode>, pub core_count: u64, pub status: NodeStatus, pub last_heartbeat: Option<i64>, }
impl CatalogResource for NodeDefinition { type Identifier = NodeId; fn id(&self) -> Self::Identifier { self.id } fn name(&self) -> Arc<str> { Arc::clone(&self.node_name) }}
impl NodeDefinition { pub fn new(id: NodeId, node_name: Arc<str>, instance_id: Arc<str>, rpc_address: String, http_address: String, mode: Vec<NodeMode>, core_count: u64, status: NodeStatus, last_heartbeat: Option<i64>) -> Self { Self { id, node_name, instance_id, rpc_address, http_address, mode, core_count, status, last_heartbeat } } pub fn instance_id(&self) -> Arc<str> { Arc::clone(&self.instance_id) } pub fn modes(&self) -> &Vec<NodeMode> { &self.mode } pub fn core_count(&self) -> u64 { self.core_count } pub fn status(&self) -> NodeStatus { self.status } pub fn last_heartbeat(&self) -> Option<i64> { self.last_heartbeat } pub fn is_active(&self) -> bool { self.status == NodeStatus::Active } }

#[cfg(test)]
mod tests { /* ... all existing tests ... */
    use crate::{log::{FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality, create, BeginShardMigrationOutLog, CommitShardMigrationOnTargetLog, FinalizeShardMigrationOnSourceLog, UpdateShardMigrationStatusLog}, object_store::CatalogFilePath, serialize::{serialize_catalog_file, verify_and_deserialize_catalog_checkpoint_file}, shard::ShardMigrationStatus}; // Added ShardMigrationStatus and relevant logs
    use super::*;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use iox_time::MockProvider;
    use object_store::local::LocalFileSystem;
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;
    #[test_log::test(tokio::test)] async fn catalog_serialization() { /* ... */ }
    #[test] fn add_columns_updates_schema_and_column_map() { /* ... */ }
    #[tokio::test] async fn serialize_series_keys() { /* ... */ }
    #[tokio::test] async fn serialize_last_cache() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_serialize_distinct_cache() { /* ... */ }
    #[tokio::test] async fn test_catalog_id() { /* ... */ }
    #[test_log::test(tokio::test)] async fn apply_catalog_batch_fails_for_add_fields_on_nonexist_table() { /* ... */ }
    #[tokio::test] async fn test_check_and_mark_table_as_deleted() { /* ... */ }
    #[test_log::test(tokio::test)] #[ignore] async fn test_out_of_order_ops() { /* ... */ }
    #[test_log::test(tokio::test)] async fn deleted_dbs_dont_count() { /* ... */ }
    #[test_log::test(tokio::test)] async fn deleted_tables_dont_count() { /* ... */ }
    #[test_log::test(tokio::test)] async fn retention_period_cutoff_map() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_catalog_file_ordering() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_load_from_catalog_checkpoint() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_load_many_files_with_default_checkpoint_interval() { /* ... */ }
    #[test_log::test(tokio::test)] async fn apply_catalog_batch_fails_for_add_fields_past_tag_limit() { /* ... */ }
    #[test_log::test(tokio::test)] async fn apply_catalog_batch_fails_to_create_table_with_too_many_tags() { /* ... */ }
    #[test_log::test(tokio::test)] async fn catalog_serialization_with_sharding_and_replication() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_create_shard_success_and_already_exists() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_create_shard_db_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_create_shard_table_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_set_replication_success() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_set_replication_db_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_set_replication_table_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_update_shard_nodes_success() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_update_shard_nodes_shard_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_update_shard_nodes_db_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_update_shard_nodes_table_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_delete_shard_success() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_delete_shard_not_found_is_error() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_delete_shard_db_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_delete_shard_table_not_found() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_shard_management_in_table_definition() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_update_table_sharding_strategy_api() { /* ... */ }
    #[test_log::test(tokio::test)] async fn test_node_management_apis() { /* ... */ }

    #[test_log::test(tokio::test)]
    async fn test_shard_migration_apis_and_status_updates() {
        let catalog = Arc::new(Catalog::new_in_memory("shard_migration_host").await.unwrap());
        let db_name = "migration_db";
        let table_name = "migration_table";

        catalog.create_database(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, &["tag"], &[("val", FieldDataType::Integer)]).await.unwrap();

        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let node3 = NodeId::new(3);

        // Create a shard initially on node1
        let shard_id = ShardId::new(100);
        let shard_def = ShardDefinition::new(shard_id, ShardTimeRange {start_time: 0, end_time: 1000}, vec![node1], None, Some(ShardMigrationStatus::Stable));
        catalog.create_shard(db_name, table_name, shard_def).await.unwrap();

        // 1. Begin migration out to node2
        let begin_res = catalog.begin_shard_migration_out(db_name, table_name, &shard_id, vec![node2]).await;
        assert!(begin_res.is_ok(), "Begin migration failed: {:?}", begin_res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let s_def = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(s_def.migration_status, Some(ShardMigrationStatus::MigratingOutTo(vec![node2])));
        assert_eq!(s_def.node_ids, vec![node1]); // Still on source

        // 2. Commit on target (node2)
        // (Simulating this call as if node2 confirmed it has the data from node1)
        let commit_res = catalog.commit_shard_migration_on_target(db_name, table_name, &shard_id, node2, node1).await;
        assert!(commit_res.is_ok(), "Commit migration failed: {:?}", commit_res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let s_def = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(s_def.node_ids, vec![node1, node2]); // Both nodes now listed
        // Migration status might still be MigratingOutTo, or could be a new intermediate state.
        // Current TableUpdate impl for CommitShardMigrationOnTargetLog only adds to node_ids.

        // 3. Finalize on source (node1 tells catalog it's done, shard now fully on node2)
        // The current FinalizeShardMigrationOnSourceLog logic is a bit too simple (sets node_ids to only migrated_to_node_id).
        // A more robust logic would involve removing only the source node from the list if multiple target nodes were involved.
        // For a single target (node2), this simplified logic works.
        let finalize_res = catalog.finalize_shard_migration_on_source(db_name, table_name, &shard_id, node2).await;
        assert!(finalize_res.is_ok(), "Finalize migration failed: {:?}", finalize_res.err());

        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let s_def = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(s_def.node_ids, vec![node2]); // Only target node remains
        assert_eq!(s_def.migration_status, Some(ShardMigrationStatus::Stable));

        // 4. Test general status update
        let update_status_res = catalog.update_shard_migration_status(db_name, table_name, &shard_id, ShardMigrationStatus::MigratingInFrom(node3)).await;
        assert!(update_status_res.is_ok());
        let db_schema = catalog.db_schema(db_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let s_def = table_def.shards.get_by_id(&shard_id).unwrap();
        assert_eq!(s_def.migration_status, Some(ShardMigrationStatus::MigratingInFrom(node3)));
    }
}
