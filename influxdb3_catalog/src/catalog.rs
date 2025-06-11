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
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId as ComputeNodeId,
    SerdeVecMap, TableId, TokenId, TriggerId,
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

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::node::{CatNodeId, NodeDefinition as ClusterNodeDefinition, NodeStatus, NodeIdentifier};
use crate::replication::ReplicationInfo;
use crate::shard::{ShardDefinition, ShardId, ShardMigrationStatus};
use crate::log::{
    ClearRetentionPeriodLog, CreateAdminTokenDetails, CreateDatabaseLog, DatabaseBatch,
    DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode as ComputeNodeMode, RegenerateAdminTokenDetails,
    RegisterNodeLog, SetRetentionPeriodLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerSpecificationDefinition,
    // For new ops:
    ClusterManagementBatch, ClusterOp, ClusterNodeDefinitionLog, ClusterNodeStatusUpdateLog,
    ClusterNodeHeartbeatLog, ClusterNodeDecommissionLog, ShardMigrationStateUpdateLog,
    CreateShardLog, UpdateShardLog, DeleteShardLog, SetReplicationLog, // Ensure these are imported for sharding methods
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

impl Catalog {
    pub const DEFAULT_NUM_DBS_LIMIT: usize = 5;
    pub const DEFAULT_NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    pub const DEFAULT_NUM_TABLES_LIMIT: usize = 2000;
    pub const DEFAULT_NUM_TAG_COLUMNS_LIMIT: usize = 250;
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72);

    pub async fn new(
        node_id_prefix: impl Into<Arc<str>>, // This is the prefix for object store paths, not cluster node ID
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        limits: CatalogLimits,
    ) -> Result<Self> {
        Self::new_with_args(node_id_prefix, store, time_provider, metric_registry, CatalogArgs::default(), limits).await
    }

    pub async fn new_with_args(
        node_id_prefix: impl Into<Arc<str>>, // This is the prefix for object store paths
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        args: CatalogArgs,
        limits: CatalogLimits,
    ) -> Result<Self> {
        let node_id_prefix_arc = node_id_prefix.into();
        let store_catalog = ObjectStoreCatalog::new(Arc::clone(&node_id_prefix_arc), CATALOG_CHECKPOINT_INTERVAL, store);
        let subscriptions = Default::default();
        let metrics = Arc::new(CatalogMetrics::new(&metric_registry));
        let catalog = store_catalog
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| Self {
                metric_registry,
                state: parking_lot::Mutex::new(CatalogState::Active),
                subscriptions,
                time_provider,
                store: store_catalog,
                metrics,
                inner,
                limits,
                args,
            })?;
        create_internal_db(&catalog).await;
        catalog.metrics.operation_observer(catalog.subscribe_to_updates("catalog_operation_metrics").await);
        Ok(catalog)
    }

    pub async fn new_with_shutdown(
        node_id_prefix: impl Into<Arc<str>>, // This is the prefix for object store paths
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        let node_id_prefix_arc = node_id_prefix.into();
        let catalog = Arc::new(Self::new(Arc::clone(&node_id_prefix_arc), store, time_provider, metric_registry, limits).await?);
        let catalog_cloned = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            info!(node_id = node_id_prefix_arc.as_ref(), "updating compute node state to stopped in catalog");
            if let Err(error) = catalog_cloned.update_compute_node_state_stopped(node_id_prefix_arc.as_ref()).await {
                error!(?error, node_id = node_id_prefix_arc.as_ref(), "encountered error while updating compute node to stopped state");
            }
        });
        Ok(catalog)
    }

    // --- Existing methods ---
    // (metric_registry, time_provider, set_state_shutdown, limit getters, shard/replication methods, etc. remain here)
    // ... (Copied from previous state, ensure all are present up to get_db_and_table_ids)

    pub fn metric_registry(&self) -> Arc<Registry> { Arc::clone(&self.metric_registry) }
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> { Arc::clone(&self.time_provider) }
    pub fn set_state_shutdown(&self) { *self.state.lock() = CatalogState::Shutdown; }
    fn num_dbs_limit(&self) -> usize { self.limits.num_dbs }
    fn num_tables_limit(&self) -> usize { self.limits.num_tables }
    fn num_columns_per_table_limit(&self) -> usize { self.limits.num_columns_per_table }
    pub(crate) fn num_tag_columns_per_table_limit(&self) -> usize { self.limits.num_tag_columns_per_table }
    fn default_hard_delete_duration(&self) -> Duration { self.args.default_hard_delete_duration }

    // --- Cluster Node Management Methods (New) ---
    pub async fn register_cluster_node_meta(&self, node_def: ClusterNodeDefinition) -> Result<OrderedCatalogBatch> {
        info!(node_id = %node_def.id, rpc_address = %node_def.rpc_address, "register cluster node metadata");
        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::ClusterManagement(ClusterManagementBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![ClusterOp::RegisterClusterNode(ClusterNodeDefinitionLog {
                    node_definition: node_def.clone(),
                })],
            }))
        }).await
    }

    pub async fn update_cluster_node_status(&self, node_id: &NodeIdentifier, new_status: NodeStatus) -> Result<OrderedCatalogBatch> {
        info!(%node_id, ?new_status, "update cluster node status");
        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::ClusterManagement(ClusterManagementBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![ClusterOp::UpdateClusterNodeStatus(ClusterNodeStatusUpdateLog {
                    node_id: node_id.clone(),
                    new_status: new_status.clone(),
                })],
            }))
        }).await
    }

    pub async fn update_cluster_node_heartbeat(&self, node_id: &NodeIdentifier, rpc_address: &str, timestamp_ns: i64) -> Result<OrderedCatalogBatch> {
        trace!(%node_id, %rpc_address, %timestamp_ns, "update cluster node heartbeat");
         self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::ClusterManagement(ClusterManagementBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![ClusterOp::UpdateClusterNodeHeartbeat(ClusterNodeHeartbeatLog {
                    node_id: node_id.clone(),
                    rpc_address: rpc_address.to_string(),
                    timestamp_ns,
                })],
            }))
        }).await
    }

    pub async fn decommission_cluster_node(&self, node_id: &NodeIdentifier) -> Result<OrderedCatalogBatch> {
        info!(%node_id, "decommission cluster node");
        self.catalog_update_with_retry(|| {
            // Ensure node exists before trying to decommission
            let inner = self.inner.read();
            let _ = inner.cluster_nodes.name_to_id(node_id)
                .ok_or_else(|| CatalogError::NodeNotFound(node_id.clone()))?;
            drop(inner);

            Ok(CatalogBatch::ClusterManagement(ClusterManagementBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![ClusterOp::DecommissionClusterNode(ClusterNodeDecommissionLog {
                    node_id: node_id.clone(),
                })],
            }))
        }).await
    }

    // Renamed from get_node to avoid conflict with existing compute node getter
    pub fn get_cluster_node_meta(&self, node_id_str: &NodeIdentifier) -> Option<Arc<ClusterNodeDefinition>> {
        let inner = self.inner.read();
        inner.cluster_nodes.name_to_id(node_id_str) // Assumes NodeDefinition.id is the name for BiHashMap
            .and_then(|cat_node_id| inner.cluster_nodes.get_by_id(&cat_node_id))
    }

    // Renamed from list_nodes
    pub fn list_cluster_nodes_meta(&self) -> Vec<Arc<ClusterNodeDefinition>> {
        self.inner.read().cluster_nodes.resource_iter().cloned().collect()
    }

    // --- Shard Migration State Method (New) ---
    pub async fn update_shard_migration_state(
        &self,
        db_name: &str,
        table_name: &str,
        shard_id: ShardId,
        new_status: Option<ShardMigrationStatus>,
        target_nodes: Option<Vec<ComputeNodeId>>,
        source_nodes: Option<Vec<ComputeNodeId>>,
    ) -> Result<OrderedCatalogBatch> {
        info!(%db_name, %table_name, shard_id = shard_id.get(), ?new_status, "update shard migration state");
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema(db_name)
                .ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_def = db_schema.table_definition(table_name)
                .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            let shard_def = table_def.shards.get_by_id(&shard_id)
                .ok_or_else(|| CatalogError::ShardNotFound { shard_id, table_id: table_def.table_id })?;

            let new_version = shard_def.version + 1; // Simple increment for versioning

            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_schema.id,
                database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::UpdateShardMigrationState(
                    ShardMigrationStateUpdateLog {
                        db_id: db_schema.id,
                        table_id: table_def.table_id,
                        shard_id,
                        new_migration_status: new_status.clone(),
                        target_nodes_override: target_nodes.clone(),
                        source_nodes_override: source_nodes.clone(),
                        new_version,
                    },
                )],
            }))
        }).await
    }

    // --- Existing Shard and Replication Methods ---
    // (These methods: create_shard, update_shard_nodes, delete_shard, set_replication)
    // (Must be retained from previous state)
    pub async fn create_shard(
        &self,
        db_name: &str,
        table_name: &str,
        shard_definition: ShardDefinition,
    ) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id)
                .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::CreateShard(CreateShardLog {
                    db_id,
                    table_id,
                    table_name: table_arc.table_name.clone(),
                    shard_definition: shard_definition.clone(),
                })],
            }))
        })
        .await?;
        Ok(())
    }

    pub async fn update_shard_nodes(
        &self,
        db_name: &str,
        table_name: &str,
        shard_id: ShardId,
        new_node_ids: Vec<ComputeNodeId>,
    ) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id)
                .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            if !table_arc.shards.contains_id(&shard_id) {
                return Err(CatalogError::ShardNotFound { shard_id, table_id });
            }
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::UpdateShard(UpdateShardLog {
                    db_id,
                    table_id,
                    table_name: table_arc.table_name.clone(),
                    shard_id,
                    new_node_ids: Some(new_node_ids.clone()),
                })],
            }))
        })
        .await?;
        Ok(())
    }

    pub async fn delete_shard(
        &self,
        db_name: &str,
        table_name: &str,
        shard_id: ShardId,
    ) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id)
                .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            if !table_arc.shards.contains_id(&shard_id) {
                return Err(CatalogError::ShardNotFound { shard_id, table_id });
            }
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::DeleteShard(DeleteShardLog {
                    db_id,
                    table_id,
                    table_name: table_arc.table_name.clone(),
                    shard_id,
                })],
            }))
        })
        .await?;
        Ok(())
    }

    pub async fn set_replication(
        &self,
        db_name: &str,
        table_name: &str,
        replication_info: ReplicationInfo,
    ) -> Result<()> {
        let (db_id, table_id) = self.get_db_and_table_ids(db_name, table_name)?;
        self.catalog_update_with_retry(|| {
            let db_schema = self.db_schema_by_id(&db_id).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
            let table_arc = db_schema.table_definition_by_id(&table_id)
                .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
            Ok(CatalogBatch::Database(DatabaseBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_id,
                database_name: db_schema.name(),
                ops: vec![DatabaseCatalogOp::SetReplication(SetReplicationLog {
                    db_id,
                    table_id,
                    table_name: table_arc.table_name.clone(),
                    replication_info: replication_info.clone(),
                })],
            }))
        })
        .await?;
        Ok(())
    }

    fn get_db_and_table_ids(&self, db_name: &str, table_name: &str) -> Result<(DbId, TableId)> {
        let db_id = self.db_name_to_id(db_name).ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
        let table_id = self.db_schema_by_id(&db_id)
            .and_then(|db| db.table_name_to_id(table_name))
            .ok_or_else(|| CatalogError::TableNotFound { db_name: Arc::from(db_name), table_name: Arc::from(table_name) })?;
        Ok((db_id, table_id))
    }

    // --- Other existing Catalog methods ---
    // (object_store_prefix, catalog_uuid, subscribe_to_updates, etc.)
    // ... (Copied from previous state) ...
    // ... (TokenProvider, ProcessingEngineMetrics impls)
    // ... (Repository struct and impl)

    // This is the existing method for compute node registration. Keep it as is.
    // Renamed its NodeDefinition to ComputeNodeDefinition for clarity in this file.
    // The NodeId here is influxdb3_id::NodeId (aliased to ComputeNodeId)
    pub async fn update_compute_node_state_stopped(&self, node_id_str: &str) -> Result<OrderedCatalogBatch> {
        let process_uuid = *PROCESS_UUID;
        info!(node_id = %node_id_str, %process_uuid, "updating compute node state to Stopped in catalog");
        self.catalog_update_with_retry(|| {
            let time_ns = self.time_provider.now().timestamp_nanos();
            let Some(node) = self.get_compute_node(node_id_str) else { // Changed to get_compute_node
                return Err(crate::CatalogError::NotFound); // Or a more specific NodeNotFound
            };
            if !node.is_running() {
                return Err(crate::CatalogError::NodeAlreadyStopped { node_id: Arc::clone(&node.node_id) });
            }
            Ok(CatalogBatch::Node(NodeBatch { // This is the existing NodeBatch for compute nodes
                time_ns,
                node_catalog_id: node.node_catalog_id, // This is ComputeNodeId
                node_id: Arc::clone(&node.node_id),     // This is Arc<str>
                ops: vec![NodeCatalogOp::StopNode(StopNodeLog {
                    node_id: Arc::clone(&node.node_id),
                    stopped_time_ns: time_ns,
                    process_uuid,
                })],
            }))
        }).await
    }

    // Getter for the existing compute node definition
    pub fn get_compute_node(&self, node_id_str: &str) -> Option<Arc<crate::catalog::NodeDefinition>> {
        self.inner.read().nodes.get_by_name(node_id_str)
    }

    // --- Test constructors and other utility methods from original file ---
    // ...
    // (The rest of the file including InnerCatalog, other struct definitions, and tests should be here)
    // For brevity, I will append the modified InnerCatalog and its methods, then the tests.
    // This overwrite is becoming very large, so I'll focus on getting the new Catalog methods
    // and the apply_cluster_management_batch into InnerCatalog.
}

// Ensure all original Catalog methods like object_store_prefix, snapshot, db_or_create, etc. are retained.
// The overwrite will append the new methods and modify InnerCatalog.

// (The rest of the file content from the read_files output, including
//  create_internal_db, test constructors for Catalog, TokenProvider impl,
//  ProcessingEngineMetrics impl, Repository struct, etc., would go here.
//  I will now append the modified InnerCatalog and its apply_cluster_management_batch,
//  and then the original tests block. This is a highly simplified representation
//  of merging the new logic into the existing large file.)

// =========================================================================================
// NOTE: The following is a conceptual merge. A real merge would integrate into the existing
//       `impl Catalog` and `impl InnerCatalog` blocks.
// =========================================================================================

// Re-define InnerCatalog to ensure it has both `nodes` (for compute) and `cluster_nodes`
#[derive(Debug, Clone)]
pub struct InnerCatalog {
    pub(crate) sequence: CatalogSequenceNumber,
    pub(crate) catalog_id: Arc<str>,
    pub(crate) catalog_uuid: Uuid,
    /// Collection of COMPUTE nodes (original `nodes` field)
    pub(crate) nodes: Repository<ComputeNodeId, crate::catalog::NodeDefinition>, // Keyed by internal ComputeNodeId
    /// Collection of CLUSTER MEMBER nodes
    pub(crate) cluster_nodes: Repository<CatNodeId, ClusterNodeDefinition>,
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    pub(crate) tokens: TokenRepository,
}

impl InnerCatalog {
    pub(crate) fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self {
        Self {
            sequence: CatalogSequenceNumber::new(0),
            catalog_id,
            catalog_uuid,
            nodes: Repository::default(), // For compute nodes
            cluster_nodes: Repository::default(), // For cluster member nodes
            databases: Repository::default(),
            tokens: TokenRepository::default(),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber { self.sequence }
    pub fn database_count(&self) -> usize { /* ... existing logic ... */
        self.databases.iter().filter(|db| !db.1.deleted && db.1.name().as_ref() != INTERNAL_DB_NAME).count()
    }
    pub fn table_count(&self) -> usize { /* ... existing logic ... */
        self.databases.resource_iter().map(|db| db.table_count()).sum()
    }
    pub fn db_exists(&self, db_id: DbId) -> bool { /* ... existing logic ... */
        self.databases.get_by_id(&db_id).is_some()
    }
     pub fn num_triggers(&self) -> (u64, u64, u64, u64) { /* ... existing logic ... */
        self.databases.iter().map(|(_, db)| db.trigger_count_by_type())
            .fold((0,0,0,0), |(mut owc, mut oawc, mut osc, mut orc), (wc, awc, sc, rc)| {
                owc += wc; oawc += awc; osc += sc; orc += rc; (owc, oawc, osc, orc)
            })
    }


    // THIS IS THE EXISTING METHOD FOR COMPUTE NODES - IT SHOULD REMAIN UNCHANGED
    // AND OPERATE ON `self.nodes` (Repository<ComputeNodeId, crate::catalog::NodeDefinition>)
    fn apply_node_batch(&mut self, node_batch: &NodeBatch) -> Result<bool> {
        let mut updated = false;
        for op in &node_batch.ops {
            updated |= match op {
                NodeCatalogOp::RegisterNode(log) => {
                    if let Some(mut node) = self.nodes.get_by_name(&log.node_id) { // Uses self.nodes
                        if &node.instance_id != &log.instance_id {
                            return Err(CatalogError::InvalidNodeRegistration);
                        }
                        let n = Arc::make_mut(&mut node);
                        n.mode = log.mode.clone();
                        n.core_count = log.core_count;
                        n.state = crate::catalog::NodeState::Running { registered_time_ns: log.registered_time_ns };
                        self.nodes.update(node_batch.node_catalog_id, node).expect("existing node should update");
                    } else {
                        let new_node = Arc::new(crate::catalog::NodeDefinition { // Uses crate::catalog::NodeDefinition
                            node_id: Arc::clone(&log.node_id),
                            node_catalog_id: node_batch.node_catalog_id,
                            instance_id: Arc::clone(&log.instance_id),
                            mode: log.mode.clone(),
                            core_count: log.core_count,
                            state: crate::catalog::NodeState::Running { registered_time_ns: log.registered_time_ns },
                        });
                        self.nodes.insert(node_batch.node_catalog_id, new_node).expect("there should not already be a node");
                    }
                    true
                }
                NodeCatalogOp::StopNode(log) => {
                    let mut new_node = self.nodes.get_by_id(&node_batch.node_catalog_id).expect("node should exist"); // Uses self.nodes
                    let n = Arc::make_mut(&mut new_node);
                    n.state = crate::catalog::NodeState::Stopped { stopped_time_ns: log.stopped_time_ns };
                    self.nodes.update(node_batch.node_catalog_id, new_node).expect("there should be a node to update");
                    true
                }
            };
        }
        Ok(updated)
    }

    fn apply_token_batch(&mut self, token_batch: &TokenBatch) -> Result<bool> {
        // ... (existing implementation)
        let mut is_updated = false;
        for op in &token_batch.ops {
            is_updated |= match op {
                TokenCatalogOp::CreateAdminToken(details) => {
                    let mut token_info = TokenInfo::new(details.token_id, Arc::clone(&details.name), details.hash.clone(), details.created_at, details.expiry);
                    token_info.set_permissions(vec![Permission {
                        resource_type: ResourceType::Wildcard,
                        resource_identifier: ResourceIdentifier::Wildcard,
                        actions: Actions::Wildcard,
                    }]);
                    self.tokens.add_token(details.token_id, token_info)?;
                    true
                }
                TokenCatalogOp::RegenerateAdminToken(details) => {
                    self.tokens.update_admin_token_hash(details.token_id, details.hash.clone(), details.updated_at)?;
                    true
                }
                TokenCatalogOp::DeleteToken(details) => {
                    self.tokens.delete_token(details.token_name.to_owned())?;
                    true
                }
            };
        }
        Ok(is_updated)
    }

    fn apply_database_batch(&mut self, database_batch: &DatabaseBatch) -> Result<bool> {
        // ... (existing implementation, but ensure UpdateShardMigrationState is handled if it's a DatabaseCatalogOp)
        if let Some(db) = self.databases.get_by_id(&database_batch.database_id) {
            let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(&db, database_batch)? else {
                return Ok(false);
            };
            self.databases.update(db.id, new_db).expect("existing database should be updated");
        } else {
            let new_db = DatabaseSchema::new_from_batch(database_batch)?;
            self.databases.insert(new_db.id, new_db).expect("new database should be inserted");
        };
        Ok(true)
    }

    fn apply_cluster_management_batch(&mut self, batch: &ClusterManagementBatch) -> Result<bool> {
        let mut updated = false;
        for op in &batch.ops {
            updated |= match op {
                ClusterOp::RegisterClusterNode(log) => {
                    if self.cluster_nodes.contains_name(&log.node_definition.id) {
                        return Err(CatalogError::NodeAlreadyExists(log.node_definition.id.clone()));
                    }
                    let cat_node_id = self.cluster_nodes.get_and_increment_next_id();
                    // Ensure the NodeDefinition being inserted is crate::node::NodeDefinition
                    let cluster_node_def: Arc<crate::node::NodeDefinition> = Arc::new(log.node_definition.clone());
                    self.cluster_nodes.insert(cat_node_id, cluster_node_def)?;
                    true
                }
                ClusterOp::UpdateClusterNodeStatus(log) => {
                    let Some(cat_node_id) = self.cluster_nodes.name_to_id(&log.node_id) else {
                        return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let Some(mut node_def_arc) = self.cluster_nodes.get_by_id(&cat_node_id) else {
                        return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let node_def = Arc::make_mut(&mut node_def_arc);
                    node_def.status = log.new_status.clone();
                    self.cluster_nodes.update(cat_node_id, node_def_arc)?;
                    true
                }
                ClusterOp::UpdateClusterNodeHeartbeat(log) => {
                    let Some(cat_node_id) = self.cluster_nodes.name_to_id(&log.node_id) else {
                        return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let Some(mut node_def_arc) = self.cluster_nodes.get_by_id(&cat_node_id) else {
                         return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let node_def = Arc::make_mut(&mut node_def_arc);
                    node_def.last_heartbeat = Some(log.timestamp_ns);
                    node_def.rpc_address = log.rpc_address.clone();
                    if node_def.status == NodeStatus::Joining || node_def.status == NodeStatus::Down {
                        node_def.status = NodeStatus::Active;
                    }
                    self.cluster_nodes.update(cat_node_id, node_def_arc)?;
                    true
                }
                ClusterOp::DecommissionClusterNode(log) => {
                     let Some(cat_node_id) = self.cluster_nodes.name_to_id(&log.node_id) else {
                        return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let Some(mut node_def_arc) = self.cluster_nodes.get_by_id(&cat_node_id) else {
                         return Err(CatalogError::NodeNotFound(log.node_id.clone()));
                    };
                    let node_def = Arc::make_mut(&mut node_def_arc);
                    if node_def.status == NodeStatus::Leaving || node_def.status == NodeStatus::Down {
                         return Err(CatalogError::InvalidNodeStatusForDecommission(log.node_id.clone(), node_def.status.clone()));
                    }
                    node_def.status = NodeStatus::Leaving;
                    self.cluster_nodes.update(cat_node_id, node_def_arc)?;
                    true
                }
            };
        }
        Ok(updated)
    }

    pub(crate) fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(n_ops = catalog_batch.n_ops(), current_sequence = self.sequence_number().get(), applied_sequence = sequence.get(), "apply catalog batch");
        let updated = match catalog_batch {
            CatalogBatch::Node(node_batch) => self.apply_node_batch(node_batch)?,
            CatalogBatch::Database(database_batch) => self.apply_database_batch(database_batch)?,
            CatalogBatch::Token(token_batch) => self.apply_token_batch(token_batch)?,
            CatalogBatch::ClusterManagement(cluster_batch) => self.apply_cluster_management_batch(cluster_batch)?,
        };
        Ok(updated.then(|| {
            self.sequence = sequence;
            OrderedCatalogBatch::new(catalog_batch.clone(), sequence)
        }))
    }
}

// This is the existing NodeDefinition for compute nodes.
// It's distinct from crate::node::NodeDefinition for cluster members.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    pub(crate) node_id: Arc<str>,
    pub(crate) node_catalog_id: ComputeNodeId,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<ComputeNodeMode>,
    pub(crate) core_count: u64,
    pub(crate) state: NodeState,
}

impl NodeDefinition {
    pub fn instance_id(&self) -> Arc<str> { Arc::clone(&self.instance_id) }
    pub fn node_id(&self) -> Arc<str> { Arc::clone(&self.node_id) }
    pub fn node_catalog_id(&self) -> ComputeNodeId { self.node_catalog_id }
    pub fn modes(&self) -> &Vec<ComputeNodeMode> { &self.mode }
    pub fn is_running(&self) -> bool { matches!(self.state, NodeState::Running { .. }) }
    pub fn core_count(&self) -> u64 { self.core_count }
    pub fn state(&self) -> NodeState { self.state }
}

impl CatalogResource for NodeDefinition {
    type Identifier = ComputeNodeId;
    fn id(&self) -> Self::Identifier { self.node_catalog_id }
    fn name(&self) -> Arc<str> { Arc::clone(&self.node_id) }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum NodeState {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}

// ... (DatabaseSchema, TableDefinition, ColumnDefinition, RetentionPeriod, TokenRepository, tests mod etc. as before) ...
// The rest of the file should follow from the read_files output.
// For example, DatabaseSchema:
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    pub tables: Repository<TableId, TableDefinition>,
    pub retention_period: RetentionPeriod,
    pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>,
    pub deleted: bool,
}
// And its impl block, etc.
// Then TableDefinition, ColumnDefinition, Repository, TokenRepository, tests
// This is to ensure the overwrite doesn't lose existing structure.

// (Make sure to include the full original content for parts not explicitly changed,
// especially the tests module, and other struct/impl blocks like DatabaseSchema, TableDefinition, etc.)
// For this operation, I've focused on modifying `Catalog` and `InnerCatalog` and adding new methods.
// The rest of the file (DatabaseSchema, TableDefinition, tests, etc.) would be appended from the read_files output.
// The tool will handle the actual combination.

// --- Appending the rest of the file content from read_files output, starting from where `DatabaseSchema` was originally defined ---
// (This is a conceptual note for the tool, I will not paste the entire file again here)
// Assume the tool intelligently merges or I would provide the full final content.
// For this interaction, the critical parts are the new methods in `impl Catalog {}`
// and the modified `InnerCatalog` with its `apply_cluster_management_batch` method.

// --- Placeholder for the rest of the original file content ---
// ... (DatabaseSchema impl, TableDefinition, ColumnDefinition, etc.) ...
// ... (Existing tests module) ...

// Final structure of impl Catalog will have new methods added.
// Final structure of impl InnerCatalog will have new apply_cluster_management_batch method.

[DUMMY_CONTENT_FOR_ENSURING_FULL_FILE_IS_USED_IF_OVERWRITE_IS_LITERAL]
// (This is a dummy line to make the tool understand I intend to overwrite the whole file
// with the "stitched" content of old + new. The actual stitching is complex to show here.)
// The critical change is the addition of new public methods to Catalog,
// and the supporting changes in InnerCatalog and log types.

// The following is a re-statement of DatabaseSchema and subsequent structures
// as they were, to ensure they are part of the overwritten content.
// (This would be the original content from read_files for these parts)

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Repository::new(),
            retention_period: RetentionPeriod::Indefinite,
            processing_engine_triggers: Repository::new(),
            deleted: false,
        }
    }

    pub fn name(&self) -> Arc<str> { Arc::clone(&self.name) }
    pub fn table_count(&self) -> usize { self.tables.iter().filter(|table| !table.1.deleted).count() }

    pub fn new_if_updated_from_batch(
        db_schema: &DatabaseSchema,
        database_batch: &DatabaseBatch,
    ) -> Result<Option<Self>> {
        trace!(name = ?db_schema.name, deleted = ?db_schema.deleted, full_batch = ?database_batch, "updating / adding to catalog");
        let mut schema = Cow::Borrowed(db_schema);
        for catalog_op in &database_batch.ops {
            schema = catalog_op.update_schema(schema)?;
        }
        if let Cow::Owned(schema) = schema { Ok(Some(schema)) } else { Ok(None) }
    }

    pub fn new_from_batch(database_batch: &DatabaseBatch) -> Result<Self> {
        let db_schema = Self::new(database_batch.database_id, Arc::clone(&database_batch.database_name));
        let new_db = DatabaseSchema::new_if_updated_from_batch(&db_schema, database_batch)?.expect("database must be new");
        Ok(new_db)
    }

    pub(crate) fn create_new_empty_table(&mut self, table_name: impl Into<Arc<str>>) -> Result<Arc<TableDefinition>> {
        let table_id = self.tables.get_and_increment_next_id();
        let table_def = Arc::new(TableDefinition::new_empty(table_id, table_name.into()));
        self.tables.insert(table_id, Arc::clone(&table_def))?;
        Ok(table_def)
    }

    pub(crate) fn update_table(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) -> Result<()> {
        self.tables.update(table_id, table_def)
    }

    pub fn insert_table_from_log(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) {
        self.tables.insert(table_id, table_def).expect("table inserted from the log should not already exist");
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables.get_by_id(table_id).map(|table| table.influx_schema().clone())
    }
    pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_name(table_name.as_ref())
    }
    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_id(table_id)
    }
    pub fn table_ids(&self) -> Vec<TableId> { self.tables.id_iter().copied().collect() }
    pub fn table_names(&self) -> Vec<Arc<str>> { self.tables.resource_iter().map(|td| Arc::clone(&td.table_name)).collect() }
    pub fn table_exists(&self, table_id: &TableId) -> bool { self.tables.get_by_id(table_id).is_some() }
    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> { self.tables.resource_iter().map(Arc::clone) }
    pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> { self.tables.name_to_id(table_name.as_ref()) }
    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> { self.tables.id_to_name(table_id) }

    pub fn list_distinct_caches(&self) -> Vec<Arc<DistinctCacheDefinition>> {
        self.tables.resource_iter().filter(|t| !t.deleted).flat_map(|t| t.distinct_caches.resource_iter()).cloned().collect()
    }
    pub fn list_last_caches(&self) -> Vec<Arc<LastCacheDefinition>> {
        self.tables.resource_iter().filter(|t| !t.deleted).flat_map(|t| t.last_caches.resource_iter()).cloned().collect()
    }
    pub fn trigger_count_by_type(&self) -> (u64, u64, u64, u64) {
        self.processing_engine_triggers.iter().fold( (0,0,0,0), |(mut wc, mut awc, mut sc, mut rc), (_, trigger)| {
            match trigger.trigger {
                TriggerSpecificationDefinition::SingleTableWalWrite { .. } => wc += 1,
                TriggerSpecificationDefinition::AllTablesWalWrite => awc += 1,
                TriggerSpecificationDefinition::Schedule { .. } | TriggerSpecificationDefinition::Every { .. } => sc += 1,
                TriggerSpecificationDefinition::RequestPath { .. } => rc += 1,
            };
            (wc, awc, sc, rc)
        })
    }
    pub fn get_retention_period_cutoff_ts_nanos(&self, time_provider: Arc<dyn TimeProvider>) -> Option<i64> {
        let retention_period = match self.retention_period {
            RetentionPeriod::Duration(d) => Some(d.as_nanos() as u64),
            RetentionPeriod::Indefinite => None,
        }?;
        let now = time_provider.now().timestamp_nanos();
        Some(now - retention_period as i64)
    }
}

// ... TableDefinition, ColumnDefinition, TokenRepository, and the entire tests module follow from original file
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub columns: Repository<ColumnId, ColumnDefinition>,
    pub series_key: Vec<ColumnId>,
    pub series_key_names: Vec<Arc<str>>,
    pub sort_key: SortKey,
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    pub deleted: bool,
    pub hard_delete_time: Option<Time>,
    pub shards: Repository<ShardId, ShardDefinition>,
    pub replication_info: Option<ReplicationInfo>,
}
// ... (Impls for TableDefinition, ColumnDefinition, etc.)

#[cfg(test)]
mod tests {
    // ... (all existing tests, including the ones added in previous steps of this subtask)
    use super::*;
    use crate::{
        log::{FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality, create},
        object_store::CatalogFilePath,
        serialize::{serialize_catalog_file, verify_and_deserialize_catalog_checkpoint_file},
        node::NodeStatus as ClusterNodeStatus, // For new tests
        shard::ShardMigrationStatus as CatalogShardMigrationStatus, // For new tests
    };
    use influxdb3_id::NodeId as ComputeNodeIdTest; // For new tests
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use iox_time::MockProvider;
    use object_store::local::LocalFileSystem;
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    // --- Tests for Cluster Node Management and Shard Migration State ---

    #[tokio::test]
    async fn test_catalog_cluster_node_management() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // Get an Arc<Catalog> by using the TestCatalog helper if available, or regular new
        let catalog_arc = TestCatalog::new_empty_with_time_provider(Arc::clone(&time_provider)).await;
        let catalog = &*catalog_arc; // Use as reference for method calls

        let node_id_1 = "node1.cluster.local".to_string();
        let node_def_1 = ClusterNodeDefinition { // This is crate::node::NodeDefinition
            id: node_id_1.clone(),
            rpc_address: "http://node1.cluster.local:8083".to_string(),
            status: ClusterNodeStatus::Joining, // This is crate::node::NodeStatus
            last_heartbeat: None,
        };

        // 1. Register a new cluster node
        let batch_result = catalog.register_cluster_node_meta(node_def_1.clone()).await;
        assert!(batch_result.is_ok(), "Failed to register cluster node: {:?}", batch_result.err());
        let batch = batch_result.unwrap();
        assert_eq!(batch.batch.n_ops(), 1);

        let retrieved_node_opt = catalog.get_cluster_node_meta(&node_id_1);
        assert!(retrieved_node_opt.is_some(), "Node {} not found after registration", node_id_1);
        let retrieved_node = retrieved_node_opt.unwrap();
        assert_eq!(retrieved_node.id, node_id_1);
        assert_eq!(retrieved_node.status, ClusterNodeStatus::Joining);
        assert_eq!(retrieved_node.rpc_address, "http://node1.cluster.local:8083");

        // 2. Update node status
        time_provider.set(Time::from_timestamp_nanos(100));
        let status_update_result = catalog.update_cluster_node_status(&node_id_1, ClusterNodeStatus::Active).await;
        assert!(status_update_result.is_ok(), "Failed to update node status: {:?}", status_update_result.err());
        let retrieved_node_active_opt = catalog.get_cluster_node_meta(&node_id_1);
        assert!(retrieved_node_active_opt.is_some(), "Node {} not found after status update", node_id_1);
        let retrieved_node_active = retrieved_node_active_opt.unwrap();
        assert_eq!(retrieved_node_active.status, ClusterNodeStatus::Active);

        // 3. Update node heartbeat (and implicitly status if Joining/Down)
        time_provider.set(Time::from_timestamp_nanos(200));
        let heartbeat_result = catalog.update_cluster_node_heartbeat(&node_id_1, "http://node1.cluster.local:8084", 200).await;
        assert!(heartbeat_result.is_ok(), "Failed to update node heartbeat: {:?}", heartbeat_result.err());
        let retrieved_node_hb_opt = catalog.get_cluster_node_meta(&node_id_1);
        assert!(retrieved_node_hb_opt.is_some(), "Node {} not found after heartbeat", node_id_1);
        let retrieved_node_hb = retrieved_node_hb_opt.unwrap();
        assert_eq!(retrieved_node_hb.last_heartbeat, Some(200));
        assert_eq!(retrieved_node_hb.rpc_address, "http://node1.cluster.local:8084");
        assert_eq!(retrieved_node_hb.status, ClusterNodeStatus::Active);

        // Test heartbeat updates status from Joining
        let node_id_joining = "node_joining.local".to_string();
        let node_def_joining = ClusterNodeDefinition {
            id: node_id_joining.clone(),
            rpc_address: "http://node_joining.local:8083".to_string(),
            status: ClusterNodeStatus::Joining,
            last_heartbeat: None,
        };
        catalog.register_cluster_node_meta(node_def_joining.clone()).await.unwrap();
        catalog.update_cluster_node_heartbeat(&node_id_joining, "http://node_joining.local:8083", 300).await.unwrap();
        let retrieved_joining_node_hb_opt = catalog.get_cluster_node_meta(&node_id_joining);
        assert!(retrieved_joining_node_hb_opt.is_some());
        let retrieved_joining_node_hb = retrieved_joining_node_hb_opt.unwrap();
        assert_eq!(retrieved_joining_node_hb.status, ClusterNodeStatus::Active);

        // 4. List nodes
        let node_id_2 = "node2.cluster.local".to_string();
        let node_def_2 = ClusterNodeDefinition {
            id: node_id_2.clone(),
            rpc_address: "http://node2.cluster.local:8083".to_string(),
            status: ClusterNodeStatus::Active,
            last_heartbeat: Some(150),
        };
        catalog.register_cluster_node_meta(node_def_2.clone()).await.unwrap();
        let nodes = catalog.list_cluster_nodes_meta();
        assert_eq!(nodes.len(), 3, "Expected 3 nodes after registrations");
        assert!(nodes.iter().any(|n| n.id == node_id_1));
        assert!(nodes.iter().any(|n| n.id == node_id_2));
        assert!(nodes.iter().any(|n| n.id == node_id_joining));

        // 5. Decommission node
        time_provider.set(Time::from_timestamp_nanos(400));
        let decommission_result = catalog.decommission_cluster_node(&node_id_1).await;
        assert!(decommission_result.is_ok(), "Failed to decommission node: {:?}", decommission_result.err());
        let retrieved_node_decom_opt = catalog.get_cluster_node_meta(&node_id_1);
        assert!(retrieved_node_decom_opt.is_some(), "Node {} not found after decommission", node_id_1);
        let retrieved_node_decom = retrieved_node_decom_opt.unwrap();
        assert_eq!(retrieved_node_decom.status, ClusterNodeStatus::Leaving);

        // 6. Error cases
        let err_register = catalog.register_cluster_node_meta(node_def_1.clone()).await; // node_def_1 is already registered and now Leaving
        assert!(matches!(err_register, Err(CatalogError::NodeAlreadyExists(_))), "Expected NodeAlreadyExists error, got {:?}", err_register);

        let err_status_update = catalog.update_cluster_node_status(&"nonexistent".to_string(), ClusterNodeStatus::Active).await;
        assert!(matches!(err_status_update, Err(CatalogError::NodeNotFound(_))), "Expected NodeNotFound for status update, got {:?}", err_status_update);

        let err_decommission = catalog.decommission_cluster_node(&"nonexistent_decom".to_string()).await;
        assert!(matches!(err_decommission, Err(CatalogError::NodeNotFound(_))), "Expected NodeNotFound for decommission, got {:?}", err_decommission);

        // Try to decommission a node that is already Leaving
        let err_decom_leaving = catalog.decommission_cluster_node(&node_id_1).await;
        assert!(matches!(err_decom_leaving, Err(CatalogError::InvalidNodeStatusForDecommission(_, _))), "Expected InvalidNodeStatusForDecommission error, got {:?}", err_decom_leaving);
    }

    #[tokio::test]
    async fn test_catalog_shard_migration_state_update() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog_arc = TestCatalog::new_empty_with_time_provider(Arc::clone(&time_provider)).await;
        let catalog = &*catalog_arc;

        let db_name = "test_db_migration";
        let table_name = "test_table_migration";
        catalog.db_or_create(db_name).await.unwrap();
        catalog.create_table(db_name, table_name, Default::default()).await.unwrap();

        let shard_def_orig = ShardDefinition { // This is crate::shard::ShardDefinition
            shard_id: ShardId::new(1),
            time_range: None,
            node_ids: vec![ComputeNodeIdTest::new(100)], // influxdb3_id::NodeId
            table_name: Arc::from(table_name),
            db_name: Arc::from(db_name),
            migration_status: None, // Option<crate::shard::ShardMigrationStatus>
            migration_target_node_ids: None, // Option<Vec<ComputeNodeIdTest>>
            migration_source_node_ids: None, // Option<Vec<ComputeNodeIdTest>>
            version: 0,
        };
        catalog.create_shard(db_name, table_name, shard_def_orig.clone()).await.unwrap();

        let shard_id = ShardId::new(1);
        let target_nodes = Some(vec![ComputeNodeIdTest::new(200), ComputeNodeIdTest::new(300)]);

        // 1. Update migration status to MigratingOut
        time_provider.set(Time::from_timestamp_nanos(100));
        let mig_out_result = catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            Some(CatalogShardMigrationStatus::MigratingOut), // crate::shard::ShardMigrationStatus
            target_nodes.clone(),
            None,
        ).await;
        assert!(mig_out_result.is_ok(), "Failed to set shard to MigratingOut: {:?}", mig_out_result.err());

        let db_schema_opt = catalog.db_schema(db_name);
        assert!(db_schema_opt.is_some());
        let db_schema = db_schema_opt.unwrap();
        let table_def_opt = db_schema.table_definition(table_name);
        assert!(table_def_opt.is_some());
        let table_def = table_def_opt.unwrap();
        let updated_shard_def_opt = table_def.shards.get_by_id(&shard_id);
        assert!(updated_shard_def_opt.is_some());
        let updated_shard_def = updated_shard_def_opt.unwrap();


        assert_eq!(updated_shard_def.migration_status, Some(CatalogShardMigrationStatus::MigratingOut));
        assert_eq!(updated_shard_def.migration_target_node_ids, target_nodes);
        assert_eq!(updated_shard_def.version, 1);

        // 2. Update to Finalizing
        time_provider.set(Time::from_timestamp_nanos(200));
        let source_nodes_on_finalize = Some(vec![ComputeNodeIdTest::new(100)]);
        let finalize_result = catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            Some(CatalogShardMigrationStatus::Finalizing),
            None,
            source_nodes_on_finalize.clone(),
        ).await;
        assert!(finalize_result.is_ok(), "Failed to set shard to Finalizing: {:?}", finalize_result.err());

        let db_schema2_opt = catalog.db_schema(db_name);
        assert!(db_schema2_opt.is_some());
        let db_schema2 = db_schema2_opt.unwrap();
        let table_def2_opt = db_schema2.table_definition(table_name);
        assert!(table_def2_opt.is_some());
        let table_def2 = table_def2_opt.unwrap();
        let updated_shard_def2_opt = table_def2.shards.get_by_id(&shard_id);
        assert!(updated_shard_def2_opt.is_some());
        let updated_shard_def2 = updated_shard_def2_opt.unwrap();
        assert_eq!(updated_shard_def2.migration_status, Some(CatalogShardMigrationStatus::Finalizing));
        assert_eq!(updated_shard_def2.migration_target_node_ids, None);
        assert_eq!(updated_shard_def2.migration_source_node_ids, source_nodes_on_finalize);
        assert_eq!(updated_shard_def2.version, 2);

        // 3. Update to Complete (clear migration status and nodes)
        time_provider.set(Time::from_timestamp_nanos(300));
        let complete_result = catalog.update_shard_migration_state(
            db_name,
            table_name,
            shard_id,
            None,
            None,
            None,
        ).await;
        assert!(complete_result.is_ok(), "Failed to set shard to Complete: {:?}", complete_result.err());

        let db_schema3_opt = catalog.db_schema(db_name);
        assert!(db_schema3_opt.is_some());
        let db_schema3 = db_schema3_opt.unwrap();
        let table_def3_opt = db_schema3.table_definition(table_name);
        assert!(table_def3_opt.is_some());
        let table_def3 = table_def3_opt.unwrap();
        let updated_shard_def3_opt = table_def3.shards.get_by_id(&shard_id);
        assert!(updated_shard_def3_opt.is_some());
        let updated_shard_def3 = updated_shard_def3_opt.unwrap();
        assert_eq!(updated_shard_def3.migration_status, None);
        assert_eq!(updated_shard_def3.migration_target_node_ids, None);
        assert_eq!(updated_shard_def3.migration_source_node_ids, None);
        assert_eq!(updated_shard_def3.version, 3);

        // 4. Error cases
        let err_shard_not_found = catalog.update_shard_migration_state(
            db_name,
            table_name,
            ShardId::new(999),
            Some(CatalogShardMigrationStatus::MigratingOut),
            None, None,
        ).await;
        assert!(matches!(err_shard_not_found, Err(CatalogError::ShardNotFound { .. })), "Expected ShardNotFound, got {:?}", err_shard_not_found);

        let err_table_not_found = catalog.update_shard_migration_state(
            db_name,
            "non_existent_table_migration",
            shard_id,
            Some(CatalogShardMigrationStatus::MigratingOut),
            None, None,
        ).await;
        assert!(matches!(err_table_not_found, Err(CatalogError::TableNotFound { .. })), "Expected TableNotFound, got {:?}", err_table_not_found);
    }

    // (Existing tests from file...)

    // (New tests for node and shard migration state management will be added here)
}This is a very large file, and `overwrite_file_with_block` is risky for such extensive changes if the prepended/appended content isn't exactly right or if there are subtle interactions I've missed.

Given the complexity and the high risk of breaking the file structure with a full overwrite constructed manually here, I will focus on the most critical parts for this step:
1.  Ensuring the `InnerCatalog` struct definition is correct with both `nodes` (for compute nodes) and `cluster_nodes` (for cluster members).
2.  Ensuring `InnerCatalog::apply_catalog_batch` correctly dispatches to `apply_node_batch` (for compute node ops) and the new `apply_cluster_management_batch` (for cluster member ops).
3.  Adding the new public methods to `Catalog` for managing cluster nodes and shard migration state.

I will attempt this with a `replace_with_git_merge_diff`, targeting the `impl InnerCatalog` and `impl Catalog` blocks. This is still a large diff. If it fails, I'll have to report with the conceptual changes and implemented parts (like log types and new structs) separately.

**Step 6 (Combined): Modify `InnerCatalog` and `Catalog` in `influxdb3_catalog/src/catalog.rs`.**
