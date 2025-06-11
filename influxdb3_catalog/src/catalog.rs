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

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::replication::ReplicationInfo;
use crate::shard::{ShardDefinition, ShardId};
use crate::log::{
    ClearRetentionPeriodLog, CreateAdminTokenDetails, CreateDatabaseLog, DatabaseBatch,
    DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode, RegenerateAdminTokenDetails,
    RegisterNodeLog, SetRetentionPeriodLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerSpecificationDefinition, AddNodeLog, RemoveNodeLog, UpdateNodeStateLog, UpdateNodeLog, // Added Node Ops
    ClusterNodeBatch, ClusterNodeCatalogOp, // Added Cluster Node Batch/Op
};
use crate::object_store::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use crate::snapshot::{
    CatalogSnapshot, ClusterNodeDefinitionSnapshot, // Added ClusterNodeDefinitionSnapshot for From impls
    NodeSnapshot, DatabaseSnapshot, TableSnapshot, ColumnDefinitionSnapshot,
    LastCacheSnapshot, DistinctCacheSnapshot, ProcessingEngineTriggerSnapshot,
    RepositorySnapshot, TokenInfoSnapshot, NodeStateSnapshot, RetentionPeriodSnapshot,
    DataType, InfluxType, TimeUnit,
};

use crate::{
    CatalogError, Result, ClusterNodeDefinition, // Made ClusterNodeDefinition accessible
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

// Limit for the number of tag columns on a table - now managed by CatalogLimits
// pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 250; // Commented out

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u64);

impl CatalogSequenceNumber {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for CatalogSequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> =
    Mutex::const_new(CatalogSequenceNumber::new(0));

/// Convenience type alias for the write permit on the catalog
///
/// This is a mutex that, when a lock is acquired, holds the next catalog sequence number at the
/// time that the permit was acquired.
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
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum CatalogState {
    Active,
    Shutdown,
}

impl CatalogState {
    fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

const CATALOG_CHECKPOINT_INTERVAL: u64 = 100;

#[derive(Clone, Copy, Debug)]
pub struct CatalogArgs {
    pub default_hard_delete_duration: Duration,
}

impl CatalogArgs {
    pub fn new(default_hard_delete_duration: Duration) -> Self {
        Self {
            default_hard_delete_duration,
        }
    }
}

impl Default for CatalogArgs {
    fn default() -> Self {
        Self {
            default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION,
        }
    }
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
        Self {
            num_dbs,
            num_tables,
            num_columns_per_table,
            num_tag_columns_per_table,
        }
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
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        limits: CatalogLimits,
    ) -> Result<Self> {
        Self::new_with_args(
            node_id,
            store,
            time_provider,
            metric_registry,
            CatalogArgs::default(),
            limits,
        )
        .await
    }

    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        args: CatalogArgs,
        limits: CatalogLimits,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let store =
            ObjectStoreCatalog::new(Arc::clone(&node_id), CATALOG_CHECKPOINT_INTERVAL, store);
        let subscriptions = Default::default();
        let metrics = Arc::new(CatalogMetrics::new(&metric_registry));
        let catalog = store
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| Self {
                metric_registry,
                state: parking_lot::Mutex::new(CatalogState::Active),
                subscriptions,
                time_provider,
                store,
                metrics,
                inner,
                limits,
                args,
            })?;

        create_internal_db(&catalog).await;
        catalog.metrics.operation_observer(
            catalog
                .subscribe_to_updates("catalog_operation_metrics")
                .await,
        );
        Ok(catalog)
    }

    pub async fn new_with_shutdown(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();
        let catalog =
            Arc::new(Self::new(Arc::clone(&node_id), store, time_provider, metric_registry, limits).await?);
        let catalog_cloned = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            info!(
                node_id = node_id.as_ref(),
                "updating node state to stopped in catalog"
            );
            if let Err(error) = catalog_cloned
                .update_node_state_stopped(node_id_str_from_arc(&node_id)) // Helper might be needed if node_id is not &str
                .await
            {
                error!(
                    ?error,
                    node_id = node_id.as_ref(),
                    "encountered error while updating node to stopped state in catalog"
                );
            }
        });
        Ok(catalog)
    }

    // Helper to convert Arc<str> to &str for methods that need it
    // This is a bit of a workaround if the original `node_id` Arc<str> isn't directly available
    // or if an owned String is needed by some API that can't take &str directly from Arc.
    // However, many functions take `&str`, so `node_id.as_ref()` is often sufficient.
    // For `update_node_state_stopped`, it takes `&str`.
    pub fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    pub fn set_state_shutdown(&self) {
        *self.state.lock() = CatalogState::Shutdown;
    }

    fn num_dbs_limit(&self) -> usize {
        self.limits.num_dbs
    }

    fn num_tables_limit(&self) -> usize {
        self.limits.num_tables
    }

    fn num_columns_per_table_limit(&self) -> usize {
        self.limits.num_columns_per_table
    }

    pub(crate) fn num_tag_columns_per_table_limit(&self) -> usize {
        self.limits.num_tag_columns_per_table
    }

    fn default_hard_delete_duration(&self) -> Duration {
        self.args.default_hard_delete_duration
    }

    // --- Cluster Node Membership Methods ---
    pub async fn add_cluster_node(&self, node_def: ClusterNodeDefinition) -> Result<OrderedCatalogBatch> {
        info!(node_id = ?node_def.id, rpc_addr = %node_def.rpc_address, http_addr = %node_def.http_address, "add cluster node");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        let mut node_def_mut = node_def;
        node_def_mut.updated_at = current_time_ns; // Ensure updated_at is set
        // created_at should be set by caller or use current_time_ns if it's 0
        if node_def_mut.created_at == 0 {
            node_def_mut.created_at = current_time_ns;
        }

        self.catalog_update_with_retry(|| {
            // Check if node with this ID already exists to prevent overwriting with new created_at
            if self.inner.read().cluster_nodes.contains_id(&node_def_mut.id) {
                 return Err(CatalogError::AlreadyExists); // Or a more specific NodeAlreadyExists
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::AddClusterNode(AddClusterNodeLog {
                    node: node_def_mut.clone(), // Clone because node_def_mut is moved if this closure retries
                })],
            }))
        }).await
    }

    pub async fn remove_cluster_node(&self, node_id: NodeId) -> Result<OrderedCatalogBatch> {
        info!(?node_id, "remove cluster node");
        let current_time_ns = self.time_provider.now().timestamp_nanos();
        self.catalog_update_with_retry(|| {
            if self.inner.read().cluster_nodes.get_by_id(&node_id).is_none() {
                return Err(CatalogError::NotFound); // Or a specific NodeNotFound
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::RemoveClusterNode(RemoveClusterNodeLog {
                    node_id,
                    removed_at_ts: current_time_ns,
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
            if self.inner.read().cluster_nodes.get_by_id(&node_id).is_none() {
                 return Err(CatalogError::NotFound); // Or a specific NodeNotFound
            }
            Ok(CatalogBatch::ClusterNode(ClusterNodeBatch {
                time_ns: current_time_ns,
                ops: vec![ClusterNodeCatalogOp::UpdateClusterNodeStatus(UpdateClusterNodeStatusLog {
                    node_id,
                    status: status.clone(),
                    updated_at_ts: current_time_ns,
                })],
            }))
        }).await
    }


    // --- Sharding and Replication Methods ---
    // ... (existing methods for shards and replication)


    // --- Helper for catalog updates (modified slightly to handle different batch types) ---
    pub(crate) async fn catalog_update_with_retry<F>(
        &self,
        batch_creator_fn: F,
    ) -> Result<OrderedCatalogBatch>
    where
        F: Fn() -> Result<CatalogBatch>,
    {
        loop {
            let sequence = self.sequence_number();
            let batch = batch_creator_fn()?; // This now can return CatalogBatch::Node, ::Database, or ::ClusterNode
            match self
                .get_permit_and_verify_catalog_batch(batch, sequence)
                .await
            {
                Prompt::Success((ordered_batch, permit)) => {
                    match self
                        .persist_ordered_batch_to_object_store(&ordered_batch, &permit)
                        .await?
                    {
                        update::UpdatePrompt::Retry => continue, // Corrected enum path
                        update::UpdatePrompt::Applied => { // Corrected enum path
                            self.apply_ordered_catalog_batch(&ordered_batch, &permit);
                            self.background_checkpoint(&ordered_batch);
                            self.broadcast_update(ordered_batch.clone().into_batch()) // This needs to work for all CatalogBatch types
                                .await?;
                            return Ok(ordered_batch);
                        }
                    }
                }
                Prompt::Retry(_) => continue,
            }
        }
    }
    // ... (rest of the Catalog methods like object_store_prefix, snapshot, etc.)
    // ... (db_or_create, token methods, etc.)

    pub fn object_store_prefix(&self) -> Arc<str> {
        Arc::clone(&self.store.prefix)
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.inner.read().catalog_uuid
    }

    pub async fn subscribe_to_updates(&self, name: &'static str) -> CatalogUpdateReceiver {
        self.subscriptions.write().await.subscribe(name)
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        self.inner.read().snapshot()
    }

    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut inner = self.inner.write();
        *inner = InnerCatalog::from_snapshot(snapshot);
    }

    pub(crate) fn apply_ordered_catalog_batch(
        &self,
        batch: &OrderedCatalogBatch,
        _permit: &CatalogWritePermit,
    ) -> CatalogBatch {
        let batch_sequence = batch.sequence_number().get();
        let current_sequence = self.sequence_number().get();
        assert_eq!(
            batch_sequence,
            current_sequence + 1,
            "catalog batch received out of order"
        );
        let catalog_batch = self
            .inner
            .write()
            .apply_catalog_batch(batch.batch(), batch.sequence_number())
            .expect("ordered catalog batch should succeed when applied")
            .expect("ordered catalog batch should contain changes");
        catalog_batch.into_batch()
    }

    pub fn node(&self, node_id: &str) -> Option<Arc<NodeDefinition>> {
        self.inner.read().nodes.get_by_name(node_id)
    }

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().databases.next_id()
    }

    pub(crate) fn db_or_create(
        &self,
        db_name: &str,
        now_time_ns: i64,
    ) -> Result<(Arc<DatabaseSchema>, Option<CatalogBatch>)> {
        match self.db_schema(db_name) {
            Some(db) => Ok((db, None)),
            None => {
                let mut inner = self.inner.write();

                if inner.database_count() >= self.num_dbs_limit() {
                    return Err(CatalogError::TooManyDbs(self.num_dbs_limit()));
                }

                info!(database_name = db_name, "creating new database");
                let db_id = inner.databases.get_and_increment_next_id();
                let db_name_arc = Arc::from(db_name);
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name_arc)));
                let batch = CatalogBatch::database(
                    now_time_ns,
                    db.id,
                    db.name(),
                    vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                        database_id: db.id,
                        database_name: Arc::clone(&db_name_arc),
                    })],
                );
                Ok((db, Some(batch)))
            }
        }
    }
    // ... (The rest of the file: db_name_to_id, db_id_to_name, etc. ... InnerCatalog, Repository, etc.)
    // ... (Make sure to include the previous changes to InnerCatalog::snapshot and from_snapshot correctly)
}


// --- InnerCatalog, Repository, NodeDefinition, DatabaseSchema, TableDefinition, ColumnDefinition, TokenRepository etc. ---
// These struct definitions and their impl blocks follow.
// I need to make sure the snapshot methods in InnerCatalog are correctly modified.

impl InnerCatalog {
    // ... (new method as defined before) ...

    // Modified to include cluster_nodes
    pub(crate) fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            nodes: self.nodes.snapshot_with_item_transform(NodeSnapshot::from),
            databases: self.databases.snapshot_with_item_transform(DatabaseSnapshot::from),
            tokens: self.tokens.snapshot(),
            cluster_nodes: self.cluster_nodes.snapshot_with_item_transform(ClusterNodeDefinitionSnapshot::from), // Added
            sequence: self.sequence,
            catalog_id: Arc::clone(&self.catalog_id),
            catalog_uuid: self.catalog_uuid,
        }
    }

    // Modified to include cluster_nodes
    pub(crate) fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        Self {
            sequence: snapshot.sequence_number(),
            catalog_id: Arc::clone(&snapshot.catalog_id),
            catalog_uuid: snapshot.catalog_uuid,
            nodes: Repository::from_snapshot_with_item_transform(snapshot.nodes, NodeDefinition::from),
            databases: Repository::from_snapshot_with_item_transform(snapshot.databases, DatabaseSchema::from),
            tokens: TokenRepository::from_snapshot(snapshot.tokens),
            cluster_nodes: Repository::from_snapshot_with_item_transform(snapshot.cluster_nodes, ClusterNodeDefinition::from), // Added
        }
    }

    // ... (apply_catalog_batch, apply_node_batch, etc. needs to be here)
    // Make sure apply_catalog_batch handles CatalogBatch::ClusterNode
    pub(crate) fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            n_ops = catalog_batch.n_ops(),
            current_sequence = self.sequence_number().get(),
            applied_sequence = sequence.get(),
            "apply catalog batch"
        );
        let updated = match catalog_batch {
            CatalogBatch::Node(node_batch) => self.apply_node_batch(node_batch)?,
            CatalogBatch::Database(database_batch) => self.apply_database_batch(database_batch)?,
            CatalogBatch::Token(token_batch) => self.apply_token_batch(token_batch)?,
            CatalogBatch::ClusterNode(cluster_node_batch) => self.apply_cluster_node_batch(cluster_node_batch)?, // Added
        };

        Ok(updated.then(|| {
            self.sequence = sequence;
            OrderedCatalogBatch::new(catalog_batch.clone(), sequence)
        }))
    }

    // New method to apply cluster node batches
    fn apply_cluster_node_batch(&mut self, batch: &ClusterNodeBatch) -> Result<bool> {
        let mut updated = false;
        for op in &batch.ops {
            updated |= match op {
                ClusterNodeCatalogOp::AddClusterNode(AddClusterNodeLog { node }) => {
                    if self.cluster_nodes.contains_id(&node.id) {
                        // Or update if semantics allow upsert through Add? For now, strict add.
                        return Err(CatalogError::AlreadyExists);
                    }
                    self.cluster_nodes.insert(node.id, Arc::new(node.clone()))?;
                    true
                }
                ClusterNodeCatalogOp::RemoveClusterNode(RemoveNodeLog { node_id, .. }) => {
                    if self.cluster_nodes.contains_id(node_id) {
                        self.cluster_nodes.remove(node_id);
                        true
                    } else {
                        false // Node not found, not an error for removal to be idempotent
                    }
                }
                ClusterNodeCatalogOp::UpdateClusterNodeStatus(UpdateNodeStatusLog { node_id, status, updated_at_ts }) => {
                    if let Some(mut node_arc) = self.cluster_nodes.get_by_id(node_id) {
                        let n = Arc::make_mut(&mut node_arc);
                        n.status = status.clone();
                        n.updated_at = *updated_at_ts;
                        self.cluster_nodes.update(*node_id, node_arc)?;
                        true
                    } else {
                        return Err(CatalogError::NotFound); // Node to update status for must exist
                    }
                }
            };
        }
        Ok(updated)
    }
    // ... (Rest of InnerCatalog impl, Repository, NodeDefinition, DatabaseSchema, TableDefinition, ColumnDefinition, TokenRepository, etc.)
}

// ... (ensure all other parts of the file like NodeDefinition, DatabaseSchema, etc. are included)
// ... (This is a very large file, so only showing relevant diffs is better with replace_with_git_merge_diff)

// Need to add From impls for Repository <-> RepositorySnapshot if they don't exist
// and for each Resource <-> ResourceSnapshot type.
// Assuming these are handled by helper methods or direct field mapping in Repository::snapshot/from_snapshot.
// For ClusterNodeDefinition, we added the From impls in cluster_node.rs.
// For NodeDefinition (the existing one), snapshotting is already handled.

// Placeholder for where Catalog Resource impl for ClusterNodeDefinition would go
impl CatalogResource for ClusterNodeDefinition {
    type Identifier = NodeId;

    fn id(&self) -> Self::Identifier {
        self.id
    }
    fn name(&self) -> Arc<str> {
        // ClusterNodeDefinition doesn't have a distinct 'name' field like others,
        // its user-facing identifier is often its http_address or a configured friendly name.
        // For Repository's internal id_name_map, we need something. Using id as string for now.
        // This might need refinement based on how nodes are looked up (e.g., by rpc_address).
        // For now, making it compatible with Repository which expects a name.
        // Let's assume NodeId can be converted to a suitable Arc<str> or has a name-like field.
        // If NodeId is just a u64, then format it.
        // For now, let's use rpc_address as a stand-in for a "name" for the BiHashMap.
        // This implies rpc_address should be unique.
        Arc::from(self.rpc_address.as_str())
    }
}

// Helper function that might have been missed in previous diffs
fn node_id_str_from_arc(arc_str: &Arc<str>) -> &str {
    arc_str.as_ref()
}


#[cfg(test)]
mod tests {
// ... (existing tests) ...

    #[test_log::test(tokio::test)]
    async fn catalog_serialization_with_nodes() {
        let catalog = Catalog::new_in_memory("ser_test_host_nodes").await.unwrap();
        let node1_id = NodeId::new(1);
        let node1 = ClusterNodeDefinition {
            id: node1_id,
            rpc_address: "node1:8082".to_string(),
            http_address: "node1:8081".to_string(),
            status: "Active".to_string(),
            created_at: 100,
            updated_at: 100,
        };
        catalog.add_cluster_node(node1.clone()).await.unwrap();

        let node2_id = NodeId::new(2);
        let node2 = ClusterNodeDefinition {
            id: node2_id,
            rpc_address: "node2:8082".to_string(),
            http_address: "node2:8081".to_string(),
            status: "Joining".to_string(),
            created_at: 200,
            updated_at: 200,
        };
        catalog.add_cluster_node(node2.clone()).await.unwrap();

        let snapshot = catalog.snapshot();
        // Basic check, more detailed snapshot tests would be good
        assert_eq!(snapshot.cluster_nodes.repo.len(), 2);

        insta::assert_json_snapshot!(snapshot, {
            ".catalog_uuid" => "[uuid]",
            // Add redactions for specific fields if needed, e.g. timestamps if they vary
            ".cluster_nodes.repo.1.id.0" => "[node1_id_val]",
            ".cluster_nodes.repo.2.id.0" => "[node2_id_val]",
        });

        let serialized = crate::serialize::serialize_catalog_file(&snapshot).unwrap();
        let deserialized_snapshot = crate::serialize::verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

        assert_eq!(snapshot.cluster_nodes.repo.len(), deserialized_snapshot.cluster_nodes.repo.len());

        catalog.update_from_snapshot(deserialized_snapshot);
        let nodes_after_restore = catalog.list_cluster_nodes();
        assert_eq!(nodes_after_restore.len(), 2);
        assert!(nodes_after_restore.iter().any(|n| n.id == node1_id && n.rpc_address == node1.rpc_address));
        assert!(nodes_after_restore.iter().any(|n| n.id == node2_id && n.status == node2.status));
    }

    #[test_log::test(tokio::test)]
    async fn test_node_management() {
        let catalog = Catalog::new_in_memory("node_mgmt_host").await.unwrap();
        let time_provider = catalog.time_provider();

        let node1_id = NodeId::new(1);
        let node1_def = ClusterNodeDefinition {
            id: node1_id,
            rpc_address: "node1:8082".to_string(),
            http_address: "node1:8081".to_string(),
            status: "Joining".to_string(),
            created_at: time_provider.now().timestamp_nanos(),
            updated_at: time_provider.now().timestamp_nanos(),
        };

        // Add node
        catalog.add_cluster_node(node1_def.clone()).await.unwrap();

        // Get node
        let fetched_node1 = catalog.get_cluster_node(node1_id).unwrap();
        assert_eq!(fetched_node1.as_ref(), &node1_def);

        // List nodes
        let nodes = catalog.list_cluster_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].as_ref(), &node1_def);

        // Update node status
        let new_status = "Active".to_string();
        catalog.update_cluster_node_status(node1_id, new_status.clone()).await.unwrap();
        let updated_node1 = catalog.get_cluster_node(node1_id).unwrap();
        assert_eq!(updated_node1.status, new_status);
        assert!(updated_node1.updated_at > node1_def.updated_at);

        // Attempt to add existing node ID (should fail)
        let node1_def_again = ClusterNodeDefinition {
            id: node1_id, // Same ID
            rpc_address: "node1-alt:8082".to_string(),
            http_address: "node1-alt:8081".to_string(),
            status: "Active".to_string(),
            created_at: time_provider.now().timestamp_nanos(),
            updated_at: time_provider.now().timestamp_nanos(),
        };
        assert!(matches!(catalog.add_cluster_node(node1_def_again).await, Err(CatalogError::AlreadyExists)));

        // Remove node
        catalog.remove_cluster_node(node1_id).await.unwrap();
        assert!(catalog.get_cluster_node(node1_id).is_none());
        assert_eq!(catalog.list_cluster_nodes().len(), 0);

        // Attempt to remove non-existent node (should fail)
        assert!(matches!(catalog.remove_cluster_node(NodeId::new(999)).await, Err(CatalogError::NotFound)));

        // Attempt to update status of non-existent node (should fail)
        assert!(matches!(catalog.update_cluster_node_status(NodeId::new(999), "Active".to_string()).await, Err(CatalogError::NotFound)));
    }
}
// ... (rest of the file, including existing test module)
// Note: The `impl CatalogResource for ClusterNodeDefinition` and `node_id_str_from_arc` helper
// are added here for completeness within the context of this overwrite.
// The existing tests module should be preserved after this block.
// If there are other `impl From` blocks for snapshotting (e.g. for NodeDefinition, DatabaseSchema etc.)
// they should be in their respective `from.rs` files or handled by the `Repository::snapshot_with_item_transform`
// and `Repository::from_snapshot_with_item_transform` if they take closures or direct From impls.
// The key here is that `InnerCatalog`'s methods correctly call snapshot/from_snapshot on its repositories.

// The `Repository::snapshot_with_item_transform` and `Repository::from_snapshot_with_item_transform` methods
// would require that `ClusterNodeDefinitionSnapshot` implements `From<&ClusterNodeDefinition>`
// and `ClusterNodeDefinition` implements `From<ClusterNodeDefinitionSnapshot>`.
// These were added in `cluster_node.rs`.

// The From impls for Repository itself (Repository -> RepositorySnapshot, RepositorySnapshot -> Repository)
// are assumed to be generic or defined elsewhere, handling the SerdeVecMap and next_id.
// Typically, Repository would have:
// fn snapshot_with_item_transform<S, F>(&self, transform: F) -> RepositorySnapshot<Self::Identifier, S>
// where F: Fn(&Arc<R>) -> S
// fn from_snapshot_with_item_transform<S, F>(snapshot: RepositorySnapshot<Self::Identifier, S>, transform: F) -> Self
// where F: Fn(S) -> Arc<R>

// For this to compile, these transform methods must exist on Repository, or simplified versions.
// Let's assume simplified versions for now for the sake of this overwrite, if they are not already present.
// If they exist and are generic, they should work with the From impls on ClusterNodeDefinition/Snapshot.

// It's more likely that Repository::snapshot and Repository::from_snapshot are already generic enough
// if ClusterNodeDefinition and ClusterNodeDefinitionSnapshot provide the necessary From traits.
// So, the main change is just adding the .snapshot() and .from_snapshot() calls for cluster_nodes.

// Final check on NodeDefinition (the original one) snapshotting:
// NodeSnapshot::from(&node_def_arc) and NodeDefinition::from(node_snapshot)
// These From impls should exist, likely in `snapshot/versions/v3/from.rs` or similar.
// If not, they would need to be added for `nodes: Repository<NodeId, NodeDefinition>` to work with snapshotting.
// This subtask focuses on ClusterNodeDefinition, so I assume the existing NodeDefinition is handled.
// The `snapshot_with_item_transform` and `from_snapshot_with_item_transform` are indeed how Repository
// handles this, so the From impls for ClusterNodeDefinition are key.

// The test module needs to be correctly placed at the end of the file.
// The overwrite block should contain the entire file content.
// The existing tests should be appended after the new ones if this structure is maintained.
// The provided diff for tests assumes it's appended to an existing tests module.
// If I'm overwriting the whole file, I must ensure the original tests module is also included.

// For safety, I will only provide the changes to InnerCatalog methods,
// the new Catalog methods for cluster nodes, the CatalogResource impl,
// and the new tests, assuming the rest of the file (including existing tests) remains.
// This means using replace_with_git_merge_diff for targeted changes is safer than a full overwrite.

// Reverting to targeted diffs.

// Previous changes to InnerCatalog::snapshot and from_snapshot were:
// snapshot: + cluster_nodes: self.cluster_nodes.snapshot(),
// from_snapshot: + cluster_nodes: Repository::from_snapshot(snapshot.cluster_nodes),
// These were correct and should be applied.
// The error was "Search block not found", meaning my local view of catalog.rs was stale.
// I will re-attempt with the correct, minimal diff for these two methods using the `read_files` output from [P030]
// as the basis for the search block.
// The `apply_catalog_batch` and `apply_cluster_node_batch` also need to be added.
// The `CatalogResource for ClusterNodeDefinition` also needs to be added.
// And the public API methods on `Catalog`.
// This will be a multi-part diff.I acknowledge the complexity and will proceed with targeted diffs for `influxdb3_catalog/src/catalog.rs`.

**Step 3c-ii & 3d-ii (re-attempt): Modify `InnerCatalog::snapshot()` and `InnerCatalog::from_snapshot()` in `influxdb3_catalog/src/catalog.rs`.**
This uses the content from prompt_id `[P030]` as the basis for the search lines.
