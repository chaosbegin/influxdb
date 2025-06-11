use std::sync::Arc;
use std::fmt::Debug;
use thiserror::Error;
use async_trait::async_trait;
use tracing::{info, warn, error};

// Re-export types from influxdb3_catalog's management module.
pub use influxdb3_catalog::management::{
    NodeId, NodeDefinition, NodeStatus,
    ShardId, ShardDefinition, ShardMigrationStatus,
};
use influxdb3_catalog::catalog::Catalog;


// NodeInfoProvider trait (can be moved to a common crate later if needed by more components)
pub trait NodeInfoProvider: Send + Sync + Debug {
    fn get_node_query_rpc_address(&self, node_id: &NodeId) -> Option<String>;
    fn get_node_management_rpc_address(&self, node_id: &NodeId) -> Option<String>;
}

// Mock for testing
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct MockNodeInfoProvider {
    pub query_addresses: std::collections::HashMap<NodeId, String>,
    pub mgmt_addresses: std::collections::HashMap<NodeId, String>,
}

#[cfg(test)]
impl MockNodeInfoProvider {
    pub fn new() -> Self { Self::default() }
    pub fn add_node(&mut self, node_id: NodeId, query_addr: String, mgmt_addr: String) {
        self.query_addresses.insert(node_id, query_addr);
        self.mgmt_addresses.insert(node_id, mgmt_addr);
    }
}

#[cfg(test)]
impl NodeInfoProvider for MockNodeInfoProvider {
    fn get_node_query_rpc_address(&self, node_id: &NodeId) -> Option<String> {
        self.query_addresses.get(node_id).cloned()
    }
    fn get_node_management_rpc_address(&self, node_id: &NodeId) -> Option<String> {
        self.mgmt_addresses.get(node_id).cloned()
    }
}


#[derive(Error, Debug)]
pub enum ClusterManagerError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] influxdb3_catalog::Error),
    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: NodeId }, // Changed to use influxdb3_id::NodeId
    #[error("Node with name '{node_name}' not found")]
    NodeNameNotFound { node_name: String },
    #[error("No suitable target nodes found for shard migration")]
    NoTargetNodes,
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T, E = ClusterManagerError> = std::result::Result<T, E>;

/// Defines strategies for rebalancing the cluster.
#[derive(Debug, Clone)]
pub enum RebalanceStrategy {
    /// Strategy for when a new node is added to the cluster.
    AddNewNode(NodeDefinition),
    /// Strategy for when a node is being decommissioned.
    DecommissionNode(NodeId), // Using the internal u64 NodeId for decommissioning
}

/// Manages node-level operations by interacting with the Catalog.
#[derive(Debug)]
pub struct ClusterManager {
    catalog: Arc<Catalog>,
}

impl ClusterManager {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn add_node(&self, node_def_payload: NodeDefinition) -> Result<()> {
        info!("ClusterManager: Adding node {:?} (ID: {:?}) to catalog", node_def_payload.node_name, node_def_payload.id);
        self.catalog.add_node(node_def_payload).await?;
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        info!("ClusterManager: Removing node ID {:?} from catalog", node_id);
        self.catalog.remove_node(node_id).await?;
        Ok(())
    }

    pub async fn update_node_status(&self, node_id: &NodeId, status: NodeStatus) -> Result<()> {
        info!("ClusterManager: Updating status of node ID {:?} to {:?}", node_id, status);
        self.catalog.update_node_status(node_id, status).await?;
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<Arc<NodeDefinition>>> {
        info!("ClusterManager: Listing nodes from catalog");
        Ok(self.catalog.list_nodes().await?)
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Arc<NodeDefinition>>> {
        info!("ClusterManager: Getting node ID {:?} from catalog", node_id);
        Ok(self.catalog.get_node(node_id).await?)
    }
}

/// Orchestrates rebalancing operations within the cluster.
#[derive(Debug)]
pub struct RebalanceOrchestrator {
    catalog: Arc<Catalog>,
    node_info_provider: Arc<dyn NodeInfoProvider>, // Will be needed for actual communication
}

impl RebalanceOrchestrator {
    pub fn new(catalog: Arc<Catalog>, node_info_provider: Arc<dyn NodeInfoProvider>) -> Self {
        Self { catalog, node_info_provider }
    }

    pub async fn initiate_rebalance(&self, strategy: RebalanceStrategy) -> Result<()> {
        info!("RebalanceOrchestrator: Initiating rebalance with strategy: {:?}", strategy);

        match strategy {
            RebalanceStrategy::AddNewNode(new_node_def_payload) => {
                // Use the payload directly for add_node. Catalog's add_node handles ID generation/validation.
                info!("Rebalance: Adding new node {:?} (ID: {:?})", new_node_def_payload.node_name, new_node_def_payload.id);
                self.catalog.add_node(new_node_def_payload.clone()).await?; // NodeDefinition is Clone

                let new_node_internal_id = new_node_def_payload.id; // ID is set by catalog.add_node or pre-assigned

                let all_dbs = self.catalog.list_db_schema().await?;
                let mut shards_to_consider_moving = vec![];

                for db_schema in all_dbs {
                    for table_def_arc in db_schema.tables() {
                        let table_def = table_def_arc.as_ref();
                        for shard_def_arc in table_def.shards.resource_iter() {
                            shards_to_consider_moving.push((
                                db_schema.name.clone(),
                                table_def.table_name.clone(),
                                shard_def_arc.id,
                                Arc::clone(shard_def_arc)
                            ));
                        }
                    }
                }

                let num_shards_to_move = 2.min(shards_to_consider_moving.len());
                let selected_shards: Vec<_> = shards_to_consider_moving.into_iter()
                    .take(num_shards_to_move)
                    .collect();

                if selected_shards.is_empty() {
                    info!("Rebalance: AddNewNode - No shards found to move to the new node.");
                }

                for (db_name, table_name, shard_id, _shard_def) in selected_shards {
                    info!(
                        "Rebalance: Selected shard {:?} from table {}.{} to migrate to new node ID {:?}",
                        shard_id, db_name, table_name, new_node_internal_id
                    );
                    self.catalog.begin_shard_migration_out(
                        &db_name,
                        &table_name,
                        &shard_id,
                        vec![new_node_internal_id],
                    ).await?;
                    info!(
                        "Conceptual: Initiate data movement for shard {:?} to new node ID {:?}.",
                        shard_id, new_node_internal_id
                    );
                }
                self.catalog.update_node_status(&new_node_internal_id, NodeStatus::Active).await?;
            }
            RebalanceStrategy::DecommissionNode(node_id_to_remove) => {
                let node_to_remove_def = self.catalog.get_node(&node_id_to_remove).await?
                    .ok_or_else(|| ClusterManagerError::NodeNotFound { node_id: node_id_to_remove })?;

                info!("Rebalance: Decommissioning node {:?} (ID: {:?})", node_to_remove_def.node_name, node_id_to_remove);
                self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Leaving).await?;

                let all_dbs = self.catalog.list_db_schema().await?;
                let mut shards_on_decommissioning_node = vec![];

                for db_schema in all_dbs {
                    for table_def_arc in db_schema.tables() {
                        let table_def = table_def_arc.as_ref();
                        for shard_def_arc in table_def.shards.resource_iter() {
                            if shard_def_arc.node_ids.contains(&node_id_to_remove) {
                                shards_on_decommissioning_node.push((
                                    db_schema.name.clone(),
                                    table_def.table_name.clone(),
                                    shard_def_arc.id,
                                ));
                            }
                        }
                    }
                }

                if shards_on_decommissioning_node.is_empty() {
                    info!("Rebalance: DecommissionNode - Node ID {:?} has no shards. Marking as Down.", node_id_to_remove);
                    self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Down).await?;
                    // Optionally, self.catalog.remove_node(&node_id_to_remove).await?; if no shards implies removable.
                    return Ok(());
                }

                let active_nodes = self.catalog.list_nodes().await?
                    .into_iter()
                    .filter(|n| n.id != node_id_to_remove && n.status == NodeStatus::Active)
                    .map(|n| n.id)
                    .collect::<Vec<_>>();

                if active_nodes.is_empty() {
                    error!("Rebalance: DecommissionNode - No active nodes available to migrate shards from ID {:?}.", node_id_to_remove);
                    // Potentially revert status from Leaving if rebalance cannot proceed
                    self.catalog.update_node_status(&node_id_to_remove, NodeStatus::Active).await.unwrap_or_else(|e| {
                        error!("Failed to revert node status for {:?}: {}", node_id_to_remove, e);
                    });
                    return Err(ClusterManagerError::NoTargetNodes);
                }

                for (db_name, table_name, shard_id) in shards_on_decommissioning_node {
                    // Simple strategy: pick the first active node as the target.
                    let target_node_ids = vec![active_nodes[0]];

                    info!(
                        "Rebalance: Selected shard {:?} from table {}.{} on decommissioning node ID {:?} to move to target nodes {:?}",
                        shard_id, db_name, table_name, node_id_to_remove, target_node_ids
                    );

                    self.catalog.begin_shard_migration_out(
                        &db_name,
                        &table_name,
                        &shard_id,
                        target_node_ids.clone(),
                    ).await?;

                    info!(
                        "Conceptual: Initiate data movement for shard {:?} from node ID {:?} to target_nodes {:?}.",
                        shard_id, node_id_to_remove, target_node_ids
                    );
                }
            }
        }
        Ok(())
    }
}
