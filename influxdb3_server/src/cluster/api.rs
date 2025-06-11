use std::sync::Arc;
use crate::cluster::manager::{ClusterManager, ClusterManagerError, NodeInfo};
// Assuming NodeStatus for register_new_node is from influxdb3_catalog::node
use influxdb3_catalog::node::NodeStatus as CatalogNodeStatus;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::node::NodeDefinition as CatalogNodeDefinition; // Explicitly using as CatalogNodeDefinition
use influxdb3_catalog::shard::{ShardId, ShardMigrationStatus};
use influxdb3_id::NodeId as ComputeNodeId; // For target_node_id in move_shard_manually
use thiserror::Error;
use observability_deps::tracing::info; // For logging

// ShardMigrator might be needed if functions directly interact with it,
// but for now, the conceptual `move_shard_manually` only interacts with Catalog.
// use crate::cluster::migration::ShardMigrator;

#[derive(Debug, Error)]
pub enum ManagementApiError {
    #[error("Cluster manager error: {0}")]
    ClusterManager(#[from] ClusterManagerError),
    #[error("Catalog error: {0}")]
    Catalog(#[from] influxdb3_catalog::CatalogError),
    #[error("Node {0} not found")]
    NodeNotFound(String),
    #[error("Shard {shard_id} not found for table {db_name}.{table_name}")]
    ShardNotFound{ shard_id: ShardId, db_name: String, table_name: String },
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Operation not implemented: {0}")]
    NotImplemented(String),
}
pub type Result<T, E = ManagementApiError> = std::result::Result<T, E>;

// --- Node Management ---

pub async fn register_new_node(
    cluster_manager: Arc<dyn ClusterManager>,
    catalog: Arc<Catalog>,
    node_id: String,
    rpc_address: String,
) -> Result<()> {
    cluster_manager.register_node(&node_id, &rpc_address).await?;
    let node_definition = CatalogNodeDefinition {
        id: node_id.clone(), // This is NodeIdentifier (String)
        rpc_address,
        status: CatalogNodeStatus::Joining, // from influxdb3_catalog::node
        last_heartbeat: None,
    };
    catalog.register_cluster_node_meta(node_definition).await?;
    info!("Node {} registered with address {}.", node_id, cluster_manager.get_node(&node_id).await?.map_or_else(|| "N/A".to_string(), |n| n.map_or_else(|| "N/A".to_string(), |ni| ni.rpc_address)) );
    Ok(())
}

pub async fn decommission_cluster_node(
    cluster_manager: Arc<dyn ClusterManager>,
    catalog: Arc<Catalog>,
    node_id: String,
) -> Result<()> {
    cluster_manager.decommission_node(&node_id).await?;
    catalog.decommission_cluster_node(&node_id).await?;
    info!("Node {} decommissioned.", node_id);
    Ok(())
}

pub async fn get_cluster_node_info(
    cluster_manager: Arc<dyn ClusterManager>,
    node_id: String,
) -> Result<NodeInfo> {
    cluster_manager
        .get_node(&node_id)
        .await?
        .ok_or_else(|| ManagementApiError::NodeNotFound(node_id.clone()))
}

pub async fn list_cluster_nodes(
    cluster_manager: Arc<dyn ClusterManager>,
) -> Result<Vec<NodeInfo>> {
    Ok(cluster_manager.list_nodes().await?)
}

// --- Cluster Status ---

pub async fn get_cluster_status(
    cluster_manager: Arc<dyn ClusterManager>,
    _catalog: Arc<Catalog>, // Catalog might be used for more detailed status later
) -> Result<String> {
    let nodes = cluster_manager.list_nodes().await?;
    let status_report = if nodes.is_empty() {
        "Cluster status: No nodes registered.".to_string()
    } else {
        let mut report = format!("Cluster status: {} nodes registered.\n", nodes.len());
        for node in nodes {
            report.push_str(&format!(
                "  - Node ID: {}, Address: {}, Status: {:?}, Last Heartbeat: {}\n",
                node.id,
                node.rpc_address,
                node.status, // This is crate::cluster::manager::NodeStatus
                node.last_heartbeat.map_or_else(|| "N/A".to_string(), |ts| ts.to_string())
            ));
        }
        report
    };
    info!("Cluster status requested.");
    Ok(status_report)
}

// --- Rebalancing (Conceptual) ---

pub async fn trigger_manual_rebalance(
    cluster_manager: Arc<dyn ClusterManager>,
) -> Result<()> {
    cluster_manager.initiate_rebalance().await?;
    info!("Manual rebalance triggered via API.");
    Ok(())
}

// --- Shard Management (Conceptual) ---

pub async fn move_shard_manually(
    catalog: Arc<Catalog>,
    shard_id: ShardId,
    db_name: String,
    table_name: String,
    target_compute_node_id: ComputeNodeId, // Expecting ComputeNodeId for target
) -> Result<()> {
    info!("API request to move shard {} of table {}.{} to compute node {}.",
        shard_id.get(), db_name, table_name, target_compute_node_id.get());

    // Check if shard exists (optional, update_shard_migration_state will also check)
    let db_schema = catalog.db_schema(&db_name)
        .ok_or_else(|| ManagementApiError::Catalog(influxdb3_catalog::CatalogError::DatabaseNotFound(db_name.clone())))?;
    let table_def = db_schema.table_definition(&table_name)
        .ok_or_else(|| ManagementApiError::Catalog(influxdb3_catalog::CatalogError::TableNotFound{db_name: Arc::from(db_name.clone()), table_name: Arc::from(table_name.clone())}))?;
    let _shard_def = table_def.shards.get_by_id(&shard_id)
        .ok_or_else(|| ManagementApiError::ShardNotFound{ shard_id, db_name: db_name.clone(), table_name: table_name.clone() })?;

    // Update shard migration status in catalog
    // Note: The catalog's update_shard_migration_state expects Vec<ComputeNodeId> for target_nodes.
    // The current function signature takes a single String for target_node_id, which is ambiguous.
    // Assuming target_node_id refers to a Compute Node's ID (u64) as per ShardDefinition.
    catalog.update_shard_migration_state(
        &db_name,
        &table_name,
        shard_id,
        Some(ShardMigrationStatus::Preparing),
        Some(vec![target_compute_node_id]), // Pass as Vec<ComputeNodeId>
        None, // source_nodes - not specified in this conceptual API call, could be derived later
    ).await?;

    info!("Shard {} migration status updated to Preparing, target compute node: {}.", shard_id.get(), target_compute_node_id.get());

    // As this is conceptual, the actual migration process isn't triggered here.
    Err(ManagementApiError::NotImplemented(
        "Conceptual shard move initiated; actual data migration process not implemented by this API call.".to_string()
    ))
}
