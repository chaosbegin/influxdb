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

use crate::cluster::migration::{ShardMigrator, MigrationProgress}; // Added ShardMigrator and MigrationProgress

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
    catalog: Arc<Catalog>,
) -> Result<String> {
    info!("Cluster status requested.");
    let nodes = cluster_manager.list_nodes().await?;
    let mut report = if nodes.is_empty() {
        "Cluster status: No nodes registered.\n".to_string()
    } else {
        let mut r = format!("Cluster status: {} nodes registered.\n", nodes.len());
        for node in nodes {
            r.push_str(&format!(
                "  - Node ID: {}, Address: {}, Status: {:?}, Last Heartbeat: {}\n",
                node.id,
                node.rpc_address,
                node.status,
                node.last_heartbeat.map_or_else(|| "N/A".to_string(), |ts| ts.to_string())
            ));
        }
        r
    };

    report.push_str("\nShard Distribution:\n");
    let db_schemas = catalog.list_db_schema();
    if db_schemas.is_empty() {
        report.push_str("  No databases found.\n");
    } else {
        for db_schema in db_schemas {
            if db_schema.name().as_ref() == "_internal" { continue; } // Skip internal db for user-facing status
            report.push_str(&format!("  Database: {}\n", db_schema.name()));
            let tables = db_schema.tables();
            let table_vec: Vec<_> = tables.collect();

            if table_vec.is_empty() {
                report.push_str("    No tables found in this database.\n");
            } else {
                for table_def in table_vec {
                    report.push_str(&format!("    Table: {}\n", table_def.table_name));
                    if table_def.shards.is_empty() {
                        report.push_str("      No shards defined for this table.\n");
                    } else {
                        for shard_def_arc in table_def.shards.resource_iter() {
                            let node_ids_str: Vec<String> = shard_def_arc.node_ids.iter().map(|id| id.get().to_string()).collect();
                            report.push_str(&format!(
                                "      - Shard ID: {}, Nodes: [{}], Version: {}, Status: {:?}, Target: {:?}, Source: {:?}\n",
                                shard_def_arc.id.get(),
                                node_ids_str.join(", "),
                                shard_def_arc.version,
                                shard_def_arc.migration_status.as_ref().unwrap_or(&ShardMigrationStatus::Stable),
                                shard_def_arc.migration_target_node_ids.as_ref().map(|ids| ids.iter().map(|id| id.get().to_string()).collect::<Vec<_>>().join(", ")).unwrap_or_else(|| "N/A".to_string()),
                                shard_def_arc.migration_source_node_ids.as_ref().map(|ids| ids.iter().map(|id| id.get().to_string()).collect::<Vec<_>>().join(", ")).unwrap_or_else(|| "N/A".to_string())
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(report)
}

// --- Rebalancing (Conceptual) ---

pub async fn trigger_manual_rebalance(
    cluster_manager: Arc<dyn ClusterManager>,
) -> Result<()> {
    cluster_manager.initiate_rebalance().await?;
    info!("Manual rebalance triggered via API.");
    Ok(())
}

// --- Shard Management ---

pub async fn move_shard_manually(
    shard_migrator: Arc<dyn ShardMigrator>,
    catalog: Arc<Catalog>, // Keep catalog for initial validation if needed, or remove if ShardMigrator handles all
    db_name: String,
    table_name: String,
    shard_id: ShardId,
    source_node_id: String,
    target_node_id: String,
) -> Result<()> {
    info!("API request to move shard {} of table {}.{} from source {} to target node {}.",
        shard_id.get(), db_name, table_name, source_node_id, target_node_id);

    if source_node_id == target_node_id {
        return Err(ManagementApiError::InvalidArgument("Source and target node IDs cannot be the same.".to_string()));
    }

    // Optional: Validate nodes exist via catalog or cluster_manager (if passed)
    // This check might also be done within ShardMigrator::prepare_migration
    if catalog.get_cluster_node_meta(&source_node_id).is_none() {
        return Err(ManagementApiError::NodeNotFound(source_node_id));
    }
    if catalog.get_cluster_node_meta(&target_node_id).is_none() {
        return Err(ManagementApiError::NodeNotFound(target_node_id));
    }

    // Check if shard exists (ShardMigrator might also do this)
    let db_schema = catalog.db_schema(&db_name)
        .ok_or_else(|| ManagementApiError::Catalog(influxdb3_catalog::CatalogError::DatabaseNotFound(db_name.clone())))?;
    let table_def = db_schema.table_definition(&table_name)
        .ok_or_else(|| ManagementApiError::Catalog(influxdb3_catalog::CatalogError::TableNotFound{db_name: Arc::from(db_name.clone()), table_name: Arc::from(table_name.clone())}))?;
    let _shard_def = table_def.shards.get_by_id(&shard_id)
        .ok_or_else(|| ManagementApiError::ShardNotFound{ shard_id, db_name: db_name.clone(), table_name: table_name.clone() })?;


    shard_migrator.prepare_migration(&db_name, &table_name, shard_id, &source_node_id, &target_node_id).await?;

    info!("Shard {} migration preparation initiated from source {} to target {}.", shard_id.get(), source_node_id, target_node_id);
    Ok(())
}

pub async fn advance_shard_migration_step(
    shard_migrator: Arc<dyn ShardMigrator>,
    shard_id: ShardId,
) -> Result<MigrationProgress, ManagementApiError> {
    info!("API request to advance migration step for shard {}.", shard_id.get());
    Ok(shard_migrator.transfer_data_step(shard_id).await?)
}

pub async fn finalize_shard_migration_api(
    shard_migrator: Arc<dyn ShardMigrator>,
    db_name: String,
    table_name: String,
    shard_id: ShardId,
) -> Result<(), ManagementApiError> {
    info!("API request to finalize migration for shard {} of table {}.{}.", shard_id.get(), db_name, table_name);
    Ok(shard_migrator.finalize_migration(&db_name, &table_name, shard_id).await?)
}

pub async fn rollback_shard_migration_api(
    shard_migrator: Arc<dyn ShardMigrator>,
    db_name: String,
    table_name: String,
    shard_id: ShardId,
    reason: String,
) -> Result<(), ManagementApiError> {
    info!("API request to rollback migration for shard {} of table {}.{} due to: {}", shard_id.get(), db_name, table_name, reason);
    Ok(shard_migrator.rollback_migration(&db_name, &table_name, shard_id, &reason).await?)
}

pub async fn get_shard_migration_status_api(
    shard_migrator: Arc<dyn ShardMigrator>,
    shard_id: ShardId,
) -> Result<Option<MigrationProgress>, ManagementApiError> {
    info!("API request to get migration status for shard {}.", shard_id.get());
    Ok(shard_migrator.get_migration_status(shard_id).await?)
}
