use std::sync::Arc;
use axum::{extract::{Path, State}, response::IntoResponse, Json};
use hyper::StatusCode;
use observability_deps::tracing::{debug, error};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::http::HttpApi;
use influxdb3_catalog::{
    catalog::Catalog, // For Arc<Catalog> type hint
    ClusterNodeDefinition,
    CatalogError
};
use influxdb3_id::{NodeId, ShardId};
use influxdb3_cluster_manager::rebalance::{
    initiate_shard_move_conceptual,
    RebalanceError,
};

// --- Payload Structs ---

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateNodeStatusPayload {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InitiateShardMovePayload {
    pub db_name: String,
    pub table_name: String,
    pub shard_id_str: String,
    pub target_node_id_str: String,
}

// --- Helper Functions ---

fn parse_id_str<T>(
    id_str: &str,
    id_name: &str, // For error messages, e.g., "node_id" or "shard_id"
    constructor: impl Fn(u64) -> T,
) -> Result<T, (StatusCode, Json<Value>)> {
    id_str.parse::<u64>().map(constructor).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Invalid {} format '{}': {}", id_name, id_str, e)})),
        )
    })
}

fn map_catalog_error(err: CatalogError) -> (StatusCode, Json<Value>) {
    match err {
        CatalogError::AlreadyExists => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Resource already exists"})),
        ),
        CatalogError::NotFound | CatalogError::DbNotFound(_) | CatalogError::TableNotFound { .. } | CatalogError::ShardNotFound { .. } => ( // Group more NotFound types
            StatusCode::NOT_FOUND,
            Json(json!({"error": err.to_string()})),
        ),
        // Add other specific CatalogError mappings as needed
        _ => {
            error!("Internal catalog error: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Internal server error: {}", err)})),
            )
        }
    }
}

fn map_rebalance_error(err: RebalanceError) -> (StatusCode, Json<Value>) {
    match err {
        RebalanceError::DbNotFound(db_name) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Database '{}' not found", db_name)})),
        ),
        RebalanceError::TableNotFound { db_name, table_name } => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Table '{}.{}' not found", db_name, table_name)})),
        ),
        RebalanceError::ShardNotFound { shard_id, db_name, table_name } => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Shard {:?} in table '{}.{}' not found", shard_id, db_name, table_name)})),
        ),
        RebalanceError::CatalogError(catalog_err) => map_catalog_error(catalog_err),
        // Add other specific RebalanceError mappings as needed
    }
}


// --- Route Handlers ---

pub(crate) async fn handle_list_cluster_nodes(
    State(http_api): State<Arc<HttpApi>>,
) -> impl IntoResponse {
    debug!("Handling list cluster nodes request");
    let nodes: Vec<Arc<ClusterNodeDefinition>> = http_api.catalog().list_cluster_nodes();
    let response_nodes: Vec<ClusterNodeDefinition> = nodes.iter().map(|arc_node| (**arc_node).clone()).collect();
    (StatusCode::OK, Json(response_nodes))
}

pub(crate) async fn handle_add_cluster_node(
    State(http_api): State<Arc<HttpApi>>,
    payload: ClusterNodeDefinition, // Already deserialized by http.rs dispatcher
) -> impl IntoResponse {
    debug!(?payload, "Handling add cluster node request");
    match http_api.catalog().add_cluster_node(payload.clone()).await { // Clone payload if needed by response
        Ok(op_batch) => {
            // Assuming add_cluster_node now returns the created node or its ID in OrderedCatalogBatch details
            // For now, just return the input payload on success as a placeholder for the actual created resource
            (StatusCode::CREATED, Json(payload))
        }
        Err(e) => map_catalog_error(e),
    }
}

pub(crate) async fn handle_remove_cluster_node(
    State(http_api): State<Arc<HttpApi>>,
    Path(node_id_str): Path<String>, // Path string provided by http.rs dispatcher
) -> impl IntoResponse {
    debug!(%node_id_str, "Handling remove cluster node request");
    let node_id = match parse_id_str(&node_id_str, "node_id", NodeId::new) {
        Ok(id) => id,
        Err(response) => return response,
    };

    match http_api.catalog().remove_cluster_node(node_id).await {
        Ok(_) => (StatusCode::NO_CONTENT, Json(json!({}))),
        Err(e) => map_catalog_error(e),
    }
}

pub(crate) async fn handle_update_node_status(
    State(http_api): State<Arc<HttpApi>>,
    Path(node_id_str): Path<String>, // Path string provided by http.rs dispatcher
    payload: UpdateNodeStatusPayload,    // Already deserialized by http.rs dispatcher
) -> impl IntoResponse {
    debug!(%node_id_str, ?payload, "Handling update node status request");
    let node_id = match parse_id_str(&node_id_str, "node_id", NodeId::new) {
        Ok(id) => id,
        Err(response) => return response,
    };

    match http_api.catalog().update_cluster_node_status(node_id, payload.status).await {
        Ok(op_batch) => {
            // Optionally, return the updated node definition or a success message
            // For now, just OK with a simple status message
            (StatusCode::OK, Json(json!({"status": "updated"})))
        }
        Err(e) => map_catalog_error(e),
    }
}

pub(crate) async fn handle_initiate_shard_move(
    State(http_api): State<Arc<HttpApi>>,
    payload: InitiateShardMovePayload, // Already deserialized by http.rs dispatcher
) -> impl IntoResponse {
    debug!(?payload, "Handling initiate shard move request");

    let shard_id = match parse_id_str(&payload.shard_id_str, "shard_id", ShardId::new) {
        Ok(id) => id,
        Err(response) => return response,
    };
    let target_node_id = match parse_id_str(&payload.target_node_id_str, "target_node_id", NodeId::new) {
        Ok(id) => id,
        Err(response) => return response,
    };

    let catalog_arc: Arc<Catalog> = http_api.catalog();

    match initiate_shard_move_conceptual(
        catalog_arc,
        &payload.db_name,
        &payload.table_name,
        shard_id,
        target_node_id,
    ).await {
        Ok(_) => (StatusCode::ACCEPTED, Json(json!({"status": "shard move initiated"}))),
        Err(e) => map_rebalance_error(e),
    }
}
