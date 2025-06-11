use std::sync::Arc;
use axum::{extract::State, response::IntoResponse, Json};
use hyper::StatusCode;
use observability_deps::tracing::debug;

use crate::http::HttpApi; // Assuming HttpApi is in crate::http
use influxdb3_catalog::ClusterNodeDefinition; // Make sure this path is correct

// Define a wrapper for the response to ensure consistent JSON structure if needed,
// or directly return Json(Vec<ClusterNodeDefinition>)
// For now, directly returning Json is fine.

use influxdb3_catalog::{CatalogError, catalog::Catalog};
use influxdb3_id::{NodeId, ShardId};
use serde::Deserialize;
use axum::extract::Path;
use serde_json::json;
use influxdb3_cluster_manager::rebalance::{initiate_shard_move_conceptual, RebalanceError};


#[derive(Debug, Deserialize)]
pub(crate) struct UpdateNodeStatusPayload {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InitiateShardMovePayload {
    pub db_name: String,
    pub table_name: String,
    pub shard_id: ShardId, // Assumes ShardId derives Deserialize (from u64)
    pub target_node_id: NodeId, // Assumes NodeId derives Deserialize (from u64)
}


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
    payload: ClusterNodeDefinition, // Changed from axum::Json(payload)
) -> impl IntoResponse {
    debug!(?payload, "Handling add cluster node request");
    match http_api.catalog().add_cluster_node(payload).await {
        Ok(_) => (StatusCode::CREATED, Json(json!({"status": "created"}))),
        Err(CatalogError::AlreadyExists) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Node already exists"})),
        ),
        Err(e) => {
            observability_deps::tracing::error!("Failed to add cluster node: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to add node: {}", e)})),
            )
        }
    }
}

fn parse_node_id(node_id_str: &str) -> Result<NodeId, (StatusCode, Json<serde_json::Value>)> {
    node_id_str.parse::<u64>().map(NodeId::new).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Invalid node_id format: {}", node_id_str)})),
        )
    })
}

pub(crate) async fn handle_remove_cluster_node(
    State(http_api): State<Arc<HttpApi>>,
    Path(node_id_str): Path<String>,
) -> impl IntoResponse {
    debug!(%node_id_str, "Handling remove cluster node request");
    let node_id = match parse_node_id(&node_id_str) {
        Ok(id) => id,
        Err(response) => return response,
    };

    match http_api.catalog().remove_cluster_node(node_id).await {
        Ok(_) => (StatusCode::NO_CONTENT, Json(json!({}))), // NO_CONTENT implies empty body
        Err(CatalogError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Node_id {} not found", node_id_str)})),
        ),
        Err(e) => {
            observability_deps::tracing::error!("Failed to remove cluster node: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to remove node {}: {}", node_id_str, e)})),
            )
        }
    }
}

pub(crate) async fn handle_update_node_status(
    State(http_api): State<Arc<HttpApi>>,
    Path(node_id_str): Path<String>, // Path extractor is fine for the dispatcher to prepare
    payload: UpdateNodeStatusPayload, // Changed from axum::Json(payload)
) -> impl IntoResponse {
    debug!(%node_id_str, ?payload, "Handling update node status request");
    let node_id = match parse_node_id(&node_id_str) {
        Ok(id) => id,
        Err(response) => return response,
    };

    match http_api.catalog().update_cluster_node_status(node_id, payload.status).await {
        Ok(_) => (StatusCode::OK, Json(json!({"status": "updated"}))),
        Err(CatalogError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Node_id {} not found", node_id_str)})),
        ),
        Err(e) => {
            observability_deps::tracing::error!("Failed to update node status: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to update node {} status: {}", node_id_str, e)})),
            )
        }
    }
}

pub(crate) async fn handle_initiate_shard_move(
    State(http_api): State<Arc<HttpApi>>,
    payload: InitiateShardMovePayload, // Changed from axum::Json(payload)
) -> impl IntoResponse {
    debug!(?payload, "Handling initiate shard move request");
    let catalog: Arc<Catalog> = http_api.catalog();

    match initiate_shard_move_conceptual(
        catalog,
        &payload.db_name,
        &payload.table_name,
        payload.shard_id,
        payload.target_node_id,
    )
    .await
    {
        Ok(_) => (StatusCode::ACCEPTED, Json(json!({"status": "shard move initiated"}))),
        Err(RebalanceError::DbNotFound(db_name)) => (
            StatusCode::NOT_FOUND, // Or BAD_REQUEST as it's part of payload
            Json(json!({"error": format!("Database '{}' not found", db_name)})),
        ),
        Err(RebalanceError::TableNotFound { db_name, table_name }) => (
            StatusCode::NOT_FOUND, // Or BAD_REQUEST
            Json(json!({"error": format!("Table '{}.{}' not found", db_name, table_name)})),
        ),
        Err(RebalanceError::ShardNotFound { shard_id, db_name, table_name }) => (
            StatusCode::NOT_FOUND, // Or BAD_REQUEST
            Json(json!({"error": format!("Shard {:?} in table '{}.{}' not found", shard_id, db_name, table_name)})),
        ),
        Err(RebalanceError::CatalogError(e)) => {
            observability_deps::tracing::error!("Catalog error during shard move initiation: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Catalog error: {}", e)})),
            )
        }
        // Other RebalanceErrors could be mapped here if they exist
    }
}
