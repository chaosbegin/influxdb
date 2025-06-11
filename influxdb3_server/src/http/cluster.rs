use std::sync::Arc;
use axum::{extract::State, response::IntoResponse, Json};
use hyper::StatusCode;
use observability_deps::tracing::debug;

use crate::http::HttpApi; // Assuming HttpApi is in crate::http
use influxdb3_catalog::ClusterNodeDefinition; // Make sure this path is correct

// Define a wrapper for the response to ensure consistent JSON structure if needed,
// or directly return Json(Vec<ClusterNodeDefinition>)
// For now, directly returning Json is fine.

pub(crate) async fn handle_list_cluster_nodes(
    State(http_api): State<Arc<HttpApi>>,
) -> impl IntoResponse {
    debug!("Handling list cluster nodes request");

    // The list_cluster_nodes method on Catalog is synchronous.
    // If it were async, we'd need .await here.
    let nodes: Vec<Arc<ClusterNodeDefinition>> = http_api.write_buffer.catalog().list_cluster_nodes();

    // Convert Arc<ClusterNodeDefinition> to ClusterNodeDefinition for serialization
    let response_nodes: Vec<ClusterNodeDefinition> = nodes.iter().map(|arc_node| (**arc_node).clone()).collect();

    (StatusCode::OK, Json(response_nodes))
}

// Potential error handling (if list_cluster_nodes could fail, though currently it doesn't return Result):
// match http_api.write_buffer.catalog().list_cluster_nodes() {
//     Ok(nodes) => {
//         let response_nodes: Vec<ClusterNodeDefinition> = nodes.iter().map(|arc_node| (**arc_node).clone()).collect();
//         (StatusCode::OK, Json(response_nodes))
//     }
//     Err(e) => {
//         error!("Failed to list cluster nodes: {:?}", e);
//         // Convert catalog error to an appropriate HTTP error response
//         (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("Failed to retrieve nodes: {}", e)})))
//     }
// }
