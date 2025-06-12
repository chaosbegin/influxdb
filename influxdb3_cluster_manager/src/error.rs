use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterManagerError {
    #[error("Catalog error: {0}")]
    CatalogError(#[from] influxdb3_catalog::CatalogError),

    #[error("Rebalance operation error: {0}")]
    RebalanceError(#[from] crate::rebalance::RebalanceError),

    #[error("Node {node_id} not found in catalog")]
    NodeNotFoundInCatalog { node_id: influxdb3_id::NodeId },

    // Can add more specific errors as the manager evolves
    #[error("Internal cluster manager error: {0}")]
    Internal(String),
}
