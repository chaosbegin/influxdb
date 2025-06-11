use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterManagerError {
    #[error("Catalog error: {0}")]
    CatalogError(#[from] influxdb3_catalog::CatalogError),
    // Add other specific errors as needed in the future
    // e.g., #[error("Node communication error: {0}")] NodeCommunication(String),
}
