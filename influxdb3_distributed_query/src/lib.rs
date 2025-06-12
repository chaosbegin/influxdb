pub mod exec;

// Re-export key components for easier use by other crates
pub use exec::{QueryFragment, RemoteScanExec};

// Potentially define common error types for this crate if needed
// pub mod error {
//     use thiserror::Error;
//     use datafusion::common::DataFusionError;

//     #[derive(Debug, Error)]
//     pub enum DistributedQueryError {
//         #[error("DataFusion error: {0}")]
//         DataFusion(#[from] DataFusionError),

//         #[error("Remote execution error on node {node_id}: {message}")]
//         RemoteError { node_id: String, message: String },

//         #[error("gRPC client error: {0}")]
//         GrpcClient(String), // Placeholder for more specific gRPC errors
//     }
// }
// For now, errors can be handled directly with DataFusionError or specific client errors.
// A dedicated error enum can be added later if the complexity grows.
