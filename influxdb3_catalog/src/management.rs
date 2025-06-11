//! Types and traits for cluster management interfaces.

// Re-export Node related types from the main catalog module where they are now defined/updated.
// These types are made public here for external use by management services.
pub use crate::catalog::{NodeDefinition, NodeStatus};

// Re-export Shard related types from the shard module.
pub use crate::shard::{ShardDefinition, ShardMigrationStatus};

// Re-export ID types from influxdb3_id for convenience, as they are used in the definitions above.
pub use influxdb3_id::{NodeId, ShardId};

// Placeholder for any future management-specific error types or shared traits.
// For example:
//
// use thiserror::Error;
//
// #[derive(Error, Debug)]
// pub enum ManagementError {
//     #[error("Node {node_id} not found")]
//     NodeNotFound { node_id: NodeId },
//
//     #[error("Shard {shard_id} not found in table {table_name} in db {db_name}")]
//     ShardNotFound { db_name: String, table_name: String, shard_id: ShardId },
//
//     #[error("Operation failed: {reason}")]
//     OperationFailed { reason: String },
//
//     #[error(transparent)]
//     CatalogError(#[from] crate::Error), // To wrap underlying catalog errors
// }
//
// pub type Result<T, E = ManagementError> = std::result::Result<T, E>;
