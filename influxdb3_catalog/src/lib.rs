pub mod catalog;
pub mod channel;
pub mod error;
pub mod id;
pub mod log;
pub mod object_store;
pub mod resource;
pub mod serialize;
pub mod snapshot;
pub mod shard;
pub mod replication;
pub mod cluster_node; // Added module

pub use error::CatalogError;
pub use cluster_node::ClusterNodeDefinition; // Added use
pub(crate) type Result<T, E = CatalogError> = std::result::Result<T, E>;
