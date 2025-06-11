pub mod rebalance;
pub mod error;
pub mod membership; // Added membership module

pub use rebalance::initiate_shard_move_conceptual;
pub use rebalance::RebalanceError; // Assuming RebalanceError is still relevant from its own module
pub use error::ClusterManagerError;
pub use membership::StaticClusterMembership; // Added use
