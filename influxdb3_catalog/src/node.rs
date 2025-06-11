use serde::{Deserialize, Serialize};
use std::sync::Arc;
use influxdb3_id::NodeId as ExternalNodeId; // User-facing node identifier from config

// Internal ID for catalog's repository, distinct from user-provided NodeId if necessary.
// For now, we'll use String (user-provided ID) as the key in the repository.
// If a distinct numeric ID is needed for the repo, a newtype around u64 could be used here.
// pub type ClusterNodeRepoId = String; // Example if we use String as key

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Joining,    // Node is new and initializing.
    Active,     // Node is healthy and fully participating.
    Leaving,    // Node is gracefully shutting down.
    Down,       // Node is considered offline/unreachable.
    Unknown,    // Status cannot be determined.
}

impl Default for NodeStatus {
    fn default() -> Self {
        NodeStatus::Unknown
    }
}

// Definition of a node within the cluster, as stored in the catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDefinition {
    /// Unique identifier for the node (e.g., from config `node_identifier_prefix`).
    /// This will be used as the key in the `Repository`.
    pub id: String,
    /// Address for internode RPC communication (e.g., replication, distributed query).
    pub rpc_address: String,
    /// Current status of the node.
    pub status: NodeStatus,
    /// Last heartbeat timestamp in nanoseconds.
    pub last_heartbeat: Option<i64>,
    /// Catalog's internal, numerical ID for this node, if different from `id`.
    /// For Repository<InternalNodeId, NodeDefinition>.
    /// If `id` (String) is the key, this might not be needed or could be `ExternalNodeId`.
    /// Let's assume for now that the `Repository` in `InnerCatalog` for nodes
    /// will be keyed by `String` (the `id` field here).
    // pub internal_catalog_node_id: SomeOtherIdType,
}

impl NodeDefinition {
    pub fn new(id: String, rpc_address: String, status: NodeStatus, last_heartbeat: Option<i64>) -> Self {
        Self {
            id,
            rpc_address,
            status,
            last_heartbeat,
        }
    }
}

// This is for the CatalogResource trait if NodeDefinition needs to be managed by the generic Repository
use crate::resource::CatalogResource;
use influxdb3_id::CatalogId; // For a generic ID type if needed

// If we decide to use a newtype for Node IDs within the catalog repository:
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct CatNodeId(u64); // Internal Catalog Node ID

impl CatNodeId {
    pub fn new(id: u64) -> Self { Self(id) }
    pub fn get(&self) -> u64 { self.0 }
}

impl CatalogId for CatNodeId {
    fn default() -> Self { CatNodeId(0) }
    fn next(&self) -> Self { CatNodeId(self.0 + 1) }
    fn DUMMY() -> Self { CatNodeId(u64::MAX) } // Example dummy
}


// If NodeDefinition is stored in Repository<String, NodeDefinition>
// then it needs to implement CatalogResource with String as its Identifier type.
// However, CatalogResource expects Identifier: CatalogId. String doesn't implement CatalogId.
// This means we either:
// 1. Don't use the generic `Repository` for nodes if keyed by String.
// 2. Create a newtype for String that implements CatalogId (messy).
// 3. Use an internal numeric ID (like CatNodeId) as the key in Repository, and NodeDefinition.id is the user-string.
// Option 3 is cleaner for Repository. NodeDefinition itself doesn't need to implement CatalogResource.
// The resource stored would be NodeDefinition, keyed by CatNodeId.
// NodeDefinition would still contain the user-visible `id: String`.

// For now, `InnerCatalog` will use `Repository<CatNodeId, NodeDefinition>`.
// `NodeDefinition` itself doesn't need to implement `CatalogResource`.
// The mapping from `node_id_string` to `CatNodeId` will be managed by the BiHashMap in Repository.
// So, `NodeDefinition.id` is the string name, and `CatNodeId` is the internal key.
// The `NodeId` type alias from the prompt refers to the string ID.
pub type NodeIdentifier = String;
