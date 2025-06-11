use async_trait::async_trait;
use iox_time::TimeProvider;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};
use thiserror::Error;

// Represents the status of a node in the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Joining,    // Node is new and initializing, not yet ready for full workload.
    Active,     // Node is healthy and fully participating.
    Leaving,    // Node is gracefully shutting down, shards should be migrated off.
    Down,       // Node is considered offline/unreachable after failed heartbeats.
}

// Information about a node in the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,                     // Unique identifier for the node (e.g., config.node_identifier_prefix)
    pub rpc_address: String,            // Address for internode RPC communication
    // pub http_address: String,        // Address for client HTTP API (optional here if RPC is primary for cluster ops)
    pub status: NodeStatus,
    pub last_heartbeat: Option<i64>,    // Timestamp in nanoseconds
    // pub version: String,             // Software version (optional)
    // pub resources_version: u64,      // Version of resources assigned (optional)
}

#[derive(Debug, Error)]
pub enum ClusterManagerError {
    #[error("Node {0} not found")]
    NodeNotFound(String),
    #[error("Node {0} already exists")]
    NodeAlreadyExists(String),
    #[error("Cannot decommission node {0} because it is in status {1:?}")]
    InvalidNodeStatusForDecommission(String, NodeStatus),
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Trait for managing cluster node membership and status.
#[async_trait]
pub trait ClusterManager: Debug + Send + Sync {
    /// Returns a list of all known nodes in the cluster.
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>, ClusterManagerError>;

    /// Returns information about a specific node.
    async fn get_node(&self, node_id: &str) -> Result<Option<NodeInfo>, ClusterManagerError>;

    /// Called by nodes to report their status and keep their membership active.
    /// If the node is not known, it might be registered, or an error might be returned
    /// depending on the implementation strategy (e.g., if pre-registration is required).
    async fn heartbeat(&self, node_id: &str, rpc_address: &str) -> Result<(), ClusterManagerError>;

    /// Called by an operator/admin (or an automated process) to formally register a new node
    /// that is expected to join the cluster.
    async fn register_node(&self, node_id: &str, rpc_address: &str) -> Result<(), ClusterManagerError>;

    /// Called by an operator/admin to initiate graceful removal of a node from the cluster.
    /// The node's status will be set to `Leaving`. Shard rebalancing should be triggered.
    async fn decommission_node(&self, node_id: &str) -> Result<(), ClusterManagerError>;

    /// Placeholder for initiating a manual cluster-wide shard rebalance.
    async fn initiate_rebalance(&self) -> Result<(), ClusterManagerError>;
}

/// An in-memory implementation of ClusterManager, suitable for single-node or testing.
#[derive(Debug)]
pub struct InMemoryClusterManager {
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl InMemoryClusterManager {
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            time_provider,
        }
    }
}

#[async_trait]
impl ClusterManager for InMemoryClusterManager {
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>, ClusterManagerError> {
        let nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        Ok(nodes_guard.values().cloned().collect())
    }

    async fn get_node(&self, node_id: &str) -> Result<Option<NodeInfo>, ClusterManagerError> {
        let nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        Ok(nodes_guard.get(node_id).cloned())
    }

    async fn heartbeat(&self, node_id: &str, rpc_address: &str) -> Result<(), ClusterManagerError> {
        let mut nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        let now = self.time_provider.now().timestamp_nanos();

        match nodes_guard.get_mut(node_id) {
            Some(node_info) => {
                node_info.last_heartbeat = Some(now);
                node_info.rpc_address = rpc_address.to_string(); // Update address if it changed
                if node_info.status == NodeStatus::Joining || node_info.status == NodeStatus::Down {
                    node_info.status = NodeStatus::Active;
                }
                // If node was 'Leaving', heartbeat might indicate it's back to 'Active' if re-registration is allowed,
                // or it could be an error. For simplicity, let's assume it becomes Active.
                // A more complex system would handle this state transition carefully.
                if node_info.status == NodeStatus::Leaving {
                     observability_deps::tracing::warn!("Node {} sent heartbeat while in Leaving state. Setting to Active.", node_id);
                     node_info.status = NodeStatus::Active;
                }

                Ok(())
            }
            None => {
                // Node not found, register it as Active (alternative: return NodeNotFound error or require pre-registration)
                observability_deps::tracing::info!("Node {} sending heartbeat was not found, registering as Active.", node_id);
                nodes_guard.insert(node_id.to_string(), NodeInfo {
                    id: node_id.to_string(),
                    rpc_address: rpc_address.to_string(),
                    status: NodeStatus::Active,
                    last_heartbeat: Some(now),
                });
                Ok(())
            }
        }
    }

    async fn register_node(&self, node_id: &str, rpc_address: &str) -> Result<(), ClusterManagerError> {
        let mut nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        if nodes_guard.contains_key(node_id) {
            return Err(ClusterManagerError::NodeAlreadyExists(node_id.to_string()));
        }
        let now = self.time_provider.now().timestamp_nanos();
        nodes_guard.insert(node_id.to_string(), NodeInfo {
            id: node_id.to_string(),
            rpc_address: rpc_address.to_string(),
            status: NodeStatus::Joining, // Starts as Joining, heartbeat will make it Active
            last_heartbeat: Some(now),
        });
        Ok(())
    }

    async fn decommission_node(&self, node_id: &str) -> Result<(), ClusterManagerError> {
        let mut nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        match nodes_guard.get_mut(node_id) {
            Some(node_info) => {
                if node_info.status == NodeStatus::Leaving || node_info.status == NodeStatus::Down {
                     return Err(ClusterManagerError::InvalidNodeStatusForDecommission(node_id.to_string(), node_info.status.clone()));
                }
                node_info.status = NodeStatus::Leaving;
                node_info.last_heartbeat = Some(self.time_provider.now().timestamp_nanos());
                Ok(())
            }
            None => Err(ClusterManagerError::NodeNotFound(node_id.to_string())),
        }
    }

    async fn initiate_rebalance(&self) -> Result<(), ClusterManagerError> {
        observability_deps::tracing::info!("(InMemoryClusterManager) Placeholder: Initiate rebalance called.");
        // In a real implementation, this would trigger complex logic:
        // 1. Analyze current shard distribution and node load.
        // 2. Determine optimal shard movements.
        // 3. Call ShardMigrator methods for each identified movement.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iox_time::MockProvider;
    use std::time::Duration;

    #[tokio::test]
    async fn test_in_memory_cluster_manager_registration() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));

        let node_id = "node1";
        let rpc_addr = "127.0.0.1:1234";

        // Register new node
        let res = manager.register_node(node_id, rpc_addr).await;
        assert!(res.is_ok());

        let node_info = manager.get_node(node_id).await.unwrap().unwrap();
        assert_eq!(node_info.id, node_id);
        assert_eq!(node_info.rpc_address, rpc_addr);
        assert_eq!(node_info.status, NodeStatus::Joining);
        assert_eq!(node_info.last_heartbeat, Some(0));

        // Try to register existing node
        let res_exists = manager.register_node(node_id, rpc_addr).await;
        assert!(matches!(res_exists, Err(ClusterManagerError::NodeAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_in_memory_cluster_manager_heartbeat() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));

        let node_id = "node1";
        let rpc_addr1 = "127.0.0.1:1234";
        let rpc_addr2 = "127.0.0.1:5678";

        // Heartbeat for non-existent node (should register it as Active)
        manager.heartbeat(node_id, rpc_addr1).await.unwrap();
        let node_info1 = manager.get_node(node_id).await.unwrap().unwrap();
        assert_eq!(node_info1.status, NodeStatus::Active);
        assert_eq!(node_info1.rpc_address, rpc_addr1);
        assert_eq!(node_info1.last_heartbeat, Some(0));

        // Advance time and send another heartbeat
        time_provider.set(iox_time::Time::from_timestamp_nanos(0).add(Duration::from_secs(10)));
        manager.heartbeat(node_id, rpc_addr2).await.unwrap();
        let node_info2 = manager.get_node(node_id).await.unwrap().unwrap();
        assert_eq!(node_info2.status, NodeStatus::Active); // Stays active
        assert_eq!(node_info2.rpc_address, rpc_addr2); // Address updated
        assert_eq!(node_info2.last_heartbeat, Some(10_000_000_000));

        // Test heartbeat changes status from Joining to Active
        let node_id_joining = "node_joining";
        manager.register_node(node_id_joining, rpc_addr1).await.unwrap();
        let joining_node_info = manager.get_node(node_id_joining).await.unwrap().unwrap();
        assert_eq!(joining_node_info.status, NodeStatus::Joining);

        time_provider.set(iox_time::Time::from_timestamp_nanos(0).add(Duration::from_secs(20)));
        manager.heartbeat(node_id_joining, rpc_addr1).await.unwrap();
        let active_node_info = manager.get_node(node_id_joining).await.unwrap().unwrap();
        assert_eq!(active_node_info.status, NodeStatus::Active);
        assert_eq!(active_node_info.last_heartbeat, Some(20_000_000_000));
    }

    #[tokio::test]
    async fn test_in_memory_cluster_manager_decommission() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));
        let node_id = "node_to_decom";
        let rpc_addr = "127.0.0.1:1234";

        manager.register_node(node_id, rpc_addr).await.unwrap();
        manager.heartbeat(node_id, rpc_addr).await.unwrap(); // Make it Active

        // Decommission
        let res_decom = manager.decommission_node(node_id).await;
        assert!(res_decom.is_ok());
        let node_info = manager.get_node(node_id).await.unwrap().unwrap();
        assert_eq!(node_info.status, NodeStatus::Leaving);

        // Try to decommission already Leaving node
        let res_decom_again = manager.decommission_node(node_id).await;
        assert!(matches!(res_decom_again, Err(ClusterManagerError::InvalidNodeStatusForDecommission(_, NodeStatus::Leaving))));

        // Try to decommission non-existent node
        let res_decom_non_existent = manager.decommission_node("node_does_not_exist").await;
        assert!(matches!(res_decom_non_existent, Err(ClusterManagerError::NodeNotFound(_))));
    }

    #[tokio::test]
    async fn test_in_memory_cluster_manager_list_nodes() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));

        assert!(manager.list_nodes().await.unwrap().is_empty());

        manager.register_node("nodeA", "addrA").await.unwrap();
        manager.register_node("nodeB", "addrB").await.unwrap();

        let nodes = manager.list_nodes().await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().any(|n| n.id == "nodeA"));
        assert!(nodes.iter().any(|n| n.id == "nodeB"));
    }
}
