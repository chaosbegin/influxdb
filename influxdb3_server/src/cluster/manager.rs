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

    pub fn check_heartbeats(&self, timeout_duration: Duration) -> Result<Vec<String>, ClusterManagerError> {
        let mut nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        let now = self.time_provider.now();
        let mut affected_node_ids = Vec::new();

        for (node_id, node_info) in nodes_guard.iter_mut() {
            if node_info.status == NodeStatus::Active {
                if let Some(last_heartbeat_ns) = node_info.last_heartbeat {
                    let last_heartbeat_time = iox_time::Time::from_timestamp_nanos(last_heartbeat_ns);
                    if now > last_heartbeat_time + timeout_duration {
                        observability_deps::tracing::warn!(
                            node_id = %node_info.id,
                            last_heartbeat = ?last_heartbeat_time,
                            timeout = ?timeout_duration,
                            "Node heartbeat timed out. Marking as Down."
                        );
                        node_info.status = NodeStatus::Down;
                        affected_node_ids.push(node_id.clone());
                    }
                } else {
                    // Node is Active but has no last_heartbeat timestamp, which is unusual.
                    // Consider it timed out if it's been active longer than the timeout without a heartbeat.
                    // This might depend on how nodes become Active (e.g., if registration sets a heartbeat).
                    // For now, let's assume if it's Active and last_heartbeat is None, it's an issue.
                    // A more robust check might involve node registration time.
                    observability_deps::tracing::warn!(
                        node_id = %node_info.id,
                        "Node is Active but has no last_heartbeat recorded. Marking as Down due to timeout check."
                    );
                    node_info.status = NodeStatus::Down;
                    affected_node_ids.push(node_id.clone());
                }
            }
        }
        Ok(affected_node_ids)
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
        let nodes_guard = self.nodes.lock().map_err(|e| ClusterManagerError::Internal(format!("Failed to lock nodes map: {}", e)))?;
        let active_nodes: Vec<NodeInfo> = nodes_guard.values().filter(|n| n.status == NodeStatus::Active).cloned().collect();

        observability_deps::tracing::info!(
            num_active_nodes = active_nodes.len(),
            "Initiating rebalance. Conceptual plan: Identify overloaded/underloaded active nodes and plan shard movements."
        );
        if active_nodes.is_empty() {
            observability_deps::tracing::info!("No active nodes available to perform rebalance.");
            return Ok(());
        }
        // Conceptual:
        // 1. Fetch shard distribution from Catalog (not available here directly).
        // 2. For each active_node, determine its current load (e.g., number of primary shards).
        //    Example: active_nodes.iter().map(|n| (n.id.clone(), /* hypothetical_shard_count_from_catalog */ 0)).collect();
        // 3. Log this conceptual information.
        let node_load_info: Vec<_> = active_nodes.iter().map(|n| format!("Node[{} Status:{:?}]", n.id, n.status)).collect();
        observability_deps::tracing::info!("Current active nodes: {:?}", node_load_info);
        observability_deps::tracing::info!("(InMemoryClusterManager) Placeholder: Actual rebalancing logic not implemented. This would involve shard migration planning.");
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

    #[tokio::test]
    async fn test_check_heartbeats() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));

        // Node 1: Will be active and recent
        manager.register_node("node1", "addr1").await.unwrap();
        manager.heartbeat("node1", "addr1").await.unwrap(); // Becomes Active, hb at 0

        // Node 2: Will be active and stale
        manager.register_node("node2", "addr2").await.unwrap();
        manager.heartbeat("node2", "addr2").await.unwrap(); // Becomes Active, hb at 0

        // Node 3: Will be Joining, should not be affected by heartbeat check
        manager.register_node("node3", "addr3").await.unwrap();

        // Node 4: Active but no heartbeat (edge case)
        manager.register_node("node4", "addr4").await.unwrap();
        {
            let mut nodes_guard = manager.nodes.lock().unwrap();
            let node4_info = nodes_guard.get_mut("node4").unwrap();
            node4_info.status = NodeStatus::Active;
            node4_info.last_heartbeat = None; // Explicitly None
        }


        // Advance time: node1 will send a new heartbeat, node2 will become stale
        time_provider.set(iox_time::Time::from_timestamp_nanos(0).add(Duration::from_secs(30)));
        manager.heartbeat("node1", "addr1").await.unwrap(); // node1 hb at 30s

        // Check heartbeats with a timeout of 15 seconds
        // - node1: hb at 30s, now is 30s. 30s is not > 30s-15s (15s). Stays Active.
        // - node2: hb at 0s, now is 30s. 0s is older than 30s-15s (15s). Becomes Down.
        // - node3: Is Joining. Unchanged.
        // - node4: Is Active, no heartbeat. Becomes Down.
        let timeout_duration = Duration::from_secs(15);
        let affected_nodes = manager.check_heartbeats(timeout_duration).unwrap();

        assert_eq!(affected_nodes.len(), 2, "Two nodes should have been marked Down");
        assert!(affected_nodes.contains(&"node2".to_string()));
        assert!(affected_nodes.contains(&"node4".to_string()));

        let node1_info = manager.get_node("node1").await.unwrap().unwrap();
        assert_eq!(node1_info.status, NodeStatus::Active);
        assert_eq!(node1_info.last_heartbeat, Some(30_000_000_000));

        let node2_info = manager.get_node("node2").await.unwrap().unwrap();
        assert_eq!(node2_info.status, NodeStatus::Down);
        assert_eq!(node2_info.last_heartbeat, Some(0)); // Last known heartbeat

        let node3_info = manager.get_node("node3").await.unwrap().unwrap();
        assert_eq!(node3_info.status, NodeStatus::Joining); // Unchanged

        let node4_info = manager.get_node("node4").await.unwrap().unwrap();
        assert_eq!(node4_info.status, NodeStatus::Down); // Marked down
    }

    #[tokio::test]
    async fn test_initiate_rebalance_logging() {
        let time_provider = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let manager = InMemoryClusterManager::new(Arc::clone(&time_provider));

        manager.register_node("nodeA", "addrA").await.unwrap();
        manager.heartbeat("nodeA", "addrA").await.unwrap(); // Active
        manager.register_node("nodeB", "addrB").await.unwrap(); // Joining

        // Call should succeed and log (actual log verification is tricky here)
        let result = manager.initiate_rebalance().await;
        assert!(result.is_ok());

        // Test with no active nodes
        let manager_empty = InMemoryClusterManager::new(Arc::clone(&time_provider));
        manager_empty.register_node("nodeC", "addrC").await.unwrap(); // Stays Joining
        let result_empty = manager_empty.initiate_rebalance().await;
        assert!(result_empty.is_ok()); // Should still be ok, just logs no active nodes
    }
}
