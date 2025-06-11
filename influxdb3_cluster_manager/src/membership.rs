use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::ClusterNodeDefinition; // Corrected path
use influxdb3_id::NodeId;
use std::sync::Arc;
use crate::ClusterManagerError; // Path to error in the same crate

#[derive(Debug)]
pub struct StaticClusterMembership {
    catalog: Arc<Catalog>,
}

impl StaticClusterMembership {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    /// Returns a list of nodes currently marked as "Active".
    /// Note: In a real system, this might involve more complex filtering or error handling.
    pub async fn get_active_nodes(&self) -> Result<Vec<ClusterNodeDefinition>, ClusterManagerError> {
        // `list_cluster_nodes` is synchronous and returns Vec<Arc<ClusterNodeDefinition>>
        let nodes = self.catalog.list_cluster_nodes();
        Ok(nodes
            .into_iter()
            .map(|arc_node| (*arc_node).clone()) // Deref Arc and clone ClusterNodeDefinition
            .filter(|n| n.status == "Active")
            .collect())
    }

    /// Gets the status of a specific node.
    pub async fn get_node_status(&self, node_id: NodeId) -> Result<Option<String>, ClusterManagerError> {
        // `get_cluster_node` is synchronous and returns Option<Arc<ClusterNodeDefinition>>
        if let Some(node_def_arc) = self.catalog.get_cluster_node(node_id) {
            Ok(Some(node_def_arc.status.clone()))
        } else {
            Ok(None)
        }
    }

    /// Conceptual: Activates all nodes currently in "Joining" state.
    /// In a real system, this would involve more safety checks and potentially
    /// be triggered by a consensus mechanism or admin action.
    pub async fn activate_joining_nodes(&self) -> Result<(), ClusterManagerError> {
        let joining_nodes_arcs = self.catalog.list_cluster_nodes();
        let joining_nodes: Vec<ClusterNodeDefinition> = joining_nodes_arcs
            .into_iter()
            .map(|arc_node| (*arc_node).clone())
            .filter(|n| n.status == "Joining")
            .collect();

        if joining_nodes.is_empty() {
            return Ok(());
        }

        for node in joining_nodes {
            self.catalog
                .update_cluster_node_status(node.id, "Active".to_string())
                .await?; // This returns Result<OrderedCatalogBatch, CatalogError>
                         // The ? will convert CatalogError to ClusterManagerError::CatalogError
            observability_deps::tracing::info!(node_id = ?node.id, "Activated node conceptually.");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::{Catalog, CatalogArgs, CatalogLimits};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;

    async fn setup_catalog_with_nodes() -> Arc<Catalog> {
        let catalog = Arc::new(
            Catalog::new_with_args(
                "test_cluster_membership",
                Arc::new(InMemory::new()),
                Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
                Arc::new(metric::Registry::new()),
                CatalogArgs::default(),
                CatalogLimits::default(),
            )
            .await
            .unwrap(),
        );

        let now = catalog.time_provider().now().timestamp_nanos();

        let node1 = ClusterNodeDefinition {
            id: NodeId::new(1),
            rpc_address: "node1:8082".to_string(),
            http_address: "node1:8081".to_string(),
            status: "Active".to_string(),
            created_at: now,
            updated_at: now,
        };
        let node2 = ClusterNodeDefinition {
            id: NodeId::new(2),
            rpc_address: "node2:8082".to_string(),
            http_address: "node2:8081".to_string(),
            status: "Joining".to_string(),
            created_at: now,
            updated_at: now,
        };
        let node3 = ClusterNodeDefinition {
            id: NodeId::new(3),
            rpc_address: "node3:8082".to_string(),
            http_address: "node3:8081".to_string(),
            status: "Active".to_string(),
            created_at: now,
            updated_at: now,
        };
        let node4 = ClusterNodeDefinition {
            id: NodeId::new(4),
            rpc_address: "node4:8082".to_string(),
            http_address: "node4:8081".to_string(),
            status: "Inactive".to_string(),
            created_at: now,
            updated_at: now,
        };

        catalog.add_cluster_node(node1).await.unwrap();
        catalog.add_cluster_node(node2).await.unwrap();
        catalog.add_cluster_node(node3).await.unwrap();
        catalog.add_cluster_node(node4).await.unwrap();

        catalog
    }

    #[tokio::test]
    async fn test_get_active_nodes() {
        let catalog = setup_catalog_with_nodes().await;
        let membership = StaticClusterMembership::new(Arc::clone(&catalog));

        let active_nodes = membership.get_active_nodes().await.unwrap();
        assert_eq!(active_nodes.len(), 2);
        assert!(active_nodes.iter().all(|n| n.status == "Active"));
        assert!(active_nodes.iter().any(|n| n.id == NodeId::new(1)));
        assert!(active_nodes.iter().any(|n| n.id == NodeId::new(3)));

        // Test with no active nodes (by updating existing ones)
        catalog.update_cluster_node_status(NodeId::new(1), "Inactive".to_string()).await.unwrap();
        catalog.update_cluster_node_status(NodeId::new(3), "Leaving".to_string()).await.unwrap();

        let active_nodes_empty = membership.get_active_nodes().await.unwrap();
        assert!(active_nodes_empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_node_status() {
        let catalog = setup_catalog_with_nodes().await;
        let membership = StaticClusterMembership::new(catalog);

        let status_node1 = membership.get_node_status(NodeId::new(1)).await.unwrap();
        assert_eq!(status_node1, Some("Active".to_string()));

        let status_node2 = membership.get_node_status(NodeId::new(2)).await.unwrap();
        assert_eq!(status_node2, Some("Joining".to_string()));

        let status_node_non_existent = membership.get_node_status(NodeId::new(999)).await.unwrap();
        assert_eq!(status_node_non_existent, None);
    }

    #[tokio::test]
    async fn test_activate_joining_nodes() {
        let catalog = setup_catalog_with_nodes().await;
        let membership = StaticClusterMembership::new(Arc::clone(&catalog));

        // Node 2 is "Joining"
        let initial_status_node2 = membership.get_node_status(NodeId::new(2)).await.unwrap();
        assert_eq!(initial_status_node2, Some("Joining".to_string()));

        membership.activate_joining_nodes().await.unwrap();

        let status_node2_after = membership.get_node_status(NodeId::new(2)).await.unwrap();
        assert_eq!(status_node2_after, Some("Active".to_string()));

        // Check that other nodes were not affected
        let status_node1_after = membership.get_node_status(NodeId::new(1)).await.unwrap();
        assert_eq!(status_node1_after, Some("Active".to_string()));
        let status_node4_after = membership.get_node_status(NodeId::new(4)).await.unwrap();
        assert_eq!(status_node4_after, Some("Inactive".to_string()));

        // Run again, should be no-op
        membership.activate_joining_nodes().await.unwrap();
        let status_node2_final = membership.get_node_status(NodeId::new(2)).await.unwrap();
        assert_eq!(status_node2_final, Some("Active".to_string()));
    }
}
