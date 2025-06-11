use crate::{ReplicateWalOpRequest, ReplicateWalOpResponse};
use std::collections::HashSet;
use tokio::sync::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MockReplicationClient {
    // Simulate which nodes will successfully replicate.
    // For testing, we can pre-populate this.
    successful_nodes: Arc<Mutex<HashSet<String>>>,
    // Simulate a total number of expected replicas for quorum calculation,
    // or specific nodes that are expected to respond.
    // For this mock, we'll just use a count of how many should succeed.
    programmed_success_count: Arc<Mutex<usize>>,
}

impl MockReplicationClient {
    pub fn new() -> Self {
        Self {
            successful_nodes: Arc::new(Mutex::new(HashSet::new())),
            programmed_success_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Configure the mock client for a test run.
    /// `nodes_to_succeed`: A list of node addresses that should return success.
    /// `total_expected_replicas`: Not directly used by `replicate_wal_op`'s mock logic,
    ///                            but could be used by the caller to determine the list of nodes to try.
    /// `programmed_successes`: How many calls should return success, regardless of node address.
    #[cfg(test)]
    pub async fnprime_with_successes(&self, programmed_successes: usize) {
        let mut count = self.programmed_success_count.lock().await;
        *count = programmed_successes;
    }

    #[cfg(test)]
    pub async fnprime_successful_nodes(&self, nodes_to_succeed: Vec<String>) {
        let mut successful_nodes = self.successful_nodes.lock().await;
        successful_nodes.clear();
        for node_id in nodes_to_succeed {
            successful_nodes.insert(node_id);
        }
    }


    pub async fn replicate_wal_op(
        &self,
        target_node_address: &str, // Used to simulate specific node failures if needed
        _request: &ReplicateWalOpRequest, // Request content not deeply inspected by this mock
    ) -> Result<ReplicateWalOpResponse, String> {
        // Simple mock logic:
        // 1. If target_node_address is in our `successful_nodes` set (primed for specific node behavior).
        // 2. Or, if `programmed_success_count` > 0, return success and decrement.
        // This allows for flexible testing.

        let mut successful_nodes = self.successful_nodes.lock().await;
        if successful_nodes.contains(target_node_address) {
            return Ok(ReplicateWalOpResponse { success: true, error_message: None });
        }
        // Fallback to programmed success count if not found in specific node list
        // This part is tricky if we want to precisely control which *call* succeeds.
        // For now, let's simplify: if successful_nodes is empty, use programmed_success_count.
        // If successful_nodes is not empty, it dictates behavior.

        if successful_nodes.is_empty() {
            let mut programmed_s_count = self.programmed_success_count.lock().await;
            if *programmed_s_count > 0 {
                *programmed_s_count -= 1;
                return Ok(ReplicateWalOpResponse { success: true, error_message: None });
            } else {
                return Ok(ReplicateWalOpResponse {
                    success: false,
                    error_message: Some(format!("Mock error: Node {} failed or no programmed successes left", target_node_address))
                });
            }
        }

        // If successful_nodes is not empty, and target_node_address was not in it, then it's a failure for that node.
        Ok(ReplicateWalOpResponse {
            success: false,
            error_message: Some(format!("Mock error: Node {} not in successful_nodes list", target_node_address)),
        })
    }
}

impl Default for MockReplicationClient {
    fn default() -> Self {
        Self::new()
    }
}
