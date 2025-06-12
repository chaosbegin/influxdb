# Distributed Integration Testing Strategy (Conceptual)

This document outlines a conceptual strategy for integration testing a distributed InfluxDB 3 cluster. The focus is on verifying data replication, distributed query execution, horizontal scaling (node addition/removal, rebalancing), and management APIs.

## 1. Test Environment Requirements

A robust distributed test environment is crucial. Key requirements include:

-   **Deployment:**
    -   **Local Simulation:** Docker Compose can be used to simulate a multi-node cluster locally. Each InfluxDB 3 instance runs in its own container.
    -   **Cloud/Kubernetes:** For more realistic and complex scenarios, deployment on Kubernetes (e.g., using `kind` for local Kubernetes, or a managed K8s service) is recommended. Helm charts or custom operators would manage the deployment.
-   **Configuration:**
    -   Each node must be configurable with:
        -   Unique Node ID.
        -   RPC and HTTP API addresses.
        -   Seed nodes or a discovery mechanism (e.g., DNS, etcd, Consul) for cluster formation (if dynamic discovery is implemented).
        -   Catalog connection details:
            -   If the catalog is embedded and uses Raft, Raft peer addresses.
            -   If the catalog is external (e.g., standalone etcd), connection string and credentials.
    -   Configuration should be manageable via environment variables, config files, or CLI arguments passed to each instance.
-   **Networking:**
    -   Nodes must be able to communicate with each other on their specified RPC ports (for internal Raft, replication, distributed queries).
    -   HTTP API ports must be accessible to the test orchestrator.
    -   Ensure no firewall rules or network policies prevent inter-node communication within the test environment.
-   **Object Storage:**
    -   Access to a shared S3-compatible object store is required for Parquet file persistence.
    -   For local testing, MinIO is a suitable option.
    -   All nodes in the cluster must be configured to use the same object store bucket and credentials.
-   **Monitoring & Logging:**
    -   Centralized logging (e.g., ELK stack, Grafana Loki) to aggregate logs from all nodes.
    -   Metrics collection (e.g., Prometheus) from all nodes, viewable in a dashboard (e.g., Grafana). This helps in debugging and observing system behavior under test.
-   **Test Orchestration:**
    -   A test framework (e.g., Pytest, Go's testing package with custom helpers) or shell scripts to:
        -   Automate cluster setup (deploying nodes, configuring them).
        -   Automate cluster teardown and cleanup of resources (volumes, object store data).
        -   Execute test scenarios, including sending API requests, triggering operations.
        -   Verify outcomes by querying data, checking catalog state, and inspecting logs/metrics.

## 2. Test Scenarios - Data Replication & Consistency

These tests verify that data written to the cluster is correctly replicated according to the defined replication factor and remains consistent.

-   **Basic Replication:**
    -   Setup: Cluster with N nodes, table with replication factor RF > 1. Shard S1 is assigned to a subset of these nodes.
    -   Action: Write data to Shard S1 via its primary owner.
    -   Verification: Read the same data from all other nodes that host replicas of Shard S1. Data should be identical.
-   **Quorum Writes:**
    -   Setup: Table with various replication factors (e.g., RF=1, RF=2, RF=3).
    -   Action:
        -   Attempt writes when enough nodes are available to meet quorum `(RF/2 + 1)`.
        -   Attempt writes when fewer than quorum nodes are available for a shard's replicas.
    -   Verification:
        -   Writes succeed when quorum is met.
        -   Writes fail (or are queued, depending on design) when quorum cannot be met.
        -   Verify data consistency for successful quorum writes.
-   **Node Failure during Write:**
    -   Setup: Table with RF=3. Shard S1 on nodes A, B, C.
    -   Action: Initiate a write to Shard S1. While the write is in progress (or before all replicas have acknowledged), simulate failure of one replica node (e.g., node C).
    -   Verification:
        -   If quorum (e.g., 2 of 3) was met before/during failure, the write should succeed. Data should be on A and B.
        -   If quorum was not met, the write should fail.
-   **Node Recovery & Catch-up:**
    -   Setup: Table with RF=3. Shard S1 on nodes A, B, C. Node C is temporarily down.
    -   Action: Write new data to Shard S1 (will go to A, B). Then, bring node C back online.
    -   Verification: Node C should automatically (or via a repair mechanism) catch up on missed writes for Shard S1. Eventually, data on C should be consistent with A and B.
-   **Data Consistency Checks:**
    -   After various operations (writes, node failures, recoveries, rebalancing), perform reads for the same data ranges/keys from different available replicas.
    -   Verification: Data returned from all available and up-to-date replicas must be identical. Checksumming or row-by-row comparison can be used.

## 3. Test Scenarios - Distributed Queries

These tests verify that queries across sharded data are processed correctly.

-   **Querying Sharded Data:**
    -   Setup: Create a table sharded across M nodes (e.g., based on time range or a tag value). Populate each shard with distinct data.
    -   Action: Execute queries that:
        -   Target data on a single shard.
        -   Span multiple/all shards (e.g., a query with a wide time range or no filter on the shard key).
    -   Verification: Results are correctly fetched from the appropriate nodes and aggregated (if applicable, e.g., `UNION ALL` behavior for `RemoteScanExec` combined with `UnionExec`).
-   **Predicates on Shard Keys (Shard Pruning):**
    -   Setup: Table sharded by a specific tag (e.g., `region`) or time.
    -   Action: Execute queries with predicates on the shard key (e.g., `WHERE region = 'us-west'`, `WHERE time > T1 AND time < T2`).
    -   Verification (Conceptual):
        -   Based on query plan inspection (if available in tests) or node-level query execution logs/metrics, verify that only the nodes hosting the relevant shards are queried.
        -   Results should only contain data matching the shard key predicate.
-   **Node Failure during Query:**
    -   Setup: Query spanning multiple shards on different nodes.
    -   Action: While the query is executing, simulate failure of one node involved in the query.
    -   Verification:
        -   The query should either fail gracefully (e.g., return an error indicating partial results or node unavailability).
        -   Or, if designed for high availability, it might complete with data from available replicas (if the failed node's shard data was replicated). Behavior depends on system design.

## 4. Test Scenarios - Horizontal Scaling & Rebalancing

These tests verify the cluster's ability to scale and rebalance shards.

-   **Add Node & Rebalance:**
    -   Setup: Start a cluster (e.g., 3 nodes). Write data to several shards.
    -   Action:
        1.  Add a 4th node to the cluster (configure it to join, verify membership via API).
        2.  Manually trigger shard rebalancing using the (conceptual) management API to move one or more shards to the new node.
    -   Verification:
        1.  Use catalog APIs/queries to check that the shard's `node_ids` list is updated to include the new node and that its status transitions correctly (e.g., "MigratingSnapshot" -> "MigratingWAL" -> "AwaitingCutover" -> "Stable" on new node).
        2.  Query data from the moved shard(s); it should be available from the new node.
        3.  Verify that data is eventually removed from the original source node(s) for that shard (status "Cleaned").
        4.  Monitor system stability (no crashes, minimal errors) and data consistency during and after rebalancing.
-   **Remove Node (Graceful Decommission):**
    -   Setup: Cluster with N nodes, data distributed.
    -   Action:
        1.  Select a node to decommission.
        2.  Trigger a graceful decommission process via an API call (conceptual). This should first rebalance all its shards to other active nodes.
    -   Verification:
        1.  Monitor shard statuses; they should transition through rebalancing states.
        2.  Once all shards are migrated off the node, verify its `node_ids` list is empty for those shards in the catalog.
        3.  The node can then be safely shut down without data loss.
        4.  Verify data availability and consistency for all shards previously on the decommissioned node.
-   **Node Failure (Hard Crash):**
    -   Setup: Cluster with N nodes, RF > 1 for all shards.
    -   Action: Simulate a hard crash of one node (e.g., `docker stop` or kill process).
    -   Verification:
        1.  Shards that had replicas on the failed node should remain available for reads and writes via their remaining replicas on other nodes.
        2.  (Future) If automatic re-replication/healing is implemented, verify that new replicas are created on other nodes to restore the desired replication factor. This would involve observing catalog changes for `node_ids` of affected shards.
-   **Querying/Writing during Rebalancing:**
    -   Setup: Initiate a shard rebalancing operation.
    -   Action: While a shard is migrating (e.g., in "MigratingSnapshot" or "MigratingWAL" state), attempt reads and writes to that shard.
    -   Verification:
        -   Behavior depends on the specific implementation (e.g., writes might be paused for the shard, redirected, or dual-written).
        -   Reads might be served from the old location until cutover.
        -   Expect potential for increased latency or temporary, well-defined errors, but the system should remain stable and achieve eventual consistency once rebalancing completes.

## 5. Test Scenarios - Management APIs

These tests directly target the functionality of the cluster management HTTP APIs.

-   Setup: A running multi-node cluster.
-   **Node Management:**
    -   **Add Node:** `POST /api/v3/cluster/nodes` with a `ClusterNodeDefinition`. Verify `201 CREATED`. Query `GET /api/v3/cluster/nodes` to see the new node. Attempt to add duplicate, verify `409 CONFLICT`.
    -   **List Nodes:** `GET /api/v3/cluster/nodes`. Verify expected nodes are present.
    -   **Remove Node:** Add a node, then `DELETE /api/v3/cluster/nodes/{node_id}`. Verify `204 NO CONTENT`. Verify node is gone from list. Attempt to delete non-existent node, verify `404 NOT FOUND`.
    -   **Update Node Status:** Add a node. `PUT /api/v3/cluster/nodes/{node_id}/status` with `{"status": "Inactive"}`. Verify `200 OK`. Check catalog/list to confirm status change. PUT to non-existent node, verify `404 NOT FOUND`.
-   **Shard Management:**
    -   **Initiate Shard Move:**
        -   Setup catalog with a DB, table, and a shard (e.g., shard S1 on node A).
        -   `POST /api/v3/cluster/shards/move` with payload to move S1 to node B. Verify `202 ACCEPTED`.
        -   Query catalog (if test framework allows, or via debug endpoints) to verify shard S1 status is now "MigratingSnapshot".
        -   Test with non-existent DB, table, shard, or target node ID. Verify appropriate error codes (`400 BAD_REQUEST` or `404 NOT_FOUND`).

## 6. Test Data Generation

-   **Time Series Data:** Generate data with varying timestamps to test time-based sharding.
-   **Tag Cardinality:** Generate data with tags that have low, medium, and high cardinality, especially for tags used as shard keys.
-   **Data Volume:** Scripts to generate small, medium, and large datasets to test behavior under different loads (though full performance testing is separate).
-   **Specific Data Patterns:** Generate data to test edge cases, e.g., all data points at the same timestamp, or data with highly skewed tag values.

## 7. Success Criteria & Metrics

-   **Correctness:**
    -   Data written is identical to data read (consistency).
    -   Query results are accurate and complete (or correctly indicate errors/partial results).
    -   Catalog state reflects the intended outcomes of operations.
-   **Availability:**
    -   System remains accessible for reads and writes during node failures and scaling operations (within expected SLOs for quorum-based systems).
    -   APIs respond within acceptable timeframes.
-   **Performance (Basic Observation):**
    -   While not full performance tests, observe basic query latency and write throughput to catch major regressions.
    -   Monitor resource utilization (CPU, memory, network) on nodes.
-   **Stability:**
    -   No unexpected crashes or deadlocks in cluster nodes.
    -   Errors are handled gracefully and reported clearly.

## 8. Tooling Considerations

-   **Cluster Orchestration:**
    -   `docker-compose` for simple local multi-node setups.
    -   `kind` (Kubernetes IN Docker) for local Kubernetes-based testing.
    -   Ansible or Terraform for more complex cloud deployments if needed.
-   **Test Execution:**
    -   Custom scripts (Bash, Python).
    -   Leverage existing Go integration test frameworks from InfluxDB 1.x/2.x if they can be adapted for API interactions and cluster management.
    -   Python `pytest` with `aiohttp` or `requests` for API testing.
-   **Data Generation:**
    -   Custom scripts.
    -   InfluxDB 3 Load Generator (if enhanced for specific sharding scenarios).
-   **Chaos Engineering (Future):**
    -   Tools like Chaos Mesh (Kubernetes-native) or custom scripts to inject failures (network latency/partitions, node crashes, disk full).
-   **Verification:**
    -   Direct catalog queries (if an API or tool allows).
    -   InfluxDB 3 client for data writes and reads.
    -   Log analysis tools.
    -   Metrics dashboards.

This document provides a starting point for designing a comprehensive distributed integration testing suite. Each scenario would need to be further detailed with specific steps, assertions, and configurations.
