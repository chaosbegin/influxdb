# Distributed Scenarios Integration Tests

This document outlines integration test scenarios for distributed functionalities, including sharding, replication, and rebalancing.

## Scenario 1: Basic Sharded Write and Query

-   **Objective:** Verify that data written to specific shards based on sharding keys (time and tag `host`) is correctly stored and queryable from the designated nodes, and that queries spanning multiple shards are correctly merged.
-   **Setup:**
    -   Nodes: Node A, Node B, Node C (3 nodes).
    -   Database: `db1`.
    -   Table: `t1` in `db1`, sharded by `TimeAndKey` on tag `host`.
    -   Initial Shard Layout (via catalog manipulation or pre-configuration):
        -   Shard S1_A: `db1.t1`, time range [T1, T2), hash range [H1_start, H1_end), Node A.
        -   Shard S1_B: `db1.t1`, time range [T1, T2), hash range [H2_start, H2_end), Node B. (H1_end < H2_start)
        -   Shard S2_A: `db1.t1`, time range [T3, T4), hash range [H1_start, H1_end), Node A. (T2 < T3)
        -   Shard S2_C: `db1.t1`, time range [T3, T4), hash range [H_full_start, H_full_end) excluding [H1_start, H1_end), Node C. (Or, if simpler, S2_C covers the full hash range for T3-T4, and data for H1_start-H1_end is filtered out by S2_A if S2_A also covers full hash range for T3-T4. The key is distinct ownership for specific data points).
            *Alternative simpler S2_C for 2 nodes (A,B): Shard S2_B (time_range2, hash_range2) on Node B.*
-   **Write Operations:**
    1.  Write data point P1 (time within [T1,T2), `host` tag maps to [H1_start,H1_end)) via Node A.
        -   Verify: P1 is queryable from Node A.
        -   Verify: P1 is *not* directly queryable from Node B / Node C (unless replication is also active, which is not the primary focus of this scenario).
    2.  Write data point P2 (time within [T1,T2), `host` tag maps to [H2_start,H2_end)) via Node B.
        -   Verify: P2 is queryable from Node B.
    3.  Write data point P3 (time within [T3,T4), `host` tag maps to [H1_start,H1_end)) via Node A (targets Shard S2_A).
        -   Verify: P3 is queryable from Node A.
    4.  Write data point P4 (time within [T3,T4), `host` tag maps to S2_C's hash range) via Node C (or Node B if 2-node setup).
        -   Verify: P4 is queryable from Node C (or B).
-   **Query Operations (executed via any node, testing distributed query planning):**
    1.  Query specific point: `SELECT * FROM db1.t1 WHERE host = P1.host AND time = P1.time_val`.
        -   Verify: Returns P1. (Planner routes to Node A).
    2.  Query time range 1: `SELECT * FROM db1.t1 WHERE time >= T1 AND time < T2 ORDER BY time, host`.
        -   Verify: Returns P1, P2. (Planner routes to Node A and Node B, results merged).
    3.  Query time range 2: `SELECT * FROM db1.t1 WHERE time >= T3 AND time < T4 ORDER BY time, host`.
        -   Verify: Returns P3, P4. (Planner routes to Node A and Node C/B, results merged).
    4.  Query all data: `SELECT * FROM db1.t1 ORDER BY time, host`.
        -   Verify: Returns P1, P2, P3, P4. (Planner routes to all relevant nodes, results merged).

## Scenario 2: Replication

-   **Objective:** Verify data replication across nodes, write/read behavior during node failure, and recovery.
-   **Setup:**
    -   Nodes: Node A, Node B, Node C (3 nodes).
    -   Database: `db1`.
    -   Table: `t2` in `db1`, with `ReplicationFactor=2`.
    -   Initial Shard Layout: A single shard S_ABC (full time/hash range) with `node_ids=[NodeA, NodeB]` in its `ShardDefinition`. Node C is not initially an owner.
-   **Write Operations:**
    1.  Write P5 to `t2` via Node A.
        -   Verify: P5 is queryable from Node A.
        -   Verify: P5 is queryable from Node B.
        -   Verify: P5 is *not* queryable from Node C.
    2.  Write P6 to `t2` via Node B.
        -   Verify: P6 is queryable from Node A.
        -   Verify: P6 is queryable from Node B.
-   **Failure Simulation & Read/Write (Node B fails):**
    1.  Action: Stop/kill Node B.
    2.  Write P7 to `t2` via Node A.
        -   Verify: Write succeeds (degraded state, 1 of 2 replicas available).
    3.  Query `SELECT * FROM db1.t2 ORDER BY time` via Node A.
        -   Verify: Returns P5, P6, P7.
    4.  Action: Restart Node B.
        -   Verify (conceptual): Node B's WAL replay or anti-entropy mechanism brings S_ABC up to date.
    5.  Query `SELECT * FROM db1.t2 ORDER BY time` via Node B (after it's caught up).
        -   Verify: Returns P5, P6, P7.
    6.  Query `SELECT * FROM db1.t2 ORDER BY time` via Node C.
        -   Verify: Still does not return data for S_ABC.
-   **(Optional) RF=3 Variation:**
    -   Setup: `t2` with `ReplicationFactor=3`, S_ABC on `node_ids=[A,B,C]`.
    -   Kill Node C. Write P8 via Node A. Verify write succeeds (2/3 replicas). Query via A or B returns P8.
    -   Restart Node C. Verify catch-up. Query via C returns P8.

## Scenario 3: Node Addition and Rebalance

-   **Objective:** Verify that adding a new node and triggering a rebalance correctly migrates some shards to the new node.
-   **Setup:**
    -   Nodes: Node A, Node B, Node C.
    -   Database `db1`, Table `t1` from Scenario 1 (e.g., S1_A on A, S1_B on B, S2_A on A, S2_C on C).
-   **Operation:**
    1.  Introduce Node D: Use `ClusterManagementService.AddNode` with Node D's definition (status `Joining`).
    2.  Trigger Rebalance: Call `ClusterManagementService.TriggerRebalance(strategy=AddNewNode(D_definition))`.
-   **Verification (Post-Rebalance):**
    1.  Catalog State:
        -   Query `ClusterManagementService.ListNodes`. Verify Node D is present and `Active`.
        -   Inspect `ShardDefinition`s for `db1.t1`. At least one shard (e.g., S2_C that was on Node C, or another selected shard) should now list Node D in its `node_ids`. The original owner of that specific migrated copy should be removed from `node_ids` for that shard.
        -   Verify `migration_status` for affected shards is `Stable`.
    2.  Data Locality & Querying:
        -   Identify a shard S_migrated that is now primarily on Node D.
        -   Write a new data point P_new (fitting S_migrated's time/key range) via Node D (or any node).
        -   Verify: P_new is queryable via Node D directly (if possible to target a node for query).
        -   Query `SELECT * FROM db1.t1 ORDER BY time, host`.
            -   Verify: Returns all original data (P1,P2,P3,P4) plus P_new. Results merged from A, B, D, and C (if C still holds some shards).
    3.  Logs: Check orchestrator and catalog logs for correct sequence of migration events (Begin, Commit, Finalize) and simulated data movement messages.

## Scenario 4: Node Decommission and Rebalance

-   **Objective:** Verify that decommissioning a node correctly migrates its shards to remaining active nodes.
-   **Setup:**
    -   Nodes: Node A, Node B, Node C.
    -   Database `db1`.
    -   Table `t1` with initial shard layout:
        -   Shard S_A (time1, key1) on Node A.
        -   Shard S_B (time1, key2) on Node B.
        -   Shard S_C (time2, key_any) on Node C.
    -   Write initial data points P_A to S_A, P_B to S_B, P_C to S_C.
-   **Operation:**
    1.  Decommission Node C: Call `ClusterManagementService.TriggerRebalance(strategy=DecommissionNode(NodeC_id))`.
-   **Verification (Post-Rebalance):**
    1.  Catalog State:
        -   Query `ClusterManagementService.ListNodes`. Node C should eventually be in `Down` status (or removed, depending on policy).
        -   Inspect `ShardDefinition` for S_C. Its `node_ids` should now list Node A or Node B (or both if the shard was replicated).
        -   Verify `migration_status` for S_C is `Stable`.
    2.  Data Availability & Querying:
        -   Query `SELECT * FROM db1.t1 ORDER BY time, host`.
            -   Verify: Returns P_A, P_B, and P_C. (P_C's data is now served by its new owner(s)).
        -   Write a new data point P_C_new that would have landed on the original S_C (based on its time/key range).
            -   Verify: Write succeeds.
            -   Query for P_C_new. Verify it's returned from the new owner of S_C's data range (Node A or Node B).
    3.  Logs: Check orchestrator and catalog logs for migration events for S_C and status changes for Node C.

## Scenario 5: Read/Write During Rebalance (Conceptual)

-   **Objective:** Document expected behavior and potential test points for operations during shard migration. This is harder to test precisely without fine-grained control over migration steps.
-   **Setup:**
    -   Node M (source), Node N (target).
    -   Shard S_X on Node M is being migrated to Node N.
-   **Test Points / Verification Areas:**
    -   **Writes during `MigratingOutTo` state (before target commit):**
        -   Strategy 1 (Dual Writes): Writes go to both M and N. Verify data on both.
        -   Strategy 2 (Write to Old, Sync Later): Writes go to M. N gets them via WAL sync.
        -   Strategy 3 (Forward to New): Writes to M are forwarded to N.
        -   Strategy 4 (Block Writes): Writes to S_X are temporarily blocked/queued.
        -   *Current system likely implies Strategy 2 or requires client to handle routing.* Document expected behavior. Verify no data loss post-migration.
    -   **Reads during `MigratingOutTo` state:**
        -   Strategy A (Read from Old): Reads for S_X go to M.
        -   Strategy B (Read from Both & Merge): Reads go to M and N, results merged/deduplicated.
        -   Strategy C (Read from New once available): Reads switch from M to N at some point.
        -   *Current system likely implies Strategy A until `FinalizeShardMigrationOnSource` is complete for S_X on M.* Document expected consistency.
    -   **Reads/Writes after `CommitShardMigrationOnTarget` but before `FinalizeShardMigrationOnSource`:**
        -   Node N is now also an owner in catalog (`node_ids` includes N).
        -   Writes: Where do they go? Still to M? To N? Both?
        -   Reads: From M? From N? Merged?
    -   **Reads/Writes after `FinalizeShardMigrationOnSource`:**
        -   Node M is no longer an owner. Writes and reads for S_X's data should go to Node N (and any other replicas).
-   **Verification:**
    -   Data consistency and no data loss are paramount.
    -   Availability: Quantify downtime/error rates for reads/writes to the migrating shard, if any.
    -   Document the specific consistency model aimed for during these transitions.

## Scenario 6: Concurrent Operations and Cluster Stability

-   **Objective:** Ensure the cluster remains stable and data consistent under concurrent workloads and administrative operations.
-   **Setup:**
    -   3-4 nodes.
    -   Multiple sharded tables, some with replication.
-   **Operations (run concurrently):**
    1.  Client A: Continuous high-throughput writes to `db1.table1`.
    2.  Client B: Continuous high-throughput writes to `db1.table2` (different sharding key or strategy if possible).
    3.  Client C: Continuous reads (various queries, some full table scans, some point lookups) from `db1.table1`.
    4.  Client D: Continuous reads from `db1.table2`.
    5.  Administrative Operations:
        -   Add a new node, trigger rebalance.
        -   Wait for rebalance to complete (or reach a stable point).
        -   Decommission one of the original nodes, trigger rebalance.
-   **Verification:**
    -   No server crashes, deadlocks, or unhandled errors on any node.
    -   All valid writes are eventually persisted and queryable. No data loss.
    -   Queries return consistent results (respecting chosen consistency model during rebalances).
    -   Performance (TPS for writes, QPS for reads) remains within acceptable bounds (though may show temporary degradation during rebalance).
    -   Catalog state remains consistent after operations.
    -   Logs are clean of unexpected critical errors.

## General Verification Points for All Scenarios

-   **Data Correctness:** All data written should be queryable and match the input. No corruption.
-   **Data Completeness:** No data loss during normal operations or during/after administrative tasks like node addition/removal/failure.
-   **Query Performance:** While not strict performance tests, observe for major regressions or excessive query times. Establish baselines if possible.
-   **System Stability:** Nodes should remain operational. No crashes, deadlocks, or resource exhaustion (memory, CPU beyond reasonable limits for the workload).
-   **Logging:** Critical lifecycle events, errors, and significant warnings should be logged appropriately on relevant nodes.
-   **Catalog State Consistency:** After operations, the catalog (as viewed from any node, if possible, or from the orchestrator's perspective) should reflect the correct state of nodes, shards, and their ownership.
-   **Metrics:** Key operational metrics (write throughput, query latency, error rates, replication lag if measurable) should be monitored.

This list provides a starting point and can be expanded with more specific edge cases and failure modes.
