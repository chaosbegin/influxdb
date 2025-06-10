# InfluxDB 3 - Conceptual Horizontal Scaling Mechanisms

## 1. Introduction

This document outlines conceptual mechanisms for horizontal scaling in InfluxDB 3. The primary goals are to enable elasticity in terms of storage and query capacity, improve fault tolerance, and allow the database to grow beyond the limits of a single node.

Horizontal scaling involves distributing data (shards) and workload (queries, writes) across multiple interconnected nodes in a cluster.

## 2. Node Discovery and Membership

Effective scaling requires nodes to be aware of each other and maintain a consistent view of cluster membership.

### 2.1. Discovery Mechanisms

How nodes initially find each other and how new nodes join the cluster.

*   **Configuration-based List (Seed Nodes):**
    *   **Description:** Each node is configured with a list of one or more "seed" nodes. When a node starts, it connects to these seed nodes to get the current list of all cluster members.
    *   **Pros:** Simple to implement and understand, good for relatively static clusters or initial bootstrapping.
    *   **Cons:** Less dynamic; if all seed nodes are down, new nodes can't join. Configuration updates might be needed to change seed nodes.

*   **DNS-based Discovery:**
    *   **Description:** Nodes query a pre-configured DNS name that resolves to the IP addresses of one or more existing cluster members (e.g., using SRV records).
    *   **Pros:** Common in cloud environments, can be updated externally without changing node configurations.
    *   **Cons:** Relies on DNS infrastructure; DNS propagation delays can be a factor.

*   **Gossip Protocol (Recommended for dynamic clusters):**
    *   **Description:** Nodes use a peer-to-peer communication protocol to disseminate information about themselves and learn about other nodes. Each node periodically sends its state to a few random peers, and information eventually propagates throughout the cluster.
    *   **Examples:** [Serf (HashiCorp)](https://www.serf.io/), [SWIM](https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf).
    *   **Pros:** Decentralized, resilient to node failures, handles new nodes joining and existing nodes leaving dynamically. Scales well.
    *   **Cons:** Eventual consistency of membership view; slightly more complex to implement.

**Proposed Initial Approach:** A combination of a configuration-based seed node list for initial bootstrapping, potentially augmented by a gossip protocol for ongoing dynamic membership and failure detection.

### 2.2. Membership Management

Maintaining an up-to-date list of active and healthy nodes.

*   **Responsibility:**
    *   A dedicated `ClusterManager` module/service could run on all nodes or a leader-elected subset.
    *   Alternatively, this functionality could be integrated into the global Catalog service, especially if the Catalog is already distributed and fault-tolerant (e.g., using Raft).
*   **Heartbeating:**
    *   Active nodes periodically send heartbeat messages to the `ClusterManager` or broadcast via gossip to signal they are alive and healthy.
*   **Failure Detection:**
    *   If heartbeats are missed for a configurable period, a node is marked as "suspect".
    *   If the node remains unresponsive, it's declared `Down` or `Failed` by the `ClusterManager` / consensus group.
*   **Node States:**
    *   `Joining`: A new node is starting up and attempting to integrate into the cluster.
    *   `Active`: The node is fully operational and participating in data storage and querying.
    *   `Leaving`: The node is gracefully shutting down; its data should be migrated off.
    *   `Down`/`Failed`: The node is unresponsive or has failed.
*   **Metadata Storage:**
    *   The list of known nodes, their network addresses (for client API, internal RPC), states, and last heartbeat times must be stored reliably, typically in the global Catalog.

## 3. Shard Rebalancing Strategy

Distributing shards efficiently and correctly across the available nodes.

### 3.1. Triggering Rebalancing

Events or conditions that initiate a shard rebalancing process.

*   **Manual Trigger:**
    *   An administrator initiates rebalancing via an API call (e.g., `POST /cluster/rebalance`). Useful for planned scaling or maintenance.
*   **Automatic - Node Events:**
    *   **Node Joins:** A new `Active` node is detected. The `ClusterManager` identifies this new capacity and may initiate moving some shards to it.
    *   **Node Leaves/Fails:** A node transitions to `Leaving` or `Down`. The `ClusterManager` must ensure shards hosted on this node (or their replicas) are moved or re-replicated to other active nodes to maintain data availability and replication factors.
*   **Automatic - Threshold-based (Future Enhancement):**
    *   Triggered by metrics exceeding predefined thresholds, such as:
        *   Disk usage imbalance between nodes.
        *   CPU/memory load imbalance.
        *   Network traffic hotspots.
        *   Large shard size relative to others.

### 3.2. Shard Selection for Movement

Determining which shards to move, from where, and to where.

*   **Source Node Selection:**
    *   Nodes in `Leaving` state.
    *   Nodes identified as `Down`/`Failed`.
    *   Nodes that are consistently overloaded based on metrics (for threshold-based rebalancing).
*   **Target Node Selection:**
    *   Nodes in `Joining` state that have become `Active`.
    *   Nodes that are underloaded.
    *   Nodes selected to satisfy replication factor requirements for shards whose replicas were on a failed node.
*   **Shard Criteria for Selection:**
    *   **Priority 1 (Data Availability):** Shards needing re-replication due to a lost replica on a failed node.
    *   **Priority 2 (Decommissioning):** Shards on nodes marked as `Leaving`.
    *   **Priority 3 (Load/Storage Balancing):**
        *   Size of the shard (moving larger shards might be more impactful but also riskier).
        *   Number of queries or writes a shard receives.
        *   Ensure that moving a shard does not violate replication factor constraints (i.e., enough replicas must remain available on other nodes during the move).
    *   **Data Locality (Advanced):** Consider rack/availability zone awareness to maintain fault tolerance domains.

### 3.3. Data Transfer Process

Moving shard data (Parquet files, relevant WAL segments) between nodes.

*   **Option A: Object Store as Intermediary (Recommended for initial simplicity)**
    1.  **Initiate Shard Move:**
        *   The `ClusterManager` selects a shard to move (e.g., `shard_X` from `node_A` to `node_B`).
        *   The Catalog is updated: `ShardDefinition` for `shard_X` has its status changed to `Migrating_Data(source=node_A, target=node_B)`. A new unique instance ID for the shard on the target node might be generated.
    2.  **Source Node Preparation:**
        *   `node_A` (source) is notified. It may stop accepting new writes for `shard_X` or buffer them with intent to forward. For simplicity, temporarily redirecting writes at the routing layer to a temporary holding queue or failing them might be an initial approach.
        *   `node_A` ensures all its local WAL data for `shard_X` is flushed and corresponding Parquet files are persisted to the shared object store.
    3.  **Target Node Bootstrap:**
        *   `node_B` (target) is notified.
        *   It consults the Catalog for `shard_X`'s definition and identifies its Parquet files in the object store.
        *   `node_B` can start warming its local caches by downloading these files or rely on on-demand fetching.
    4.  **Delta Sync / WAL Catch-up:**
        *   Any writes that occurred (or were buffered) on `node_A` for `shard_X` since the Parquet files were finalized must be transferred to `node_B`.
        *   This can be achieved by replicating the relevant WAL segments/entries from `node_A` to `node_B`. `node_B` replays these WAL entries into its own buffer for `shard_X`.
    5.  **Cutover Phase:**
        *   **Pause Writes (Briefly):** Globally (or via routing layer) pause or queue new writes targeting `shard_X`.
        *   **Final Sync:** Ensure `node_B` has processed all pending WAL entries from `node_A` for `shard_X`.
        *   **Catalog Update (Critical):**
            *   Atomically (or via a 2PC-like mechanism if Catalog doesn't support strong atomicity across all nodes immediately):
                *   Update `ShardDefinition.node_ids` for `shard_X` to remove `node_A` and add `node_B`.
                *   Update `ShardDefinition.status` to `Stable` (or remove migration status).
                *   Increment `ShardDefinition.version`.
            *   This catalog update signals all nodes (especially query planners and write routers) about the new location of `shard_X`.
        *   **Resume Writes:** Resume writes, now directed to `node_B` for `shard_X`.
    6.  **Cleanup:**
        *   `node_A` is notified that the move is complete and `node_B` has taken ownership.
        *   `node_A` can then delete its local copy of `shard_X`'s data (if it had any local copies beyond what's in object store) and WAL segments. The Parquet files in object store remain as they are now owned by `node_B`.

*   **Option B: Direct Node-to-Node Streaming**
    *   Similar logical steps but involves `node_A` directly sending Parquet files and/or WAL data to `node_B`.
    *   Requires a separate internal data transfer service/protocol.
    *   Potentially faster if network bandwidth between nodes is high and object store is a bottleneck, but more complex to implement robustly.

### 3.4. Consistency and Impact
*   **Read Operations:**
    *   During migration (before catalog cutover): Reads for `shard_X` can be served by `node_A`.
    *   After cutover: Reads are directed to `node_B`. Query planners need to use the updated catalog information. Caches holding old location information might need invalidation.
*   **Write Operations:**
    *   Option 1: Buffer writes on `node_A` and forward them to `node_B` after it's bootstrapped but before cutover.
    *   Option 2: Redirect new writes to `node_B` as soon as it's capable of buffering them, even if historical data isn't fully synced. `node_B` handles merging.
    *   Option 3 (Simpler but with downtime): Briefly reject writes to `shard_X` during the final cutover phase.
*   **Atomicity of Catalog Updates:** Critical to ensure all components see a consistent view of shard ownership. If the Catalog uses a consensus protocol (like Raft), this is inherently handled. Otherwise, a 2-phase commit or careful state management is needed for these updates.

## 4. Required Catalog Changes for Scaling

Modifications and additions to the `influxdb3_catalog` metadata.

*   **`NodeDefinition` (extensions to existing struct):**
    *   `id: NodeId` (already exists, ensure it's cluster-unique).
    *   `rpc_address: String` (or similar, for internode communication like replication, shard migration RPCs).
    *   `http_address: String` (client API address).
    *   `status: NodeStatus` (enum: `Joining`, `Active`, `Leaving_Initiated`, `Leaving_DataOffload`, `Leaving_Complete`, `Down`, `Suspect`).
    *   `last_heartbeat: i64` (timestamp).
    *   `version: u64` (for optimistic locking or state tracking).
    *   `resources_version: u64` (version of resources assigned to this node, like shards)

*   **`ShardDefinition` (extensions to existing struct):**
    *   `status: ShardMigrationStatus` (enum: `Stable`, `Migrating_DataCopy`, `Migrating_WalSync`, `Awaiting_Cutover`, `Cleaning_Up_Source`).
    *   `target_node_ids: Option<Vec<NodeId>>` (during migration, the destination nodes).
    *   `source_node_ids: Option<Vec<NodeId>>` (during migration, the origin nodes).
    *   `version: u64` (incremented on each successful move/reconfiguration for state tracking and cache invalidation).

*   **New Cluster-Global Metadata (e.g., stored under a specific key in Catalog):**
    *   `ClusterConfiguration`:
        *   `default_replication_factor: u8`
        *   `rebalance_mode: RebalanceMode` (enum: `Manual`, `Automatic_NodeEvents`, `Automatic_Full`)
        *   `rebalance_thresholds: Option<RebalanceThresholds>` (e.g., disk usage %, CPU load %).
        *   `decommissioned_nodes: Vec<NodeId>` (nodes that have left and whose data should be fully cleaned if not already).
    *   `RebalanceOperationLog`:
        *   A log of current and recent rebalancing operations (shard, source, target, status, progress, start/end time).

## 5. Conceptual Management APIs

HTTP or gRPC APIs for administrators to manage and observe the cluster.

*   **Node Management:**
    *   `POST /cluster/v1/nodes`: (Admin) Attempts to add a new node to the cluster. Node provides its ID and RPC/HTTP addresses. Cluster manager verifies and incorporates.
    *   `GET /cluster/v1/nodes`: (Admin/Monitor) Lists all known nodes, their status, addresses, and basic load/storage metrics.
    *   `GET /cluster/v1/nodes/{node_id}`: (Admin/Monitor) Get detailed status and metrics for a specific node.
    *   `DELETE /cluster/v1/nodes/{node_id}`: (Admin) Initiates graceful decommissioning of a node. The node transitions to `Leaving_Initiated`, and the system starts migrating its shards.
    *   `POST /cluster/v1/nodes/{node_id}/force_remove`: (Admin - Dangerous) Forcibly removes a node assumed to be permanently failed, triggering immediate re-replication of its shards.

*   **Shard & Rebalancing Management:**
    *   `GET /cluster/v1/databases/{db_name}/tables/{table_name}/shards`: (Admin/Monitor) Lists all shards for a given table, including their current primary/replica nodes, status, size, etc.
    *   `POST /cluster/v1/shards/{shard_id_str}/move`: (Admin - Advanced) Manually request to move a specific shard instance to a target node. Requires careful coordination.
    *   `POST /cluster/v1/rebalance/start`: (Admin) Manually triggers a cluster-wide rebalancing process based on current strategy (e.g., even out shard distribution).
    *   `GET /cluster/v1/rebalance/status`: (Admin/Monitor) Shows status of ongoing rebalancing tasks, progress, estimated completion.
    *   `POST /cluster/v1/rebalance/pause`: (Admin) Temporarily pause ongoing automatic rebalancing.
    *   `POST /cluster/v1/rebalance/resume`: (Admin) Resume paused rebalancing.

*   **Cluster Configuration & Status:**
    *   `GET /cluster/v1/status`: (Admin/Monitor) Provides an overall health report of the cluster: number of nodes by status, overall shard health, active rebalancing operations, potential issues.
    *   `GET /cluster/v1/config`: (Admin) View current cluster-wide configurations (replication factor, rebalancing settings).
    *   `PUT /cluster/v1/config`: (Admin) Update mutable cluster-wide configurations.

## 6. Key Code Modules for Future Implementation

Primary new or significantly modified Rust modules/crates.

*   **`influxdb3_cluster_manager` (Potentially New Crate/Module):**
    *   Core logic for node discovery (if custom gossip), membership tracking, heartbeating, and failure detection.
    *   Orchestration of shard rebalancing: decision-making on when and what to rebalance.
    *   Interfaces with the Catalog for all cluster state changes.
*   **`influxdb3_catalog` (Significant Extensions):**
    *   Implement new fields in `NodeDefinition`, `ShardDefinition` as outlined.
    *   Store and manage new cluster-global configurations and rebalancing logs.
    *   Provide strong consistency or careful transactionality for updates related to shard movements and node status changes.
*   **`influxdb3_replication` (or extensions to `influxdb3_server::replication_service`):**
    *   If shard data transfer (Option B) involves direct node-to-node streaming of WAL segments or Parquet files, this service might be extended or a new one created.
*   **`influxdb3_distributed_query` (Potentially New Crate or part of `influxdb3_server`):**
    *   If a dedicated gRPC service is used for remote query fragments:
        *   `.proto` definition for `DistributedQueryService` (e.g., `ExecuteQueryFragment`).
        *   Server-side implementation to execute fragments.
        *   Client-side stub/logic for `Planner` to use.
*   **`influxdb3_server` (Significant Extensions):**
    *   `query_planner.rs`: Full implementation of distributed query plan generation. This involves creating/using `ExecutionPlan` operators for remote calls (e.g., `RemoteExchangeExec` or similar) and data merging/aggregation.
    *   `query_executor/mod.rs`:
        *   `QueryTable::scan()` needs to be adaptable to execute scans for specific local shards based on directives from the distributed planner.
        *   `WriteBuffer::get_table_chunks()` and downstream components might need `ShardId` filtering.
    *   Integration of new gRPC services (`ClusterManagerService` if centralized, `DistributedQueryService`) into server startup.
    *   Implementation of the new HTTP Management APIs.
*   **`influxdb3_write` (Extensions):**
    *   `write_buffer/mod.rs`:
        *   During shard migration, `WriteBufferImpl::write_lp` might need to handle writes for shards being moved (e.g., forward to new owner, buffer locally, or temporarily reject based on strategy).
*   **`ShardMigrator` (New Component/Module, likely within `influxdb3_cluster_manager`):**
    *   Encapsulates the detailed workflow for moving a single shard instance from a source to a target node, including data synchronization, WAL catch-up, catalog updates, and cleanup.

This conceptual design provides a foundation for building out horizontal scaling features. Each major section (Node Discovery, Rebalancing, Distributed Querying) is a significant project in itself.
