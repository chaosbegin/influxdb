# Cluster Management API (V3)

This document describes the V3 APIs for managing an InfluxDB 3 cluster.

## Node Management

### List Cluster Nodes

-   **Endpoint:** `GET /api/v3/cluster/nodes`
-   **Description:** Retrieves a list of all registered cluster nodes and their current status and metadata.
-   **Responses:**
    -   `200 OK`: Returns a JSON array of `ClusterNodeDefinition` objects.
    -   `500 INTERNAL_SERVER_ERROR`: If there's an issue retrieving data from the catalog.

### Add Cluster Node

-   **Endpoint:** `POST /api/v3/cluster/nodes`
-   **Description:** Registers a new node with the cluster. The node definition, including its ID, RPC address, HTTP address, and initial status, should be provided in the request body.
-   **Request Body:** JSON representation of `ClusterNodeDefinition`.
    ```json
    {
        "id": 123, // u64 Node ID
        "rpc_address": "node1.example.com:8082",
        "http_address": "node1.example.com:8081",
        "status": "Active", // Or other initial status
        "created_at": 0, // Will be set by server if 0
        "updated_at": 0  // Will be set by server
    }
    ```
-   **Responses:**
    -   `201 CREATED`: Node successfully registered. Returns the created `ClusterNodeDefinition`.
    -   `400 BAD_REQUEST`: Invalid request payload.
    -   `409 CONFLICT`: A node with the same ID already exists.
    -   `500 INTERNAL_SERVER_ERROR`: Internal catalog error.

### Remove Cluster Node

-   **Endpoint:** `DELETE /api/v3/cluster/nodes/:node_id`
-   **Description:** Unregisters a node from the cluster. This should typically be done after all data/shards have been migrated off the node.
-   **Path Parameters:**
    -   `node_id` (u64): The ID of the node to remove.
-   **Responses:**
    -   `204 NO CONTENT`: Node successfully removed.
    -   `400 BAD_REQUEST`: Invalid `node_id` format.
    -   `404 NOT FOUND`: Node with the specified ID not found.
    -   `500 INTERNAL_SERVER_ERROR`: Internal catalog error.

### Update Cluster Node Status

-   **Endpoint:** `PUT /api/v3/cluster/nodes/:node_id/status`
-   **Description:** Updates the status of a registered cluster node (e.g., "Active", "Inactive", "Draining").
-   **Path Parameters:**
    -   `node_id` (u64): The ID of the node to update.
-   **Request Body:**
    ```json
    {
        "status": "Inactive"
    }
    ```
-   **Responses:**
    -   `200 OK`: Status successfully updated. Returns `{"status": "updated"}`.
    -   `400 BAD_REQUEST`: Invalid `node_id` format or invalid payload.
    -   `404 NOT FOUND`: Node with the specified ID not found.
    -   `500 INTERNAL_SERVER_ERROR`: Internal catalog error.

## Shard Management

### Initiate Shard Move (Conceptual)

-   **Endpoint:** `POST /api/v3/cluster/shards/move`
-   **Description:** Initiates the conceptual process of moving a shard from a source node to a target node. This typically involves updating the shard's status in the catalog to indicate migration has started (e.g., "MigratingSnapshot").
-   **Request Body:**
    ```json
    {
        "db_name": "my_database",
        "table_name": "my_table",
        "shard_id_str": "1", // String representation of ShardId (u64)
        "target_node_id_str": "2" // String representation of NodeId (u64) for the target
    }
    ```
-   **Responses:**
    -   `202 ACCEPTED`: Shard move process successfully initiated. Returns `{"status": "shard move initiated"}`.
    -   `400 BAD_REQUEST`: Invalid payload (e.g., non-numeric IDs).
    -   `404 NOT FOUND`: Specified database, table, or shard not found in the catalog.
    -   `500 INTERNAL_SERVER_ERROR`: Internal catalog or rebalancing error.

### Update Table Sharding Configuration

-   **Endpoint:** `PUT /api/v3/cluster/tables/sharding`
-   **Description:** Updates the sharding configuration (shard keys and number of hash partitions) for a specific table. This allows for defining how data within a time-slice shard should be further partitioned using consistent hashing.
-   **Request Body:** `UpdateTableShardingPayload`
    ```json
    {
        "db_name": "my_database",
        "table_name": "my_table",
        "shard_keys": ["tagA", "tagB"], // Optional: List of column names to be used as shard keys.
                                        // Send null or omit to not change. Send empty array to clear.
        "num_hash_partitions": 4        // Optional: Number of hash partitions (must be >= 1 if set).
                                        // Send null or omit to not change.
    }
    ```
-   **Responses:**
    -   `200 OK`: Table sharding configuration successfully updated. Returns `{"status": "table sharding configuration updated"}`.
    -   `400 BAD_REQUEST`: Invalid payload (e.g., `num_hash_partitions` is 0). `num_hash_partitions = 1` is the default and means that hash-based sharding by key is effectively disabled; data within a time-slice shard will not be further partitioned by hash.
    -   `404 NOT FOUND`: Specified database or table not found in the catalog.
    -   `500 INTERNAL_SERVER_ERROR`: Internal catalog error (e.g., `InvalidConfiguration` if catalog rules are violated).

*(Further endpoints for other migration steps like `complete_snapshot_transfer`, `complete_wal_sync`, `complete_cutover`, `complete_cleanup` would follow a similar pattern if exposed via API.)*
