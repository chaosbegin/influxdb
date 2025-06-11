// Module for cluster management, node discovery, and shard migration logic.

pub mod manager;
pub mod migration;
pub mod api;

// Potentially shared types for the cluster module could also go here if not large enough for their own files.
// For example, if NodeId or ShardId needed cluster-specific wrappers or context here.
// However, NodeId and ShardId are typically imported from influxdb3_id or influxdb3_catalog.
