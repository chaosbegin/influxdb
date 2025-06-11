// Copyright (c) 2024 InfluxData Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

// Generated gRPC types
pub use generated_cluster_management::{
    cluster_management_service_server::{ClusterManagementService, ClusterManagementServiceServer},
    AddNodeRequest, AddNodeResponse,
    RemoveNodeRequest, RemoveNodeResponse,
    UpdateNodeStatusRequest, UpdateNodeStatusResponse, NodeStatusMessage,
    ListNodesRequest, ListNodesResponse,
    GetNodeRequest, GetNodeResponse,
    TriggerRebalanceRequest, TriggerRebalanceResponse,
    NodeIdMessage, NodeDefinitionMessage,
    trigger_rebalance_request, // For the oneof enum
};

// Types from cluster_manager and catalog
use influxdb3_cluster_manager::{
    ClusterManager, RebalanceOrchestrator, RebalanceStrategy,
    NodeInfoProvider, // Will be needed by RebalanceOrchestrator
};
use influxdb3_catalog::management::{
    NodeId as CatalogNodeId, // u64
    NodeDefinition as CatalogNodeDefinition,
    NodeStatus as CatalogNodeStatus,
};
use influxdb3_catalog::catalog::Catalog; // For Arc<Catalog>

// This module will include the generated code from the .proto file.
pub mod generated_cluster_management {
    tonic::include_proto!("influxdb3.internal.management.v1");
}

// Placeholder Mock NodeInfoProvider for now, until it's moved to a shared location
// This is needed because RebalanceOrchestrator expects an Arc<dyn NodeInfoProvider>.
// In a real server setup, a concrete implementation would be provided.
#[derive(Debug, Clone, Default)]
struct ServerMockNodeInfoProvider {}
impl NodeInfoProvider for ServerMockNodeInfoProvider {
    fn get_node_query_rpc_address(&self, _node_id: &CatalogNodeId) -> Option<String> { None }
    fn get_node_management_rpc_address(&self, _node_id: &CatalogNodeId) -> Option<String> { None }
}


#[derive(Debug)]
pub struct ClusterManagementServerImpl {
    cluster_manager: Arc<ClusterManager>,
    rebalance_orchestrator: Arc<RebalanceOrchestrator>,
}

impl ClusterManagementServerImpl {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        let node_info_provider: Arc<dyn NodeInfoProvider> = Arc::new(ServerMockNodeInfoProvider::default());

        Self {
            cluster_manager: Arc::new(ClusterManager::new(Arc::clone(&catalog))),
            rebalance_orchestrator: Arc::new(RebalanceOrchestrator::new(catalog, node_info_provider)),
        }
    }
}

// Helper to convert from proto NodeStatusMessage to catalog NodeStatus
fn convert_proto_status_to_catalog(proto_status: i32) -> Result<CatalogNodeStatus, Status> {
    match NodeStatusMessage::try_from(proto_status) {
        Ok(NodeStatusMessage::NodeStatusJoining) => Ok(CatalogNodeStatus::Joining),
        Ok(NodeStatusMessage::NodeStatusActive) => Ok(CatalogNodeStatus::Active),
        Ok(NodeStatusMessage::NodeStatusLeaving) => Ok(CatalogNodeStatus::Leaving),
        Ok(NodeStatusMessage::NodeStatusDown) => Ok(CatalogNodeStatus::Down),
        Ok(NodeStatusMessage::NodeStatusUnknown) | Ok(NodeStatusMessage::NodeStatusUnspecified) => Ok(CatalogNodeStatus::Unknown),
        Err(_) => Err(Status::invalid_argument(format!("Invalid NodeStatus value: {}", proto_status))),
    }
}

// Helper to convert from catalog NodeStatus to proto NodeStatusMessage
fn convert_catalog_status_to_proto(catalog_status: CatalogNodeStatus) -> NodeStatusMessage {
    match catalog_status {
        CatalogNodeStatus::Joining => NodeStatusMessage::NodeStatusJoining,
        CatalogNodeStatus::Active => NodeStatusMessage::NodeStatusActive,
        CatalogNodeStatus::Leaving => NodeStatusMessage::NodeStatusLeaving,
        CatalogNodeStatus::Down => NodeStatusMessage::NodeStatusDown,
        CatalogNodeStatus::Unknown => NodeStatusMessage::NodeStatusUnknown,
    }
}

// Helper to convert from catalog NodeDefinition to proto NodeDefinitionMessage
fn convert_catalog_node_def_to_proto(catalog_def: Arc<CatalogNodeDefinition>) -> NodeDefinitionMessage {
    NodeDefinitionMessage {
        id: catalog_def.id.get(),
        node_name: catalog_def.node_name.to_string(),
        instance_id: catalog_def.instance_id.to_string(),
        rpc_address: catalog_def.rpc_address.clone(),
        http_address: catalog_def.http_address.clone(),
        // mode: vec![], // TODO: map mode if defined in proto and catalog
        core_count: catalog_def.core_count,
        status: convert_catalog_status_to_proto(catalog_def.status) as i32,
        last_heartbeat_ns: catalog_def.last_heartbeat,
    }
}


#[tonic::async_trait]
impl ClusterManagementService for ClusterManagementServerImpl {
    async fn add_node(&self, request: Request<AddNodeRequest>) -> Result<Response<AddNodeResponse>, Status> {
        info!("gRPC AddNode request: {:?}", request);
        let req_node_def_msg = request.into_inner().node_definition
            .ok_or_else(|| Status::invalid_argument("NodeDefinition is required"))?;

        // Convert proto NodeDefinitionMessage to catalog NodeDefinition
        // The catalog's add_node method might generate the ID if not provided, or expect it.
        // For now, assume ID is provided from request or is defaulted if 0.
        let id_to_use = if req_node_def_msg.id == 0 {
            self.cluster_manager.catalog.next_node_id() // Conceptual: get next ID
        } else {
            CatalogNodeId::new(req_node_def_msg.id)
        };

        let catalog_node_def = CatalogNodeDefinition {
            id: id_to_use,
            node_name: Arc::from(req_node_def_msg.node_name),
            instance_id: Arc::from(req_node_def_msg.instance_id),
            rpc_address: req_node_def_msg.rpc_address,
            http_address: req_node_def_msg.http_address,
            mode: vec![], // TODO: Map mode from proto if added
            core_count: req_node_def_msg.core_count,
            status: convert_proto_status_to_catalog(req_node_def_msg.status)?,
            last_heartbeat: req_node_def_msg.last_heartbeat_ns,
        };

        match self.cluster_manager.add_node(catalog_node_def).await {
            Ok(_) => Ok(Response::new(AddNodeResponse { success: true, error_message: None })),
            Err(e) => Ok(Response::new(AddNodeResponse { success: false, error_message: Some(e.to_string()) })),
        }
    }

    async fn remove_node(&self, request: Request<RemoveNodeRequest>) -> Result<Response<RemoveNodeResponse>, Status> {
        info!("gRPC RemoveNode request: {:?}", request);
        let node_id_msg = request.into_inner().node_id
            .ok_or_else(|| Status::invalid_argument("NodeIdMessage is required"))?;
        let catalog_node_id = CatalogNodeId::new(node_id_msg.id);

        match self.cluster_manager.remove_node(&catalog_node_id).await {
            Ok(_) => Ok(Response::new(RemoveNodeResponse { success: true, error_message: None })),
            Err(e) => Ok(Response::new(RemoveNodeResponse { success: false, error_message: Some(e.to_string()) })),
        }
    }

    async fn update_node_status(&self, request: Request<UpdateNodeStatusRequest>) -> Result<Response<UpdateNodeStatusResponse>, Status> {
        info!("gRPC UpdateNodeStatus request: {:?}", request);
        let req = request.into_inner();
        let node_id_msg = req.node_id.ok_or_else(|| Status::invalid_argument("NodeIdMessage is required"))?;
        let catalog_node_id = CatalogNodeId::new(node_id_msg.id);
        let catalog_status = convert_proto_status_to_catalog(req.status)?;

        match self.cluster_manager.update_node_status(&catalog_node_id, catalog_status).await {
            Ok(_) => Ok(Response::new(UpdateNodeStatusResponse { success: true, error_message: None })),
            Err(e) => Ok(Response::new(UpdateNodeStatusResponse { success: false, error_message: Some(e.to_string()) })),
        }
    }

    async fn list_nodes(&self, request: Request<ListNodesRequest>) -> Result<Response<ListNodesResponse>, Status> {
        info!("gRPC ListNodes request: {:?}", request);
        match self.cluster_manager.list_nodes().await {
            Ok(nodes) => {
                let proto_nodes = nodes.into_iter().map(convert_catalog_node_def_to_proto).collect();
                Ok(Response::new(ListNodesResponse { nodes: proto_nodes }))
            }
            Err(e) => Err(Status::internal(format!("Failed to list nodes: {}", e))),
        }
    }

    async fn get_node(&self, request: Request<GetNodeRequest>) -> Result<Response<GetNodeResponse>, Status> {
        info!("gRPC GetNode request: {:?}", request);
        let node_id_msg = request.into_inner().node_id
            .ok_or_else(|| Status::invalid_argument("NodeIdMessage is required"))?;
        let catalog_node_id = CatalogNodeId::new(node_id_msg.id);

        match self.cluster_manager.get_node(&catalog_node_id).await {
            Ok(Some(node_def)) => Ok(Response::new(GetNodeResponse { node_definition: Some(convert_catalog_node_def_to_proto(node_def)) })),
            Ok(None) => Ok(Response::new(GetNodeResponse { node_definition: None })),
            Err(e) => Err(Status::internal(format!("Failed to get node: {}", e))),
        }
    }

    async fn trigger_rebalance(&self, request: Request<TriggerRebalanceRequest>) -> Result<Response<TriggerRebalanceResponse>, Status> {
        info!("gRPC TriggerRebalance request: {:?}", request);
        let req_oneof_strategy = request.into_inner().strategy
            .ok_or_else(|| Status::invalid_argument("Rebalance strategy is required"))?;

        let strategy = match req_oneof_strategy {
            trigger_rebalance_request::Strategy::AddNode(add_node_req) => {
                let node_def_msg = add_node_req.node_definition
                    .ok_or_else(|| Status::invalid_argument("NodeDefinition is required for AddNode strategy"))?;

                let id_to_use = if node_def_msg.id == 0 {
                    self.cluster_manager.catalog.next_node_id()
                } else {
                    CatalogNodeId::new(node_def_msg.id)
                };

                let catalog_node_def = CatalogNodeDefinition {
                    id: id_to_use,
                    node_name: Arc::from(node_def_msg.node_name),
                    instance_id: Arc::from(node_def_msg.instance_id),
                    rpc_address: node_def_msg.rpc_address,
                    http_address: node_def_msg.http_address,
                    mode: vec![],
                    core_count: node_def_msg.core_count,
                    status: convert_proto_status_to_catalog(node_def_msg.status)?,
                    last_heartbeat: node_def_msg.last_heartbeat_ns,
                };
                RebalanceStrategy::AddNewNode(catalog_node_def)
            }
            trigger_rebalance_request::Strategy::DecommissionNode(decom_node_req) => {
                let node_id_msg = decom_node_req.node_id
                    .ok_or_else(|| Status::invalid_argument("NodeId is required for DecommissionNode strategy"))?;
                RebalanceStrategy::DecommissionNode(CatalogNodeId::new(node_id_msg.id))
            }
        };

        match self.rebalance_orchestrator.initiate_rebalance(strategy).await {
            Ok(_) => Ok(Response::new(TriggerRebalanceResponse { success: true, message: "Rebalance initiated successfully".to_string() })),
            Err(e) => Ok(Response::new(TriggerRebalanceResponse { success: false, message: format!("Failed to initiate rebalance: {}", e) })),
        }
    }
}
