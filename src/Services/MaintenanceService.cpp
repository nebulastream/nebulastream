/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
//
// Created by balint on 28.02.21.
//

#include "Services/MaintenanceService.hpp"
#include <Catalogs/QueryCatalog.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <optional>

namespace NES {
MaintenanceService::MaintenanceService(TopologyPtr topology, QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue,
                                       GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRPCClient):
                                       topology{topology}, queryCatalog{queryCatalog},
                                       queryRequestQueue{queryRequestQueue}, globalExecutionPlan{globalExecutionPlan}, workerRPCClient{workerRPCClient}
{
    NES_DEBUG("MaintenanceService: Initializing");
};
MaintenanceService::~MaintenanceService() {
    NES_DEBUG("Destroying MaintenanceService");
}
void MaintenanceService::submitMaintenanceRequest(uint64_t nodeId, uint8_t strategy){
    NES_DEBUG("In MaintenanceService and doing Stuff!");
    if(strategy == 1){
         firstStrat(nodeId);
    }
    else if(strategy == 2){
         secondStrat(nodeId);
    }
    else if (strategy == 3){
         thirdStrat(nodeId);
    }
    else{
        NES_DEBUG("Not a valid strategy");
        throw std::runtime_error("MaintenanceService: Strategy doesnt exist");
    }
}



std::vector<uint64_t> MaintenanceService::firstStrat(uint64_t nodeId) {
    NES_DEBUG("Applying first strategy for maintenance on node" << std::to_string(nodeId));
    std::vector<uint64_t> parentQueryIds;
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    markNodeForMaintenance(nodeId);
    ExecutionNodePtr executionNode = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
    if(!executionNode){
        NES_DEBUG("No queries deployed on node with id " << std::to_string(nodeId));
        return parentQueryIds;
    }
    auto map = executionNode->getAllQuerySubPlans();

    for(auto it: map){
        for(auto it2: it.second){
          uint64_t id = it2->getQueryId();
           if(std::find(parentQueryIds.begin(), parentQueryIds.end(),id) == parentQueryIds.end()){
               parentQueryIds.push_back(id);
           }
        }
    }
    for(auto id : parentQueryIds){
        queryCatalog->markQueryAs(id,QueryStatus::Restart);
        queryRequestQueue->add(queryCatalog->getQueryCatalogEntry(id));
    }
    auto  ID = parentQueryIds.front();
    NES_DEBUG("ID of first query :" << std::to_string(ID));


    return parentQueryIds;
}
std::optional<TopologyNodePtr> MaintenanceService::secondStrat(uint64_t nodeId) {

    NES_DEBUG("Applying second strategy for maintenance on node" << std::to_string(nodeId));
    std::optional<TopologyNodePtr> destinatonTopNode;
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }

    auto markedExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
    auto mapOfQuerySubPlans = markedExecutionNode->getAllQuerySubPlans();
    //for each query on node marked for maintenance, we need to find the parent execution node; the execution node to which
    //the current execution Node as part of a specific query is sending data to
    // we need this to find a valid new node location, which must be reachable from current exec node and parent exec node
    //TODO: dont throw runtime error. Try to migrate every subquery and just return success/failure + reason status
    for(auto entry: mapOfQuerySubPlans) {
        auto parentQueryId = entry.first;
        auto parentOfMarkedExecutionNode = findParentExecutionNode(markedExecutionNode, parentQueryId);
        auto childOfMarkedExecutionNode =  findChildExecutionNode(markedExecutionNode, parentQueryId );
        if (!parentOfMarkedExecutionNode) {
            //TODO: proper error handling
            throw std::runtime_error("Execution node marked for maintenance doesnt have a parent Execution Node. It is the sink of the query. Marking this node for maintenance leads to query failure");

        }
        if (!childOfMarkedExecutionNode) {
            //TODO: proper error handling
            throw std::runtime_error("Execution node marked for maintenance doesnt have a child Execution Node. It is the source of the query. Marking this node for maintenance leads to query failure");

        }
        uint32_t occupiedResources = markedExecutionNode->getOccupiedResources(parentQueryId);
        //TODO: currently only find one path. need to find all paths between
        auto candidateValidNodes = topology->findNodesBetween(childOfMarkedExecutionNode->get()->getTopologyNode(),parentOfMarkedExecutionNode->get()->getTopologyNode());
        auto selectedTopNode = findValidTopologyNode(occupiedResources, candidateValidNodes);
        if (!selectedTopNode) {
            //TODO: proper error handling
            throw std::runtime_error("No valid topology nodes to migrate queries on to");
        }
        migrateSubqueries(selectedTopNode->get()->getId(),parentQueryId,entry.second,occupiedResources);

    }

    return destinatonTopNode;
}

std::vector<uint64_t> MaintenanceService::thirdStrat(uint64_t nodeId) {
    std::vector<uint64_t> placeholder;
    NES_DEBUG("Applying third strategy for maintenance on node" << std::to_string(nodeId));
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    return placeholder;
}
std::optional<ExecutionNodePtr> MaintenanceService::findParentExecutionNode(ExecutionNodePtr markedNode, QueryId queryId) {
    auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    std::vector<NodePtr> parentsOfMarkedExecutionNode = markedNode->getParents();
    bool found = false;
    std::optional<ExecutionNodePtr> parentOfMarkedExecutionNode;
    for (ExecutionNodePtr candidateExecutionNode : executionNodes) {
        if (found) {
            break;
        }
        for (NodePtr parentOfExecutionNode : parentsOfMarkedExecutionNode) {
            if (candidateExecutionNode->equal(parentOfExecutionNode)) {
                parentOfMarkedExecutionNode = candidateExecutionNode;
                found = true;
                break;
            }
        }
    }
    return parentOfMarkedExecutionNode;
}

std::optional<ExecutionNodePtr> MaintenanceService::findChildExecutionNode(ExecutionNodePtr markedNode, QueryId queryId) {
    auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    std::vector<NodePtr> childrenOfMarkedExecutionNode = markedNode->getChildren();
    bool found = false;
    std::optional<ExecutionNodePtr> childOfMarkedExecutionNode;
    for (ExecutionNodePtr candidateExecutionNode : executionNodes) {
        if (found) {
            break;
        }
        for (NodePtr childOfExecutionNode : childrenOfMarkedExecutionNode) {
            if (candidateExecutionNode->equal(childOfExecutionNode)) {
                childOfMarkedExecutionNode = candidateExecutionNode;
                found = true;
                break;
            }
        }
    }
    return childOfMarkedExecutionNode;
}


std::optional<TopologyNodePtr> MaintenanceService::findValidTopologyNode(uint32_t resourceUsage, std::vector<TopologyNodePtr> candidateNodes){
    //findNodesBetween is inclusive. First element is source and last is destination. These must be skipped
    std::optional<TopologyNodePtr> topNode;
    for (int i = 1; i < candidateNodes.size()-1 ; ++i) {
        if(candidateNodes[i]->getAvailableResources()>= resourceUsage){
            topNode = candidateNodes[i];
            break;
        }
    }
    return topNode;
}


bool MaintenanceService::markNodeForMaintenance(uint64_t nodeId) {
    TopologyNodePtr node = topology->findNodeWithId(nodeId);
    bool maintenanceFlag = node->getMaintenanceFlag();
    if(maintenanceFlag){
        NES_DEBUG("MaintenanceService: Node with id " << nodeId << " already marked for maintenance");
        return true;
    }
    NES_DEBUG("Setting maintenance flag for node with id: " + std::to_string(nodeId));
    node->setMaintenanceFlag(true);
    return true;
}
void MaintenanceService::migrateSubqueries(uint64_t topNodeId, QueryId queryId, std::vector<queryPlanPtr> querySubPlans, uint32_t resourceUsage) {
            ExecutionNodePtr candidateExecutionNode = getExecutionNode(topNodeId);
            //TODO: currently, candidateExecutionNode cant contain querySubPlans from QueryID. But if it can, changes must be made here
            for(auto subPlan : querySubPlans){
                candidateExecutionNode->addNewQuerySubPlan(queryId,subPlan);
            }
            globalExecutionPlan->addExecutionNode(candidateExecutionNode);
            //TODO: update operatorToExecutionNodeMap in BasePlacementStrategy. This maps OP IDs to ExecutionNode IDs
            //TODO: should be done in combination with removing query subplans from old ExecutionNode
            //TODO: passing uint32_t to a function that works on uint16_t -> should fix. Write new function that takes in uint32_t
            topology->findNodeWithId(topNodeId)->reduceResources(resourceUsage);
            topology->reduceResources(topNodeId,resourceUsage);
            NES_DEBUG("Id: " <<topNodeId);
}

ExecutionNodePtr MaintenanceService::getExecutionNode(uint64_t candidateTopologyNode){
    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode)) {
        NES_TRACE("BasePlacementStrategy: node " << candidateTopologyNode << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateTopologyNode);
    } else {
        NES_TRACE("BasePlacementStrategy: create new execution node with id: " << candidateTopologyNode);
        candidateExecutionNode = ExecutionNode::createExecutionNode(topology->findNodeWithId(candidateTopologyNode));
    }
    return candidateExecutionNode;
}
bool MaintenanceService::deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryDeploymentPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress);

        for (auto& querySubPlan : querySubPlans) {
            //enable this for sync calls
            //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
            bool success = workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
            if (success) {
                NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
        completionQueues[queueForExecutionNode] = querySubPlans.size();
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Register);
    NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id " << queryId << " success=" << result);
    return result;
}

}//namespace NES