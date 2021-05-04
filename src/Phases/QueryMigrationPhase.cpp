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
// Created by balint on 17.04.21.
//
#include <Exceptions/QueryMigrationException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Phases/QueryMigrationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
namespace NES {

QueryMigrationPhase::QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                         WorkerRPCClientPtr workerRPCClient) : globalExecutionPlan(globalExecutionPlan), topology(topology),
                                                                               workerRPCClient(workerRPCClient){
                                                                                   NES_DEBUG("QueryMigrationPhase()");
}

QueryMigrationPhase::~QueryMigrationPhase() { NES_DEBUG("~QueryDeploymentPhase()"); }

QueryMigrationPhasePtr QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,TopologyPtr topology,
                                                     WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(globalExecutionPlan, topology, workerRpcClient));
}

bool QueryMigrationPhase::execute(MigrateQueryRequestPtr req) {
    auto queryId = req->getQueryId();
    auto topNode = req->getTopologyNode();
    auto childNodes = findChildExecutionNodesAsTopologyNodes(queryId,topNode);
    auto parentNodes = findParentExecutionNodesAsTopologyNodes(queryId,topNode);
    auto path = topology->findPathBetween(childNodes,parentNodes);
    auto subqueries = globalExecutionPlan->getExecutionNodeByNodeId(topNode)->getQuerySubPlans(queryId);

    NES_DEBUG(req->toString());
    if(path.size() == 0){
        NES_INFO("QueryMigrationPhase: no path found between child and parent nodes. No path to migrate subqueries to");
        return false;
    }
    bool successful = migrateSubqueries(path,subqueries);
    return successful;
}

std::vector<TopologyNodePtr> QueryMigrationPhase::findChildExecutionNodesAsTopologyNodes(QueryId queryId,TopologyNodeId topologyNodeId) {
    std::vector<TopologyNodePtr>childNodes = {};
    auto allExecutionNodesInvolvedInAQuery = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto allChildNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getChildren();
    for(auto executionNode : allExecutionNodesInvolvedInAQuery){
        for(auto node: allChildNodesOfTopologyNode){
            auto nodeAsTopologyNode =node->as<TopologyNode>();
            if (executionNode->getId() == nodeAsTopologyNode->getId() ){
                childNodes.push_back(nodeAsTopologyNode);
            }
        }
    }
    return childNodes;
}

std::vector<TopologyNodePtr> QueryMigrationPhase::findParentExecutionNodesAsTopologyNodes(QueryId queryId,TopologyNodeId topologyNodeId) {
    std::vector<TopologyNodePtr>parentNodes = {};
    auto allExecutionNodesInvolvedInAQuery = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto allChildNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getParents();
    for(auto executionNode : allExecutionNodesInvolvedInAQuery){
        for(auto node: allChildNodesOfTopologyNode){
            auto nodeAsTopologyNode =node->as<TopologyNode>();
            if (executionNode->getId() == nodeAsTopologyNode->getId() ){
                parentNodes.push_back(nodeAsTopologyNode);
            }
        }
    }
    return parentNodes;
}

bool QueryMigrationPhase::migrateSubqueries(std::vector<TopologyNodePtr> candidateTopologyNodes,
                                            std::vector<QueryPlanPtr> queryPlans) {
    //TODO: calculate resource usage of subqueries
    auto queryId = queryPlans[0]->getQueryId();
    auto childOfNodeMarkedForMaintenance = candidateTopologyNodes.at(0);
    auto candidateTopologyNode = candidateTopologyNodes.at(0)->getParents().at(0)->as<TopologyNode>();
    auto exNode = getExecutionNode(candidateTopologyNode->getId());

    uint64_t nodeId = candidateTopologyNode->getId();
    std::string hostname = candidateTopologyNode->getIpAddress();
    uint32_t port = candidateTopologyNode->getDataPort();
    //auto map = buildNetworkSinks(queryPlans,queryId,candidateTopologyNode);
    auto ipAddress =  childOfNodeMarkedForMaintenance->getIpAddress();
    auto grpcPort =  childOfNodeMarkedForMaintenance->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
    std::vector<OperatorId> destinationOperators;
    std::vector<QuerySubPlanId> querySubPlanIds;
    for (auto& queryPlan : queryPlans){
        querySubPlanIds.push_back(queryPlan->getQueryId());
        std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        if(sinkOperators.size()< 1){
            throw QueryMigrationException(queryId, "QuerySubPlan " + std::to_string(queryPlan->getQueryId()) + "has more than one Sink! Currently not supported" );
        }
        exNode->addNewQuerySubPlan(queryPlan->getQueryId(),queryPlan);
        OperatorId destinationOperator = sinkOperators.at(0)->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNesPartition().getOperatorId();
        destinationOperators.push_back(destinationOperator);
    }






    workerRPCClient->updateNetworkSinks(rpcAddress, queryId,querySubPlanIds,nodeId,hostname,port,destinationOperators);

    //for every QuerySubPlan, change the networkSink to point to new node
    auto deploySuccess = deployQuery(queryPlans.at(0)->getQueryId(), {exNode});
    auto startSuccess =  startQuery(queryPlans.at(0)->getQueryId(),{exNode});
    if(deploySuccess&&startSuccess){
        return true;
    }
    return false;

}


ExecutionNodePtr QueryMigrationPhase::getExecutionNode(TopologyNodeId nodeId) {
    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(nodeId)) {
        NES_TRACE("QueryMigrationPhase: node " << nodeId << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
    } else {
        NES_TRACE("QueryMigrationPhase: create new execution node with id: " << nodeId);
        candidateExecutionNode = ExecutionNode::createExecutionNode(topology->findNodeWithId(nodeId));
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);
    }
    return candidateExecutionNode;
}







































































bool QueryMigrationPhase::deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
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

bool QueryMigrationPhase::startQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (ExecutionNodePtr executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id=" << executionNode->getId()

                                                                                << " and IP=" << ipAddress);
        //enable this for sync calls
        //bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        bool success = workerRPCClient->startQueryAsyn(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    bool result = workerRPCClient->checkAsyncResult(completionQueues, Start);
    NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id " << queryId << " success=" << result);
    return result;
}

}//namespace NES