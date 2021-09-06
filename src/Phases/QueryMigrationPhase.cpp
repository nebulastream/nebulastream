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
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryMigrationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
#include <utility>
namespace NES {

QueryMigrationPhase::QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                         WorkerRPCClientPtr workerRPCClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase) : workerRPCClient(std::move(workerRPCClient)),topology(std::move(std::move(topology))),
                                                                               globalExecutionPlan(std::move(globalExecutionPlan)),queryPlacementPhase(std::move(queryPlacementPhase))
                                                                                {
                                                                                   NES_DEBUG("QueryMigrationPhase()");
                                                                                }

QueryMigrationPhasePtr QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,TopologyPtr topology,
                                                     WorkerRPCClientPtr workerRpcClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(std::move(globalExecutionPlan), std::move(topology), std::move(workerRpcClient), std::move(queryPlacementPhase)));
}


bool QueryMigrationPhase::execute(const MigrateQueryRequestPtr& req) {

    TopologyNodeId markedTopNodeId = req->getTopologyNode();
    QueryId queryId = req->getQueryId();
    //check if path exists
    auto path = findPath(queryId,markedTopNodeId);
    if(path.empty()){
        NES_INFO("QueryMigrationPhase: no path found between child and parent nodes. No path to migrate subqueries to");
        return false;
    }

    auto markedExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(markedTopNodeId);
    //subqueries of a query that need to be migrated
    auto subqueries = markedExecutionNode->getQuerySubPlans(queryId);
    globalExecutionPlan->removeExecutionNode(markedTopNodeId);
    if(req->isWithBuffer()){
        return executeMigrationWithBuffer(subqueries, markedExecutionNode );
    }
            return executeMigrationWithoutBuffer(subqueries, markedExecutionNode);

}

bool QueryMigrationPhase::executeMigrationWithBuffer(std::vector<QueryPlanPtr>& queryPlans, const ExecutionNodePtr& markedNode) {

    auto queryId = queryPlans[0]->getQueryId();

    for (const auto& queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        std::map<OperatorId, QueryMigrationPhase::InformationForFindingSink>  mapOfNetworkSourceIdToInfoForFindingNetworkSink = getInfoForAllSinks(sourceOperators, queryId);
        sendBufferRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink);
        std::vector<ExecutionNodePtr> exeNodes = executePartialPlacement(queryPlan);
        bool deploySuccess = deployQuery(queryId,exeNodes);
        if(!deploySuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        bool startSuccess = startQuery(queryId,exeNodes);
        if(!startSuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        sendReconfigurationRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink, queryId,exeNodes);
        bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
        if(!success){
            throw Exception("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
        }
    }
    globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
    return true;
}

bool QueryMigrationPhase::executeMigrationWithoutBuffer(const std::vector<QueryPlanPtr>& queryPlans,const ExecutionNodePtr& markedNode) {

    auto queryId = queryPlans[0]->getQueryId();

    for (const auto& queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        std::map<OperatorId, QueryMigrationPhase::InformationForFindingSink>  mapOfNetworkSourceIdToInfoForFindingNetworkSink = getInfoForAllSinks(sourceOperators, queryId);
        std::vector<ExecutionNodePtr> exeNodes = executePartialPlacement(queryPlan);
        bool deploySuccess = deployQuery(queryId,exeNodes);
        if(!deploySuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        bool startSuccess = startQuery(queryId,exeNodes);
        if(!startSuccess){
            NES_DEBUG("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
            return false;
        }
        sendReconfigurationRequests(mapOfNetworkSourceIdToInfoForFindingNetworkSink, queryId,exeNodes);
        bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
        if(!success){
            throw Exception("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
        }
    }
    globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
    return true;
}

std::vector<TopologyNodePtr> QueryMigrationPhase::findPath(QueryId queryId, TopologyNodeId topologyNodeId) {
    auto childNodes = findChildExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    auto parentNodes = findParentExecutionNodesAsTopologyNodes(queryId, topologyNodeId);
    return topology->findPathBetween(childNodes,parentNodes);
}

std::vector<TopologyNodePtr> QueryMigrationPhase::findChildExecutionNodesAsTopologyNodes(QueryId queryId,TopologyNodeId topologyNodeId) {
    std::vector<TopologyNodePtr>childNodes = {};
    auto allExecutionNodesInvolvedInAQuery = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto allChildNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getChildren();
    for(const auto& executionNode : allExecutionNodesInvolvedInAQuery){
        for(const auto& node: allChildNodesOfTopologyNode){
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
    auto allParentNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getParents();
    for(const auto& executionNode : allExecutionNodesInvolvedInAQuery){
        auto id = executionNode->getId();
        auto it = std::find_if(allParentNodesOfTopologyNode.begin(), allParentNodesOfTopologyNode.end(), [id](NodePtr& node){

          return id == (node->as<TopologyNode>()->getId());
        });
        if(it != allParentNodesOfTopologyNode.end()){
            parentNodes.push_back(it->get()->as<TopologyNode>());
        }
    }
    return parentNodes;
}
TopologyNodePtr QueryMigrationPhase::findNewNodeLocationOfNetworkSource(QueryId queryId, OperatorId sourceOperatorId,std::vector<ExecutionNodePtr>& potentialLocations) {
    for (const auto& exeNode : potentialLocations) {
        const auto& querySubPlans = exeNode->getQuerySubPlans(queryId);
        for (const auto& plan : querySubPlans) {
            auto sourceOperators = plan->getSourceOperators();
            for(const auto& op : sourceOperators){
                auto id = op->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition().getOperatorId();
                if(id== sourceOperatorId){
                    return exeNode->getTopologyNode();
                }
            }
        }
    }
    return {};
}
std::map<OperatorId,QueryMigrationPhase::InformationForFindingSink> QueryMigrationPhase::getInfoForAllSinks(const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators, QueryId queryId) {
    std::map<OperatorId,QueryMigrationPhase::InformationForFindingSink> mapOfNetworkSourceIdToInfoForFindingNetworkSink;
    for(const SourceLogicalOperatorNodePtr& sourceOperator:sourceOperators){
        auto networkSourceDescriptor = sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>();
        TopologyNodeId sinkTopolgoyNodeId = networkSourceDescriptor->getSinkTopologyNode();
        TopologyNodePtr sinkTopologyNode = topology->findNodeWithId(sinkTopolgoyNodeId);
        //A pair of NetworkSource and Sink "share" a NESPartition. They each hold their own individual partition but are paired together once a Channel is established.
        //This pairing, in the end, is done over the OperatorId, which is also the OperatorId of the NetworkSink.
        //We make use of this fact to find the NetworkSink that will be told to bufferData/reconfigure
        //In the future, an ID for a NetworkSink/Source pair would be useful
        uint64_t partitionIdentifier = networkSourceDescriptor->getNesPartition().getOperatorId();
        SinkLogicalOperatorNodePtr sinkOperator;
        QuerySubPlanId qspOfNetworkSink;
        auto querySubPlansOnSinkTopologyNode = globalExecutionPlan->getExecutionNodeByNodeId(sinkTopolgoyNodeId)->getQuerySubPlans(queryId);
        for (const auto& plan : querySubPlansOnSinkTopologyNode) {
            auto sinkOperators = plan->getSinkOperators();
            bool found = false;
            for (const auto& sink : sinkOperators) {
                auto id = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNesPartition().getOperatorId();
                if (id == partitionIdentifier) {
                    sinkOperator = sink;
                    qspOfNetworkSink = plan->getQuerySubPlanId();
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (!sinkOperator) {
            NES_DEBUG("QueryMigrationPhase: No matching NetworkSink found to NetworkSource with partitionIdentifier "
            << partitionIdentifier);
            throw Exception("QueryMigratrionPhase: Couldnt find mathcing NetworkSink.");
        }
        OperatorId globalOperatorIdOfNetworkSink = sinkOperator->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getGlobalId();
        QueryMigrationPhase::InformationForFindingSink info{};
        info.sinkTopologyNode = sinkTopolgoyNodeId;
        info.querySubPlanOfNetworkSink = qspOfNetworkSink;
        info.globalOperatorIdOfNetworkSink = globalOperatorIdOfNetworkSink;
        mapOfNetworkSourceIdToInfoForFindingNetworkSink[partitionIdentifier] = info;
    }
    return mapOfNetworkSourceIdToInfoForFindingNetworkSink;
}
bool QueryMigrationPhase::sendBufferRequests(std::map<OperatorId, QueryMigrationPhase::InformationForFindingSink> map) {
    for(auto entry: map){
        auto info = entry.second;
        TopologyNodePtr sinkNode = topology->findNodeWithId(info.sinkTopologyNode);
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        //mapping on worker side must be done using globalId.
        workerRPCClient->bufferData(rpcAddress,info.querySubPlanOfNetworkSink,info.globalOperatorIdOfNetworkSink);
    }
    //TODO: error handling
    return true;
}
std::vector<ExecutionNodePtr> QueryMigrationPhase::executePartialPlacement(QueryPlanPtr queryPlan) {
    std::unordered_set<ExecutionNodePtr> executionNodesCreatedForSubquery =
        queryPlacementPhase->executePartialPlacement("BottomUp", std::move(queryPlan));
    std::vector<ExecutionNodePtr> exeNodes(executionNodesCreatedForSubquery.begin(), executionNodesCreatedForSubquery.end());
    return exeNodes;
}
bool QueryMigrationPhase::sendReconfigurationRequests(std::map<OperatorId,QueryMigrationPhase::InformationForFindingSink>& map, uint64_t queryId, std::vector<ExecutionNodePtr>& exeNodes) {
    for(auto entry: map){
        auto partitionId = entry.first;
        auto info = entry.second;
        TopologyNodePtr sinkNode = topology->findNodeWithId(info.sinkTopologyNode);
        TopologyNodePtr newSourceTopologyNode = findNewNodeLocationOfNetworkSource(queryId, partitionId, exeNodes);
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        workerRPCClient->updateNetworkSink(rpcAddress,
                                           newSourceTopologyNode->getId(),
                                           newSourceTopologyNode->getIpAddress(),
                                           newSourceTopologyNode->getDataPort(),
                                           info.querySubPlanOfNetworkSink,
                                           info.globalOperatorIdOfNetworkSink);
    }
    //TODO: add error handling
    return true;
}
bool QueryMigrationPhase::deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
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

bool QueryMigrationPhase::startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
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