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
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
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
namespace NES {

QueryMigrationPhase::QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                         WorkerRPCClientPtr workerRPCClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase) : workerRPCClient(workerRPCClient),topology(topology),
                                                                               globalExecutionPlan(globalExecutionPlan),queryPlacementPhase(queryPlacementPhase)
                                                                                {
                                                                                   NES_DEBUG("QueryMigrationPhase()");
                                                                                }

QueryMigrationPhasePtr QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,TopologyPtr topology,
                                                     WorkerRPCClientPtr workerRpcClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(globalExecutionPlan, topology, workerRpcClient, queryPlacementPhase));
}


bool QueryMigrationPhase::execute(MigrateQueryRequestPtr req) {

    //Following steps are done first regardless of strategy
    //Step 1. Find a path between all child and parent execution nodes
    TopologyNodeId markedTopNodeId = req->getTopologyNode();
    QueryId queryId = req->getQueryId();
    auto path = findPath(queryId,markedTopNodeId);
    if(path.empty()){
        NES_INFO("QueryMigrationPhase: no path found between child and parent nodes. No path to migrate subqueries to");
        return false;
    }

    // Step 2. Build all necessary execution nodes
    auto markedExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(markedTopNodeId);
    auto subqueries = markedExecutionNode->getQuerySubPlans(queryId);
    globalExecutionPlan->removeExecutionNode(markedTopNodeId);
    //Both migration types have many steps in common, but are ordered differently
    if(req->isWithBuffer()){
        return executeMigrationWithBuffer(subqueries, markedExecutionNode );
    }
    else{
        return executeMigrationWithoutBuffer(subqueries, markedExecutionNode);
    }
}

bool QueryMigrationPhase::executeMigrationWithBuffer(std::vector<QueryPlanPtr>& queryPlans, ExecutionNodePtr markedNode) {

    auto queryId = queryPlans[0]->getQueryId();

    for (auto queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.size() > 1) {
            NES_DEBUG("QueryMigrationPhase: partial placement currently only supports stateless operators."
                      " As such QuerySubPlans can only have one NetworkSource operator. QuerySubPlan "
                      << queryPlan->getQuerySubPlanId() << " , however,has " << sourceOperators.size());
            throw Exception("QueryMigrationPhase: partial placement currently only support stateless operators");
        }
        //Find TopologyNode, QuerySubPlanId and OperatorId of corresponding NetworkSink
        auto sourceOperator = sourceOperators[0];
        auto networkSourceDescriptor = sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>();
        TopologyNodeId sinkTopolgoyNodeId = networkSourceDescriptor->getSinkTopologyNode();
        TopologyNodePtr sinkNode = topology->findNodeWithId(sinkTopolgoyNodeId);
        //A pair of NetworkSource and Sink "share" a NESPartition. They each hold their own individual partition but are paired together once a Channel is established.
        //This pairing, in the end, is done over the OperatorId, which is also the OperatorId of the NetworkSink.
        //We make use of this fact to find the NetworkSink that will be told to bufferData/reconfigure
        //In the future, an ID for a NetworkSink/Source pair would be useful
        uint64_t partitionIdentifier = networkSourceDescriptor->getNesPartition().getOperatorId();
        SinkLogicalOperatorNodePtr sinkOperator;
        QuerySubPlanId qspOfNetworkSink;
        auto querySubPlansOnSinkTopologyNode = globalExecutionPlan->getExecutionNodeByNodeId(sinkTopolgoyNodeId)->getQuerySubPlans(queryId);
        for (auto plan : querySubPlansOnSinkTopologyNode) {
            auto sinkOperators = plan->getSinkOperators();
            bool found = false;
            for (auto sink : sinkOperators) {
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
        auto networkSinkDescriptor = sinkOperator->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        std::map<QuerySubPlanId, std::vector<uint64_t>> helperMap;
        //mapping on worker side must be done using globalId.
        helperMap.insert({qspOfNetworkSink, {networkSinkDescriptor->getGlobalId()}});
        workerRPCClient->bufferData(rpcAddress, helperMap);

        std::unordered_set<ExecutionNodePtr> executionNodesCreatedForSubquery =
            queryPlacementPhase->executePartialPlacement("BottomUp", queryPlan);
        std::vector<ExecutionNodePtr> exeNodes(executionNodesCreatedForSubquery.begin(), executionNodesCreatedForSubquery.end());
        TopologyNodePtr newSourceTopologyNode = findNewNodeLocationOfNetworkSource(queryId, partitionIdentifier, exeNodes);
        NES_DEBUG("Check if ExecutionNode has been added" << globalExecutionPlan->checkIfExecutionNodeExists(3));
        if (deployQuery(queryId, exeNodes)) {
            if (startQuery(queryId, exeNodes)) {
                workerRPCClient->updateNetworkSinks(rpcAddress,
                                                    newSourceTopologyNode->getId(),
                                                    newSourceTopologyNode->getIpAddress(),
                                                    newSourceTopologyNode->getDataPort(),
                                                    helperMap);
                NES_DEBUG(globalExecutionPlan->getAsString());
                bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
                if(!success){
                    throw Exception("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
                }
            } else {
                NES_DEBUG("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
                //workerRPCClient->unbufferData(rpcAddress, helperMap);
            }
        } else {
            NES_DEBUG("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
        }
    }
    bool success = globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
    return true;
}

bool QueryMigrationPhase::executeMigrationWithoutBuffer(const std::vector<QueryPlanPtr>& queryPlans,ExecutionNodePtr markedNode) {

    auto queryId = queryPlans[0]->getQueryId();

    for (auto queryPlan : queryPlans) {
        auto sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.size() > 1) {
            NES_DEBUG("QueryMigrationPhase: partial placement currently only supports stateless operators."
                      " As such QuerySubPlans can only have one NetworkSource operator. QuerySubPlan "
                          << queryPlan->getQuerySubPlanId() << " , however,has " << sourceOperators.size());
            throw Exception("QueryMigrationPhase: partial placement currently only support stateless operators");
        }
        //Find TopologyNode, QuerySubPlanId and OperatorId of corresponding NetworkSink
        auto sourceOperator = sourceOperators[0];
        auto networkSourceDescriptor = sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>();
        TopologyNodeId sinkTopolgoyNodeId = networkSourceDescriptor->getSinkTopologyNode();
        TopologyNodePtr sinkNode = topology->findNodeWithId(sinkTopolgoyNodeId);
        //A pair of NetworkSource and Sink "share" a NESPartition. They each hold their own individual partition but are paired together once a Channel is established.
        //This pairing, in the end, is done over the OperatorId, which is also the OperatorId of the NetworkSink.
        //We make use of this fact to find the NetworkSink that will be told to bufferData/reconfigure
        //In the future, an ID for a NetworkSink/Source pair would be useful
        uint64_t partitionIdentifier = networkSourceDescriptor->getNesPartition().getOperatorId();
        SinkLogicalOperatorNodePtr sinkOperator;
        QuerySubPlanId qspOfNetworkSink;
        auto querySubPlansOnSinkTopologyNode = globalExecutionPlan->getExecutionNodeByNodeId(sinkTopolgoyNodeId)->getQuerySubPlans(queryId);
        for (auto plan : querySubPlansOnSinkTopologyNode) {
            auto sinkOperators = plan->getSinkOperators();
            bool found = false;
            for (auto sink : sinkOperators) {
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
        auto networkSinkDescriptor = sinkOperator->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
        auto ipAddress = sinkNode->getIpAddress();
        auto grpcPort = sinkNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        std::map<QuerySubPlanId, std::vector<uint64_t>> helperMap;
        //mapping on worker side must be done using globalId.
        helperMap.insert({qspOfNetworkSink, {networkSinkDescriptor->getGlobalId()}});


        std::unordered_set<ExecutionNodePtr> executionNodesCreatedForSubquery =
            queryPlacementPhase->executePartialPlacement("BottomUp", queryPlan);
        std::vector<ExecutionNodePtr> exeNodes(executionNodesCreatedForSubquery.begin(), executionNodesCreatedForSubquery.end());
        TopologyNodePtr newSourceTopologyNode = findNewNodeLocationOfNetworkSource(queryId, partitionIdentifier, exeNodes);
        NES_DEBUG("Check if ExecutionNode has been added" << globalExecutionPlan->checkIfExecutionNodeExists(3));
        if (deployQuery(queryId, exeNodes)) {
            if (startQuery(queryId, exeNodes)) {
                workerRPCClient->updateNetworkSinks(rpcAddress,
                                                    newSourceTopologyNode->getId(),
                                                    newSourceTopologyNode->getIpAddress(),
                                                    newSourceTopologyNode->getDataPort(),
                                                    helperMap);
                bool success = markedNode->removeSingleQuerySubPlan(queryId,queryPlan->getQuerySubPlanId());
                if(!success){
                    throw Exception("QueryMigrationPhase: Error while removing a QSP from an ExecutionNode. No such QSP found");
                }
                NES_DEBUG(globalExecutionPlan->getAsString());
            } else {
                NES_DEBUG("QueryMigrationPhase: Issues while starting execution nodes. Triggering unbuffering of data");
                //workerRPCClient->unbufferData(rpcAddress, helperMap);
            }
        } else {
            NES_DEBUG("QueryMigrationPhase: Issues while deploying execution nodes. Triggering unbuffering of data");
            //TODO:
            //workerRPCClient->unbufferData(rpcAddress, helperMap);
        }
    }
    bool success = globalExecutionPlan->removeExecutionNodeFromQueryIdIndex(queryId,markedNode->getId());
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
    auto allParentNodesOfTopologyNode = topology->findNodeWithId(topologyNodeId)->as<Node>()->getParents();
    for(auto executionNode : allExecutionNodesInvolvedInAQuery){
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
}//namespace NES