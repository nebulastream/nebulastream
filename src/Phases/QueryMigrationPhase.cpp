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
                                         WorkerRPCClientPtr workerRPCClient) : workerRPCClient(workerRPCClient),topology(topology),
                                                                               globalExecutionPlan(globalExecutionPlan){
                                                                                   NES_DEBUG("QueryMigrationPhase()");
}

QueryMigrationPhase::~QueryMigrationPhase() { NES_DEBUG("~QueryDeploymentPhase()"); }

QueryMigrationPhasePtr QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,TopologyPtr topology,
                                                     WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(globalExecutionPlan, topology, workerRpcClient));
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
    auto subqueries = globalExecutionPlan->getExecutionNodeByNodeId(markedTopNodeId)->getQuerySubPlans(queryId);
    auto execNodes = buildExecutionNodes(path,subqueries);
    auto addresses = getAddresses(findChildExecutionNodesAsTopologyNodes(queryId,markedTopNodeId));

    //Both migration types have many steps in common, but are ordered differently
    if(req->isWithBuffer()){
        return executeMigrationWithBuffer(path,execNodes, addresses, queryId, markedTopNodeId);
    }
    else{
        return executeMigrationWithoutBuffer(path, execNodes, addresses, queryId, markedTopNodeId);
    }
}

std::vector<TopologyNodePtr> QueryMigrationPhase::findPath(QueryId queryId, TopologyNodeId topologyNodeId) {
    auto childNodes = findChildExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    if(childNodes.size()>1){
        throw QueryMigrationException(queryId, "Currently migration is only supported for cases in which there is only a single CHILD node");
    }
    auto parentNodes = findParentExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    if(childNodes.size()>1){
        throw QueryMigrationException(queryId, "Currently migration is only supported for cases in which there is only a single PARENT node");
    }
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

std::vector<ExecutionNodePtr> QueryMigrationPhase::buildExecutionNodes(const std::vector<TopologyNodePtr>& path,
                                                                       const std::vector<QueryPlanPtr>& subqueries) {

    //TODO: update path selection to support more than one exec Node!
    std::vector<ExecutionNodePtr> execNodes = {};
    auto candidateTopologyNode = path.at(0)->getParents().at(0)->as<TopologyNode>();
    auto exNode = getExecutionNode(candidateTopologyNode->getId());
    for (auto& queryPlan : subqueries){
        exNode->addNewQuerySubPlan(queryPlan->getQueryId(),queryPlan);
    }

    //addExecutionNode updates mapping of executionNodes to QueryIds, which checks for querySubPlans in a map which is updated by the above code
    globalExecutionPlan->addExecutionNode(exNode);
    execNodes.push_back(exNode);


    return execNodes;
}
std::map<TopologyNodeId ,std::string> QueryMigrationPhase::getAddresses(const std::vector<TopologyNodePtr>& childExecutionNodes){
    std::map<TopologyNodeId ,std::string> nodeIdToaddresses = {};
    for(TopologyNodePtr node: childExecutionNodes){
        auto ipAddress =  node->getIpAddress();
        auto grpcPort =  node->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        nodeIdToaddresses[node->getId()] = rpcAddress;

    }
    return nodeIdToaddresses;
}

bool QueryMigrationPhase::executeMigrationWithBuffer(const std::vector<TopologyNodePtr>& path,const std::vector<ExecutionNodePtr>& execNodes, const std::map<TopologyNodeId ,std::string>& addresses, QueryId queryId, TopologyNodeId markedTopNodeId) {

    //Step 1. deploy queries on new execution nodes. DO NOT START THEM YET
    //This is done here so that when clean up is triggered by bufferData, queryPlans are not lost
    NES_DEBUG(path.size());
    auto deploySuccess = deployQuery(queryId, execNodes);

    if(deploySuccess){

        Network::NodeLocation markedNodeLocation(markedTopNodeId, topology->findNodeWithId(markedTopNodeId)->getIpAddress(),
                                                 topology->findNodeWithId(markedTopNodeId)->getDataPort());
        for(auto pair : addresses){
            auto map = querySubPlansAndNetworkSinksToReconfigure(queryId,globalExecutionPlan->getExecutionNodeByNodeId(pair.first),markedNodeLocation);
            if(workerRPCClient->bufferData(pair.second, map)){
                auto startSuccess =  startQuery(queryId,execNodes);
                if(!startSuccess){
                    NES_DEBUG("QueryMigrationPhase: executeMigrationWithBuffer: starting queries has failed");
                    return false;
                }
                else{
                    //workerRPCClient->unbufferData();
                    NES_DEBUG("QueryMigrationPhase: executeMigrationWithBuffer: successfully unbuffered Data");
                    continue;
                }
            }
            NES_DEBUG("QueryMigrationPhase: executeMigrationWithBuffer: GRPC call to buffer data has failed");
            return false;
        }
        return true;
    }
    NES_DEBUG("QueryMigrationPhase: executeMigrationWithBuffer: Deployment of queries failed. Aborting Migration");
    return false;
}

bool QueryMigrationPhase::executeMigrationWithoutBuffer(const std::vector<TopologyNodePtr>& path,
                                                        const std::vector<ExecutionNodePtr>& execNodes, const std::map<TopologyNodeId ,std::string>& addresses, QueryId queryId, TopologyNodeId markedTopNodeId) {

    // Step 1. Immediately deploy AND start queries on new execution Nodes
    auto deploySuccess = deployQuery(queryId, execNodes);
    auto startSuccess = startQuery(queryId, execNodes);

    if (deploySuccess && startSuccess) {
        //Step 2. Determine where to send updateNetworkSinks GRPC and what to send there

        Network::NodeLocation markedNodeLocation(markedTopNodeId,
                                                 topology->findNodeWithId(markedTopNodeId)->getIpAddress(),
                                                 topology->findNodeWithId(markedTopNodeId)->getDataPort());
        //TODO: change this when path selection is updated so there is more than just one candidateTopologyNode
        auto candidateTopologyNode = path.at(0)->getParents().at(0)->as<TopologyNode>();
        uint64_t newNodeId = candidateTopologyNode->getId();
        std::string newHostname = candidateTopologyNode->getIpAddress();
        uint32_t newPort = candidateTopologyNode->getDataPort();

        for (auto pair : addresses) {
            auto map = querySubPlansAndNetworkSinksToReconfigure(queryId,
                                                                 globalExecutionPlan->getExecutionNodeByNodeId(pair.first),
                                                                 markedNodeLocation);
            //Step 3. Send GRPC to update networksinks. Triggers clean up of downstream node!
            if (workerRPCClient->updateNetworkSinks(pair.second, newNodeId, newHostname, newPort, map)) {
                continue;
            }
            NES_DEBUG("QueryMigrationPhase: GRPC call to update network sinks failed.");
            return false;
        }
        return true;
    }
    NES_DEBUG("QueryMigrationPhase: Issue during deployment or start of queries");
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

    }
    return candidateExecutionNode;
}

std::map<QuerySubPlanId, std::vector<OperatorId>> QueryMigrationPhase::querySubPlansAndNetworkSinksToReconfigure(QueryId queryId, const ExecutionNodePtr& childExecutionNode, const Network::NodeLocation& markedNodeLocation) {
    std::map<QuerySubPlanId, std::vector<OperatorId>> querySubPlansToNetworkSinksMap;
    std::vector<QueryPlanPtr> candidateSubQueryPlans = childExecutionNode->getQuerySubPlans(queryId);
    for(auto& queryPlan : candidateSubQueryPlans){
       bool created = false;
        for(auto& logicalSink : queryPlan->getSinkOperators()){
            if(logicalSink->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()){
                if((logicalSink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNodeLocation()) == markedNodeLocation ){
                    if(!created){
                        querySubPlansToNetworkSinksMap.insert(std::pair<uint64_t, std::vector<uint64_t>> (queryPlan->getQuerySubPlanId(), {}));
                        created = true;
                    }
                    querySubPlansToNetworkSinksMap[queryPlan->getQuerySubPlanId()].push_back(logicalSink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getGlobalId());
                }
            }
        }
        //TODO: this should be neccesary, but is never reached. Could be an issue?
        created = false;
    }

    return querySubPlansToNetworkSinksMap;
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