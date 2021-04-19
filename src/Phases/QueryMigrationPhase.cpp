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
#include <Phases/QueryMigrationPhase.hpp>
#include <Util/Logger.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
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


bool QueryMigrationPhase::execute(QueryId queryId, TopologyNodeId topologyNodeId) {
    auto childNodes = findChildExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    auto parentNodes = findParentExecutionNodesAsTopologyNodes(queryId,topologyNodeId);
    NES_DEBUG(queryId);
    NES_DEBUG(topologyNodeId);
        return false;
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



}//namespace NES