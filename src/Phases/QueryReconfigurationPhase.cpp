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

#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryReconfigurationPhase::QueryReconfigurationPhase(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan,
                                                     WorkerRPCClientPtr workerRpcClient, StreamCatalogPtr streamCatalog)
    : topology(topology), globalExecutionPlan(globalExecutionPlan), workerRPCClient(workerRpcClient),
      streamCatalog(streamCatalog) {
    NES_DEBUG("QueryReconfigurationPhase()");
}

QueryReconfigurationPhasePtr QueryReconfigurationPhase::create(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan,
                                                               WorkerRPCClientPtr workerRpcClient,
                                                               StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryReconfigurationPhase>(
        QueryReconfigurationPhase(topology, globalExecutionPlan, workerRpcClient, streamCatalog));
}

QueryReconfigurationPhase::~QueryReconfigurationPhase() { NES_DEBUG("~QueryReconfigurationPhase()"); }

bool QueryReconfigurationPhase::execute(QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryReconfigurationPhase: reconfigure the query");
    auto queryId = queryPlan->getQueryId();
    auto sinkTopologyNode = findSinkTopologyNode(queryPlan);
    auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (auto executionNode : executionNodes) {
        if (executionNode->getTopologyNode()->getId() == sinkTopologyNode->getId()) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            for (auto plan : querySubPlans) {
                NES_DEBUG("QueryPlan is: " + plan->toString());
            }
        }
    }
    return false;
}

//Copied from BasePlacementStrategy::mapPinnedOperatorToTopologyNodes
TopologyNodePtr QueryReconfigurationPhase::findSinkTopologyNode(QueryPlanPtr queryPlan) {
    auto queryId = queryPlan->getQueryId();
    TopologyNodePtr sinkNode = topology->getRoot();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    std::vector<TopologyNodePtr> allSourceNodes;
    for (auto& sourceOperator : sourceOperators) {
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw QueryDeploymentException("QueryReconfigurationPhase: Source Descriptor need stream name: "
                                           + std::to_string(queryId));
        }
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        if (mapOfSourceToTopologyNodes.find(streamName) != mapOfSourceToTopologyNodes.end()) {
            NES_TRACE("QueryReconfigurationPhase: Entry for the logical stream " << streamName << " already present.");
            continue;
        }
        NES_TRACE("BasePlacementStrategy: Get all topology nodes for the logical source stream");
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        if (sourceNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: No source found in the topology for stream " << streamName
                                                                                           << " for query with id : " << queryId);
            throw QueryDeploymentException("QueryReconfigurationPhase: No source found in the topology for stream " + streamName
                                           + " for query with id : " + std::to_string(queryId));
        }
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
        NES_TRACE("QueryReconfigurationPhase: Find the topology sub graph for the source nodes.");
        std::vector<TopologyNodePtr> topoSubGraphSourceNodes = this->topology->findPathBetween(sourceNodes, {sinkNode});
        allSourceNodes.insert(allSourceNodes.end(), topoSubGraphSourceNodes.begin(), topoSubGraphSourceNodes.end());
    }

    NES_DEBUG("QueryReconfigurationPhase: Merge all the topology sub-graphs found using their source nodes");
    std::vector<TopologyNodePtr> mergedGraphSourceNodes = topology->mergeSubGraphs(allSourceNodes);

    NES_TRACE("QueryReconfigurationPhase: Locating sink node.");
    //TODO: change here if the topology can have more than one root node
    TopologyNodePtr sourceTopologyNode = mergedGraphSourceNodes[0];
    std::vector<NodePtr> rootNodes = sourceTopologyNode->getAllRootNodes();

    if (rootNodes.empty()) {
        NES_ERROR("QueryReconfigurationPhase: Found no root nodes in the topology plan. Please check the topology graph.");
        throw QueryDeploymentException(
            "QueryReconfigurationPhase: Found no root nodes in the topology plan. Please check the topology graph.");
    }
    return rootNodes[0]->as<TopologyNode>();
}

bool QueryReconfigurationPhase::reconfigureSinks(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("QueryReconfigurationPhase::reconfigure sinks queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryReconfigurationPhase::reconfigureQuery serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryReconfigurationPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress);

        for (auto& querySubPlan : querySubPlans) {
            bool success = workerRPCClient->reconfigureQuery(rpcAddress, querySubPlan);
            if (success) {
                NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
    }
    NES_INFO("QueryReconfigurationPhase: Finished reconfiguring sinks for query with Id " << queryId);
    return true;
}

}// namespace NES
