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
    auto planSinkOperators = queryPlan->getSinkOperators();
    auto sinkComparator = [](SinkLogicalOperatorNodePtr aSink, SinkLogicalOperatorNodePtr bSink) {
        return aSink->getId() < bSink->getId();
    };
    std::sort(planSinkOperators.begin(), planSinkOperators.end(), sinkComparator);
    bool successfulReconfiguration = false;
    for (auto executionNode : executionNodes) {
        if (executionNode->getTopologyNode()->getId() == sinkTopologyNode->getId()) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            for (auto querySubPlan : querySubPlans) {
                auto found = std::find_if(planSinkOperators.begin(), planSinkOperators.end(),
                                          [querySubPlan](SinkLogicalOperatorNodePtr sinkLogicalOperatorNode) {
                                              return querySubPlan->hasOperatorWithId(sinkLogicalOperatorNode->getId());
                                          });
                if (found != planSinkOperators.end()) {
                    auto subPlanSinks = querySubPlan->getSinkOperators();
                    std::sort(subPlanSinks.begin(), subPlanSinks.end(), sinkComparator);
                    std::vector<SinkLogicalOperatorNodePtr> sinksToBeAdded;
                    std::set_difference(planSinkOperators.begin(), planSinkOperators.end(), subPlanSinks.begin(),
                                        subPlanSinks.end(), std::back_inserter(sinksToBeAdded), sinkComparator);
                    std::vector<SinkLogicalOperatorNodePtr> sinksToBeDeleted;
                    std::set_difference(subPlanSinks.begin(), subPlanSinks.end(), planSinkOperators.begin(),
                                        planSinkOperators.end(), std::back_inserter(sinksToBeDeleted), sinkComparator);
                    for (auto a : sinksToBeAdded) {
                        NES_DEBUG("QueryReconfigurationPhase:sinksToBeAdded = " + a->toString());
                    }
                    for (auto a : sinksToBeDeleted) {
                        NES_DEBUG("QueryReconfigurationPhase:sinksToBeDeleted = " + a->toString());
                    }
                    auto children = subPlanSinks[0]->getChildren();
                    for (auto sinkToDelete : sinksToBeDeleted) {
                        NES_DEBUG("QueryReconfigurationPhase:sinksToBeDeleted: deleting sink = " + sinkToDelete->toString());
                        querySubPlan->removeAsRootOperator(sinkToDelete);
                    }
                    for (auto sinkToAdd : sinksToBeAdded) {
                        auto newSink = sinkToAdd->copy();
                        NES_DEBUG("QueryReconfigurationPhase:sinksToBeAdded: adding sink = " + newSink->toString());
                        for (auto child : children) {
                            newSink->addChild(child);
                        }
                        querySubPlan->addRootOperator(newSink);
                    }
                    successfulReconfiguration = reconfigureQuery(executionNode, querySubPlan);
                }
            }
        }
    }
    return successfulReconfiguration;
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

bool QueryReconfigurationPhase::reconfigureQuery(ExecutionNodePtr executionNode, QueryPlanPtr querySubPlan) {
    QueryId queryId = querySubPlan->getQueryId();
    NES_DEBUG("QueryReconfigurationPhase::reconfigure sinks queryId=" << queryId);
    const auto& nesNode = executionNode->getTopologyNode();
    auto ipAddress = nesNode->getIpAddress();
    auto grpcPort = nesNode->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
    NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress);
    bool success = workerRPCClient->reconfigureQuery(rpcAddress, querySubPlan);
    if (success) {
        NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << " successful");
    } else {
        NES_ERROR("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << "  failed");
        return false;
    }
    NES_INFO("QueryReconfigurationPhase: Finished reconfiguring sinks for query with Id " << queryId);
    return true;
}
}// namespace NES
