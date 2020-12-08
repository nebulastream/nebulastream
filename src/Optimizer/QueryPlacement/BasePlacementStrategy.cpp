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

#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Topology/TopologyNode.hpp>
//#include <Optimizer/QueryPlacement/HighAvailabilityStrategy.hpp>
//#include <Optimizer/QueryPlacement/HighThroughputStrategy.hpp>
//#include <Optimizer/QueryPlacement/LowLatencyStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumEnergyConsumptionStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumResourceConsumptionStrategy.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topologyPtr,
                                             TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), topology(topologyPtr), typeInferencePhase(typeInferencePhase),
      streamCatalog(streamCatalog), pinnedOperatorLocationMap(), operatorToExecutionNodeMap() {}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(QueryId queryId,
                                                             std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {

    NES_DEBUG("BasePlacementStrategy: Prepare a map of source to physical nodes");
    NES_TRACE("BasePlacementStrategy: Clear the previous pinned operator mapping");
    pinnedOperatorLocationMap.clear();
    TopologyNodePtr sinkNode = topology->getRoot();
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    std::vector<TopologyNodePtr> allSourceNodes;
    for (auto& sourceOperator : sourceOperators) {
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw QueryPlacementException("BasePlacementStrategy: Source Descriptor need stream name: " + queryId);
        }
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        if (mapOfSourceToTopologyNodes.find(streamName) != mapOfSourceToTopologyNodes.end()) {
            NES_TRACE("BasePlacementStrategy: Entry for the logical stream " << streamName << " already present.");
            continue;
        }
        NES_TRACE("BasePlacementStrategy: Get all topology nodes for the logical source stream");
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        if (sourceNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: No source found in the topology for stream " << streamName
                                                                                           << " for query with id : " << queryId);
            throw QueryPlacementException("BasePlacementStrategy: No source found in the topology for stream " + streamName
                                          + " for query with id : " + std::to_string(queryId));
        }
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
        NES_TRACE("BasePlacementStrategy: Find the topology sub graph for the source nodes.");
        std::vector<TopologyNodePtr> topoSubGraphSourceNodes = this->topology->findPathBetween(sourceNodes, {sinkNode});
        allSourceNodes.insert(allSourceNodes.end(), topoSubGraphSourceNodes.begin(), topoSubGraphSourceNodes.end());
    }

    NES_DEBUG("BasePlacementStrategy: Merge all the topology sub-graphs found using their source nodes");
    std::vector<TopologyNodePtr> mergedGraphSourceNodes = topology->mergeSubGraphs(allSourceNodes);

    NES_TRACE("BasePlacementStrategy: Collecting all sink operators.");
    std::set<OperatorNodePtr> sinkOperators;
    for (const auto& sourceOperator : sourceOperators) {
        const std::vector<NodePtr>& rootOperatorNodes = sourceOperator->getAllRootNodes();
        for (const auto& rootOperator : rootOperatorNodes) {
            OperatorNodePtr sinkOperator = rootOperator->as<SinkLogicalOperatorNode>();
            sinkOperators.insert(sinkOperator);
        }
    }

    NES_TRACE("BasePlacementStrategy: Locating sink node.");
    //TODO: change here if the topology can have more than one root node
    TopologyNodePtr sourceTopologyNode = mergedGraphSourceNodes[0];
    std::vector<NodePtr> rootNodes = sourceTopologyNode->getAllRootNodes();

    if (rootNodes.empty()) {
        NES_ERROR("BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
        throw QueryPlacementException(
            "BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
    }

    TopologyNodePtr rootNode = rootNodes[0]->as<TopologyNode>();

    NES_TRACE("BasePlacementStrategy: Adding location for sink operator.");
    for (const auto& sinkOperator : sinkOperators) {
        pinnedOperatorLocationMap[sinkOperator->getId()] = rootNode;
    }

    NES_DEBUG(
        "BasePlacementStrategy: Update the source to topology node map using the source nodes from the merged topology graph");
    for (auto& entry : mapOfSourceToTopologyNodes) {
        NES_TRACE(
            "BasePlacementStrategy: Taking nodes from the merged sub graphs and replacing the initial source topology nodes.");
        std::vector<TopologyNodePtr> sourceNodes;
        for (auto sourceNode : entry.second) {
            auto found = std::find_if(mergedGraphSourceNodes.begin(), mergedGraphSourceNodes.end(),
                                      [&](const TopologyNodePtr& sourceNodeToUse) {
                                          return sourceNode->getId() == sourceNodeToUse->getId();
                                      });
            if (found == mergedGraphSourceNodes.end()) {
                NES_ERROR("BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
                throw QueryPlacementException(
                    "BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
            }
            NES_TRACE(
                "BasePlacementStrategy: Inserting the source node from the merged graph into the new source node location.");
            sourceNodes.push_back(*found);
        }
        NES_TRACE("BasePlacementStrategy: Updating the source to topology node map.");
        mapOfSourceToTopologyNodes[entry.first] = sourceNodes;
    }

    for (const auto& sourceNode : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceNode->getSourceDescriptor();
        std::string logicalSourceName = sourceDescriptor->getStreamName();
        NES_TRACE("BasePlacementStrategy: Get the topology node for logical source name " << logicalSourceName);
        std::vector<TopologyNodePtr> topologyNodes = mapOfSourceToTopologyNodes[logicalSourceName];
        if (topologyNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: unable to find topology nodes for logical source " << logicalSourceName);
            throw QueryPlacementException("BasePlacementStrategy: unable to find topology nodes for logical source "
                                          + logicalSourceName);
        }

        TopologyNodePtr candidateTopologyNode = topologyNodes[0];
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
            throw QueryPlacementException(
                "BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        }
        pinnedOperatorLocationMap[sourceNode->getId()] = candidateTopologyNode;
        topologyNodes.erase(topologyNodes.begin());
        mapOfSourceToTopologyNodes[logicalSourceName] = topologyNodes;
    }
}

ExecutionNodePtr BasePlacementStrategy::getExecutionNode(const TopologyNodePtr& candidateTopologyNode) {

    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode->getId())) {
        NES_TRACE("BasePlacementStrategy: node " << candidateTopologyNode->toString() << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateTopologyNode->getId());
    } else {
        NES_TRACE("BasePlacementStrategy: create new execution node with id: " << candidateTopologyNode->getId());
        candidateExecutionNode = ExecutionNode::createExecutionNode(candidateTopologyNode);
    }
    return candidateExecutionNode;
}

TopologyNodePtr BasePlacementStrategy::getTopologyNodeForPinnedOperator(uint64_t operatorId) {

    NES_DEBUG("BasePlacementStrategy: Get the topology node for logical operator with id " << operatorId);
    auto found = pinnedOperatorLocationMap.find(operatorId);

    if (found == pinnedOperatorLocationMap.end()) {
        NES_ERROR("BasePlacementStrategy: unable to find location for the pinned operator");
        throw QueryPlacementException("BasePlacementStrategy: unable to find location for the pinned operator");
    }

    TopologyNodePtr candidateTopologyNode = pinnedOperatorLocationMap[operatorId];
    if (candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        throw QueryPlacementException(
            "BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
    }
    return candidateTopologyNode;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId, uint64_t sourceOperatorId,
                                                                 const TopologyNodePtr& sourceTopologyNode) {

    NES_DEBUG("BasePlacementStrategy: create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(), sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    OperatorNodePtr networkSink = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
    return networkSink;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId, SchemaPtr inputSchema, uint64_t operatorId) {
    NES_DEBUG("BasePlacementStrategy: create Network Source operator");
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
    OperatorNodePtr networkSource = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(std::move(inputSchema), nesPartition), operatorId);
    return networkSource;
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    NES_TRACE("BasePlacementStrategy: For each source operator check for the assignment of network source and sink operators");
    for (auto& sourceOperator : sourceOperators) {
        placeNetworkOperator(queryId, sourceOperator);
    }
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_TRACE("BasePlacementStrategy: Get execution node where operator is placed");
    ExecutionNodePtr executionNode = operatorToExecutionNodeMap[operatorNode->getId()];
    addExecutionNodeAsRoot(executionNode);

    for (auto& parent : operatorNode->getParents()) {

        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        NES_TRACE("BasePlacementStrategy: Get execution node where parent operator is placed");
        ExecutionNodePtr parentExecutionNode = operatorToExecutionNodeMap[parentOperator->getId()];

        if (executionNode->getId() != parentExecutionNode->getId()) {
            NES_TRACE("BasePlacementStrategy: Parent and its child operator are placed on different physical node.");

            NES_TRACE(
                "BasePlacementStrategy: Find the nodes between the topology node (inclusive) for child and parent operators.");
            TopologyNodePtr sourceNode = executionNode->getTopologyNode();
            TopologyNodePtr destinationNode = parentExecutionNode->getTopologyNode();
            std::vector<TopologyNodePtr> nodesBetween = topology->findNodesBetween(sourceNode, destinationNode);

            NES_TRACE("BasePlacementStrategy: For all the nodes between the topology node for child and parent operators add "
                      "respective source or sink operator.");
            const SchemaPtr& inputSchema = operatorNode->getOutputSchema();
            uint64_t sourceOperatorId = UtilityFunctions::getNextOperatorId();
            for (int i = 0; i < nodesBetween.size(); i++) {

                NES_TRACE("BasePlacementStrategy: Find the execution node for the topology node.");
                ExecutionNodePtr candidateExecutionNode = getExecutionNode(nodesBetween[i]);

                if (i == 0) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with child operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetUpStreamOperator = querySubPlan->getOperatorWithId(operatorNode->getId());
                        if (targetUpStreamOperator) {
                            NES_TRACE("BasePlacementStrategy: Add network sink operator as root of the query plan with child "
                                      "operator.");
                            OperatorNodePtr networkSink =
                                createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                            targetUpStreamOperator->addParent(networkSink);
                            querySubPlan->removeAsRootOperator(targetUpStreamOperator);
                            querySubPlan->addRootOperator(networkSink);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_ERROR("BasePlacementStrategy: unable to place network sink operator for the child operator");
                        throw QueryPlacementException(
                            "BasePlacementStrategy: unable to place network sink operator for the child operator");
                    }
                    continue;
                } else if (i == nodesBetween.size() - 1) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with parent operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    OperatorNodePtr sourceOperator = createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetDownstreamOperator = querySubPlan->getOperatorWithId(parentOperator->getId());
                        if (targetDownstreamOperator) {
                            NES_TRACE("BasePlacementStrategy: add network source operator as child to the parent operator.");
                            targetDownstreamOperator->addChild(sourceOperator);
                            if (parentOperator->getChildren().size() != targetDownstreamOperator->getChildren().size()) {
                                NES_WARNING("BasePlacementStrategy: parent operator need more network source operators");
                                // Add the parent-child relation
                                globalExecutionPlan->addExecutionNodeAsParentTo(nodesBetween[i-1]->getId(), candidateExecutionNode);
                                return;
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_WARNING("BasePlacementStrategy: unable to place network source operator for the parent operator");
                        throw QueryPlacementException(
                            "BasePlacementStrategy: unable to place network source operator for the parent operator");
                    }
                }else {

                    NES_TRACE(
                        "BasePlacementStrategy: Create a new query plan and add pair of network source and network sink operators.");
                    QueryPlanPtr querySubPlan = QueryPlan::create();
                    querySubPlan->setQueryId(queryId);
                    querySubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                    NES_TRACE("BasePlacementStrategy: add network source operator");
                    const OperatorNodePtr networkSource = createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId);
                    querySubPlan->appendOperatorAsNewRoot(networkSource);

                    NES_TRACE("BasePlacementStrategy: add network sink operator");
                    sourceOperatorId = UtilityFunctions::getNextOperatorId();
                    OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSink);

                    NES_TRACE("BasePlacementStrategy: add query plan to execution node and update the global execution plan");
                    candidateExecutionNode->addNewQuerySubPlan(queryId, querySubPlan);
                    globalExecutionPlan->addExecutionNode(candidateExecutionNode);
                }
                //FIXME: parent child relationship missing
                // Add the parent-child relation
                globalExecutionPlan->addExecutionNodeAsParentTo(nodesBetween[i-1]->getId(), candidateExecutionNode);
            }
        }

        NES_TRACE("BasePlacementStrategy: add network source and sink operator for the parent operator");
        placeNetworkOperator(queryId, parentOperator);
    }
}

void BasePlacementStrategy::addExecutionNodeAsRoot(ExecutionNodePtr& executionNode) {
    // check if the topology of the execution node is a root node
    NES_TRACE("BasePlacementStrategy: Adding new execution node with id: " << executionNode->getTopologyNode()->getId());
    // Check if the candidateTopologyNode is a root node of the topology
    if (executionNode->getTopologyNode()->getParents().empty()) {
        // Check if the candidateExecutionNode is a root node
        if (!globalExecutionPlan->checkIfExecutionNodeIsARoot(executionNode->getId())) {
            if (!globalExecutionPlan->addExecutionNodeAsRoot(executionNode)) {
                NES_ERROR("BasePlacementStrategy: failed to add execution node as root");
                throw QueryPlacementException("BasePlacementStrategy: failed to add execution node as root");
            }
        }
    }
}

bool BasePlacementStrategy::runTypeInferencePhase(QueryId queryId) {
    NES_DEBUG("BasePlacementStrategy: Run type inference phase for all the query sub plans to be deployed.");
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (const auto& executionNode : executionNodes) {
        NES_TRACE("BasePlacementStrategy: Get all query sub plans on the execution node for the query with id " << queryId);
        const std::vector<QueryPlanPtr>& querySubPlans = executionNode->getQuerySubPlans(queryId);
        for (auto& querySubPlan : querySubPlans) {
            typeInferencePhase->execute(querySubPlan);
        }
    }
    return true;
}

}// namespace NES
