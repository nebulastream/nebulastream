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
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Network/NetworkSource.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                             TopologyPtr topologyPtr,
                                             TypeInferencePhasePtr typeInferencePhase,
                                             StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topologyPtr)),
      typeInferencePhase(std::move(typeInferencePhase)), streamCatalog(std::move(streamCatalog)) {}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(QueryId queryId,
                                                             const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators) {

    NES_DEBUG("BasePlacementStrategy: Prepare a map of source to physical nodes");
    NES_TRACE("BasePlacementStrategy: Clear the previous pinned operator mapping");
    pinnedOperatorLocationMap.clear();
    topologyNodesWithSourceOperators.clear();
    TopologyNodePtr sinkNode = topology->getRoot();
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    std::vector<TopologyNodePtr> allSourceNodes;
    for (const auto& sourceOperator : sourceOperators) {
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw Exception("BasePlacementStrategy: Source Descriptor need stream name: " + std::to_string(queryId));
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
            throw Exception("BasePlacementStrategy: No source found in the topology for stream " + streamName
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
        throw Exception("BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
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
            auto found = std::find_if(mergedGraphSourceNodes.begin(),
                                      mergedGraphSourceNodes.end(),
                                      [&](const TopologyNodePtr& sourceNodeToUse) {
                                          return sourceNode->getId() == sourceNodeToUse->getId();
                                      });
            if (found == mergedGraphSourceNodes.end()) {
                NES_ERROR("BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
                throw Exception("BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
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
        NES_DEBUG("BasePlacementStrategy: Get the topology node for logical source name " << logicalSourceName);
        std::vector<TopologyNodePtr> topologyNodes = mapOfSourceToTopologyNodes[logicalSourceName];
        if (topologyNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: unable to find topology nodes for logical source " << logicalSourceName);
            throw Exception("BasePlacementStrategy: unable to find topology nodes for logical source " + logicalSourceName);
        }

        TopologyNodePtr candidateTopologyNode = topologyNodes[0];
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
            throw Exception(
                "BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        }
        pinnedOperatorLocationMap[sourceNode->getId()] = candidateTopologyNode;
        topologyNodesWithSourceOperators.emplace_back(candidateTopologyNode);
        topologyNodes.erase(topologyNodes.begin());
        mapOfSourceToTopologyNodes[logicalSourceName] = topologyNodes;
    }
}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(QueryId queryId, QueryPlanPtr queryPlan, std::vector<TopologyNodePtr> sourceNodes, TopologyNodePtr rootNode){

    NES_DEBUG("BasePlacementStrategy: Performing partial placement on QueryPlan: " << queryId);
    NES_DEBUG("BasePlacementStrategy: Prepare a map of source to physical nodes");
    NES_TRACE("BasePlacementStrategy: Clear the previous pinned operator mapping");
    pinnedOperatorLocationMap.clear();
    topologyNodesWithSourceOperators.clear();

    NES_TRACE("BasePlacementStrategy: Find the topology sub graph for the source nodes.");
    std::vector<TopologyNodePtr> topoSubGraphSourceNodes = this->topology->findPathBetween(sourceNodes, {rootNode});

    if(topoSubGraphSourceNodes.empty()){
        throw Exception("BasePlacementStrategy: unable to path between source and sink nodes ");
    }

    if(sourceNodes.size() > 1 && (topology->findCommonAncestor(topoSubGraphSourceNodes)->getId() == rootNode->getId())){
        NES_DEBUG("BasePlacementStrategy: More than one source node indicating a merge is necessary but only common ancestor is root node. Placement on root node is not allowed");
        throw Exception("BasePlacementStrategy: More than one source node indicating a merge is necessary but only common ancestor is root node. Placement on root node is not allowed ");
    }

    NES_TRACE("BasePlacementStrategy: Adding location for sink operator.");
    TopologyNodePtr root = topoSubGraphSourceNodes[0]->getAllRootNodes()[0]->as<TopologyNode>();
    std::vector<OperatorNodePtr> sinkOperators = queryPlan->getRootOperators();
    for (const auto& sinkOperator : sinkOperators) {
        pinnedOperatorLocationMap[sinkOperator->getId()] = root;
    }
    for(auto const& sourceOperator : queryPlan->getSourceOperators()){
        auto candidateTopologyNodeId = sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getSinkTopologyNode();
        auto found = std::find_if(topoSubGraphSourceNodes.begin(),
                                         topoSubGraphSourceNodes.end(),
                                          [&](const TopologyNodePtr & topNode) {
                                            return topNode->getId() == candidateTopologyNodeId;
                                          });
                if (found == sourceNodes.end()) {
                        NES_DEBUG("BasePlacementStrategy: candidateTopologyNode for Source Operator was not found in list of sourceNodes");
                        throw Exception("BasePlacementStrategy: candidateTopologyNode for Source Operator was not found in list of sourceNodes");

                }
                pinnedOperatorLocationMap[sourceOperator->getId()] = (*found);
                topologyNodesWithSourceOperators.emplace_back((*found));

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

TopologyNodePtr BasePlacementStrategy::getTopologyNodeForPinnedOperator(uint64_t operatorId, bool partialPlacement) {

    NES_DEBUG("BasePlacementStrategy: Get the topology node for logical operator with id " << operatorId);
    auto found = pinnedOperatorLocationMap.find(operatorId);

    if (found == pinnedOperatorLocationMap.end()) {
        NES_ERROR("BasePlacementStrategy: unable to find location for the pinned operator");
        throw Exception("BasePlacementStrategy: unable to find location for the pinned operator");
    }

    TopologyNodePtr candidateTopologyNode = pinnedOperatorLocationMap[operatorId];
    if (candidateTopologyNode->getAvailableResources() == 0 && !partialPlacement) {
        NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        throw Exception("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
    }
    return candidateTopologyNode;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId,
                                                                 uint64_t sourceOperatorId,
                                                                 const TopologyNodePtr& sourceTopologyNode) {

    NES_DEBUG("BasePlacementStrategy: create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    return LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
}

OperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId, SchemaPtr inputSchema, uint64_t operatorId, const TopologyNodePtr& sinkTopologyNode) {
    NES_DEBUG("BasePlacementStrategy: create Network Source operator");
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
    return LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(std::move(inputSchema), nesPartition,sinkTopologyNode->getId()),
        operatorId);
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan, bool partialPlacement) {
    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    NES_TRACE("BasePlacementStrategy: For each source operator check for the assignment of network source and sink operators");
    for (auto& sourceOperator : sourceOperators) {
        placeNetworkOperator(queryId, sourceOperator, partialPlacement);
    }
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode, bool partialPlacement) {

    NES_TRACE("BasePlacementStrategy: Get execution node where operator is placed");
    ExecutionNodePtr executionNode = operatorToExecutionNodeMap[operatorNode->getId()];
    if(!partialPlacement)
        addExecutionNodeAsRoot(executionNode);
    bool isSourceOperator = operatorNode->instanceOf<SourceLogicalOperatorNode>();
    bool partialPlacementAndOperatorIsNetworkSource = false;
    if(isSourceOperator && partialPlacement){
        if(!operatorNode->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()) {
            NES_DEBUG("BasePlacementStrategy: During partial placement, SourceLogicalOperatorNodes are expected to be NetworkSources. However, the current SourceLogicalOperator isnt");
            throw Exception("BasePlacementStrategy: SourceLogicalOperators isnt a NetworkSource");
        }
        if(operatorNode->getParents().size() > 1){
            NES_DEBUG("BasePlacementStrategy: NetworkSources cannot have more than 1 parent Operator");
            throw Exception("BasePlacementStrategy: NetworkSources cannot have more than 1 parent Operator");
        }
        partialPlacementAndOperatorIsNetworkSource= true;
    }

    for (const auto& parent : operatorNode->getParents()) {

        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        NES_TRACE("BasePlacementStrategy: Get execution node where parent operator is placed");
        ExecutionNodePtr parentExecutionNode = operatorToExecutionNodeMap[parentOperator->getId()];
        bool allChildrenPlaced = true;
        if (executionNode->getId() != parentExecutionNode->getId()) {

            bool parentIsSinkLogicalOperator = parent->instanceOf<SinkLogicalOperatorNode>();
            bool partialPlacementAndParentIsNetworkSink = false;
            if(parentIsSinkLogicalOperator && partialPlacement){
                if(!parentOperator->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                    NES_DEBUG("BasePlacementStrategy: During partial placement, SinkLogicalOperatorNodes are expected to be NetworkSinks. However, the current SinkLogicalOperator isnt");
                    throw Exception("BasePlacementStrategy: SinkLogicalOperators isnt a NetworkSink");
                }
                if(!parentOperator->getParents().empty()){
                    NES_DEBUG("BasePlacementStrategy: During partial placement, NetworkSinks are expected to have no parents. However, the current NetworkSink has parents");
                    throw Exception("BasePlacementStrategy: NetworkSink has parents");
                }
                partialPlacementAndParentIsNetworkSink = true;
            }

            NES_TRACE("BasePlacementStrategy: Parent and its child operator are placed on different physical node.");

            NES_TRACE(
                "BasePlacementStrategy: Find the nodes between the topology node (inclusive) for child and parent operators.");
            TopologyNodePtr sourceNode;
            TopologyNodePtr destinationNode;
            //ManualPlacement still uses old elements. Must be updated
            if(!topologyNodesWithSourceOperators.empty()) {
                sourceNode = topology->findTopologyNodeByIdInSubGraph(executionNode->getTopologyNode()->getId(), topologyNodesWithSourceOperators);
                destinationNode = topology->findTopologyNodeByIdInSubGraph(parentExecutionNode->getTopologyNode()->getId(), topologyNodesWithSourceOperators);
            }
            else{
                sourceNode = executionNode->getTopologyNode();
                destinationNode = parentExecutionNode->getTopologyNode();
            }

            std::vector<TopologyNodePtr> nodesBetween = topology->findNodesBetween(sourceNode, destinationNode);

            NES_TRACE("BasePlacementStrategy: For all the nodes between the topology node for child and parent operators add "
                      "respective source or sink operator.");
            const SchemaPtr& inputSchema = operatorNode->getOutputSchema();
            uint64_t sourceOperatorId;
            if(partialPlacementAndOperatorIsNetworkSource) {
                NES_DEBUG("BasePlacementStrategy: During partial placement, the first NetworkSource placed must share the partition operator ID of the original NetworkSource");
                sourceOperatorId = operatorNode->as<SourceLogicalOperatorNode>()
                                       ->getSourceDescriptor()
                                       ->as<Network::NetworkSourceDescriptor>()
                                       ->getNesPartition()
                                       .getOperatorId();
            }
            else
                sourceOperatorId = Util::getNextOperatorId();
            for (std::size_t i = static_cast<std::size_t>(0ul); i < nodesBetween.size(); ++i) {

                NES_TRACE("BasePlacementStrategy: Find the execution node for the topology node.");
                ExecutionNodePtr candidateExecutionNode = getExecutionNode(nodesBetween[i]);

                if (i == 0) {
                    if(partialPlacementAndOperatorIsNetworkSource) {
                        NES_DEBUG("BasePlacementStrategy: during partial placement, if the current Operator is a NetworkSource, no new NetworkSink needs to be placed");
                        continue;
                    }
                    else{
                        NES_TRACE("BasePlacementStrategy: Find the query plan with child operator.");
                        std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                        bool found = false;
                        for (auto& querySubPlan : querySubPlans) {
                            OperatorNodePtr targetUpStreamOperator = querySubPlan->getOperatorWithId(operatorNode->getId());
                            if (targetUpStreamOperator) {
                                NES_TRACE("BasePlacementStrategy: Add network sink operator as root of the query plan with child "
                                          "operator.");
                                if(partialPlacementAndParentIsNetworkSink && (i == nodesBetween.size() -2))
                                    sourceOperatorId = parentOperator->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNesPartition().getOperatorId();
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
                            throw Exception("BasePlacementStrategy: unable to place network sink operator for the child operator");
                        }
                    }
                    continue;
                }
                if (i == nodesBetween.size() - 1) {
                    if(!partialPlacementAndParentIsNetworkSink) {
                        NES_TRACE("BasePlacementStrategy: Find the query plan with parent operator.");
                        std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                        OperatorNodePtr sourceOperator =
                            createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId, nodesBetween[i - 1]);
                        bool found = false;
                        for (auto& querySubPlan : querySubPlans) {
                            OperatorNodePtr targetDownstreamOperator = querySubPlan->getOperatorWithId(parentOperator->getId());
                            if (targetDownstreamOperator) {
                                NES_TRACE("BasePlacementStrategy: add network source operator as child to the parent operator.");
                                targetDownstreamOperator->addChild(sourceOperator);
                                allChildrenPlaced =
                                    (parentOperator->getChildren().size() == targetDownstreamOperator->getChildren().size());
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            NES_WARNING("BasePlacementStrategy: unable to place network source operator for the parent operator");
                            throw Exception(
                                "BasePlacementStrategy: unable to place network source operator for the parent operator");
                        }
                    }
                } else {

                    NES_TRACE("BasePlacementStrategy: Create a new query plan and add pair of network source and network sink "
                              "operators.");
                    QueryPlanPtr querySubPlan = QueryPlan::create();
                    querySubPlan->setQueryId(queryId);
                    querySubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                    NES_TRACE("BasePlacementStrategy: add network source operator");
                    const OperatorNodePtr networkSource = createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId,nodesBetween[i-1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSource);

                    NES_TRACE("BasePlacementStrategy: add network sink operator");
                    if(partialPlacementAndParentIsNetworkSink && (i == nodesBetween.size() -2))
                        sourceOperatorId = parentOperator->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNesPartition().getOperatorId();
                    else
                        sourceOperatorId = Util::getNextOperatorId();
                    OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSink);

                    NES_TRACE("BasePlacementStrategy: add query plan to execution node and update the global execution plan");
                    candidateExecutionNode->addNewQuerySubPlan(queryId, querySubPlan);
                    globalExecutionPlan->addExecutionNode(candidateExecutionNode);
                    if(partialPlacement)
                        executionNodesCreatedDuringPartialPlacement.insert(candidateExecutionNode);
                }
                // Add the parent-child relation
                globalExecutionPlan->addExecutionNodeAsParentTo(nodesBetween[i - 1]->getId(), candidateExecutionNode);
            }
        }

        if (allChildrenPlaced) {
            NES_TRACE("BasePlacementStrategy: add network source and sink operator for the parent operator");
            placeNetworkOperator(queryId, parentOperator,partialPlacement);
        }
        NES_TRACE("BasePlacementStrategy: Skipping network source and sink operator for the parent operator as all children "
                  "operators are not processed");
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
                throw Exception("BasePlacementStrategy: failed to add execution node as root");
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
        for (const auto& querySubPlan : querySubPlans) {
            typeInferencePhase->execute(querySubPlan);
        }
    }
    return true;
}
bool BasePlacementStrategy::assignMappingToTopology(const NES::TopologyPtr topologyPtr,
                                                    const NES::QueryPlanPtr queryPlan,
                                                    const PlacementMatrix mapping) {

    auto topologyIterator = NES::DepthFirstNodeIterator(topologyPtr->getRoot()).begin();
    auto mappingIterator = mapping.begin();

    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        auto currentExecutionNode = BasePlacementStrategy::getExecutionNode(currentTopologyNode);

        // create an empty query sub plan
        QueryPlanPtr candidateQueryPlan;

        // get operators to place in the current topology nodes
        auto queryMappingIterator = mappingIterator->begin();

        // iterate to all operator in current query plan
        auto queryPlanIterator = NES::QueryPlanIterator(queryPlan);
        for (auto&& op : queryPlanIterator) {
            if (*queryMappingIterator) {
                // place the operator in the current node
                auto querySubPlans = currentExecutionNode->getQuerySubPlans(queryPlan->getQueryId());

                // If there is no existing query sub plan for this query, create a new one
                if (querySubPlans.empty()) {
                    candidateQueryPlan = QueryPlan::create();
                    candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
                    candidateQueryPlan->addRootOperator(op->as<OperatorNode>()->copy());
                } else {
                    // Check if existing operator is the parent of current operator to place

                    // 1. get the parent of the current operator to place
                    std::vector<NodePtr> parents = op->as<OperatorNode>()->getParents();

                    // 2. find all query plan in the current execution node that are parents to the current operator to place
                    std::vector<QueryPlanPtr> queryPlansWithParent;
                    for (auto& parent : parents) {
                        auto found =
                            std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
                                return querySubPlan->hasOperatorWithId(parent->as<OperatorNode>()->getId());
                            });

                        if (found != querySubPlans.end()) {
                            queryPlansWithParent.push_back(*found);
                            querySubPlans.erase(found);
                        }
                    }

                    // 3. If we have parents for the current operator to place, update the existing query sub plan
                    if (!queryPlansWithParent.empty()) {
                        currentExecutionNode->updateQuerySubPlans(queryPlan->getQueryId(), querySubPlans);
                        if (queryPlansWithParent.size() > 1) {
                            candidateQueryPlan = QueryPlan::create();
                            candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                            for (auto& queryPlanWithChildren : queryPlansWithParent) {
                                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                                    candidateQueryPlan->addRootOperator(root);
                                }
                            }
                        } else if (queryPlansWithParent.size() == 1) {
                            // if there is only one parent, then use this
                            candidateQueryPlan = queryPlansWithParent[0];
                        }

                        if (candidateQueryPlan) {
                            for (const auto& candidateParent : parents) {
                                auto parentOperator =
                                    candidateQueryPlan->getOperatorWithId(candidateParent->as<OperatorNode>()->getId());
                                if (parentOperator) {
                                    parentOperator->addChild(op->as<OperatorNode>()->copy());
                                }
                            }
                        }
                    } else {
                        // if queryPlansWithParent is empty, create a new candidateQueryPlan
                        candidateQueryPlan = QueryPlan::create();
                        candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
                        candidateQueryPlan->addRootOperator(op->as<OperatorNode>()->copy());
                    }
                }
                currentExecutionNode->addNewQuerySubPlan(queryPlan->getQueryId(), candidateQueryPlan);
                globalExecutionPlan->addExecutionNode(currentExecutionNode);
                operatorToExecutionNodeMap[op->as<OperatorNode>()->getId()] = currentExecutionNode;
            }
            ++queryMappingIterator;
        }
        ++topologyIterator;
        ++mappingIterator;
    }
    return true;
}

}// namespace NES::Optimizer
