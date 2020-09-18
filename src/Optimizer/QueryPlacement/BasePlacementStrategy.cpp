#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
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

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topologyPtr, TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), topology(topologyPtr), typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog), pinnedOperatorLocationMap(), operatorToExecutionNodeMap() {}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(QueryId queryId, std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {

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
            NES_ERROR("BasePlacementStrategy: No source found in the topology for stream " << streamName << "for query with id : " << queryId);
            throw QueryPlacementException("BasePlacementStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
        }
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
        NES_TRACE("BasePlacementStrategy: Find the topology sub graph for the source nodes.");
        std::vector<TopologyNodePtr> topoSubGraphSourceNodes = this->topology->findPathBetween(sourceNodes, {sinkNode});
        allSourceNodes.insert(allSourceNodes.end(), topoSubGraphSourceNodes.begin(), topoSubGraphSourceNodes.end());
    }

    NES_DEBUG("BasePlacementStrategy: Merge all the topology sub-graphs found using their source nodes");
    std::vector<TopologyNodePtr> mergedGraphSourceNodes = topology->mergeSubGraphs(allSourceNodes);

    NES_DEBUG("BasePlacementStrategy: Adding location for sink operator.");
    //TODO: Change this when we assume more than one sink nodes
    OperatorNodePtr sourceOperator = sourceOperators[0];
    const std::vector<NodePtr>& rootOperatorNodes = sourceOperator->getAllRootNodes();
    OperatorNodePtr sinkOperator = rootOperatorNodes[0]->as<SinkLogicalOperatorNode>();
    TopologyNodePtr sourceTopologyNode = mergedGraphSourceNodes[0];
    std::vector<NodePtr> rootNodes = sourceTopologyNode->getAllRootNodes();
    pinnedOperatorLocationMap[sinkOperator->getId()] = rootNodes[0]->as<TopologyNode>();

    NES_DEBUG("BasePlacementStrategy: Update the source to topology node map using the source nodes from the merged topology graph");
    for (auto& entry : mapOfSourceToTopologyNodes) {
        NES_TRACE("BasePlacementStrategy: Taking nodes from the merged sub graphs and replacing the initial source topology nodes.");
        std::vector<TopologyNodePtr> sourceNodes;
        for (auto sourceNode : entry.second) {
            auto found = std::find_if(mergedGraphSourceNodes.begin(), mergedGraphSourceNodes.end(), [&](TopologyNodePtr sourceNodeToUse) {
                return sourceNode->getId() == sourceNodeToUse->getId();
            });
            if (found == mergedGraphSourceNodes.end()) {
                NES_ERROR("BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
                throw QueryPlacementException("BasePlacementStrategy: unable to locate the initial source node in the merged sub graph.");
            }
            NES_TRACE("BasePlacementStrategy: Inserting the source node from the merged graph into the new source node location.");
            sourceNodes.push_back(*found);
        }
        NES_TRACE("BasePlacementStrategy: Updating the source to topology node map.");
        mapOfSourceToTopologyNodes[entry.first] = sourceNodes;
    }

    for (auto sourceNode : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceNode->getSourceDescriptor();
        std::string logicalSourceName = sourceDescriptor->getStreamName();
        NES_TRACE("BasePlacementStrategy: Get the topology node for logical source name " << logicalSourceName);
        std::vector<TopologyNodePtr> topologyNodes = mapOfSourceToTopologyNodes[logicalSourceName];
        if (topologyNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: unable to find topology nodes for logical source " << logicalSourceName);
            throw QueryPlacementException("BasePlacementStrategy: unable to find topology nodes for logical source " + logicalSourceName);
        }

        TopologyNodePtr candidateTopologyNode = topologyNodes[0];
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
            throw QueryPlacementException("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        }
        pinnedOperatorLocationMap[sourceNode->getId()] = candidateTopologyNode;
        topologyNodes.erase(topologyNodes.begin());
        mapOfSourceToTopologyNodes[logicalSourceName] = topologyNodes;
    }
}

ExecutionNodePtr BasePlacementStrategy::getExecutionNode(TopologyNodePtr candidateTopologyNode) {

    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode->getId())) {
        NES_TRACE("BottomUpStrategy: node " << candidateTopologyNode->toString() << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateTopologyNode->getId());
    } else {
        NES_TRACE("BottomUpStrategy: create new execution node with id: " << candidateTopologyNode->getId());
        candidateExecutionNode = ExecutionNode::createExecutionNode(candidateTopologyNode);
        NES_TRACE("BottomUpStrategy: Adding new execution node with id: " << candidateTopologyNode->getId());
        if (!globalExecutionPlan->addExecutionNode(candidateExecutionNode)) {
            NES_ERROR("BottomUpStrategy: failed to add execution node");
            throw QueryPlacementException("BottomUpStrategy: failed to add execution node");
        }
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
        throw QueryPlacementException("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
    }
    return candidateTopologyNode;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId, uint64_t sourceOperatorId, TopologyNodePtr sourceTopologyNode) {

    NES_DEBUG("BasePlacementStrategy: create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(), sourceTopologyNode->getIpAddress(), sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    const OperatorNodePtr networkSink = LogicalOperatorFactory::createSinkOperator(Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
    networkSink->setId(UtilityFunctions::getNextOperatorId());
    return networkSink;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId, SchemaPtr inputSchema, uint64_t operatorId) {
    NES_DEBUG("BasePlacementStrategy: create Network Source operator");
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
    const OperatorNodePtr networkSource = LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(inputSchema, nesPartition));
    networkSource->setId(operatorId);
    return networkSource;
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(QueryPlanPtr queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    NES_TRACE("BasePlacementStrategy: For each source operator check for the assignment of network source and sink operators");
    for (auto& sourceOperator : sourceOperators) {
        placeNetworkOperator(queryId, sourceOperator);
    }
}

bool BasePlacementStrategy::runTypeInferencePhase(QueryId queryId) {
    NES_DEBUG("BasePlacementStrategy: Run type inference phase for all the query sub plans to be deployed.");
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (auto executionNode : executionNodes) {
        NES_TRACE("BasePlacementStrategy: Get all query sub plans on the execution node for the query with id " << queryId);
        const std::vector<QueryPlanPtr>& querySubPlans = executionNode->getQuerySubPlans(queryId);
        for (auto& querySubPlan : querySubPlans) {
            typeInferencePhase->execute(querySubPlan);
        }
    }
    return true;
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId, OperatorNodePtr operatorNode) {

    NES_TRACE("BasePlacementStrategy: Get execution node where operator is placed");
    ExecutionNodePtr executionNode = operatorToExecutionNodeMap[operatorNode->getId()];

    for (auto& parent : operatorNode->getParents()) {

        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        NES_TRACE("BasePlacementStrategy: Get execution node where parent operator is placed");
        ExecutionNodePtr parentExecutionNode = operatorToExecutionNodeMap[parentOperator->getId()];

        if (executionNode->getId() != parentExecutionNode->getId()) {
            NES_TRACE("BasePlacementStrategy: Parent and its child operator are placed on different physical node.");

            NES_TRACE("BasePlacementStrategy: Find the nodes between the topology node (inclusive) for child and parent operators.");
            TopologyNodePtr sourceNode = executionNode->getTopologyNode();
            TopologyNodePtr destinationNode = parentExecutionNode->getTopologyNode();
            std::vector<TopologyNodePtr> nodesBetween = topology->findTopologyNodesBetween(sourceNode, destinationNode);

            NES_TRACE("BasePlacementStrategy: For all the nodes between the topology node for child and parent operators add respective source or sink operator.");
            const SchemaPtr& inputSchema = operatorNode->getOutputSchema();
            uint64_t sourceOperatorId = UtilityFunctions::getNextOperatorId();
            for (int i = 0; i < nodesBetween.size(); i++) {

                if (i == 0) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with child operator.");
                    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr rootOperator = querySubPlan->getRootOperators()[0];
                        if (rootOperator->getId() == operatorNode->getId()) {
                            NES_TRACE("BasePlacementStrategy: Add network sink operator as root of the query plan with child operator.");
                            OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                            querySubPlan->appendOperatorAsNewRoot(networkSink);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_WARNING("BasePlacementStrategy: unable to place network sink operator for the child operator");
                        throw QueryPlacementException("BasePlacementStrategy: unable to place network sink operator for the child operator");
                    }
                    continue;
                }

                if (i == nodesBetween.size() - 1) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with parent operator.");
                    std::vector<QueryPlanPtr> querySubPlans = parentExecutionNode->getQuerySubPlans(queryId);
                    OperatorNodePtr sourceOperator = createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        if (parentOperator->isNAryOperator()) {
                            NES_TRACE("BasePlacementStrategy: parent operator is binary operator.");
                            const OperatorNodePtr& operatorWithParentId = querySubPlan->getOperatorWithId(parentOperator->getId());
                            if (operatorWithParentId) {
                                NES_TRACE("BasePlacementStrategy: add network source operator as child to the parent operator.");
                                operatorWithParentId->addChild(sourceOperator);
                                found = true;
                                break;
                            }
                        } else {
                            NES_TRACE("BasePlacementStrategy: Get all leaf operators and check if it contains the parent operator");
                            std::vector<OperatorNodePtr> leafOperators = querySubPlan->getLeafOperators();
                            for (auto& leafOperator : leafOperators) {
                                if (leafOperator->getId() == parentOperator->getId()) {
                                    leafOperator->addChild(sourceOperator);
                                    NES_TRACE("BasePlacementStrategy: add network source operator as child to the parent operator.");
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!found) {
                        NES_WARNING("BasePlacementStrategy: unable to place network source operator for the parent operator");
                        throw QueryPlacementException("BasePlacementStrategy: unable to place network source operator for the parent operator");
                    }
                    continue;
                }

                NES_TRACE("BasePlacementStrategy: Find the execution node for the topology node.");
                ExecutionNodePtr candidateExecutionNode = getExecutionNode(nodesBetween[i]);

                NES_TRACE("BasePlacementStrategy: Create a new query plan and add pair of network source and network sink operators.");
                QueryPlanPtr querySubPlan = QueryPlan::create();
                querySubPlan->setQueryId(queryId);
                querySubPlan->setQuerySubPlanId(UtilityFunctions::getNextQuerySubPlanId());

                NES_TRACE("BasePlacementStrategy: add network source operator");
                const OperatorNodePtr networkSource = createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId);
                querySubPlan->appendOperatorAsNewRoot(networkSource);

                NES_TRACE("BasePlacementStrategy: add network sink operator");
                sourceOperatorId = UtilityFunctions::getNextOperatorId();
                OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                querySubPlan->appendOperatorAsNewRoot(networkSink);

                NES_TRACE("BasePlacementStrategy: add query plan to execution node and update the global execution plan");
                candidateExecutionNode->addNewQuerySubPlan(queryId, querySubPlan);
                globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
            }
        }

        NES_TRACE("BasePlacementStrategy: add network source and sink operator for the parent operator");
        placeNetworkOperator(queryId, parentOperator);
    }
}

}// namespace NES
