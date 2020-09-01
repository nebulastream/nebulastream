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

using namespace std;
namespace NES {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topologyPtr, TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), topology(topologyPtr), typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog), pinnedOperatorLocationMap() {}

void BasePlacementStrategy::mapLogicalSourceToTopologyNodes(QueryId queryId, std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {

    NES_DEBUG("BasePlacementStrategy: Prepare a map of source to physical nodes");
    NES_DEBUG("BasePlacementStrategy: Clear the previous mapping");
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
        vector<TopologyNodePtr> topoSubGraphSourceNodes = this->topology->findPathBetween(sourceNodes, {sinkNode});
        allSourceNodes.insert(allSourceNodes.end(), topoSubGraphSourceNodes.begin(), topoSubGraphSourceNodes.end());
    }

    NES_DEBUG("BasePlacementStrategy: Merge all the topology sub-graphs found using their source nodes");
    std::vector<TopologyNodePtr> mergedGraphSourceNodes = topology->mergeGraphs(allSourceNodes);

    NES_DEBUG("BasePlacementStrategy: Adding location for sink operator.");
    //TODO: Change this when we assume more than one sink nodes
    OperatorNodePtr sourceOperator = sourceOperators[0];
    const vector<NodePtr>& rootOperatorNodes = sourceOperator->getAllRootNodes();
    OperatorNodePtr sinkOperator = rootOperatorNodes[0]->as<SinkLogicalOperatorNode>();
    TopologyNodePtr sourceTopologyNode = mergedGraphSourceNodes[0];
    vector<NodePtr> rootNodes = sourceTopologyNode->getAllRootNodes();
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
        string logicalSourceName = sourceDescriptor->getStreamName();
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

ExecutionNodePtr BasePlacementStrategy::getCandidateExecutionNode(TopologyNodePtr candidateTopologyNode) {

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

uint64_t BasePlacementStrategy::addNetworkSinkOperator(QueryPlanPtr queryPlan, TopologyNodePtr parentNesNode) {

    NES_DEBUG("BasePlacementStrategy: found no existing network source operator in the parent execution node");
    QueryId queryId = queryPlan->getQueryId();

    uint64_t operatorId = UtilityFunctions::getNextOperatorId();
    uint64_t nextNetworkSourceOperatorId = UtilityFunctions::getNextOperatorId();
    Network::NodeLocation nodeLocation(parentNesNode->getId(), parentNesNode->getIpAddress(), parentNesNode->getDataPort());
    Network::NesPartition nesPartition(queryId, nextNetworkSourceOperatorId, nextNetworkSourceOperatorId, 0);
    const OperatorNodePtr sysSinkOperator = createSinkLogicalOperatorNode(Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
    sysSinkOperator->setId(operatorId);
    queryPlan->appendPreExistingOperator(sysSinkOperator);
    return nextNetworkSourceOperatorId;
}

void BasePlacementStrategy::addNetworkSourceOperator(QueryPlanPtr queryPlan, SchemaPtr inputSchema, uint64_t operatorId) {
    const Network::NesPartition nesPartition = Network::NesPartition(queryPlan->getQueryId(), operatorId, operatorId, 0);
    const OperatorNodePtr sysSourceOperator = createSourceLogicalOperatorNode(Network::NetworkSourceDescriptor::create(inputSchema, nesPartition));
    sysSourceOperator->setId(operatorId);
    queryPlan->appendPreExistingOperator(sysSourceOperator);
}

}// namespace NES
