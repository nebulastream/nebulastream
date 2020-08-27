#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Network/NodeLocation.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace std;
namespace NES {

std::unique_ptr<BottomUpStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("BottomUpStrategy: Performing placement of the input query plan with id " << queryId);

    NES_DEBUG("BottomUpStrategy: Get all source operators");
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    if (!sourceOperators.empty()) {
        NES_ERROR("BottomUpStrategy: No source operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("BottomUpStrategy: No source operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("BottomUpStrategy: Prepare a map of source to physical nodes");
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    for (auto& sourceOperator : sourceOperators) {
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw QueryPlacementException("BottomUpStrategy: Source Descriptor need stream name: " + queryId);
        }
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        if (mapOfSourceToTopologyNodes.find(streamName) != mapOfSourceToTopologyNodes.end()) {
            NES_TRACE("BottomUpStrategy: Entry for the logical stream " << streamName << " already present.");
            continue;
        }
        NES_TRACE("BottomUpStrategy: Get all topology nodes for the logical source stream");
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        if (sourceNodes.empty()) {
            NES_ERROR("BottomUpStrategy: No source found in the topology for stream " << streamName << "for query with id : " << queryId);
            throw QueryPlacementException("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
        }
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
    }

    NES_DEBUG("BottomUpStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryPlan, mapOfSourceToTopologyNodes);
    return true;
}

void BottomUpStrategy::placeOperators(QueryPlanPtr queryPlan, std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes) {


    TopologyNodePtr sinkNode = topology->getRoot();

    vector<TopologyNodePtr> topoSubGrphForOP;
    for (auto& entry : mapOfSourceToTopologyNodes) {
        NES_TRACE("BottomUpStrategy: ");
        vector<TopologyNodePtr> topoSubGrphForSrc = topology->findPathBetween(entry.second, {sinkNode});
        if (topoSubGrphForOP.empty()) {
            topoSubGrphForOP = topoSubGrphForSrc;
            continue;
        } else {
            topoSubGrphForOP.insert(topoSubGrphForOP.end(), topoSubGrphForSrc.begin(), topoSubGrphForSrc.end());
        }
        NES_TRACE("BottomUpStrategy: Merge the topology sub-graphs found till now");
        topoSubGrphForOP = topology->mergeGraphs(topoSubGrphForOP);
    }

    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;

    for (auto& sourceOperator : sourceOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        std::vector<TopologyNodePtr> topologyNodes = mapOfSourceToTopologyNodes[sourceDescriptor->getStreamName()];
        if (topologyNodes.empty()) {
            NES_ERROR("BottomUpStrategy: unable to find topology nodes for logical source " << sourceDescriptor->getStreamName());
            throw QueryPlacementException("BottomUpStrategy: unable to find topology nodes for logical source " + sourceDescriptor->getStreamName());
        }

        TopologyNodePtr candidateTopologyNode = topologyNodes[0];
        topologyNodes.erase(topologyNodes.begin());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            throw QueryPlacementException("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
        }

        QueryPlanPtr queryPlan = QueryPlan::create();
        recursiveOperatorPlacement(queryPlan, sourceOperator, candidateTopologyNode, operatorToExecutionNodeMap);
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

ExecutionNodePtr BottomUpStrategy::getCandidateExecutionNode(TopologyNodePtr candidateTopologyNode) {

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

void BottomUpStrategy::recursiveOperatorPlacement(QueryPlanPtr candidateQueryPlan, OperatorNodePtr candidateOperator,
                                                  TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    ExecutionNodePtr candidateExecutionNode;
    QueryId queryId = candidateQueryPlan->getQueryId();
    if (candidateOperator->isNAryOperator() || candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
        placeNAryOrSinkOperator(queryId, candidateOperator, candidateTopologyNode, operatorToExecutionNodeMap);
        return;
    } else if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
        while (!candidateTopologyNode->getParents().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();

            SchemaPtr inputSchema = candidateQueryPlan->getSourceOperators()[0]->getOutputSchema();
            uint64_t networkSourceOperatorId = addNetworkSinkOperator(candidateQueryPlan, candidateTopologyNode);

            candidateExecutionNode = getCandidateExecutionNode(candidateTopologyNode);
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            addNetworkSourceOperator(candidateQueryPlan, inputSchema, networkSourceOperatorId);

            candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan);

            if (candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
        throw QueryPlacementException("BottomUpStrategy: No node available for further placement of operators");
    }

    candidateQueryPlan->appendPreExistingOperator(candidateOperator->copy());
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;

    for (auto& parent : candidateOperator->getParents()) {
        recursiveOperatorPlacement(candidateQueryPlan, parent->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

void BottomUpStrategy::placeNAryOrSinkOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode,
                                               std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    //Check if all children operators already placed
    std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(candidateOperator, operatorToExecutionNodeMap);

    //Find the candidate node
    if (candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
        candidateTopologyNode = topology->getRoot();
    } else {
        candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
        if (candidateTopologyNode->getAvailableResources() == 0) {
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    break;
                }
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
        throw QueryPlacementException("BottomUpStrategy: No node available for further placement of operators");
    }

    //Perform placement of fwd operators
    std::vector<uint64_t> childrenOperatorIds;
    vector<NodePtr> childrenOperator = candidateOperator->getChildren();
    for (auto& child : childrenOperator) {
        OperatorNodePtr childOperator = child->as<OperatorNode>();
        uint64_t childOperatorId = childOperator->getId();
        ExecutionNodePtr startExecutionNode = operatorToExecutionNodeMap[childOperatorId];
        TopologyNodePtr startTopologyNode = startExecutionNode->getTopologyNode();

        if (startTopologyNode->getId() == candidateTopologyNode->getId()) {
            childrenOperatorIds.push_back(childOperatorId);
            NES_DEBUG("BottomUpStrategy: Skip adding candidate operator for now");
            continue;
        }

        vector<QueryPlanPtr> querySubPlans = startExecutionNode->getQuerySubPlans(queryId);
        auto found = find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr queryPlan) {
            //FIXME: We assume that there should not be more than one root operator for a query sub Plan
            // This may not work when we bring in split operator
            OperatorNodePtr rootOperator = queryPlan->getRootOperators()[0];
            return rootOperator->getId() == childOperatorId;
        });

        if (found == querySubPlans.end()) {
            NES_ERROR("BottomUpStrategy: unable to find query plan with child operator as root.");
            throw QueryPlacementException("BottomUpStrategy: unable to find query plan with child operator as root.");
        }

        vector<TopologyNodePtr> pathToCandidateOperator = topology->findPathBetween({startTopologyNode}, {candidateTopologyNode});
        //remove the child node
        pathToCandidateOperator.erase(pathToCandidateOperator.begin());
        QueryPlanPtr startQueryPlan = *found;

        uint64_t networkSourceOperatorId = -1;
        for (auto pathNode : pathToCandidateOperator) {
            networkSourceOperatorId = addNetworkSinkOperator(startQueryPlan, pathNode);

            ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(pathNode);
            startQueryPlan = QueryPlan::create();
            startQueryPlan->setQueryId(queryId);
            SchemaPtr inputSchema = childOperator->getOutputSchema();
            addNetworkSourceOperator(startQueryPlan, inputSchema, networkSourceOperatorId);

            candidateExecutionNode->addNewQuerySubPlan(queryId, startQueryPlan);
        }
        childrenOperatorIds.push_back(networkSourceOperatorId);
    }

    if (!childrenOperatorIds.empty()) {
        NES_ERROR("BottomUpStrategy: ");
        throw QueryPlacementException("BottomUpStrategy: ");
    }

    ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(candidateTopologyNode);
    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
    std::vector<QueryPlanPtr> queryPlansWithChildren;
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : candidateOperator->getChildren()) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr querySubPlan) {
            const vector<OperatorNodePtr> rootOperators = querySubPlan->getRootOperators();
            for (auto root : rootOperators) {
                if (root->as<OperatorNode>()->getId() == child->as<OperatorNode>()->getId()) {
                    return true;
                }
            }
            return false;
        });

        if (found != querySubPlans.end()) {
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    const OperatorNodePtr copyOfCandidateOperator = candidateOperator->copy();
    NES_DEBUG("BottomUpStrategy: Merging the Query plans");
    for (auto& queryPlan : queryPlansWithChildren) {
        for (auto& rootOperator : queryPlan->getRootOperators()) {
            copyOfCandidateOperator->addChild(rootOperator);
        }
    }

    QueryPlanPtr querySubPlan = QueryPlan::create(copyOfCandidateOperator);
    querySubPlan->setQueryId(queryId);
    querySubPlans.push_back(querySubPlan);
    if (!candidateExecutionNode->updateQuerySubPlans(queryId, querySubPlans)) {
        NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
    }

    for (auto& parent : candidateOperator->getParents()) {
        recursiveOperatorPlacement(querySubPlan, parent->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

std::vector<TopologyNodePtr> BottomUpStrategy::getTopologyNodesForChildrenOperators(OperatorNodePtr candidateOperator,
                                                                                    std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    std::vector<TopologyNodePtr> childTopologyNodes;
    vector<NodePtr> children = candidateOperator->getChildren();
    for (auto& child : children) {
        const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        childTopologyNodes.push_back(childTopologyNode);
    }
    return childTopologyNodes;
}

}// namespace NES
