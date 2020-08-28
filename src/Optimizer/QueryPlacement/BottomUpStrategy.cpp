#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
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
    if (sourceOperators.empty()) {
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

    QueryId queryId = queryPlan->getQueryId();
    TopologyNodePtr sinkNode = topology->getRoot();

    vector<TopologyNodePtr> topoSubGrphForOP;
    for (auto& entry : mapOfSourceToTopologyNodes) {
        NES_TRACE("BottomUpStrategy: ");
        vector<TopologyNodePtr> topoSubGrphForSrc = topology->findPathBetween(entry.second, {sinkNode});
        mapOfSourceToTopologyNodes[entry.first] = topoSubGrphForSrc;
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
        mapOfSourceToTopologyNodes[sourceDescriptor->getStreamName()] = topologyNodes;
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            throw QueryPlacementException("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
        }

        QueryPlanPtr startingQuerySubPlan = QueryPlan::create();
        startingQuerySubPlan->setQueryId(queryId);
        startingQuerySubPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
        ExecutionNodePtr executionNode = getCandidateExecutionNode(candidateTopologyNode);
        executionNode->addNewQuerySubPlan(queryId, startingQuerySubPlan);
        recursiveOperatorPlacement(startingQuerySubPlan, sourceOperator, candidateTopologyNode, operatorToExecutionNodeMap);
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
            typeInferencePhase->execute(candidateQueryPlan);
            ExecutionNodePtr executionNode = getCandidateExecutionNode(candidateTopologyNode);
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
            addNetworkSourceOperator(candidateQueryPlan, inputSchema, networkSourceOperatorId);
            typeInferencePhase->execute(candidateQueryPlan);
            executionNode->addNewQuerySubPlan(queryId, candidateQueryPlan);
            globalExecutionPlan->scheduleExecutionNode(executionNode);
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

    ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(candidateTopologyNode);
    candidateQueryPlan->appendPreExistingOperator(candidateOperator->copy());
    typeInferencePhase->execute(candidateQueryPlan);
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
    NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    topology->reduceResources(candidateTopologyNode->getId(), 1);

    for (auto& parent : candidateOperator->getParents()) {
        recursiveOperatorPlacement(candidateQueryPlan, parent->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

void BottomUpStrategy::placeNAryOrSinkOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode,
                                               std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    //Check if all children operators already placed
    std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(candidateOperator, operatorToExecutionNodeMap);

    if(childTopologyNodes.empty()){
        return;
    }

    //Find the candidate node
    if (candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
        if(candidateTopologyNode->getId() != topology->getRoot()->getId()){
            candidateTopologyNode = candidateTopologyNode->getAllRootNodes()[0]->as<TopologyNode>();
        }
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

        //        vector<TopologyNodePtr> startNodesForSubGraph = topology->findPathBetween({startTopologyNode}, {candidateTopologyNode});
        QueryPlanPtr startQueryPlan = *found;

        uint64_t networkSourceOperatorId = -1;

        //Start from next node
        TopologyNodePtr parentTopologyNode = startTopologyNode->getParents()[0]->as<TopologyNode>();

        while (parentTopologyNode) {

            networkSourceOperatorId = addNetworkSinkOperator(startQueryPlan, parentTopologyNode);
            typeInferencePhase->execute(startQueryPlan);
            ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(parentTopologyNode);
            startQueryPlan = QueryPlan::create();
            startQueryPlan->setQueryId(queryId);
            startQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
            SchemaPtr inputSchema = childOperator->getOutputSchema();
            addNetworkSourceOperator(startQueryPlan, inputSchema, networkSourceOperatorId);
            typeInferencePhase->execute(startQueryPlan);
            candidateExecutionNode->addNewQuerySubPlan(queryId, startQueryPlan);
            globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
            if (parentTopologyNode->getParents().empty()) {
                parentTopologyNode = nullptr;
            } else {
                parentTopologyNode = parentTopologyNode->getParents()[0]->as<TopologyNode>();
            }
        }
        childrenOperatorIds.push_back(networkSourceOperatorId);
    }

    if (childrenOperatorIds.empty()) {
        NES_ERROR("BottomUpStrategy: unable to find the children operator ids for the operator " << candidateOperator->toString());
        throw QueryPlacementException("BottomUpStrategy: unable to find the children operator ids for the operator " + candidateOperator->toString());
    }

    ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(candidateTopologyNode);
    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
    std::vector<QueryPlanPtr> queryPlansWithChildren;
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& childrenId : childrenOperatorIds) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr querySubPlan) {
            const vector<OperatorNodePtr> rootOperators = querySubPlan->getRootOperators();
            for (auto root : rootOperators) {
                if (root->as<OperatorNode>()->getId() == childrenId) {
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

    if (queryPlansWithChildren.size() != childrenOperatorIds.size()) {
        NES_ERROR("BottomUpStrategy: Unable to find all query sub plans with children nodes on the execution node.");
        throw QueryPlacementException("BottomUpStrategy: Unable to find all query sub plans with children nodes on the execution node.");
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
    querySubPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
    typeInferencePhase->execute(querySubPlan);
    querySubPlans.push_back(querySubPlan);
    NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;
    topology->reduceResources(candidateTopologyNode->getId(), 1);
    if (!candidateExecutionNode->updateQuerySubPlans(queryId, querySubPlans)) {
        NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
    }
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
    for (auto& parent : candidateOperator->getParents()) {
        recursiveOperatorPlacement(querySubPlan, parent->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

std::vector<TopologyNodePtr> BottomUpStrategy::getTopologyNodesForChildrenOperators(OperatorNodePtr candidateOperator, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

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
