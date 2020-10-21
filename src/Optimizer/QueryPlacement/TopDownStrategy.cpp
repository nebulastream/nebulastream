#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

std::unique_ptr<TopDownStrategy> TopDownStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                                         StreamCatalogPtr streamCatalog) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

TopDownStrategy::TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                 StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool TopDownStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("TopDownStrategy: Performing placement of the input query plan with id " << queryId);

    NES_DEBUG("TopDownStrategy: Get all source operators");
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    if (sourceOperators.empty()) {
        NES_ERROR("TopDownStrategy: No source operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("TopDownStrategy: No source operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("TopDownStrategy: map pinned operators to the physical location");
    mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);

    NES_DEBUG("TopDownStrategy: Get all sink operators");
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
    if (sinkOperators.empty()) {
        NES_ERROR("TopDownStrategy: No sink operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("TopDownStrategy: No sink operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("TopDownStrategy: place query plan with id : " << queryId);
    placeQueryPlan(queryPlan);
    NES_DEBUG("TopDownStrategy: Add system generated operators for query with id : " << queryId);
    addNetworkSourceAndSinkOperators(queryPlan);
    NES_DEBUG("TopDownStrategy: clear the temporary map : " << queryId);
    operatorToExecutionNodeMap.clear();
    pinnedOperatorLocationMap.clear();
    NES_DEBUG("TopDownStrategy: Run type inference phase for query plans in global execution plan for query with id : " << queryId);

    NES_DEBUG("TopDownStrategy: Update Global Execution Plan : \n"
              << globalExecutionPlan->getAsString());
    return runTypeInferencePhase(queryId);
}

void TopDownStrategy::placeQueryPlan(QueryPlanPtr queryPlan) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("TopDownStrategy: Get the all sink operators for performing the placement.");
    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = queryPlan->getSinkOperators();

    NES_TRACE("TopDownStrategy: Place all sink operators.");
    for (auto sinkOperator : sinkOperators) {
        NES_TRACE("TopDownStrategy: Get the topology node for the sink operator.");
        //TODO: Handle when we assume more than one sink nodes
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sinkOperator->getId());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
            throw QueryPlacementException("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
        }
        placeOperator(queryId, sinkOperator, candidateTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}

void TopDownStrategy::placeOperator(QueryId queryId, OperatorNodePtr operatorNode, TopologyNodePtr candidateTopologyNode) {

    NES_DEBUG("TopDownStrategy: Place " << operatorNode);
    if ((operatorNode->isNAryOperator() || operatorNode->instanceOf<SourceLogicalOperatorNode>()) && !operatorNode->instanceOf<SinkLogicalOperatorNode>()) {

        NES_TRACE("TopDownStrategy: Received an NAry operator for placement.");
        NES_TRACE("TopDownStrategy: Get the topology nodes where parent operators are placed.");
        std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForParentOperators(operatorNode);
        if (parentTopologyNodes.empty()) {
            NES_WARNING("TopDownStrategy: No topology node found where parent operators are placed.");
            return;
        }

        NES_TRACE("TopDownStrategy: Get the topology nodes where children source operators are to be placed.");
        std::vector<TopologyNodePtr> childNodes = getTopologyNodesForSourceOperators(operatorNode);

        NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
        candidateTopologyNode = topology->findCommonNodeBetween(childNodes, parentTopologyNodes);

        if (!candidateTopologyNode) {
            NES_ERROR("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator " << operatorNode);
            throw QueryPlacementException("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator " + operatorNode->toString());
        }

        if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
            NES_ERROR("TopDownStrategy: Received Source operator for placement.");

            auto pinnedSourceOperatorLocation = getTopologyNodeForPinnedOperator(operatorNode->getId());
            if (pinnedSourceOperatorLocation->getId() == candidateTopologyNode->getId() || pinnedSourceOperatorLocation->containAsParent(candidateTopologyNode)) {
                candidateTopologyNode = pinnedSourceOperatorLocation;
            } else {
                NES_ERROR("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is to be placed.");
                throw QueryPlacementException("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is to be placed.");
            }

            if (candidateTopologyNode->getAvailableResources() == 0) {
                NES_ERROR("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                throw QueryPlacementException("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
            }
        }
    }

    if (candidateTopologyNode->getAvailableResources() == 0) {
        NES_DEBUG("TopDownStrategy: Find the next Topology node where operator can be placed");
        while (!candidateTopologyNode) {
            //FIXME: we are considering only one root node currently
            NES_TRACE("TopDownStrategy: Get the topology nodes where children source operators are to be placed.");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForSourceOperators(operatorNode);
            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            candidateTopologyNode = topology->findCommonNodeBetween(childNodes, {candidateTopologyNode});
            if (candidateTopologyNode && candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("TopDownStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("TopDownStrategy: No node available for further placement of operators");
        throw QueryPlacementException("TopDownStrategy: No node available for further placement of operators");
    }

    NES_TRACE("TopDownStrategy: Get the candidate execution node for the candidate topology node.");
    ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

    NES_TRACE("TopDownStrategy: Find and prepend operator to the candidate query plan.");
    QueryPlanPtr candidateQueryPlan = addOperatorToCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);

    NES_TRACE("TopDownStrategy: Add the query plan to the candidate execution node.");
    if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
        NES_ERROR("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
    }
    NES_TRACE("TopDownStrategy: Update the global execution plan with candidate execution node");
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);

    NES_TRACE("TopDownStrategy: Place the information about the candidate execution plan and operator id in the map.");
    operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
    NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    topology->reduceResources(candidateTopologyNode->getId(), 1);

    NES_TRACE("TopDownStrategy: Place the children operators.");
    for (auto& child : operatorNode->getChildren()) {
        placeOperator(queryId, child->as<OperatorNode>(), candidateTopologyNode);
    }
}

QueryPlanPtr TopDownStrategy::addOperatorToCandidateQueryPlan(QueryId queryId, OperatorNodePtr candidateOperator, ExecutionNodePtr executionNode) {

    OperatorNodePtr candidateOperatorCopy = candidateOperator->copy();
    NES_DEBUG("TopDownStrategy: Get candidate query plan for the operator " << candidateOperator << " on execution node with id " << executionNode->getId());
    NES_TRACE("TopDownStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("TopDownStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(UtilityFunctions::getNextQuerySubPlanId());
        candidateQueryPlan->addRootOperator(candidateOperatorCopy);
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithParent;
    NES_TRACE("TopDownStrategy: Find query plans with parent operators for the input logical operator.");
    std::vector<NodePtr> parents = candidateOperator->getParents();
    //NOTE: we do not check for child operators as we are performing Top down placement.
    for (auto& parent : parents) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr querySubPlan) {
            return querySubPlan->hasOperatorWithId(parent->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("TopDownStrategy: Found query plan with parent operator " << parent);
            queryPlansWithParent.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithParent.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithParent.size() > 1) {
            NES_TRACE("TopDownStrategy: Found more than 1 query plan with the parent operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQuerySubPlanId(UtilityFunctions::getNextQuerySubPlanId());
            NES_TRACE("TopDownStrategy: Prepare a new query plan and add the root of the query plans with parent operators as the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithParent) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
        } else if (queryPlansWithParent.size() == 1) {
            NES_TRACE("TopDownStrategy: Found only 1 query plan with the parent operator of the input logical operator.");
            candidateQueryPlan = queryPlansWithParent[0];
        }

        if (candidateQueryPlan) {
            NES_TRACE("TopDownStrategy: Prepend candidate operator and update the plan.");
            for (auto candidateParent : parents) {
                auto parentOperator = candidateQueryPlan->getOperatorWithId(candidateParent->as<OperatorNode>()->getId());
                if (parentOperator) {
                    parentOperator->addChild(candidateOperatorCopy);
                }
            }
            return candidateQueryPlan;
        }
    }
    NES_TRACE("TopDownStrategy: no query plan exists with the parent operator of the input logical operator. Returning an empty query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(UtilityFunctions::getNextQuerySubPlanId());
    candidateQueryPlan->addRootOperator(candidateOperatorCopy);
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator) {

    std::vector<TopologyNodePtr> parentTopologyNodes;
    NES_DEBUG("TopDownStrategy: Get topology nodes with parent operators");
    std::vector<NodePtr> parents = candidateOperator->getParents();
    //iterate over parent operators and get the physical location where operator is placed
    for (auto& parent : parents) {
        const auto& found = operatorToExecutionNodeMap.find(parent->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("TopDownStrategy: unable to find topology for parent operator.");
            return {};
        }
        TopologyNodePtr parentTopologyNode = found->second->getTopologyNode();
        parentTopologyNodes.push_back(parentTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: returning list of topology nodes where parent operators are placed");
    return parentTopologyNodes;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForSourceOperators(OperatorNodePtr candidateOperator) {
    NES_TRACE("TopDownStrategy: Get the pinned topology nodes for the source operators of the input operator.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = candidateOperator->getNodesByType<SourceLogicalOperatorNode>();
    if (sourceOperators.empty()) {
        NES_ERROR("TopDownStrategy: Unable to find the source operators to the candidate operator");
        throw QueryPlacementException("TopDownStrategy: Unable to find the source operators to the candidate operator");
    }
    std::vector<TopologyNodePtr> childNodes;
    for (auto& sourceOperator : sourceOperators) {
        childNodes.push_back(getTopologyNodeForPinnedOperator(sourceOperator->getId()));
    }
    return childNodes;
}

}// namespace NES
