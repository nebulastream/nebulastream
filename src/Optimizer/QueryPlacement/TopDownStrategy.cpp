#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
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

    NES_DEBUG("TopDownStrategy: Get all sink operators");
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
    if (sinkOperators.empty()) {
        NES_ERROR("TopDownStrategy: No sink operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("TopDownStrategy: No sink operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("TopDownStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryPlan);
    addSystemGeneratedOperators(queryPlan);
    runTypeInferencePhase(queryId);
    return true;
}

void TopDownStrategy::placeOperators(QueryPlanPtr queryPlan) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("TopDownStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    mapLogicalSourceToTopologyNodes(queryId, sourceOperators);

    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = queryPlan->getSinkOperators();

    for (auto sinkOperator : sinkOperators) {
        //TODO: Handle when we assume more than one sink nodes
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sinkOperator->getId());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
            throw QueryPlacementException("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
        }

        recursiveOperatorPlacement(queryId, sinkOperator, candidateTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}

void TopDownStrategy::recursiveOperatorPlacement(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode) {

    if (candidateOperator->isNAryOperator() && !candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
        std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForParentOperators(candidateOperator);
        if (parentTopologyNodes.empty()) {
            return;
        }

        std::vector<SourceLogicalOperatorNodePtr> sourceOperators = candidateOperator->getNodesByType<SourceLogicalOperatorNode>();
        if (sourceOperators.empty()) {
            throw QueryPlacementException("Unable to find the source operators to the candidate operator");
        }

        std::vector<TopologyNodePtr> childNodes;

        for (auto& sourceOperator : sourceOperators) {
            childNodes.push_back(getTopologyNodeForPinnedOperator(sourceOperator->getId()));
        }
        candidateTopologyNode = topology->findCommonNodeBetween(childNodes, parentTopologyNodes);
    } else if (candidateOperator->instanceOf<SourceLogicalOperatorNode>()) {
        candidateTopologyNode = getTopologyNodeForPinnedOperator(candidateOperator->getId());
    }

    if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("TopDownStrategy: Find the next NES node in the path where operator can be placed");

        while (!candidateTopologyNode->getChildren().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getChildren()[0]->as<TopologyNode>();
            if (candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("TopDownStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("TopDownStrategy: No node available for further placement of operators");
        throw QueryPlacementException("TopDownStrategy: No node available for further placement of operators");
    }

    ExecutionNodePtr candidateExecutionNode = getCandidateExecutionNode(candidateTopologyNode);

    QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, candidateOperator, candidateExecutionNode);
    candidateQueryPlan->prependOperator(candidateOperator->copy());
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;
    if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
        NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
    }
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
    NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    topology->reduceResources(candidateTopologyNode->getId(), 1);

    for (auto& child : candidateOperator->getChildren()) {
        recursiveOperatorPlacement(queryId, child->as<OperatorNode>(), candidateTopologyNode);
    }
}

QueryPlanPtr TopDownStrategy::getCandidateQueryPlan(QueryId queryId, OperatorNodePtr candidateOperator, ExecutionNodePtr executionNode) {
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);

    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
        return candidateQueryPlan;
    }

    std::vector<NodePtr> children = candidateOperator->getChildren();
    std::vector<QueryPlanPtr> queryPlansWithChildren;
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr querySubPlan) {
            return querySubPlan->hasOperator(child->as<OperatorNode>());
        });

        if (found != querySubPlans.end()) {
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithChildren.size() > 1) {
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            return candidateQueryPlan;
        } else if (queryPlansWithChildren.size() == 1) {
            return queryPlansWithChildren[0];
        }
    }
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator) {

    std::vector<TopologyNodePtr> parentTopologyNodes;
    std::vector<NodePtr> parents = candidateOperator->getParents();
    for (auto& parent : parents) {
        const auto& found = operatorToExecutionNodeMap.find(parent->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        parentTopologyNodes.push_back(childTopologyNode);
    }
    return parentTopologyNodes;
}

}// namespace NES
