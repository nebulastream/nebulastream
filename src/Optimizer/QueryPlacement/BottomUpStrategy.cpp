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
    mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);
    NES_DEBUG("BottomUpStrategy: Get all sink operators");
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
    if (sinkOperators.empty()) {
        NES_ERROR("BottomUpStrategy: No sink operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("BottomUpStrategy: No sink operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("BottomUpStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryPlan);
    addSystemGeneratedOperators(queryPlan);
    runTypeInferencePhase(queryId);
    return true;
}

void BottomUpStrategy::placeOperators(QueryPlanPtr queryPlan) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    for (auto& sourceOperator : sourceOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sourceOperator->getId());
        recursiveOperatorPlacement(queryId, sourceOperator, candidateTopologyNode);
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::recursiveOperatorPlacement(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode) {

    if (candidateOperator->isNAryOperator()) {
        //Check if all children operators already placed
        std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(candidateOperator);
        if (childTopologyNodes.empty()) {
            return;
        }
        candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
        if (!candidateTopologyNode) {
            NES_ERROR("BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
            throw QueryPlacementException("BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
        }
    } else if (candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
        candidateTopologyNode = getTopologyNodeForPinnedOperator(candidateOperator->getId());
    }

    if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
        while (!candidateTopologyNode->getParents().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
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
    QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, candidateOperator, candidateExecutionNode);
    candidateQueryPlan->appendOperator(candidateOperator->copy());
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;
    if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
        NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
    }
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
    NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    topology->reduceResources(candidateTopologyNode->getId(), 1);

    for (auto& parent : candidateOperator->getParents()) {
        recursiveOperatorPlacement(queryId, parent->as<OperatorNode>(), candidateTopologyNode);
    }
}

QueryPlanPtr BottomUpStrategy::getCandidateQueryPlan(QueryId queryId, OperatorNodePtr candidateOperator, ExecutionNodePtr executionNode) {
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

std::vector<TopologyNodePtr> BottomUpStrategy::getTopologyNodesForChildrenOperators(OperatorNodePtr candidateOperator) {

    std::vector<TopologyNodePtr> childTopologyNodes;
    std::vector<NodePtr> children = candidateOperator->getChildren();
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
