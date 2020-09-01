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
    return placeOperators(queryPlan);
}

bool TopDownStrategy::placeOperators(QueryPlanPtr queryPlan) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("TopDownStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    mapLogicalSourceToTopologyNodes(queryId, sourceOperators);

    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = queryPlan->getSinkOperators();

    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;
    for (auto sinkOperator : sinkOperators) {
        //TODO: Handle when we assume more than one sink nodes
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sinkOperator->getId());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
            throw QueryPlacementException("TopDownStrategy: Unable to find resources on the physical node for placement of sink operator");
        }

        QueryPlanPtr startingQuerySubPlan = QueryPlan::create();
        startingQuerySubPlan->setQueryId(queryId);
        startingQuerySubPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
        ExecutionNodePtr executionNode = getCandidateExecutionNode(candidateTopologyNode);
        executionNode->addNewQuerySubPlan(queryId, startingQuerySubPlan);
        recursiveOperatorPlacement(startingQuerySubPlan, sinkOperator, candidateTopologyNode, operatorToExecutionNodeMap);
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
    return true;
}

void TopDownStrategy::recursiveOperatorPlacement(QueryPlanPtr candidateQueryPlan, OperatorNodePtr candidateOperator,
                                                 TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    QueryId queryId = candidateQueryPlan->getQueryId();
    if (candidateOperator->isNAryOperator() || candidateOperator->instanceOf<SourceLogicalOperatorNode>()) {
        placeNAryOrSinkOperator(queryId, candidateOperator, candidateTopologyNode, operatorToExecutionNodeMap);
        return;
    } else if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");



        while (!candidateTopologyNode->getChildren().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getChildren()[0]->as<TopologyNode>();

            SchemaPtr inputSchema = candidateOperator->getOutputSchema();
//            addNetworkSourceOperator(candidateQueryPlan, inputSchema, networkSourceOperatorId);
            typeInferencePhase->execute(candidateQueryPlan);
            ExecutionNodePtr executionNode = getCandidateExecutionNode(candidateTopologyNode);
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQueryExecutionPlanId(UtilityFunctions::getNextQueryExecutionId());
//            uint64_t networkSourceOperatorId = addNetworkSinkOperator(candidateQueryPlan, candidateTopologyNode);
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
//    typeInferencePhase->execute(candidateQueryPlan);
    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;
    globalExecutionPlan->scheduleExecutionNode(candidateExecutionNode);
    NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    candidateTopologyNode->reduceResources(1);
    topology->reduceResources(candidateTopologyNode->getId(), 1);

    for (auto& child : candidateOperator->getChildren()) {
        recursiveOperatorPlacement(candidateQueryPlan, child->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

void TopDownStrategy::placeNAryOrSinkOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode,
                                              std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    //Check if all children operators already placed
    std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForParentOperators(candidateOperator, operatorToExecutionNodeMap);

    if (parentTopologyNodes.empty()) {
        return;
    }

    //Find the candidate node
    if (candidateOperator->instanceOf<SourceLogicalOperatorNode>()) {
        if (candidateTopologyNode->getId() != topology->getRoot()->getId()) {
            candidateTopologyNode = candidateTopologyNode->getAllRootNodes()[0]->as<TopologyNode>();
        }
    } else {
        candidateTopologyNode = topology->findCommonAncestor(parentTopologyNodes);
        if (!candidateTopologyNode) {
            NES_ERROR("BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
            throw QueryPlacementException("BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
        }
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
    std::vector<NodePtr> childrenOperator = candidateOperator->getChildren();
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

        std::vector<QueryPlanPtr> querySubPlans = startExecutionNode->getQuerySubPlans(queryId);
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
            const std::vector<OperatorNodePtr> rootOperators = querySubPlan->getRootOperators();
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
    for (auto& child : candidateOperator->getChildren()) {
        recursiveOperatorPlacement(querySubPlan, child->as<OperatorNode>(), candidateTopologyNode, operatorToExecutionNodeMap);
    }
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

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
