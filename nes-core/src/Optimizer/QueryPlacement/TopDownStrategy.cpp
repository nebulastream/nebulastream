/*
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

#include <API/Query.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategyPtr TopDownStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                 TopologyPtr topology,
                                                 TypeInferencePhasePtr typeInferencePhase,
                                                 SourceCatalogPtr streamCatalog) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(std::move(globalExecutionPlan),
                                                             std::move(topology),
                                                             std::move(typeInferencePhase),
                                                             std::move(streamCatalog)));
}

TopDownStrategy::TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase,
                                 SourceCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool TopDownStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                FaultToleranceType faultToleranceType,
                                                LineageType lineageType,
                                                const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        performOperatorPlacement(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void TopDownStrategy::performOperatorPlacement(QueryId queryId,
                                               const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                               const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_TRACE("TopDownStrategy: Place all sink operators.");
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        NES_TRACE("TopDownStrategy: Get the topology node for the sink operator.");
        auto nodeId = std::any_cast<uint64_t>(pinnedDownStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned down stream operator was already placed then place all its upstream operators
        if (pinnedDownStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedDownStreamOperator->getProperty(PLACED))) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedDownStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& upStreamOperator : pinnedDownStreamOperator->getChildren()) {
                placeOperator(queryId, upStreamOperator->as<OperatorNode>(), candidateTopologyNode, pinnedUpStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedDownStreamOperator->getId())) {
                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
                throw Exception(
                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            placeOperator(queryId, pinnedDownStreamOperator, candidateTopologyNode, pinnedUpStreamOperators);
        }
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}

void TopDownStrategy::placeOperator(QueryId queryId,
                                    const OperatorNodePtr& operatorNode,
                                    TopologyNodePtr candidateTopologyNode,
                                    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators) {

    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
        operatorToExecutionNodeMap[operatorNode->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
        return;
    }

    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) {
        NES_DEBUG("TopDownStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() || operatorNode->instanceOf<SourceLogicalOperatorNode>())
            && !operatorNode->instanceOf<SinkLogicalOperatorNode>()) {

            NES_TRACE("TopDownStrategy: Received an NAry operator for placement.");
            NES_TRACE("TopDownStrategy: Get the topology nodes where parent operators are placed.");
            std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForParentOperators(operatorNode);
            if (parentTopologyNodes.empty()) {
                NES_WARNING("TopDownStrategy: No topology node found where parent operators are placed.");
                return;
            }

            NES_TRACE("TopDownStrategy: Get the topology nodes where children source operators are to be placed.");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForChildOperators(operatorNode);

            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            candidateTopologyNode = topology->findCommonNodeBetween(childNodes, parentTopologyNodes);

            if (!candidateTopologyNode) {
                NES_ERROR("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator "
                          << operatorNode->toString());
                throw Exception("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator "
                                + operatorNode->toString());
            }

            if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("TopDownStrategy: Received Source operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSourceOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSourceOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSourceOperatorLocation->containAsParent(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSourceOperatorLocation;
                } else {
                    NES_ERROR("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is to be "
                              "placed.");
                    throw Exception("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is "
                                    "to be placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                    throw Exception("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_DEBUG("TopDownStrategy: Find the next Topology node where operator can be placed");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForChildOperators(operatorNode);
            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            //FIXME: we are considering only one root node currently
            auto candidateTopologyNodes = topology->findNodesBetween(childNodes, {candidateTopologyNode});
            for (const auto& topologyNodes : candidateTopologyNodes) {
                if (topologyNodes && topologyNodes->getAvailableResources() > 0) {
                    candidateTopologyNode = topologyNodes;
                    NES_DEBUG(
                        "TopDownStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: No node available for further placement of operators");
            throw Exception("TopDownStrategy: No node available for further placement of operators");
        }

        //Pin the operator to the candidate node and mark as placed
        operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
        operatorNode->addProperty(PLACED, true);

        NES_TRACE("TopDownStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("TopDownStrategy: Find and prepend operator to the candidate query plan.");
        QueryPlanPtr candidateQueryPlan = addOperatorToCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);

        NES_TRACE("TopDownStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
            throw Exception("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query "
                            + std::to_string(queryId));
        }
        NES_TRACE("TopDownStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("TopDownStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedUpStreamOperator = std::find_if(pinnedUpStreamOperators.begin(),
                                                          pinnedUpStreamOperators.end(),
                                                          [operatorNode](const OperatorNodePtr& pinnedUpStreamOperator) {
                                                              return pinnedUpStreamOperator->getId() == operatorNode->getId();
                                                          });

    if (isOperatorAPinnedUpStreamOperator != pinnedUpStreamOperators.end()) {
        NES_DEBUG("BottomUpStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("TopDownStrategy: Place the children operators.");
    for (const auto& upstreamOperator : operatorNode->getChildren()) {
        placeOperator(queryId, upstreamOperator->as<OperatorNode>(), candidateTopologyNode, pinnedUpStreamOperators);
    }
}

QueryPlanPtr TopDownStrategy::addOperatorToCandidateQueryPlan(QueryId queryId,
                                                              const OperatorNodePtr& candidateOperator,
                                                              const ExecutionNodePtr& executionNode) {

    OperatorNodePtr candidateOperatorCopy = candidateOperator->copy();
    NES_DEBUG("TopDownStrategy: Get candidate query plan for the operator " << candidateOperator << " on execution node with id "
                                                                            << executionNode->getId());
    NES_TRACE("TopDownStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("TopDownStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
        candidateQueryPlan->addRootOperator(candidateOperatorCopy);
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithParent;
    NES_TRACE("TopDownStrategy: Find query plans with parent operators for the input logical operator.");
    std::vector<NodePtr> parents = candidateOperator->getParents();
    //NOTE: we do not check for child operators as we are performing Top down placement.
    for (auto& parent : parents) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
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
            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("TopDownStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
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
            for (const auto& candidateParent : parents) {
                auto parentOperator = candidateQueryPlan->getOperatorWithId(candidateParent->as<OperatorNode>()->getId());
                if (parentOperator) {
                    parentOperator->addChild(candidateOperatorCopy);
                }
            }
            return candidateQueryPlan;
        }
    }
    NES_TRACE("TopDownStrategy: no query plan exists with the parent operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    candidateQueryPlan->addRootOperator(candidateOperatorCopy);
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForParentOperators(const OperatorNodePtr& candidateOperator) {

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

std::vector<TopologyNodePtr> TopDownStrategy::getTopologyNodesForChildOperators(const OperatorNodePtr& candidateOperator) {
    std::vector<TopologyNodePtr> upStreamTopologyNodes;

    NES_TRACE("TopDownStrategy::getTopologyNodesForChildOperators: Get the pinned or closest placed upStreamOperators nodes for "
              "the the "
              "input operator.");
    //FIXME: This logic needs to be changed now as non source operators can also be pinned
    std::vector<NodePtr> upStreamOperators = {candidateOperator};

    while (!upStreamOperators.empty()) {
        auto upStreamOperator = upStreamOperators.back()->as<OperatorNode>();
        upStreamOperators.pop_back();
        if (upStreamOperator->hasProperty(PINNED_NODE_ID)) {
            auto nodeId = std::any_cast<uint64_t>(upStreamOperator->getProperty(PINNED_NODE_ID));
            auto pinnedTopologyNode = getTopologyNode(nodeId);
            upStreamTopologyNodes.emplace_back(pinnedTopologyNode);
            continue;
        }
        //TODO: Is this required?
        if (operatorToExecutionNodeMap.contains(upStreamOperator->getId())) {
            upStreamTopologyNodes.push_back(operatorToExecutionNodeMap[upStreamOperator->getId()]->getTopologyNode());
            continue;
        }
        upStreamOperators.insert(upStreamOperators.end(),
                                 upStreamOperator->getChildren().begin(),
                                 upStreamOperator->getChildren().end());
    }
    if (upStreamTopologyNodes.empty()) {
        NES_ERROR(
            "TopDownStrategy::getTopologyNodesForChildOperators: Unable to find the upStreamOperators operators to the candidate "
            "operator");
        throw Exception(
            "TopDownStrategy::getTopologyNodesForChildOperators: Unable to find the upStreamOperators operators to the "
            "candidate operator");
    }
    return upStreamTopologyNodes;
}
}// namespace NES::Optimizer
