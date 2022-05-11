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

#include "Exceptions/QueryPlacementException.hpp"
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <log4cxx/helpers/exception.h>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<ManualPlacementStrategy>
ManualPlacementStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                NES::TopologyPtr topology,
                                NES::Optimizer::TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<ManualPlacementStrategy>(
        ManualPlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

ManualPlacementStrategy::ManualPlacementStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                 NES::TopologyPtr topology,
                                                 NES::Optimizer::TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

void ManualPlacementStrategy::setBinaryMapping(PlacementMatrix userDefinedBinaryMapping) {
    this->binaryMapping = std::move(userDefinedBinaryMapping);
}

std::vector<TopologyNodePtr> ManualPlacementStrategy::getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode) {

    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("ManualStrategy: Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("ManualStrategy: unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        childTopologyNodes.push_back(childTopologyNode);
    }
    NES_DEBUG("ManualStrategy: returning list of topology nodes where children operators are placed");
    return childTopologyNodes;
}

QueryPlanPtr ManualPlacementStrategy::getCandidateQueryPlan(QueryId queryId,
                                                            const OperatorNodePtr& operatorNode,
                                                            const ExecutionNodePtr& executionNode) {

    NES_DEBUG("ManualStrategy: Get candidate query plan for the operator " << operatorNode << " on execution node with id "
                                                                             << executionNode->getId());
    NES_TRACE("ManualStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("ManualStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithChildren;
    NES_TRACE("ManualStrategy: Find query plans with child operators for the input logical operator.");
    std::vector<NodePtr> children = operatorNode->getChildren();
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(child->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("ManualStrategy: Found query plan with child operator " << child);
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithChildren.size() > 1) {
            NES_TRACE("ManualStrategy: Found more than 1 query plan with the child operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("ManualStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            NES_TRACE("ManualStrategy: return the updated query plan.");
            return candidateQueryPlan;
        }
        if (queryPlansWithChildren.size() == 1) {
            NES_TRACE("ManualStrategy: Found only 1 query plan with the child operator of the input logical operator. "
                      "Returning the query plan.");
            return queryPlansWithChildren[0];
        }
    }
    NES_TRACE("ManualStrategy: no query plan exists with the child operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    return candidateQueryPlan;
}

void ManualPlacementStrategy::placeOperator(QueryId queryId,
                                            const OperatorNodePtr& operatorNode,
                                            TopologyNodePtr candidateTopologyNode,
                                            const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) {

        NES_DEBUG("ManualStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("ManualStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("ManualStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
            if (childTopologyNodes.empty()) {
                NES_WARNING(
                    "ManualStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
                return;
            }

            NES_TRACE("ManualStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR(
                    "ManualStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
                    << operatorNode->getId());
                topology->print();
                throw log4cxx::helpers::Exception(
                    "ManualStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("ManualStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR("ManualStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                              "placed.");
                    throw log4cxx::helpers::Exception(

                        "ManualStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("ManualStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw log4cxx::helpers::Exception(
                        "ManualStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        NES_TRACE("ManualStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("ManualStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);
        operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
        operatorNode->addProperty(PLACED, true);
        auto operatorCopy = operatorNode->copy();
        if (candidateQueryPlan->getRootOperators().empty()) {
            candidateQueryPlan->appendOperatorAsNewRoot(operatorCopy);
        } else {
            auto children = operatorNode->getChildren();
            for (const auto& child : children) {
                auto rootOperators = candidateQueryPlan->getRootOperators();
                if (candidateQueryPlan->hasOperatorWithId(child->as<OperatorNode>()->getId())) {
                    candidateQueryPlan->getOperatorWithId(child->as<OperatorNode>()->getId())->addParent(operatorCopy);
                }
                auto found =
                    std::find_if(rootOperators.begin(), rootOperators.end(), [child](const OperatorNodePtr& rootOperator) {
                        return rootOperator->getId() == child->as<OperatorNode>()->getId();
                    });
                if (found != rootOperators.end()) {
                    candidateQueryPlan->removeAsRootOperator(*(found));
                    auto updatedRootOperators = candidateQueryPlan->getRootOperators();
                    auto operatorAlreadyExistsAsRoot =
                        std::find_if(updatedRootOperators.begin(),
                                     updatedRootOperators.end(),
                                     [operatorCopy](const OperatorNodePtr& rootOperator) {
                                         return rootOperator->getId() == operatorCopy->as<OperatorNode>()->getId();
                                     });
                    if (operatorAlreadyExistsAsRoot == updatedRootOperators.end()) {
                        candidateQueryPlan->addRootOperator(operatorCopy);
                    }
                }
            }
            if (!candidateQueryPlan->hasOperatorWithId(operatorCopy->getId())) {
                candidateQueryPlan->addRootOperator(operatorCopy);
            }
        }

        NES_TRACE("ManualStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("ManualStrategy: failed to create a new QuerySubPlan execution node for query.");
            throw log4cxx::helpers::Exception("ManualStrategy: failed to create a new QuerySubPlan execution node for query.");
        }
        NES_TRACE("ManualStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("ManualStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("ManualStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                            pinnedDownStreamOperators.end(),
                                                            [operatorNode](const OperatorNodePtr& pinnedDownStreamOperator) {
                                                                return pinnedDownStreamOperator->getId() == operatorNode->getId();
                                                            });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("ManualStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("ManualStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperator(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

bool ManualPlacementStrategy::updateGlobalExecutionPlan(
    QueryId queryId /*queryId*/,
    FaultToleranceType faultToleranceType /*faultToleranceType*/,
    LineageType lineageType /*lineageType*/,
    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::vector<OperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    try {
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 2. Place the operators
        for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
            auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
            TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);
            placeOperator(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (log4cxx::helpers::Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
};

}// namespace NES::Optimizer
