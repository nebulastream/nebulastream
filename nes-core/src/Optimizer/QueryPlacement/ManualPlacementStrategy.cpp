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
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
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

bool ManualPlacementStrategy::updateGlobalExecutionPlan(
    QueryId queryId /*queryId*/,
    FaultToleranceType faultToleranceType /*faultToleranceType*/,
    LineageType lineageType /*lineageType*/,
    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::vector<OperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    try {
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place the operators
        performOperatorPlacement(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (log4cxx::helpers::Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
};

void ManualPlacementStrategy::pinOperators(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                           const std::vector<OperatorNodePtr>& pinnedDownStreamOperators,
                                           TopologyPtr topology,
                                           NES::Optimizer::PlacementMatrix& matrix) {
    matrix.size();
    std::vector<TopologyNodePtr> topologyNodes;
    auto topologyIterator = NES::BreadthFirstNodeIterator(topology->getRoot());
    for (auto itr = topologyIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
        topologyNodes.emplace_back((*itr)->as<TopologyNode>());
    }

    auto operators = QueryPlanIterator(std::move(queryPlan)).snapshot();

    for (uint64_t i = 0; i < topologyNodes.size(); i++) {
        // Set the Pinned operator property
        auto currentRow = matrix[i];
        for (uint64_t j = 0; j < operators.size(); j++) {
            if (currentRow[j]) {
                // if the the value of the matrix at (i,j) is 1, then add a PINNED_NODE_ID of the topologyNodes[i] to operators[j]
                operators[j]->as<OperatorNode>()->addProperty("PINNED_NODE_ID", topologyNodes[i]->getId());
            }
        }
    }
}

void ManualPlacementStrategy::performOperatorPlacement(QueryId queryId,
                                                       const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                       const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("ManualPlacementStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("ManualPlacementStrategy: Get the topology node for source operator " << pinnedUpStreamOperator->toString()
                                                                                        << " placement.");

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedUpStreamOperator->getProperty(PLACED))) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                placeOperator(queryId, downStreamNode->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            placeOperator(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("ManualPlacementStrategy: Finished placing query operators into the global execution plan");
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

        NES_DEBUG("ManualPlacementStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("ManualPlacementStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("ManualPlacementStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
            if (childTopologyNodes.empty()) {
                NES_WARNING("ManualPlacementStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators "
                            "are placed.");
                return;
            }

            NES_TRACE("ManualPlacementStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR("ManualPlacementStrategy: Unable to find a common ancestor topology node to place the binary operator, "
                          "operatorId: "
                          << operatorNode->getId());
                topology->print();
                throw log4cxx::helpers::Exception(
                    "ManualPlacementStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("ManualPlacementStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR(
                        "ManualPlacementStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                    throw log4cxx::helpers::Exception(

                        "ManualPlacementStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }
            }
        }

        NES_TRACE("ManualPlacementStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("ManualPlacementStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlanForOperator(queryId, operatorNode, candidateExecutionNode);
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

        NES_TRACE("ManualPlacementStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("ManualPlacementStrategy: failed to create a new QuerySubPlan execution node for query.");
            throw log4cxx::helpers::Exception(
                "ManualPlacementStrategy: failed to create a new QuerySubPlan execution node for query.");
        }
        NES_TRACE("ManualPlacementStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE(
            "ManualPlacementStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("ManualPlacementStrategy: Reducing the node remaining CPU capacity by 1");
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
        NES_DEBUG("ManualPlacementStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("ManualPlacementStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperator(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}
}// namespace NES::Optimizer
