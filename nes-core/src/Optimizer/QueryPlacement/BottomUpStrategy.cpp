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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <log4cxx/helpers/exception.h>
#include <utility>
#include "Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp"
#include "Topology/TopologyNodeId.hpp"
#include "Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp"

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                TopologyPtr topology,
                                                                TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<BottomUpStrategy>(
        BottomUpStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                 FaultToleranceType faultToleranceType,
                                                 LineageType lineageType,
                                                 const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators, [[maybe_unused]] bool partialPlacement) {
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
    } catch (log4cxx::helpers::Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void BottomUpStrategy::performOperatorPlacement(QueryId queryId,
                                                const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << pinnedUpStreamOperator->toString()
                                                                                 << " placement.");

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedUpStreamOperator->getProperty(PLACED))) { //When would this be the case? BottomUp this would never happen?
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                if(pinnedUpStreamOperator->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->as<NetworkSinkDescriptor>()){
                    candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                }
                placeOperator(queryId, downStreamNode->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators); //take exe node of where the OP is placed and try to place downstrea OP on it
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) { //why is this second case neccesary
                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
                throw log4cxx::helpers::Exception(
                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            placeOperator(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::placeOperator(QueryId queryId,
                                     const OperatorNodePtr& operatorNode,
                                     TopologyNodePtr candidateTopologyNode,
                                     const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) { //how can you reach an operator that has already been placed?
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) { //how is this different from an OP already being placed? If its on an execution node, it is placed. Logically this doesnt make sense to me

        NES_DEBUG("BottomUpStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>()) //describe in english what this means and when it would happen
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("BottomUpStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("BottomUpStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode); //here we use the word placed to mean found on an execution node, which above we apparently dont?
            //if we have a sink, make sure all children are placed. If not, return
            if (childTopologyNodes.empty()) {
                NES_WARNING(
                    "BottomUpStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
                return;
            }

            NES_TRACE("BottomUpStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR(
                    "BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
                    << operatorNode->getId());
                topology->print();
                throw log4cxx::helpers::Exception(
                    "BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("BottomUpStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR("BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                              "placed.");
                    throw log4cxx::helpers::Exception(

                        "BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw log4cxx::helpers::Exception(
                        "BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {

            NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : "
                              << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
            throw log4cxx::helpers::Exception("BottomUpStrategy: No node available for further placement of operators");
        }

        NES_TRACE("BottomUpStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("BottomUpStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlanForOperator(queryId, operatorNode, candidateExecutionNode);
        operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
        operatorNode->addProperty(PLACED, true);
        auto operatorCopy = operatorNode->copy();
        if (candidateQueryPlan->getRootOperators().empty()) {
            candidateQueryPlan->appendOperatorAsNewRoot(operatorCopy);
        } else { // explain this loop, what is the purpose of this?
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

        NES_TRACE("BottomUpStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query.");
            throw log4cxx::helpers::Exception("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query.");
        }
        NES_TRACE("BottomUpStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("BottomUpStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1); //doesnt this reduce capacity by two effectively?
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                            pinnedDownStreamOperators.end(),
                                                            [operatorNode](const OperatorNodePtr& pinnedDownStreamOperator) {
                                                                return pinnedDownStreamOperator->getId() == operatorNode->getId();
                                                            });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) { //all this really means, is there are no more parent OP to place??
        NES_DEBUG("BottomUpStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("BottomUpStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperator(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

}// namespace NES::Optimizer
