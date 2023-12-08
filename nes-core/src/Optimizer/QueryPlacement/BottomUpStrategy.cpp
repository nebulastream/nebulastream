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
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>

#include <utility>

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
                                                 FaultToleranceType::Value faultToleranceType,
                                                 LineageType::Value lineageType,
                                                 FaultTolerancePlacement::Value ftPlacement,
                                                 const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");

        if (ftPlacement == FaultTolerancePlacement::NONE) {
            faultToleranceType = FaultToleranceType::NONE;
        }
        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        auto epoch = now_ms.time_since_epoch();
        auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators, faultToleranceType, ftPlacement);

        now = std::chrono::system_clock::now();
        now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        epoch = now_ms.time_since_epoch();
        auto valueAfter = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);

        std::cout << "The Decision time was: " << valueAfter.count() - value.count();

        // 2. Pin all unpinned operators
        pinOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place all pinned operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);

    } catch (std::exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void BottomUpStrategy::pinOperators(QueryId queryId,
                                    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                    const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << pinnedUpStreamOperator->toString()
                                                                                 << " placement.");

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);
        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedUpStreamOperator->getProperty(PLACED))) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                identifyPinningLocation(queryId,
                                        downStreamNode->as<OperatorNode>(),
                                        candidateTopologyNode,
                                        pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
//            if (candidateTopologyNode->getAvailableResources() == 0
//                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) {
//                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
//                throw Exceptions::RuntimeException(
//                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
//            }
            identifyPinningLocation(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::identifyPinningLocation(QueryId queryId,
                                               const OperatorNodePtr& operatorNode,
                                               TopologyNodePtr candidateTopologyNode,
                                               const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    uint64_t availableSlot = candidateTopologyNode->getAvailableResources();

    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) {
//        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

//    NES_DEBUG("BottomUpStrategy: Place " << operatorNode);
    if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
        || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("BottomUpStrategy: Received an NAry operator for placement.");
        //Check if all children operators already placed
        NES_TRACE("BottomUpStrategy: Get the topology nodes where child operators are placed.");
        std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
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
            throw Exceptions::RuntimeException(
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
                throw Exceptions::RuntimeException(
                    "BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                    "placed.");
            }

            //            if (availableSlot == 0) {
            //                NES_ERROR("BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
            //                throw Exceptions::RuntimeException(
            //                    "BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
            //            }
        }
    }

    if (!operatorNode->instanceOf<SinkLogicalOperatorNode>() && !operatorNode->instanceOf<SourceLogicalOperatorNode>()
        && !operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        if (availableSlot == 0) {

//            NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                uint64_t availableSlot = candidateTopologyNode->getAvailableResources();
                if (availableSlot > 0) {
                    if (!operatorNode->instanceOf<SinkLogicalOperatorNode>()
                        && !operatorNode->instanceOf<SourceLogicalOperatorNode>()
                        && !operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
                        topology->reduceResources(candidateTopologyNode->getId(), 1);
//                        std::cout << "Reduce resources to:" << candidateTopologyNode->getAvailableResources();
                    }
//                    NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : "
//                              << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || availableSlot <= 0) {
            NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
            throw Exceptions::RuntimeException("BottomUpStrategy: No node available for further placement of operators");
        }

        topology->reduceResources(candidateTopologyNode->getId(), 1);
//        std::cout << "Reduce resources to:" << candidateTopologyNode->getAvailableResources();
    }

    operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());

    auto isOperatorAPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                            pinnedDownStreamOperators.end(),
                                                            [operatorNode](const OperatorNodePtr& pinnedDownStreamOperator) {
                                                                return pinnedDownStreamOperator->getId() == operatorNode->getId();
                                                            });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
//        NES_DEBUG("BottomUpStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("BottomUpStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        identifyPinningLocation(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

}// namespace NES::Optimizer
