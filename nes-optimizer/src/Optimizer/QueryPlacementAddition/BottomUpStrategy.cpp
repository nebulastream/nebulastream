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
#include <Catalogs/Topology/PathFinder.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/CopiedPinnedOperators.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementAdditionStrategy> BottomUpStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                        const TopologyPtr& topology,
                                                                        const TypeInferencePhasePtr& typeInferencePhase,
                                                                        PlacementAmendmentMode placementAmendmentMode,
                                                                        FaultToleranceType faultTolerance) {
    return std::make_unique<BottomUpStrategy>(globalExecutionPlan,
                                              topology,
                                              typeInferencePhase,
                                              placementAmendmentMode,
                                              faultTolerance);
}

BottomUpStrategy::BottomUpStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                   const TopologyPtr& topology,
                                   const TypeInferencePhasePtr& typeInferencePhase,
                                   PlacementAmendmentMode placementAmendmentMode,
                                   FaultToleranceType faultTolerance)
    : BasePlacementAdditionStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode, faultTolerance) {}

PlacementAdditionResult BottomUpStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                                    const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                                    const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                                    DecomposedQueryPlanVersion querySubPlanVersion) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");

        // 1. Create copy of the query plan
        auto copy =
            CopiedPinnedOperators::create(pinnedUpStreamOperators, pinnedDownStreamOperators, operatorIdToOriginalOperatorMap);

        // 2. Find the path where operators need to be placed
        performPathSelection(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 3. Pin all unpinned operators
        pinOperators(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 4. Compute query sub plans
        auto computedQuerySubPlans =
            computeDecomposedQueryPlans(sharedQueryId, copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);
        NES_DEBUG("GlobalExecutionPlan:{}", globalExecutionPlan->getAsString());
        // 5. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 6. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans, querySubPlanVersion);
    } catch (std::exception& ex) {
        NES_ERROR("Exception occurred during bottom up placement: {}", ex.what());
        throw Exceptions::QueryPlacementAdditionException(sharedQueryId, ex.what());
    }
}

void BottomUpStrategy::pinOperators(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                    const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {

    NES_DEBUG("Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("Get the topology node for source operator {} placement.", pinnedUpStreamOperator->toString());

        auto workerId = std::any_cast<WorkerId>(pinnedUpStreamOperator->getProperty(PINNED_WORKER_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(workerId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->getOperatorState() == OperatorState::PLACED) {
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                identifyPinningLocation(downStreamNode->as<LogicalOperator>(), candidateTopologyNode, pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0) {
                NES_ERROR("Unable to find resources on the physical node for placement of source operator");
                throw Exceptions::RuntimeException(
                    "Unable to find resources on the physical node for placement of source operator");
            }
            identifyPinningLocation(pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("Finished pinning all operators.");
}

void BottomUpStrategy::identifyPinningLocation(const LogicalOperatorPtr& logicalOperator,
                                               TopologyNodePtr candidateTopologyNode,
                                               const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED || logicalOperator->hasProperty("placed")) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    NES_DEBUG("Place {}", logicalOperator->toString());

    if (!logicalOperator->instanceOf<SourceLogicalOperator>() && !logicalOperator->instanceOf<SinkLogicalOperator>()
        && logicalOperator->getOriginalId() == logicalOperator->getId()) {
        candidateTopologyNode = findCandidateTopologyNode(candidateTopologyNode);
    }

    if ((logicalOperator->hasMultipleChildrenOrParents() && !logicalOperator->instanceOf<SourceLogicalOperator>())
        || logicalOperator->instanceOf<SinkLogicalOperator>()) {
        NES_TRACE("Received an NAry operator for placement.");
        //Check if all children operators already placed
        NES_TRACE("Get the topology nodes where child operators are placed.");
        std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(logicalOperator);
        if (childTopologyNodes.empty()) {
            NES_WARNING("No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
            return;
        }

        NES_TRACE("Find a node reachable from all topology nodes where child operators are placed.");
        if (childTopologyNodes.size() == 1) {
            candidateTopologyNode = childTopologyNodes[0];
        } else {
            candidateTopologyNode = pathFinder->findCommonAncestor(childTopologyNodes);
        }

        if (!candidateTopologyNode) {
            NES_ERROR("Unable to find a common ancestor topology node to place the binary operator, operatorId: {}",
                      logicalOperator->getId());
            topology->print();
            throw Exceptions::RuntimeException("Unable to find a common ancestor topology node to place the binary operator");
        }

        if (logicalOperator->instanceOf<SinkLogicalOperator>()) {
            NES_TRACE("Received Sink operator for placement.");
            auto workerId = std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID));
            auto pinnedSinkOperatorLocation = getTopologyNode(workerId);
            if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                candidateTopologyNode = pinnedSinkOperatorLocation;
            } else {
                NES_ERROR("Unexpected behavior. Could not find Topology node where sink operator is to be "
                          "placed.");
                throw Exceptions::RuntimeException(
                    "Unexpected behavior. Could not find Topology node where sink operator is to be "
                    "placed.");
            }

            if (candidateTopologyNode->getAvailableResources() == 0) {
                NES_ERROR("Topology node where sink operator is to be placed has no capacity.");
                throw Exceptions::RuntimeException("Topology node where sink operator is to be placed has no capacity.");
            }
        }
    }

    if (candidateTopologyNode->getAvailableResources() == 0) {

        NES_DEBUG("Find the next NES node in the path where operator can be placed");
        while (!candidateTopologyNode->getParents().empty()) {
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
            if (candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("Found NES node for placing the operators with id : {}", candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("No node available for further placement of operators");
        throw Exceptions::RuntimeException("No node available for further placement of operators");
    }

    candidateTopologyNode->occupySlots(1);
    logicalOperator->addProperty(PINNED_WORKER_ID, candidateTopologyNode->getId());

    if (faultTolerance == FaultToleranceType::AS && !logicalOperator->instanceOf<SourceLogicalOperator>()
        && !logicalOperator->instanceOf<SinkLogicalOperator>() && logicalOperator->getOriginalId() == logicalOperator->getId()) {
        // Get alternative nodes for the candidate topology node

        auto alternativeNodeId = candidateTopologyNode->getAlternativeNodeCandidateIds()[0];
        TopologyNodePtr alternativeNode = lockedTopologyNodeMap.at(alternativeNodeId)->operator*();

        LogicalOperatorPtr operatorCopy = logicalOperator->copy()->as<LogicalOperator>();
        operatorCopy->setId(getNextOperatorId());
        operatorCopy->setOriginalId(logicalOperator->getId());

        for (const auto& parent : logicalOperator->getParents()) {
            LogicalOperatorPtr parentOperator = parent->as<LogicalOperator>();

            parentOperator->addChild(operatorCopy);
        }

        for (const auto& child : logicalOperator->getChildren()) {
            LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
            childOperator->addParent(operatorCopy);
        }
        operatorIdToOriginalOperatorMap[operatorCopy->getId()] = operatorCopy;
        identifyPinningLocation(operatorCopy, alternativeNode, pinnedDownStreamOperators);
    }
    auto isOperatorAPinnedDownStreamOperator =
        std::find_if(pinnedDownStreamOperators.begin(),
                     pinnedDownStreamOperators.end(),
                     [logicalOperator](const OperatorPtr& pinnedDownStreamOperator) {
                         return pinnedDownStreamOperator->getId() == logicalOperator->getId();
                     });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        logicalOperator->addProperty("placed", 1);
        NES_DEBUG("Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("Place further upstream operators.");
    for (const auto& parent : logicalOperator->getParents()) {
        identifyPinningLocation(parent->as<LogicalOperator>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

TopologyNodePtr BottomUpStrategy::findCandidateTopologyNode(TopologyNodePtr startNode) {
    TopologyNodePtr candidateNode = startNode;
    while ((candidateNode->getAvailableResources() == 0 || candidateNode->getAlternativeNodeCandidateIds().empty())
           && !candidateNode->getParents().empty()) {
        candidateNode = candidateNode->getParents()[0]->as<TopologyNode>();
    }
    if (candidateNode->getAvailableResources() == 0 || candidateNode->getAlternativeNodeCandidateIds().empty()) {
        NES_ERROR("No node available for placement with available resources and alternative nodes.");
        throw Exceptions::RuntimeException("No suitable node found for operator placement.");
    }
    return candidateNode;
}

}// namespace NES::Optimizer
