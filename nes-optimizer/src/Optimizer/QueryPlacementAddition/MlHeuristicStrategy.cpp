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

#include <API/Schema.hpp>
#include <Catalogs/Topology/PathFinder.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/MlHeuristicStrategy.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/CopiedPinnedOperators.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <z3++.h>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementAdditionStrategy> MlHeuristicStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                           const TopologyPtr& topology,
                                                                           const TypeInferencePhasePtr& typeInferencePhase,
                                                                           PlacementAmendmentMode placementAmendmentMode) {
    return std::make_unique<MlHeuristicStrategy>(
        MlHeuristicStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode));
}

MlHeuristicStrategy::MlHeuristicStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                         const TopologyPtr& topology,
                                         const TypeInferencePhasePtr& typeInferencePhase,
                                         PlacementAmendmentMode placementAmendmentMode)
    : BasePlacementAdditionStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode) {}

PlacementAdditionResult
MlHeuristicStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                               const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                               const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                               DecomposedQueryPlanVersion querySubPlanVersion,
                                               FaultToleranceType faultTolerance,
                                               CheckpointStorageType checkpointStorage) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Create copy of the query plan
        auto copy =
            CopiedPinnedOperators::create(pinnedUpStreamOperators, pinnedDownStreamOperators, operatorIdToOriginalOperatorMap);

        // 2. Find the path where operators need to be placed
        performPathSelection(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 3. Pin all unpinned operators
        performOperatorPlacement(sharedQueryId, copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 4. Compute query sub plans
        auto computedQuerySubPlans =
            computeDecomposedQueryPlans(sharedQueryId, copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 5. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 6. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans, querySubPlanVersion, faultTolerance, checkpointStorage);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementAdditionException(sharedQueryId, ex.what());
    }
}

void MlHeuristicStrategy::performOperatorPlacement(SharedQueryId sharedQueryId,
                                                   const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
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
                    identifyPinningLocation(sharedQueryId,
                                            downStreamNode->as<LogicalOperator>(),
                                            candidateTopologyNode,
                                            pinnedDownStreamOperators);
                }
            } else {// 2. If pinned operator is not placed then start by placing the operator
                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("Unable to find resources on the physical node for placement of source operator");
                    throw Exceptions::RuntimeException(
                        "Unable to find resources on the physical node for placement of source operator");
                }
                identifyPinningLocation(sharedQueryId,
                                        pinnedUpStreamOperator,
                                        candidateTopologyNode,
                                        pinnedDownStreamOperators);
            }
        }
    NES_DEBUG("Finished placing query operators into the global execution plan");
}

bool MlHeuristicStrategy::pushUpBasedOnFilterSelectivity(const LogicalOperatorPtr& operatorNode) {
    auto infModl = operatorNode->as<InferModel::LogicalInferModelOperator>();
    float f0 = infModl->getInputSchema()->getSize();

    auto ancestors = operatorNode->getAndFlattenAllAncestors();
    auto sink = ancestors.at(ancestors.size() - 1);
    float f_new = sink->as<UnaryOperator>()->getOutputSchema()->getSize();

    float s = 1.0;

    for (auto ancestor : ancestors) {
        if (ancestor->instanceOf<LogicalFilterOperator>()) {
            auto fltr = ancestor->as<LogicalFilterOperator>();
            s *= fltr->getSelectivity();
        }
    }
    float fields_measure = f_new / f0;
    float selectivity_measure = 1 / s;
    return fields_measure > selectivity_measure;
}

void MlHeuristicStrategy::identifyPinningLocation(SharedQueryId sharedQueryId,
                                                  const LogicalOperatorPtr& logicalOperator,
                                                  TopologyNodePtr candidateTopologyNode,
                                                  const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!logicalOperator->hasProperty(PINNED_WORKER_ID)) {

        NES_DEBUG("Place operatorNode with Id:{}.", logicalOperator->getId());
        if ((logicalOperator->hasMultipleChildrenOrParents() && !logicalOperator->instanceOf<SourceLogicalOperator>())
            || logicalOperator->instanceOf<SinkLogicalOperator>()) {
            NES_TRACE("Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(logicalOperator);
            if (childTopologyNodes.empty()) {
                NES_WARNING("No topology node isOperatorAPinnedDownStreamOperator where child operators are "
                            "placed.");
                return;
            }

            NES_TRACE("Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = pathFinder->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR("Unable to find a common ancestor topology node to place the binary operator, "
                          "operatorId: {}",
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

        bool shouldPushUp = false;
        bool canBePlacedHere = true;

        bool tfNotInstalled = logicalOperator->instanceOf<InferModel::LogicalInferModelOperator>()
            && (!candidateTopologyNode->hasNodeProperty("tf_installed")
                || !std::any_cast<bool>(candidateTopologyNode->getNodeProperty("tf_installed")));
        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0 || tfNotInstalled) {
            canBePlacedHere = false;
        }

        if (!canBePlacedHere) {
            shouldPushUp = true;
            if (candidateTopologyNode->getParents().empty()) {
                NES_ERROR("");
                return;
            }
        }

        if (logicalOperator->instanceOf<InferModel::LogicalInferModelOperator>()) {

            bool ENABLE_CPU_SAVER_MODE = DEFAULT_ENABLE_CPU_SAVER_MODE;
            int MIN_RESOURCE_LIMIT = DEFAULT_MIN_RESOURCE_LIMIT;
            bool LOW_THROUGHPUT_SOURCE = DEFAULT_LOW_THROUGHPUT_SOURCE;
            bool ML_HARDWARE = DEFAULT_ML_HARDWARE;

            if (candidateTopologyNode->hasNodeProperty("enable_cpu_saver_mode")) {
                ENABLE_CPU_SAVER_MODE = std::any_cast<bool>(candidateTopologyNode->getNodeProperty("enable_cpu_saver_mode"));
            }
            if (candidateTopologyNode->hasNodeProperty("min_resource_limit")) {
                MIN_RESOURCE_LIMIT = std::any_cast<int>(candidateTopologyNode->getNodeProperty("min_resource_limit"));
            }
            if (candidateTopologyNode->hasNodeProperty("low_throughput_source")) {
                LOW_THROUGHPUT_SOURCE = std::any_cast<bool>(candidateTopologyNode->getNodeProperty("low_throughput_source"));
            }
            if (candidateTopologyNode->hasNodeProperty("ml_hardware")) {
                ML_HARDWARE = std::any_cast<bool>(candidateTopologyNode->getNodeProperty("ml_hardware"));
            }

            if (candidateTopologyNode->getAvailableResources() < MIN_RESOURCE_LIMIT && ENABLE_CPU_SAVER_MODE) {
                shouldPushUp = true;
            }
            if (pushUpBasedOnFilterSelectivity(logicalOperator)) {
                shouldPushUp = true;
            }
            if (LOW_THROUGHPUT_SOURCE) {
                shouldPushUp = true;
            }
            if (ML_HARDWARE) {
                shouldPushUp = false;
            }
        }

        if (candidateTopologyNode->getParents().empty()) {
            shouldPushUp = false;
        }
        if (shouldPushUp) {
            identifyPinningLocation(sharedQueryId,
                                    logicalOperator,
                                    candidateTopologyNode->getParents()[0]->as<TopologyNode>(),
                                    pinnedDownStreamOperators);
        }
        if (!canBePlacedHere) {
            NES_ERROR("Operator can not be placed on {}", candidateTopologyNode->getId());
            return;
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

        NES_TRACE("Pinn operator to the candidate topology node.");
        logicalOperator->addProperty(PINNED_WORKER_ID, candidateTopologyNode->getId());
    } else {
        candidateTopologyNode = getTopologyNode(std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID)));
    }

    auto isOperatorAPinnedDownStreamOperator =
        std::find_if(pinnedDownStreamOperators.begin(),
                     pinnedDownStreamOperators.end(),
                     [logicalOperator](const OperatorPtr& pinnedDownStreamOperator) {
                         return pinnedDownStreamOperator->getId() == logicalOperator->getId();
                     });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("Place further upstream operators.");
    for (const auto& parent : logicalOperator->getParents()) {
        identifyPinningLocation(sharedQueryId,
                                parent->as<LogicalOperator>(),
                                candidateTopologyNode,
                                pinnedDownStreamOperators);
    }
}

}// namespace NES::Optimizer
