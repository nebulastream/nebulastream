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

#ifdef TFDEF

#include <API/Schema.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/MlHeuristicStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>

#include <utility>
#include <z3++.h>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> MlHeuristicStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                   TopologyPtr topology,
                                                                   TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<MlHeuristicStrategy>(
        MlHeuristicStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

MlHeuristicStrategy::MlHeuristicStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

bool MlHeuristicStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                    FaultToleranceType faultToleranceType,
                                                    LineageType lineageType,
                                                    const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                    const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        performOperatorPlacement(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. Place pinned operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        if (DEFAULT_ENABLE_OPERATOR_REDUNDANCY_ELIMINATION) {
            performOperatorRedundancyElimination(queryId, faultToleranceType, lineageType);
        }

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementException(queryId, ex.what());
    }
}

void MlHeuristicStrategy::performOperatorRedundancyElimination(QueryId queryId,
                                                               FaultToleranceType faultToleranceType,
                                                               LineageType lineageType) {
    auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    auto context = std::make_shared<z3::context>();
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context, QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);

    for (auto executionNode : executionNodes) {
        auto querysubplans = executionNode->getQuerySubPlans(queryId);

        SignatureEqualityUtilPtr signatureEqualityUtil = SignatureEqualityUtil::create(context);

        if (querysubplans.size() >= 2) {
            runTypeInferencePhase(queryId, faultToleranceType, lineageType);
            std::vector<QuerySignaturePtr> signatures;
            std::vector<int> querysubplansToRemove;

            for (auto qsp : querysubplans) {
                signatureInferencePhase->execute(qsp);
                auto sinkOperator = qsp->getSinkOperators().at(0);
                signatures.push_back(sinkOperator->as<LogicalUnaryOperatorNode>()->getZ3Signature());
            }
            for (int i = 0; i < (int) querysubplans.size() - 1; ++i) {
                for (int j = i + 1; j < (int) querysubplans.size(); ++j) {
                    if (!std::count(querysubplansToRemove.begin(), querysubplansToRemove.end(), j)
                        && signatureEqualityUtil->checkEquality(signatures[i], signatures[j])) {
                        auto targetRootOperator = querysubplans[i]->getSourceOperators().at(0)->getParents().at(0);
                        auto hostRootOperator = querysubplans[j]->getSourceOperators().at(0)->getParents().at(0);
                        auto hostRootChildren = hostRootOperator->getChildren();

                        for (auto& hostChild : hostRootChildren) {
                            bool addedNewParent = hostChild->addParent(targetRootOperator);
                            if (!addedNewParent) {
                                NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                            }
                        }
                        querysubplansToRemove.push_back(j);
                    }
                }
            }
            for (int i = (int) querysubplansToRemove.size() - 1; i >= 0; i--) {
                querysubplans.erase(querysubplans.begin() + querysubplansToRemove[i]);
            }
        }
        executionNode->updateQuerySubPlans(queryId, querysubplans);
    }
    NES_DEBUG("MlHeuristicStrategy: Updated Global Execution Plan:\n{}", globalExecutionPlan->getAsString());
}

void MlHeuristicStrategy::performOperatorPlacement(QueryId queryId,
                                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("MlHeuristicStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("MlHeuristicStrategy: Get the topology node for source operator {} placement.",
                  pinnedUpStreamOperator->toString());

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->getOperatorState() == OperatorState::PLACED) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                identifyPinningLocation(queryId,
                                        downStreamNode->as<LogicalOperatorNode>(),
                                        candidateTopologyNode,
                                        pinnedDownStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) {
                NES_ERROR("MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator");
                throw Exceptions::RuntimeException(
                    "MlHeuristicStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            identifyPinningLocation(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("MlHeuristicStrategy: Finished placing query operators into the global execution plan");
}

bool MlHeuristicStrategy::pushUpBasedOnFilterSelectivity(const LogicalOperatorNodePtr& operatorNode) {
    auto infModl = operatorNode->as<InferModel::InferModelLogicalOperatorNode>();
    float f0 = infModl->getInputSchema()->getSize();

    auto ancestors = operatorNode->getAndFlattenAllAncestors();
    auto sink = ancestors.at(ancestors.size() - 1);
    float f_new = sink->as<UnaryOperatorNode>()->getOutputSchema()->getSize();

    float s = 1.0;

    for (auto ancestor : ancestors) {
        if (ancestor->instanceOf<FilterLogicalOperatorNode>()) {
            auto fltr = ancestor->as<FilterLogicalOperatorNode>();
            s *= fltr->getSelectivity();
        }
    }
    float fields_measure = f_new / f0;
    float selectivity_measure = 1 / s;
    return fields_measure > selectivity_measure;
}

void MlHeuristicStrategy::identifyPinningLocation(QueryId queryId,
                                                  const LogicalOperatorNodePtr& logicalOperator,
                                                  TopologyNodePtr candidateTopologyNode,
                                                  const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        auto nodeId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_NODE_ID));
        operatorToExecutionNodeMap[logicalOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
        return;
    }

    if (!operatorToExecutionNodeMap.contains(logicalOperator->getId())) {

        NES_DEBUG("MlHeuristicStrategy: Place operatorNode with Id:{}.", logicalOperator->getId());
        if ((logicalOperator->hasMultipleChildrenOrParents() && !logicalOperator->instanceOf<SourceLogicalOperatorNode>())
            || logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("MlHeuristicStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("MlHeuristicStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(logicalOperator);
            if (childTopologyNodes.empty()) {
                NES_WARNING("MlHeuristicStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators are "
                            "placed.");
                return;
            }

            NES_TRACE("MlHeuristicStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR("MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator, "
                          "operatorId: {}",
                          logicalOperator->getId());
                topology->print();
                throw Exceptions::RuntimeException(
                    "MlHeuristicStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("MlHeuristicStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR(
                        "MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                    throw Exceptions::RuntimeException(

                        "MlHeuristicStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw Exceptions::RuntimeException(
                        "MlHeuristicStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        bool shouldPushUp = false;
        bool canBePlacedHere = true;

        bool tfNotInstalled = logicalOperator->instanceOf<InferModel::InferModelLogicalOperatorNode>()
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

        if (logicalOperator->instanceOf<InferModel::InferModelLogicalOperatorNode>()) {

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
            identifyPinningLocation(queryId,
                                    logicalOperator,
                                    candidateTopologyNode->getParents()[0]->as<TopologyNode>(),
                                    pinnedDownStreamOperators);
        }
        if (!canBePlacedHere) {
            NES_ERROR("");
            return;
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {

            NES_DEBUG("MlHeuristicStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    NES_DEBUG("MlHeuristicStrategy: Found NES node for placing the operators with id : {}",
                              candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("MlHeuristicStrategy: No node available for further placement of operators");
            throw Exceptions::RuntimeException("MlHeuristicStrategy: No node available for further placement of operators");
        }

        NES_TRACE("MlHeuristicStrategy: Pinn operator to the candidate topology node.");
        logicalOperator->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[logicalOperator->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedDownStreamOperator =
        std::find_if(pinnedDownStreamOperators.begin(),
                     pinnedDownStreamOperators.end(),
                     [logicalOperator](const OperatorNodePtr& pinnedDownStreamOperator) {
                         return pinnedDownStreamOperator->getId() == logicalOperator->getId();
                     });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("MlHeuristicStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("MlHeuristicStrategy: Place further upstream operators.");
    for (const auto& parent : logicalOperator->getParents()) {
        identifyPinningLocation(queryId, parent->as<LogicalOperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}

}// namespace NES::Optimizer

#endif//TFDEF