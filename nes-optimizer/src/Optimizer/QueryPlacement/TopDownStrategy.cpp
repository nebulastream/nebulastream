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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

#include <utility>

namespace NES::Optimizer {

BasePlacementStrategyPtr TopDownStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const TypeInferencePhasePtr& typeInferencePhase,
                                                 PlacementAmenderMode placementAmenderMode) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmenderMode));
}

TopDownStrategy::TopDownStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                 const TopologyPtr& topology,
                                 const TypeInferencePhasePtr& typeInferencePhase,
                                 PlacementAmenderMode placementAmenderMode)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmenderMode) {}

bool TopDownStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        pinOperators(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. Compute query sub plans
        auto computedQuerySubPlans = computeQuerySubPlans(sharedQueryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 5. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans);
    } catch (std::exception& ex) {
        //Release all locked topology nodes in case of pessimistic approach
        if (placementAmenderMode == PlacementAmenderMode::PESSIMISTIC) {
            unlockTopologyNodes();
        }
        throw Exceptions::QueryPlacementException(sharedQueryId, ex.what());
    }
}

void TopDownStrategy::pinOperators(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_TRACE("Place all sink operators.");
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        NES_TRACE("Get the topology node for the sink operator.");
        auto workerId = std::any_cast<uint64_t>(pinnedDownStreamOperator->getProperty(PINNED_WORKER_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(workerId);

        // 1. If pinned down stream operator was already placed then place all its upstream operators
        if (pinnedDownStreamOperator->getOperatorState() == OperatorState::PLACED) {
            //Place all downstream nodes
            for (auto& upStreamOperator : pinnedDownStreamOperator->getChildren()) {
                identifyPinningLocation(upStreamOperator->as<LogicalOperatorNode>(),
                                        candidateTopologyNode,
                                        pinnedUpStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0) {
                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
                throw Exceptions::RuntimeException(
                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            identifyPinningLocation(pinnedDownStreamOperator, candidateTopologyNode, pinnedUpStreamOperators);
        }
    }
    NES_DEBUG("Finished pinning all operators.");
}

void TopDownStrategy::identifyPinningLocation(const LogicalOperatorNodePtr& logicalOperator,
                                              TopologyNodePtr candidateTopologyNode,
                                              const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!logicalOperator->hasProperty(PINNED_WORKER_ID)) {// If operator is not pinned yet
        NES_DEBUG("Place {}", logicalOperator->toString());
        if ((logicalOperator->hasMultipleChildrenOrParents() || logicalOperator->instanceOf<SourceLogicalOperatorNode>())
            && !logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {

            NES_TRACE("Received an NAry operator for placement.");
            NES_TRACE("Get the topology nodes where parent operators are placed.");
            std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForDownStreamOperators(logicalOperator);
            if (parentTopologyNodes.empty()) {
                NES_WARNING("No topology node found where parent operators are placed.");
                return;
            }

            NES_TRACE("Get the topology nodes where children source operators are to be placed.");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForUpStreamOperators(logicalOperator);

            NES_TRACE("Find a node reachable from all child and parent topology nodes.");
            candidateTopologyNode = topology->findCommonNodeBetween(childNodes, parentTopologyNodes);

            if (!candidateTopologyNode) {
                NES_ERROR("Unable to find the candidate topology node for placing Nary operator {}", logicalOperator->toString());
                throw Exceptions::RuntimeException("Unable to find the candidate topology node for placing Nary operator "
                                                   + logicalOperator->toString());
            }

            if (logicalOperator->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("Received Source operator for placement.");
                auto workerId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_WORKER_ID));
                auto pinnedSourceOperatorLocation = getTopologyNode(workerId);
                if (pinnedSourceOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSourceOperatorLocation->containAsParent(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSourceOperatorLocation;
                } else {
                    NES_ERROR("Unexpected behavior. Could not find Topology node where source operator is to be "
                              "placed.");
                    throw Exceptions::RuntimeException(
                        "Unexpected behavior. Could not find Topology node where source operator is "
                        "to be placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("Topology node where source operator is to be placed has no capacity.");
                    throw Exceptions::RuntimeException("Topology node where source operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_DEBUG("Find the next Topology node where operator can be placed");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForUpStreamOperators(logicalOperator);
            NES_TRACE("Find a node reachable from all child and parent topology nodes.");
            //FIXME: we are considering only one root topology node currently
            auto candidateTopologyNodes = topology->findNodesBetween(childNodes, {candidateTopologyNode});
            for (const auto& topologyNodes : candidateTopologyNodes) {
                if (topologyNodes && topologyNodes->getAvailableResources() > 0) {
                    candidateTopologyNode = topologyNodes;
                    NES_DEBUG("Found NES node for placing the operators with id : {}", candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("No node available for further placement of operators");
            throw Exceptions::RuntimeException("No node available for further placement of operators");
        }

        //Pin the operator to the candidate node
        candidateTopologyNode->occupyResources(1);
        logicalOperator->addProperty(PINNED_WORKER_ID, candidateTopologyNode->getId());
        logicalOperator->addProperty(PROCESSED, true);
    } else {
        candidateTopologyNode = getTopologyNode(std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID)));
        //Pin the operator to the candidate node
        candidateTopologyNode->occupyResources(1);
        logicalOperator->addProperty(PROCESSED, true);
    }

    auto isOperatorAPinnedUpStreamOperator = std::find_if(pinnedUpStreamOperators.begin(),
                                                          pinnedUpStreamOperators.end(),
                                                          [logicalOperator](const OperatorNodePtr& pinnedUpStreamOperator) {
                                                              return pinnedUpStreamOperator->getId() == logicalOperator->getId();
                                                          });

    if (isOperatorAPinnedUpStreamOperator != pinnedUpStreamOperators.end()) {
        NES_DEBUG("BottomUpStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("Place the children operators.");
    for (const auto& upstreamOperator : logicalOperator->getChildren()) {
        identifyPinningLocation(upstreamOperator->as<LogicalOperatorNode>(), candidateTopologyNode, pinnedUpStreamOperators);
    }
}

std::vector<TopologyNodePtr>
TopDownStrategy::getTopologyNodesForDownStreamOperators(const LogicalOperatorNodePtr& candidateOperator) {

    std::vector<TopologyNodePtr> parentTopologyNodes;
    NES_DEBUG("Get topology nodes with parent operators");
    std::vector<NodePtr> downstreamOperators = candidateOperator->getParents();
    //iterate over parent operators and get the physical location where operator was processed to be placed
    for (auto& downstreamOperator : downstreamOperators) {
        if (!downstreamOperator->as<OperatorNode>()->hasProperty(PROCESSED)) {
            NES_WARNING("Not all downstream operators are processed for placement yet. Placement of this "
                        "operator will be re-attempted.");
            return {};
        }

        TopologyNodePtr parentTopologyNode = workerIdToTopologyNodeMap[std::any_cast<uint64_t>(
            downstreamOperator->as_if<OperatorNode>()->getProperty(PINNED_WORKER_ID))];
        parentTopologyNodes.push_back(parentTopologyNode);
    }
    NES_DEBUG("returning list of topology nodes where parent operators are placed");
    return parentTopologyNodes;
}

std::vector<TopologyNodePtr>
TopDownStrategy::getTopologyNodesForUpStreamOperators(const LogicalOperatorNodePtr& candidateOperator) {
    std::vector<TopologyNodePtr> upStreamTopologyNodes;

    NES_TRACE(
        "TopDownStrategy::getTopologyNodesForUpStreamOperators: Get the pinned or closest placed upStreamOperators nodes for "
        "the the input operator.");
    std::vector<NodePtr> upStreamOperators = {candidateOperator};

    while (!upStreamOperators.empty()) {
        auto upStreamOperator = upStreamOperators.back()->as<OperatorNode>();
        upStreamOperators.pop_back();
        if (upStreamOperator->hasProperty(PINNED_WORKER_ID)) {
            auto workerId = std::any_cast<uint64_t>(upStreamOperator->getProperty(PINNED_WORKER_ID));
            auto pinnedTopologyNode = getTopologyNode(workerId);
            upStreamTopologyNodes.emplace_back(pinnedTopologyNode);
            continue;
        }
        upStreamOperators.insert(upStreamOperators.end(),
                                 upStreamOperator->getChildren().begin(),
                                 upStreamOperator->getChildren().end());
    }

    if (upStreamTopologyNodes.empty()) {
        NES_ERROR("TopDownStrategy::getTopologyNodesForUpStreamOperators: Unable to find the upStreamOperators operators to the "
                  "candidate operator");
        throw Exceptions::RuntimeException(
            "TopDownStrategy::getTopologyNodesForUpStreamOperators: Unable to find the upStreamOperators operators to the "
            "candidate operator");
    }
    return upStreamTopologyNodes;
}
}// namespace NES::Optimizer
