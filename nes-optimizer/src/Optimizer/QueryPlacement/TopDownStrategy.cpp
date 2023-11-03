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

BasePlacementStrategyPtr TopDownStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                 TopologyPtr topology,
                                                 TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<TopDownStrategy>(
        TopDownStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

TopDownStrategy::TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

bool TopDownStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                FaultToleranceType faultToleranceType,
                                                LineageType lineageType,
                                                const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {
    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        pinOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. Place pinned operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 5. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementException(queryId, ex.what());
    }
}

void TopDownStrategy::pinOperators(QueryId queryId,
                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_TRACE("TopDownStrategy: Place all sink operators.");
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        NES_TRACE("TopDownStrategy: Get the topology node for the sink operator.");
        auto nodeId = std::any_cast<uint64_t>(pinnedDownStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned down stream operator was already placed then place all its upstream operators
        if (pinnedDownStreamOperator->getOperatorState() == OperatorState::PLACED) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedDownStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& upStreamOperator : pinnedDownStreamOperator->getChildren()) {
                identifyPinningLocation(queryId,
                                        upStreamOperator->as<LogicalOperatorNode>(),
                                        candidateTopologyNode,
                                        pinnedUpStreamOperators);
            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedDownStreamOperator->getId())) {
                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
                throw Exceptions::RuntimeException(
                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            identifyPinningLocation(queryId, pinnedDownStreamOperator, candidateTopologyNode, pinnedUpStreamOperators);
        }
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}

void TopDownStrategy::identifyPinningLocation(QueryId queryId,
                                              const LogicalOperatorNodePtr& logicalOperator,
                                              TopologyNodePtr candidateTopologyNode,
                                              const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators) {

    if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        auto nodeId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_NODE_ID));
        operatorToExecutionNodeMap[logicalOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
        return;
    }

    if (!operatorToExecutionNodeMap.contains(logicalOperator->getId())) {
        NES_DEBUG("TopDownStrategy: Place {}", logicalOperator->toString());
        if ((logicalOperator->hasMultipleChildrenOrParents() || logicalOperator->instanceOf<SourceLogicalOperatorNode>())
            && !logicalOperator->instanceOf<SinkLogicalOperatorNode>()) {

            NES_TRACE("TopDownStrategy: Received an NAry operator for placement.");
            NES_TRACE("TopDownStrategy: Get the topology nodes where parent operators are placed.");
            std::vector<TopologyNodePtr> parentTopologyNodes = getTopologyNodesForDownStreamOperators(logicalOperator);
            if (parentTopologyNodes.empty()) {
                NES_WARNING("TopDownStrategy: No topology node found where parent operators are placed.");
                return;
            }

            NES_TRACE("TopDownStrategy: Get the topology nodes where children source operators are to be placed.");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForUpStreamOperators(logicalOperator);

            if (childNodes.empty()) {
                NES_WARNING("TopDownStrategy: No topology node found where child operators are placed.");
                return;
            }

            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            auto nodesBetween = topology->sort(topology->findCommonNodesBetween(childNodes, parentTopologyNodes), Upstream);

            if (nodesBetween.empty()) {
                NES_ERROR("TopDownStrategy: Unable to find the candidate topology node for placing Nary operator {}",
                          logicalOperator->toString());
                throw Exceptions::RuntimeException(
                    "TopDownStrategy: Unable to find the candidate topology node for placing Nary operator "
                    + logicalOperator->toString());
            } else if (nodesBetween.size() <= childNodes.size() + parentTopologyNodes.size()) {
                //Some childNodes == parentNodes --> Pick any of them
                candidateTopologyNode = nodesBetween[0];
            } else {
                // Nodes in between --> Pick the first non-parent node
                candidateTopologyNode = nodesBetween[parentTopologyNodes.size()];
            }

            if (logicalOperator->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("TopDownStrategy: Received Source operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(logicalOperator->getProperty(PINNED_NODE_ID));
                auto pinnedSourceOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSourceOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSourceOperatorLocation->containAsParent(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSourceOperatorLocation;
                } else {
                    NES_ERROR("TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is to be "
                              "placed.");
                    throw Exceptions::RuntimeException(
                        "TopDownStrategy: Unexpected behavior. Could not find Topology node where source operator is "
                        "to be placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                    throw Exceptions::RuntimeException(
                        "TopDownStrategy: Topology node where source operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_DEBUG("TopDownStrategy: Find the next Topology node where operator can be placed");
            std::vector<TopologyNodePtr> childNodes = getTopologyNodesForUpStreamOperators(logicalOperator);
            NES_TRACE("TopDownStrategy: Find a node reachable from all child and parent topology nodes.");
            //FIXME: we are considering only one root topology node currently
            auto candidateTopologyNodes = topology->findNodesBetween(childNodes, {candidateTopologyNode});
            for (const auto& topologyNodes : candidateTopologyNodes) {
                if (topologyNodes && topologyNodes->getAvailableResources() > 0) {
                    candidateTopologyNode = topologyNodes;
                    NES_DEBUG("TopDownStrategy: Found NES node for placing the operators with id : {}",
                              candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("TopDownStrategy: No node available for further placement of operators");
            throw Exceptions::RuntimeException("TopDownStrategy: No node available for further placement of operators");
        }

        //Pin the operator to the candidate node and mark as placed
        candidateTopologyNode->reduceResources(1);
        logicalOperator->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());

    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[logicalOperator->getId()]->getTopologyNode();
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

    NES_TRACE("TopDownStrategy: Place the children operators.");
    for (const auto& upstreamOperator : logicalOperator->getChildren()) {
        identifyPinningLocation(queryId,
                                upstreamOperator->as<LogicalOperatorNode>(),
                                candidateTopologyNode,
                                pinnedUpStreamOperators);
    }
}

std::vector<TopologyNodePtr>
TopDownStrategy::getTopologyNodesForDownStreamOperators(const LogicalOperatorNodePtr& candidateOperator) {

    std::vector<TopologyNodePtr> parentTopologyNodes;
    NES_DEBUG("TopDownStrategy: Get topology nodes with parent operators");
    std::vector<NodePtr> downstreamOperators = candidateOperator->getParents();
    //iterate over parent operators and get the physical location where operator is placed
    for (auto& downstreamOperator : downstreamOperators) {
        if (!downstreamOperator->as_if<OperatorNode>()->hasProperty(PINNED_NODE_ID)) {
            NES_WARNING("TopDownStrategy: unable to find topology for downstreamOperator operator.");
            return {};
        }

        TopologyNodePtr parentTopologyNode =
            topologyMap[std::any_cast<uint64_t>(downstreamOperator->as_if<OperatorNode>()->getProperty(PINNED_NODE_ID))];
        parentTopologyNodes.push_back(parentTopologyNode);
    }
    NES_DEBUG("TopDownStrategy: returning list of topology nodes where parent operators are placed");
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
        if (upStreamOperator->hasProperty(PINNED_NODE_ID)) {
            auto nodeId = std::any_cast<uint64_t>(upStreamOperator->getProperty(PINNED_NODE_ID));
            auto pinnedTopologyNode = getTopologyNode(nodeId);
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
