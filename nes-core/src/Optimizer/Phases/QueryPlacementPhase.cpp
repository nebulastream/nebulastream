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

#include <Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <utility>

namespace NES::Optimizer {

QueryPlacementPhase::QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase,
                                         bool queryReconfiguration)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      typeInferencePhase(std::move(typeInferencePhase)), queryReconfiguration(queryReconfiguration) {
    NES_DEBUG("QueryPlacementPhase()");
}

QueryPlacementPhasePtr QueryPlacementPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                   TopologyPtr topology,
                                                   TypeInferencePhasePtr typeInferencePhase,
                                                   bool queryReconfiguration) {
    return std::make_shared<QueryPlacementPhase>(QueryPlacementPhase(std::move(globalExecutionPlan),
                                                                     std::move(topology),
                                                                     std::move(typeInferencePhase),
                                                                     queryReconfiguration));
}

bool QueryPlacementPhase::execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("QueryPlacementPhase: Perform query placement phase for shared query plan "
             + std::to_string(sharedQueryPlan->getSharedQueryId()));
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    auto placementStrategyPtr =
        PlacementStrategyFactory::getStrategy(placementStrategy, globalExecutionPlan, topology, typeInferencePhase);

    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    auto faultToleranceType = queryPlan->getFaultToleranceType();
    auto lineageType = queryPlan->getLineageType();
    auto ftPlacement = queryPlan->getFaultTolerancePlacement();
    NES_DEBUG("QueryPlacementPhase: Perform query placement for query plan \n " + queryPlan->toString());

    //1. Fetch all upstream pinned operators
    auto upStreamPinnedOperators = getUpStreamPinnedOperators(sharedQueryPlan);

    //2. Fetch all downstream pinned operators
    auto downStreamPinnedOperators = getDownStreamPinnedOperators(upStreamPinnedOperators);

    //3. Check if all operators are pinned
    if (!checkPinnedOperators(upStreamPinnedOperators) || !checkPinnedOperators(downStreamPinnedOperators)) {
        throw QueryPlacementException(queryId, "QueryPlacementPhase: Found operators without pinning.");
    }

    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    std::set<TopologyNodePtr> topologyNodesWithUpStreamPinnedOperators;

    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findNodeWithId(nodeId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical sources located on same physical node)
        topologyNodesWithUpStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }
    std::set<TopologyNodePtr> topologyNodesWithDownStreamPinnedOperators;
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findNodeWithId(nodeId);
        topologyNodesWithDownStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());

    std::optional<TopologyNodePtr> topologiesForPlacement;
    std::vector<std::vector<TopologyNodePtr>> availablePaths;
    for (auto upstreamTopologyNode : upstreamTopologyNodes) {
        for (auto downstreamTopologyNode : downstreamTopologyNodes) {
            topologiesForPlacement = topology->findAllPathBetween(upstreamTopologyNode, downstreamTopologyNode);
        }
        if (!topologiesForPlacement.has_value()) {
            throw Exceptions::RuntimeException("BasePlacementStrategy: Could not find the path for placement.");
        }
        auto rootNode = topologiesForPlacement.value()->getAllRootNodes()[0];
        for (auto child : rootNode->getChildren()) {
            std::vector<TopologyNodePtr> path;
            path.emplace_back(rootNode->as<TopologyNode>());
            auto topologyIterator = NES::DepthFirstNodeIterator(child).begin();
            while (topologyIterator != DepthFirstNodeIterator::end()) {
                // get the ExecutionNode for the current topology Node
                auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
                path.emplace_back(currentTopologyNode);
                ++topologyIterator;
            }
            availablePaths.emplace_back(path);
        }
    }
    bool success = false;
    for (auto i : availablePaths) {
        success = placementStrategyPtr->updateGlobalExecutionPlan(queryId,
                                                                       faultToleranceType,
                                                                       lineageType,
                                                                       ftPlacement,
                                                                       upStreamPinnedOperators,
                                                                       downStreamPinnedOperators);
    }

    now = std::chrono::system_clock::now();
    now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    epoch = now_ms.time_since_epoch();
    auto valueAfter = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);

    std::cout << "The Decision time was: " << valueAfter.count() - value.count();

    NES_DEBUG("QueryPlacementPhase: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
    return success;
}

bool QueryPlacementPhase::checkPinnedOperators(const std::vector<OperatorNodePtr>& pinnedOperators) {

    for (auto pinnedOperator : pinnedOperators) {
        if (!pinnedOperator->hasProperty(PINNED_NODE_ID)) {
            return false;
        }
    }
    return true;
}

std::vector<OperatorNodePtr> QueryPlacementPhase::getUpStreamPinnedOperators(SharedQueryPlanPtr sharedQueryPlan) {
    std::vector<OperatorNodePtr> upStreamPinnedOperators;
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    if (!queryReconfiguration || SharedQueryPlanStatus::Created == sharedQueryPlan->getStatus()) {
        //Fetch all Source operators
        upStreamPinnedOperators = queryPlan->getLeafOperators();
    } else {
        auto changeLogs = sharedQueryPlan->getChangeLog();
        for (auto& addition : changeLogs->getAddition()) {
            //Only consider selecting already placed and pinned upstream operator
            if (addition.first->hasProperty(PINNED_NODE_ID) && addition.first->hasProperty(PLACED)) {
                upStreamPinnedOperators.emplace_back(addition.first);
            }
        }
        //TODO: #2496 We need to figure out how to deal with removals
    }
    return upStreamPinnedOperators;
}

std::vector<OperatorNodePtr>
QueryPlacementPhase::getDownStreamPinnedOperators(std::vector<OperatorNodePtr> upStreamPinnedOperators) {
    std::vector<OperatorNodePtr> downStreamPinnedOperators;
    auto rootTopologyNode = topology->getRoot();
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        //We check all root (sink) operators
        auto downStreamOperators = pinnedOperator->getAllRootNodes();
        for (auto& downStreamOperator : downStreamOperators) {
            //Only place sink operators that are not pinned or that are not placed yet
            auto logicalOperator = downStreamOperator->as<OperatorNode>();

            if (!logicalOperator->hasProperty(PINNED_NODE_ID) || !logicalOperator->hasProperty(PLACED)
                || !std::any_cast<bool>(logicalOperator->getProperty(PLACED))) {

                //Check if the operator already in the pinned operator list
                auto found = std::find_if(downStreamPinnedOperators.begin(),
                                          downStreamPinnedOperators.end(),
                                          [logicalOperator](const OperatorNodePtr& operatorNode) {
                                              return operatorNode->getId() == logicalOperator->getId();
                                          });

                if (found != downStreamPinnedOperators.end()) {
                    continue;
                }
                logicalOperator->addProperty(PINNED_NODE_ID, rootTopologyNode->getId());
                downStreamPinnedOperators.emplace_back(logicalOperator);
            }
        }
    }
    return downStreamPinnedOperators;
}
}// namespace NES::Optimizer