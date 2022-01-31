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
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

QueryPlacementPhase::QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase,
                                         SourceCatalogPtr streamCatalog,
                                         z3::ContextPtr z3Context,
                                         bool queryReconfiguration)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      typeInferencePhase(std::move(typeInferencePhase)), streamCatalog(std::move(streamCatalog)), z3Context(std::move(z3Context)),
      queryReconfiguration(queryReconfiguration) {
    NES_DEBUG("QueryPlacementPhase()");
}

QueryPlacementPhasePtr QueryPlacementPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                   TopologyPtr topology,
                                                   TypeInferencePhasePtr typeInferencePhase,
                                                   SourceCatalogPtr streamCatalog,
                                                   z3::ContextPtr z3Context,
                                                   bool queryReconfiguration) {
    return std::make_shared<QueryPlacementPhase>(QueryPlacementPhase(std::move(globalExecutionPlan),
                                                                     std::move(topology),
                                                                     std::move(typeInferencePhase),
                                                                     std::move(streamCatalog),
                                                                     std::move(z3Context),
                                                                     queryReconfiguration));
}

bool QueryPlacementPhase::execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("NESOptimizer: Placing input Query Plan on Global Execution Plan");
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    auto placementStrategyPtr = PlacementStrategyFactory::getStrategy(placementStrategy,
                                                                      globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      streamCatalog,
                                                                      z3Context);

    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    auto rootTopology = topology->getRoot();

    //1. Fetch all upstream pinned operators

    std::vector<OperatorNodePtr> upStreamPinnedOperators;
    if (!queryReconfiguration) {
        upStreamPinnedOperators = queryPlan->getLeafOperators();
    } else {
        auto changeLogs = sharedQueryPlan->getChangeLog();
        for (auto& addition : changeLogs->getAddition()) {
            upStreamPinnedOperators.emplace_back(addition.first);
        }
        //TODO: We need to figure out how to deal with removals
    }

    //2. Fetch all downstream pinned operators

    std::vector<OperatorNodePtr> downStreamPinnedOperators;
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        //We pin all root (sink) operators
        auto downStreamOperators = pinnedOperator->getAllRootNodes();
        for (auto& downStreamOperator : downStreamPinnedOperators) {
            //Only place sink operators that are not pinned or that are not placed yet
            if (!downStreamOperator->hasProperty(PINNED_NODE_ID) || !downStreamOperator->hasProperty(PLACED)
                || !std::any_cast<bool>(downStreamOperator->getProperty(PLACED))) {
                downStreamOperator->addProperty(PINNED_NODE_ID, rootTopology->getId());
                downStreamPinnedOperators.emplace_back(downStreamOperator);
            }
        }
    }

    if (checkPinnedOperators(upStreamPinnedOperators)) {
        return placementStrategyPtr->updateGlobalExecutionPlan(upStreamPinnedOperators, downStreamPinnedOperators);
    } else {
        throw QueryPlacementException(queryId, "Found start leaf operator without pinning");
    }
}

bool QueryPlacementPhase::checkPinnedOperators(const std::vector<OperatorNodePtr>& pinnedOperators) {

    for (auto pinnedOperator : pinnedOperators) {
        if (!pinnedOperator->hasProperty(PINNED_NODE_ID)) {
            return false;
        }
    }
    return true;
}

}// namespace NES::Optimizer