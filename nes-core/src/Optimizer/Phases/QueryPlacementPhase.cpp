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
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
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
    NES_DEBUG2("QueryPlacementPhase()");
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

bool QueryPlacementPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO2("QueryPlacementPhase: Perform query placement phase for shared query plan {}",
              std::to_string(sharedQueryPlan->getId()));
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
    auto placementStrategyPtr =
        PlacementStrategyFactory::getStrategy(placementStrategy, globalExecutionPlan, topology, typeInferencePhase);

    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    auto faultToleranceType = queryPlan->getFaultToleranceType();
    auto lineageType = queryPlan->getLineageType();
    NES_DEBUG2("QueryPlacementPhase: Perform query placement for query plan\n{}", queryPlan->toString());

    for (const auto& changeLogEntry : sharedQueryPlan->getChangeLogEntries(1)) {

        //1. Fetch all upstream pinned operators
        auto pinnedUpstreamOperators = changeLogEntry.second->upstreamOperators;

        //2. Fetch all downstream pinned operators
        auto pinnedDownStreamOperators = changeLogEntry.second->downstreamOperators;

        //3. Check if all operators are pinned
        if (!checkPinnedOperators(pinnedDownStreamOperators) || !checkPinnedOperators(pinnedUpstreamOperators)) {
            throw QueryPlacementException(sharedQueryId, "QueryPlacementPhase: Found operators without pinning.");
        }

        bool success = placementStrategyPtr->updateGlobalExecutionPlan(sharedQueryId,
                                                                       faultToleranceType,
                                                                       lineageType,
                                                                       pinnedUpstreamOperators,
                                                                       pinnedDownStreamOperators);
        if (!success) {
            return false;
        }
    }
    NES_DEBUG2("QueryPlacementPhase: Update Global Execution Plan:\n{}", globalExecutionPlan->getAsString());
    return true;
}

bool QueryPlacementPhase::checkPinnedOperators(const std::set<OperatorNodePtr>& pinnedOperators) {

    //Find if anyone of the operator does not have PINNED_NODE_ID property
    return std::any_of(pinnedOperators.begin(), pinnedOperators.end(), [](const OperatorNodePtr& pinnedOperator) {
        return !pinnedOperator->hasProperty(PINNED_NODE_ID);
    });
}
}// namespace NES::Optimizer