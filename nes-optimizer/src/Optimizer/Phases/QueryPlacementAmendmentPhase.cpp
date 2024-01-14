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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/ElegantPlacementStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/ILPStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/MlHeuristicStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/TopDownStrategy.hpp>
#include <Optimizer/QueryPlacementRemoval/PlacementRemovalStrategy.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <algorithm>
#include <utility>

namespace NES::Optimizer {

DecomposedQueryPlanVersion getNextQuerySubPlanVersion() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

QueryPlacementAmendmentPhase::QueryPlacementAmendmentPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           Configurations::CoordinatorConfigurationPtr coordinatorConfiguration)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      typeInferencePhase(std::move(typeInferencePhase)), coordinatorConfiguration(std::move(coordinatorConfiguration)) {
    NES_DEBUG("QueryPlacementAmendmentPhase()");
    placementAmendmentMode = this->coordinatorConfiguration->optimizer.placementAmendmentMode;
}

QueryPlacementAmendmentPhasePtr
QueryPlacementAmendmentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                     TopologyPtr topology,
                                     TypeInferencePhasePtr typeInferencePhase,
                                     Configurations::CoordinatorConfigurationPtr coordinatorConfiguration) {
    return std::make_shared<QueryPlacementAmendmentPhase>(QueryPlacementAmendmentPhase(std::move(globalExecutionPlan),
                                                                                       std::move(topology),
                                                                                       std::move(typeInferencePhase),
                                                                                       std::move(coordinatorConfiguration)));
}

bool QueryPlacementAmendmentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("QueryPlacementAmendmentPhase: Perform query placement phase for shared query plan {}",
             std::to_string(sharedQueryPlan->getId()));
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    bool queryReconfiguration = coordinatorConfiguration->enableQueryReconfiguration;

    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    NES_DEBUG("QueryPlacementAmendmentPhase: Perform query placement for query plan\n{}", queryPlan->toString());

    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    if (queryReconfiguration) {
        for (const auto& changeLogEntry : sharedQueryPlan->getChangeLogEntries(nowInMicroSec)) {

            //1. Fetch all upstream pinned operators
            auto pinnedUpstreamOperators = changeLogEntry.second->upstreamOperators;

            //2. Fetch all downstream pinned operators
            auto pinnedDownStreamOperators = changeLogEntry.second->downstreamOperators;

            //3. Pin all sink operators
            pinAllSinkOperators(pinnedDownStreamOperators);

            //4. Check if all operators are pinned
            if (!containsOnlyPinnedOperators(pinnedDownStreamOperators)
                || !containsOnlyPinnedOperators(pinnedUpstreamOperators)) {
                throw Exceptions::QueryPlacementAdditionException(
                    sharedQueryId,
                    "QueryPlacementAmendmentPhase: Found operators without pinning.");
            }

            auto nextQuerySubPlanVersion = getNextQuerySubPlanVersion();

            if (containsOperatorsForRemoval(pinnedDownStreamOperators)) {
                auto placementRemovalStrategy =
                    PlacementRemovalStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
                bool success = placementRemovalStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                                   pinnedUpstreamOperators,
                                                                                   pinnedDownStreamOperators,
                                                                                   nextQuerySubPlanVersion);
                if (!success) {
                    NES_ERROR("Unable to perform placement removal for the change log entry");
                    return false;
                }
            } else {
                NES_WARNING("Skipping placement removal phase as no pinned downstream operator in the state TO_BE_REMOVED or "
                            "TO_BE_REPLACED state.");
            }

            if (containsOperatorsForPlacement(pinnedDownStreamOperators)) {
                auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
                auto placementAdditionStrategy = getStrategy(placementStrategy);
                bool success = placementAdditionStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                                    pinnedUpstreamOperators,
                                                                                    pinnedDownStreamOperators,
                                                                                    nextQuerySubPlanVersion);

                if (!success) {
                    NES_ERROR("Unable to perform placement addition for the change log entry");
                    return false;
                }
            } else {
                NES_WARNING("Skipping placement addition phase as no pinned downstream operator in the state PLACED or "
                            "TO_BE_PLACED state.");
            }
        }
        //Update the change log's till processed timestamp and clear all entries before the timestamp
        sharedQueryPlan->updateProcessedChangeLogTimestamp(nowInMicroSec);
    } else {

        //1. Fetch all upstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedUpstreamOperators;
        for (const auto& leafOperator : queryPlan->getLeafOperators()) {
            pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperatorNode>());
        };

        //2. Fetch all downstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedDownStreamOperators;
        for (const auto& rootOperator : queryPlan->getRootOperators()) {
            pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperatorNode>());
        };

        //3. Pin all sink operators
        pinAllSinkOperators(pinnedDownStreamOperators);

        //4. Check if all operators are pinned
        if (!containsOnlyPinnedOperators(pinnedDownStreamOperators) || !containsOnlyPinnedOperators(pinnedUpstreamOperators)) {
            throw Exceptions::QueryPlacementAdditionException(sharedQueryId,
                                                              "QueryPlacementAmendmentPhase: Found operators without pinning.");
        }

        auto nextQuerySubPlanVersion = getNextQuerySubPlanVersion();

        if (containsOperatorsForRemoval(pinnedDownStreamOperators)) {
            auto placementRemovalStrategy =
                PlacementRemovalStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);

            bool success = placementRemovalStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                               pinnedUpstreamOperators,
                                                                               pinnedDownStreamOperators,
                                                                               nextQuerySubPlanVersion);

            if (!success) {
                NES_ERROR("Unable to perform placement removal for the change log entry");
                return false;
            }
        } else {
            NES_WARNING("Skipping placement removal phase as no pinned downstream operator in the state TO_BE_REMOVED or "
                        "TO_BE_REPLACED state.");
        }

        if (containsOperatorsForPlacement(pinnedDownStreamOperators)) {
            auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
            auto placementAdditionStrategy = getStrategy(placementStrategy);
            bool success = placementAdditionStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                                pinnedUpstreamOperators,
                                                                                pinnedDownStreamOperators,
                                                                                nextQuerySubPlanVersion);

            if (!success) {
                NES_ERROR("Unable to perform query placement for the change log entry");
                return false;
            }
        } else {
            NES_WARNING("Skipping placement addition phase as no pinned downstream operator in the state PLACED or "
                        "TO_BE_PLACED state.");
        }
    }

    NES_DEBUG("QueryPlacementAmendmentPhase: Update Global Execution Plan:\n{}", globalExecutionPlan->getAsString());
    return true;
}

bool QueryPlacementAmendmentPhase::containsOnlyPinnedOperators(const std::set<LogicalOperatorNodePtr>& pinnedOperators) {

    //Find if one of the operator does not have PINNED_NODE_ID property
    return !std::any_of(pinnedOperators.begin(), pinnedOperators.end(), [](const LogicalOperatorNodePtr& pinnedOperator) {
        return !pinnedOperator->hasProperty(PINNED_WORKER_ID);
    });
}

bool QueryPlacementAmendmentPhase::containsOperatorsForPlacement(const std::set<LogicalOperatorNodePtr>& operatorsToCheck) {
    return std::any_of(operatorsToCheck.begin(), operatorsToCheck.end(), [](const LogicalOperatorNodePtr& operatorToCheck) {
        return (operatorToCheck->getOperatorState() == OperatorState::TO_BE_PLACED
                || operatorToCheck->getOperatorState() == OperatorState::PLACED);
    });
}

bool QueryPlacementAmendmentPhase::containsOperatorsForRemoval(const std::set<LogicalOperatorNodePtr>& operatorsToCheck) {
    return std::any_of(operatorsToCheck.begin(), operatorsToCheck.end(), [](const LogicalOperatorNodePtr& operatorToCheck) {
        return (operatorToCheck->getOperatorState() == OperatorState::TO_BE_REPLACED
                || operatorToCheck->getOperatorState() == OperatorState::TO_BE_REMOVED
                || operatorToCheck->getOperatorState() == OperatorState::PLACED);
    });
}

void QueryPlacementAmendmentPhase::pinAllSinkOperators(const std::set<LogicalOperatorNodePtr>& operators) {
    uint64_t rootNodeId = topology->getRootTopologyNodeId();
    for (const auto& operatorToCheck : operators) {
        if (!operatorToCheck->hasProperty(PINNED_WORKER_ID) && operatorToCheck->instanceOf<SinkLogicalOperatorNode>()) {
            operatorToCheck->addProperty(PINNED_WORKER_ID, rootNodeId);
        }
    }
}

BasePlacementStrategyPtr QueryPlacementAmendmentPhase::getStrategy(PlacementStrategy placementStrategy) {

    auto plannerURL = coordinatorConfiguration->elegant.plannerServiceURL;

    switch (placementStrategy) {
        case PlacementStrategy::ILP:
            return ILPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
        case PlacementStrategy::BottomUp:
            return BottomUpStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
        case PlacementStrategy::TopDown:
            return TopDownStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
        case PlacementStrategy::ELEGANT_PERFORMANCE:
        case PlacementStrategy::ELEGANT_ENERGY:
        case PlacementStrategy::ELEGANT_BALANCED:
            return ElegantPlacementStrategy::create(plannerURL,
                                                    placementStrategy,
                                                    globalExecutionPlan,
                                                    topology,
                                                    typeInferencePhase,
                                                    placementAmendmentMode);
            // #2486        case PlacementStrategy::IFCOP:
            //            return IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
        case PlacementStrategy::MlHeuristic:
            return MlHeuristicStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
        default:
            throw Exceptions::RuntimeException("Unknown placement strategy type "
                                               + std::string(magic_enum::enum_name(placementStrategy)));
    }
}

}// namespace NES::Optimizer