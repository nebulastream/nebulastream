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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
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
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <algorithm>
#include <set>
#include <utility>

namespace NES::Optimizer {

DecomposedQueryPlanVersion getDecomposedQuerySubVersion() {
    static std::atomic_uint64_t version = 0;
    return ++version;
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

std::set<DeploymentContextPtr> QueryPlacementAmendmentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("QueryPlacementAmendmentPhase: Perform query placement phase for shared query plan {}",
             std::to_string(sharedQueryPlan->getId()));
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    bool enableIncrementalPlacement = coordinatorConfiguration->optimizer.enableIncrementalPlacement;

    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    NES_DEBUG("QueryPlacementAmendmentPhase: Perform query placement for query plan\n{}", queryPlan->toString());

    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    // Create container to record all deployment contexts
    std::map<DecomposedQueryPlanId, DeploymentContextPtr> deploymentContexts;

    if (enableIncrementalPlacement) {
        std::vector<Experimental::ChangeLogEntryPtr> failedChangelogEntries;
        for (const auto& changeLogEntry : sharedQueryPlan->getChangeLogEntries(nowInMicroSec)) {
            try {
                //1. Fetch all upstream pinned operators that are not removed
                std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
                for (const auto& upstreamOperator : changeLogEntry.second->upstreamOperators) {
                    if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                        pinnedUpstreamOperators.insert(upstreamOperator->as<LogicalOperator>());
                    }
                };

                //2. Fetch all downstream pinned operators that are not removed
                std::set<LogicalOperatorPtr> pinnedDownStreamOperators;
                for (const auto& downstreamOperator : changeLogEntry.second->downstreamOperators) {
                    if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                        pinnedDownStreamOperators.insert(downstreamOperator->as<LogicalOperator>());
                    }
                };

                //3. Pin all sink operators
                pinAllSinkOperators(pinnedDownStreamOperators);

                //4. Check if all operators are pinned
                if (!containsOnlyPinnedOperators(pinnedDownStreamOperators)
                    || !containsOnlyPinnedOperators(pinnedUpstreamOperators)) {
                    throw Exceptions::QueryPlacementAdditionException(
                        sharedQueryId,
                        "QueryPlacementAmendmentPhase: Found operators without pinning.");
                }

                //5. Get next query sub plan versions
                auto nextDecomposedQueryPlanVersion = getDecomposedQuerySubVersion();

                //6. Call placement removal strategy
                if (containsOperatorsForRemoval(pinnedDownStreamOperators)) {
                    auto placementRemovalStrategy = PlacementRemovalStrategy::create(globalExecutionPlan,
                                                                                     topology,
                                                                                     typeInferencePhase,
                                                                                     placementAmendmentMode);
                    auto placementRemovalDeploymentContexts =
                        placementRemovalStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                            pinnedUpstreamOperators,
                                                                            pinnedDownStreamOperators,
                                                                            nextDecomposedQueryPlanVersion);

                    // Collect all deployment contexts returned by placement removal strategy
                    for (const auto& [decomposedQueryPlanId, deploymentContext] : placementRemovalDeploymentContexts) {
                        deploymentContexts[decomposedQueryPlanId] = deploymentContext;
                    }

                } else {
                    NES_WARNING("Skipping placement removal phase as no pinned downstream operator in the state TO_BE_REMOVED or "
                                "TO_BE_REPLACED state.");
                }

                //7. Fetch all upstream pinned operators that are not removed
                pinnedUpstreamOperators.clear();
                for (const auto& upstreamOperator : changeLogEntry.second->upstreamOperators) {
                    if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                        pinnedUpstreamOperators.insert(upstreamOperator->as<LogicalOperator>());
                    }
                };

                //8. Fetch all downstream pinned operators that are not removed
                pinnedDownStreamOperators.clear();
                for (const auto& downstreamOperator : changeLogEntry.second->downstreamOperators) {
                    if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                        pinnedDownStreamOperators.insert(downstreamOperator->as<LogicalOperator>());
                    }
                };

                //9. Call placement addition strategy
                if (containsOperatorsForPlacement(pinnedDownStreamOperators)) {
                    auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
                    auto placementAdditionStrategy = getStrategy(placementStrategy);
                    auto placementAdditionResults =
                        placementAdditionStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                             pinnedUpstreamOperators,
                                                                             pinnedDownStreamOperators,
                                                                             nextDecomposedQueryPlanVersion);

                    // Collect all deployment contexts returned by placement removal strategy
                    for (const auto& [decomposedQueryPlanId, deploymentContext] : placementAdditionResults.deploymentContexts) {
                        deploymentContexts[decomposedQueryPlanId] = deploymentContext;
                    }

                    if (!placementAdditionResults.completedSuccessfully) {
                        throw std::runtime_error("Placement addition phase unsuccessfully completed");
                    }
                } else {
                    NES_WARNING("Skipping placement addition phase as no pinned downstream operator in the state PLACED or "
                                "TO_BE_PLACED state.");
                }
            } catch (std::exception& ex) {
                NES_ERROR("Failed to process change log. Marking shared query plan as partially processed and recording the "
                          "failed changelog for further processing. {}",
                          ex.what());
                sharedQueryPlan->setStatus(SharedQueryPlanStatus::PARTIALLY_PROCESSED);
                failedChangelogEntries.emplace_back(changeLogEntry.second);
            }
        }
        if (!failedChangelogEntries.empty()) {
            sharedQueryPlan->recordFailedChangeLogEntries(failedChangelogEntries);
        }
    } else {
        try {
            //1. Mark all PLACED operators as TO-BE-REPLACED
            for (const auto& operatorToCheck : queryPlan->getAllOperators()) {
                const auto& logicalOperator = operatorToCheck->as<LogicalOperator>();
                if (logicalOperator->getOperatorState() == OperatorState::PLACED) {
                    logicalOperator->setOperatorState(OperatorState::TO_BE_REPLACED);
                }
            }

            //2. Fetch all upstream pinned operators that are not removed
            std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
            for (const auto& leafOperator : queryPlan->getLeafOperators()) {
                if (leafOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                    pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperator>());
                }
            };

            //3. Fetch all downstream pinned operators that are not removed
            std::set<LogicalOperatorPtr> pinnedDownStreamOperators;
            for (const auto& rootOperator : queryPlan->getRootOperators()) {
                if (rootOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                    pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperator>());
                }
            };

            //4. Pin all sink operators
            pinAllSinkOperators(pinnedDownStreamOperators);

            //5. Check if all operators are pinned
            if (!containsOnlyPinnedOperators(pinnedDownStreamOperators)
                || !containsOnlyPinnedOperators(pinnedUpstreamOperators)) {
                throw Exceptions::QueryPlacementAdditionException(
                    sharedQueryId,
                    "QueryPlacementAmendmentPhase: Found operators without pinning.");
            }

            //6. Get the next decomposed query plan version
            auto nextDecomposedQueryPlanVersion = getDecomposedQuerySubVersion();

            //7. Call placement removal strategy
            if (containsOperatorsForRemoval(pinnedDownStreamOperators)) {
                auto placementRemovalStrategy =
                    PlacementRemovalStrategy::create(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode);
                auto placementRemovalDeploymentContexts =
                    placementRemovalStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                        pinnedUpstreamOperators,
                                                                        pinnedDownStreamOperators,
                                                                        nextDecomposedQueryPlanVersion);

                // Collect all deployment contexts returned by placement removal strategy
                for (const auto& [decomposedQueryPlanId, deploymentContext] : placementRemovalDeploymentContexts) {
                    deploymentContexts[decomposedQueryPlanId] = deploymentContext;
                }
            } else {
                NES_WARNING("Skipping placement removal phase as no pinned downstream operator in the state TO_BE_REMOVED or "
                            "TO_BE_REPLACED state.");
            }

            //8. Fetch all upstream pinned operators that are not removed
            pinnedUpstreamOperators.clear();
            for (const auto& leafOperator : queryPlan->getLeafOperators()) {
                if (leafOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                    pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperator>());
                }
            };

            //9. Fetch all downstream pinned operators that are not removed
            pinnedDownStreamOperators.clear();
            for (const auto& rootOperator : queryPlan->getRootOperators()) {
                if (rootOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
                    pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperator>());
                }
            };

            //10. Call placement addition strategy
            if (containsOperatorsForPlacement(pinnedDownStreamOperators)) {
                auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
                auto placementAdditionStrategy = getStrategy(placementStrategy);
                auto placementAdditionResults =
                    placementAdditionStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                         pinnedUpstreamOperators,
                                                                         pinnedDownStreamOperators,
                                                                         nextDecomposedQueryPlanVersion);

                // Collect all deployment contexts returned by placement removal strategy
                for (const auto& [decomposedQueryPlanId, deploymentContext] : placementAdditionResults.deploymentContexts) {
                    deploymentContexts[decomposedQueryPlanId] = deploymentContext;
                }

                if (!placementAdditionResults.completedSuccessfully) {
                    throw std::runtime_error("Placement addition phase unsuccessfully completed");
                }
            } else {
                NES_WARNING("Skipping placement addition phase as no pinned downstream operator in the state PLACED or "
                            "TO_BE_PLACED state.");
            }
        } catch (std::exception& ex) {
            NES_ERROR("Failed to process query delta due to: {}", ex.what());
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::PARTIALLY_PROCESSED);
        }
    }

    //Update the change log's till processed timestamp and clear all entries before the timestamp
    sharedQueryPlan->updateProcessedChangeLogTimestamp(nowInMicroSec);
    sharedQueryPlan->removeQueryMarkedForRemoval();

    if (sharedQueryPlan->getStatus() != SharedQueryPlanStatus::PARTIALLY_PROCESSED
        && sharedQueryPlan->getStatus() != SharedQueryPlanStatus::STOPPED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::PROCESSED);
    }

    NES_INFO("QueryPlacementAmendmentPhase: Update Global Execution Plan:\n{}", globalExecutionPlan->getAsString());
    std::set<DeploymentContextPtr> computedDeploymentContexts;
    for (const auto& [decomposedQueryPlanId, deploymentContext] : deploymentContexts) {
        computedDeploymentContexts.emplace(deploymentContext);
    }
    return computedDeploymentContexts;
}

bool QueryPlacementAmendmentPhase::containsOnlyPinnedOperators(const std::set<LogicalOperatorPtr>& pinnedOperators) {

    //Find if one of the operator does not have PINNED_WORKER_ID property
    return !std::any_of(pinnedOperators.begin(), pinnedOperators.end(), [](const LogicalOperatorPtr& pinnedOperator) {
        return !pinnedOperator->hasProperty(PINNED_WORKER_ID);
    });
}

bool QueryPlacementAmendmentPhase::containsOperatorsForPlacement(const std::set<LogicalOperatorPtr>& operatorsToCheck) {
    return std::any_of(operatorsToCheck.begin(), operatorsToCheck.end(), [](const LogicalOperatorPtr& operatorToCheck) {
        return (operatorToCheck->getOperatorState() == OperatorState::TO_BE_PLACED
                || operatorToCheck->getOperatorState() == OperatorState::PLACED);
    });
}

bool QueryPlacementAmendmentPhase::containsOperatorsForRemoval(const std::set<LogicalOperatorPtr>& operatorsToCheck) {
    return std::any_of(operatorsToCheck.begin(), operatorsToCheck.end(), [](const LogicalOperatorPtr& operatorToCheck) {
        return (operatorToCheck->getOperatorState() == OperatorState::TO_BE_REPLACED
                || operatorToCheck->getOperatorState() == OperatorState::TO_BE_REMOVED
                || operatorToCheck->getOperatorState() == OperatorState::PLACED);
    });
}

void QueryPlacementAmendmentPhase::pinAllSinkOperators(const std::set<LogicalOperatorPtr>& operators) {
    uint64_t rootNodeId = topology->getRootWorkerNodeIds()[0];
    for (const auto& operatorToCheck : operators) {
        if (!operatorToCheck->hasProperty(PINNED_WORKER_ID) && operatorToCheck->instanceOf<SinkLogicalOperator>()) {
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