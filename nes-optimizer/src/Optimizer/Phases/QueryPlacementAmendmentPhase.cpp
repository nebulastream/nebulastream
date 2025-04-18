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
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
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
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/CompilerConstants.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/SysPlanMetadata.hpp>
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

DeploymentUnit QueryPlacementAmendmentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("QueryPlacementAmendmentPhase: Perform query placement phase for shared query plan {}", sharedQueryPlan->getId());

    auto startPlacementAmendmentTime = getTimestamp();
    uint64_t deploymentTime = 0;
    bool enableIncrementalPlacement = coordinatorConfiguration->optimizer.enableIncrementalPlacement;
    NES_DEBUG("Enable incremental placement: {}", enableIncrementalPlacement);

    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlan = sharedQueryPlan->getQueryPlan();
    auto placementStrategy = sharedQueryPlan->getPlacementStrategy();
    NES_DEBUG("Placement strategy {}", magic_enum::enum_name(placementStrategy));
    NES_DEBUG("Topology {}",topology->toString());
    NES_DEBUG("QueryPlacementAmendmentPhase: Perform query placement for query plan\n{}", queryPlan->toString());

    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    //Compute next decomposed query plan version
    DecomposedQueryPlanVersion nextDecomposedQueryPlanVersion = getDecomposedQuerySubVersion();

    std::set<DeploymentContextPtr> computedDeploymentRemovalContexts;
    std::set<DeploymentContextPtr> computedDeploymentAdditionContexts;
    std::set<ReconfigurationMarkerUnit> reconfigurationMarkerUnitComparator;

    if (enableIncrementalPlacement) {

        // Create container to record all deployment contexts
        std::map<DecomposedQueryId, DeploymentContextPtr> deploymentContexts;
        std::vector<ChangeLogEntryPtr> failedChangelogEntries;

        for (const auto& [_, changeLogEntry] : sharedQueryPlan->getChangeLogEntries(nowInMicroSec)) {
            try {
                auto deploymentTimeStart = getTimestamp();
                // P0: Identify workers where reconfiguration markers need to be sent
                computeReconfigurationMarkerDeploymentUnit(sharedQueryId, changeLogEntry, reconfigurationMarkerUnitComparator);

                // Identify stateful operators that possibly needs to be migrated
                std::vector<OperatorPtr> statefulOperatorsPossiblyToBeMigrated;
                // Save old decomposed plan id and old worker id for this stateful operators
                std::unordered_map<OperatorId, std::pair<DecomposedQueryId, WorkerId>> statefulOperatorToOldProperties;
                // Make a copy of current decomposed plan, which includes this stateful operators
                std::unordered_map<DecomposedQueryId, std::shared_ptr<DecomposedQueryPlan>> planIdToPlanCopy;

                // TODO [#5149]: rewrite to give stateful operators not including upstream and downstream [https://github.com/nebulastream/nebulastream/issues/5149]
                for (auto operatorId : changeLogEntry->poSetOfSubQueryPlan) {
                    auto logicalOperator = queryPlan->getOperatorWithOperatorId(operatorId);
                    // TODO [#5149]: convert to property isStatefulOperator [https://github.com/nebulastream/nebulastream/issues/5149]
                    if (logicalOperator->instanceOf<LogicalJoinOperator>()) {
                        NES_DEBUG("QueryPlacementAmendmentPhase: Stateful operator with id {} found", logicalOperator->getId());
                        // get operator's worker and plan Ids and save
                        if (logicalOperator->hasProperty(PINNED_WORKER_ID)
                            && logicalOperator->hasProperty(PLACED_DECOMPOSED_PLAN_ID)) {
                            auto executionNode = std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID));
                            auto decomposedPlanId =
                                std::any_cast<DecomposedQueryId>(logicalOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
                            NES_DEBUG("QueryPlacementAmendmentPhase: Stateful operator with id {} belongs to plan {} and is "
                                      "placed on node {}",
                                      logicalOperator->getId(),
                                      decomposedPlanId,
                                      executionNode);
                            statefulOperatorsPossiblyToBeMigrated.push_back(logicalOperator);
                            statefulOperatorToOldProperties.insert(
                                std::pair(operatorId, std::pair(decomposedPlanId, executionNode)));
                            // make copy of old decomposed plan
                            planIdToPlanCopy.insert(
                                std::pair(decomposedPlanId,
                                          globalExecutionPlan->getCopyOfDecomposedQueryPlan(executionNode,
                                                                                            sharedQueryId,
                                                                                            decomposedPlanId)));
                        }
                    }
                }
                deploymentTime = deploymentTime + (getTimestamp() - deploymentTimeStart);

                // P1: Compute placement removal
                handlePlacementRemoval(sharedQueryId,
                                       changeLogEntry->upstreamOperators,
                                       changeLogEntry->downstreamOperators,
                                       nextDecomposedQueryPlanVersion,
                                       deploymentContexts);
                // P2: Compute placement addition
                handlePlacementAddition(placementStrategy,
                                        sharedQueryId,
                                        changeLogEntry->upstreamOperators,
                                        changeLogEntry->downstreamOperators,
                                        nextDecomposedQueryPlanVersion,
                                        deploymentContexts);

                deploymentTimeStart = getTimestamp();
                // get new location of migrating stateful operator
                std::unordered_map<OperatorId, std::shared_ptr<MigrateOperatorProperties>> migratingOperatorToProperties;
                std::unordered_map<OperatorId, std::string> migratingOperatorToFileSink;
                for (const auto& logicalOperator : statefulOperatorsPossiblyToBeMigrated) {
                    if (logicalOperator->hasProperty(PINNED_WORKER_ID)) {
                        auto newNodeId = std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID));
                        auto [oldPlanId, oldNodeId] = statefulOperatorToOldProperties.at(logicalOperator->getId());
                        // if location changed then operator's state needs to be migrated
                        if (newNodeId != oldNodeId) {
                            auto logicalOperatorId = logicalOperator->getId();
                            // store old decomposed plan id, old and new location of operator
                            migratingOperatorToProperties.insert(
                                std::pair(logicalOperatorId, MigrateOperatorProperties::create(oldPlanId, oldNodeId, newNodeId)));
                            // store recreation of operator handler file name and add it to logical operator on new node
                            auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                            std::string recreationFileName = std::to_string(time) + "-" + sharedQueryId.toString() + "-" + oldPlanId.toString() + "-"
                                + logicalOperatorId.toString();
                            migratingOperatorToFileSink.insert(std::pair(logicalOperatorId, recreationFileName + ".bin"));
                            if (logicalOperator->hasProperty(PLACED_DECOMPOSED_PLAN_ID)) {
                                auto newDecomposedPlanId = std::any_cast<DecomposedQueryId>(logicalOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
                                auto newDecomposedPlan = deploymentContexts.find(newDecomposedPlanId);
                                if (newDecomposedPlan != deploymentContexts.end()) {
                                    auto copyOfLogicalOperator = newDecomposedPlan->second->getDecomposedQueryPlan()->getOperatorWithOperatorId(logicalOperatorId);
                                    copyOfLogicalOperator->addProperty(QueryCompilation::MIGRATION_FILE, recreationFileName + "-completed.bin");
                                }
                            }
                            NES_DEBUG("QueryPlacementAmendmentPhase: Location of stateful operator with id {} changed, state "
                                      "needs to be migrated",
                                      logicalOperator->getId());
                        }
                    } else {
                        NES_ERROR("Failed to get new workerId.");
                    }
                }
                // add new decomposed plans with migration path from old node to new node
                handleMigrationPlacement(placementStrategy,
                                         migratingOperatorToProperties,
                                         migratingOperatorToFileSink,
                                         planIdToPlanCopy,
                                         sharedQueryId,
                                         nextDecomposedQueryPlanVersion,
                                         deploymentContexts);

                deploymentTime = deploymentTime + (getTimestamp() -deploymentTimeStart);

            } catch (std::exception& ex) {
                NES_ERROR("Failed to process change log. Marking shared query plan as partially processed and recording the "
                          "failed changelog for further processing. {}",
                          ex.what());
                sharedQueryPlan->setStatus(SharedQueryPlanStatus::PARTIALLY_PROCESSED);
                failedChangelogEntries.emplace_back(changeLogEntry);
            }
        }

        // Record all failed change log entries
        if (!failedChangelogEntries.empty()) {
            sharedQueryPlan->recordFailedChangeLogEntries(failedChangelogEntries);
        }

        // Extract placement deployment contexts
        for (const auto& [decomposedQueryId, deploymentContext] : deploymentContexts) {
            computedDeploymentAdditionContexts.emplace(deploymentContext);
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

            //2. Fetch all leaf operators of the query plan to compute upstream pinned operators that are to be removed
            std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
            for (const auto& leafOperator : queryPlan->getLeafOperators()) {
                pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperator>());
            };

            //3. Fetch all root operators of the query plan to compute downstream pinned operators that are to be removed
            std::set<LogicalOperatorPtr> pinnedDownStreamOperators;
            for (const auto& rootOperator : queryPlan->getRootOperators()) {
                pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperator>());
            };

            //4. fetch placement removal deployment contexts
            std::map<DecomposedQueryId, DeploymentContextPtr> placementRemovalDeploymentContexts;
            handlePlacementRemoval(sharedQueryId,
                                   pinnedUpstreamOperators,
                                   pinnedDownStreamOperators,
                                   nextDecomposedQueryPlanVersion,
                                   placementRemovalDeploymentContexts);

            //5. Collect all deployment contexts returned by placement removal phase
            for (const auto& [_, deploymentContext] : placementRemovalDeploymentContexts) {
                computedDeploymentRemovalContexts.insert(deploymentContext);
            }

            std::map<DecomposedQueryId, DeploymentContextPtr> placementAdditionDeploymentContexts;
            handlePlacementAddition(placementStrategy,
                                    sharedQueryId,
                                    pinnedUpstreamOperators,
                                    pinnedDownStreamOperators,
                                    nextDecomposedQueryPlanVersion,
                                    placementAdditionDeploymentContexts);

            // Collect all deployment contexts returned by placement removal strategy
            for (const auto& [_, deploymentContext] : placementAdditionDeploymentContexts) {
                computedDeploymentAdditionContexts.insert(deploymentContext);
            }
        } catch (std::exception& ex) {
            NES_ERROR("Failed to process query delta due to: {}", ex.what());
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::PARTIALLY_PROCESSED);
        }
    }

    //Update the change log's till processed timestamp and clear all entries before the timestamp
    sharedQueryPlan->updateProcessedChangeLogTimestamp(nowInMicroSec);

    if (sharedQueryPlan->getStatus() != SharedQueryPlanStatus::PARTIALLY_PROCESSED
        && sharedQueryPlan->getStatus() != SharedQueryPlanStatus::STOPPED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::PROCESSED);
    }

    auto placementTime = getTimestamp() - startPlacementAmendmentTime;
    NES_ERROR("GlobalExecutionPlan:{}", globalExecutionPlan->getAsString());
    return {computedDeploymentRemovalContexts, computedDeploymentAdditionContexts, reconfigurationMarkerUnitComparator, deploymentTime, placementTime};
}

void QueryPlacementAmendmentPhase::handleMigrationPlacement(
    Optimizer::PlacementStrategy placementStrategy,
    const std::unordered_map<OperatorId, std::shared_ptr<MigrateOperatorProperties>>& migratingOperatorToProperties,
    std::unordered_map<OperatorId, std::string>& migratingOperatorToFileSink,
    std::unordered_map<DecomposedQueryId, std::shared_ptr<DecomposedQueryPlan>> planIdToCopy,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
    std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts) {

    // go over operators that needs to be migrated
    for (const auto& [migratingOperatorId, migratingOperatorProperties] : migratingOperatorToProperties) {
        // 1. Create new operators for state migration
        // create source operator for gathering state on the source node
        // TODO: REMOVE THIS SOURCE. Check why it can't find path without source in updateGlobalExecutionPlan
        auto fakeMigrateSourceOperator =
            LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("fake_migration_source"));
        // set node id where operator was located
        fakeMigrateSourceOperator->addProperty(Optimizer::PINNED_WORKER_ID, migratingOperatorProperties->getOldWorkerId());

        // create sink operator to save state to the file on destination node
        auto recreationFileName = migratingOperatorToFileSink[migratingOperatorId];
        auto migrateSinkOperator = LogicalOperatorFactory::createSinkOperator(
            FileSinkDescriptor::create(recreationFileName, "MIGRATION_FORMAT", "OVERWRITE"));
        // set node id where operator is located now
        migrateSinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, migratingOperatorProperties->getNewWorkerId());
        // set downstream sink as a parent to source
        fakeMigrateSourceOperator->addParent(migrateSinkOperator);

        // 2. Make placement of new operators and path between them
        auto strategy = getStrategy(placementStrategy);
        auto placementResults = strategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                    {fakeMigrateSourceOperator},
                                                                    {migrateSinkOperator},
                                                                    nextDecomposedQueryPlanVersion);

        // save new contexts
        for (auto& [decomposedQueryPlanId, deploymentContext] : placementResults.deploymentContexts) {
            deploymentContext->setForMigration(true);
            // deploymentContext->getDecomposedQueryPlan()->setPlanForMigration(true);
            deploymentContexts[decomposedQueryPlanId] = deploymentContext;
        }

        // 3. Get necessary information to link old stateful operator decomposed plan and newly created migration plan
        // get copy of plan before migration
        auto statefulOperatorOldPlan = planIdToCopy.find(migratingOperatorProperties->getOldDecomposedQueryId());
        auto migrationPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
        // get plan id of the newly created plan for migration
        if (fakeMigrateSourceOperator->hasProperty(PLACED_DECOMPOSED_PLAN_ID)) {
            migrationPlanId = std::any_cast<DecomposedQueryId>(fakeMigrateSourceOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
        } else {
            NES_ERROR("Failed to get migration decomposed plan id");
        }
        // find deployment context for this plan
        auto migrationFlowNewPlan = deploymentContexts.find(migrationPlanId);

        // 4. Link old stateful operator decomposed plan and newly created migration plan
        if (statefulOperatorOldPlan != planIdToCopy.end() && migrationFlowNewPlan != deploymentContexts.end()) {
            // get stateful operator
            auto statefulOperatorInOldPlan = statefulOperatorOldPlan->second->getOperatorWithOperatorId(migratingOperatorId);
            statefulOperatorInOldPlan->addProperty("MIGRATION_FLAG", true);
            // get source operator in newly created plan
            auto networkSinkOperatorInMigrationPlan = migrationFlowNewPlan->second->getDecomposedQueryPlan()->getRootOperators().front();
            networkSinkOperatorInMigrationPlan->removeChildren();
            networkSinkOperatorInMigrationPlan->addProperty("MIGRATION_SINK", true);

            // link operators
            statefulOperatorInOldPlan->addParent(networkSinkOperatorInMigrationPlan);
            statefulOperatorOldPlan->second->addRootOperator(networkSinkOperatorInMigrationPlan);

            statefulOperatorOldPlan->second->setOldVersion(statefulOperatorOldPlan->second->getVersion());
            statefulOperatorOldPlan->second->setVersion(nextDecomposedQueryPlanVersion);
            statefulOperatorOldPlan->second->setState(QueryState::MARKED_FOR_UPDATE_AND_DRAIN);

            // delete old migrating deployment context for the old node
            deploymentContexts.erase(migrationPlanId);
            auto oldDeploymentContext = deploymentContexts[migratingOperatorProperties->getOldDecomposedQueryId()];
            // create new context for the updated plan
            auto newContext = DeploymentContext::create(oldDeploymentContext->getGrpcAddress(), statefulOperatorOldPlan->second);
            // save this plan
            deploymentContexts[migratingOperatorProperties->getOldDecomposedQueryId()] = newContext;
            globalExecutionPlan->removeDecomposedQueryPlan(migratingOperatorProperties->getOldWorkerId(),
                                                           sharedQueryId,
                                                           migrationPlanId,
                                                           nextDecomposedQueryPlanVersion);
            globalExecutionPlan->addDecomposedQueryPlan(migratingOperatorProperties->getOldWorkerId(),
                                                        {statefulOperatorOldPlan->second});
        } else {
            NES_ERROR("Failed to find old decomposed plan");
        }

        if (!placementResults.completedSuccessfully) {
            throw std::runtime_error("Placement addition phase unsuccessfully completed");
        }
    }
}

void QueryPlacementAmendmentPhase::computeReconfigurationMarkerDeploymentUnit(
    SharedQueryId& sharedQueryId,
    const ChangeLogEntryPtr& changeLogEntry,
    std::set<ReconfigurationMarkerUnit>& reconfigurationMarkerUnitComparator) const {

    // Iterate over the upstream operators to identify workers where reconfiguration markers need to be sent.
    // This algorithm considers the following workers:
    // Cond-1:
    // All nodes where upstream operators of the changeLogEntry are placed.
    // Cond-2:
    // If the connected logical downstream node is placed on a different node and the state is either placed or ToBeReplaced
    // then all nodes where connected "downstream decomposed plan" is placed by checking the CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS
    // property of the upstream operator.
    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        // Check if the operator is in the state PLACED or TO_BE_REMOVED
        if (upstreamOperator->getOperatorState() == OperatorState::PLACED
            || upstreamOperator->getOperatorState() == OperatorState::TO_BE_REMOVED) {
            // To fulfill Cond-1:
            // Find the pinned worker and decomposed plan Id of the pinned upstream operator
            auto upstreamLogicalOperatorPinnedWorkerId = std::any_cast<WorkerId>(upstreamOperator->getProperty(PINNED_WORKER_ID));
            auto upstreamWorkerAddress = topology->getGrpcAddress(upstreamLogicalOperatorPinnedWorkerId);
            if (!upstreamWorkerAddress.has_value()) {
                NES_ERROR("Unable to find the grpc address for the upstream worker {} ", upstreamLogicalOperatorPinnedWorkerId)
                throw std::runtime_error("Unable to find the grpc address for the upstream worker "
                                         + upstreamLogicalOperatorPinnedWorkerId.toString());
            }
            auto upstreamPlacedDecomposedId =
                std::any_cast<DecomposedQueryId>(upstreamOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
            reconfigurationMarkerUnitComparator.emplace(ReconfigurationMarkerUnit(upstreamWorkerAddress.value(),
                                                                                  upstreamLogicalOperatorPinnedWorkerId,
                                                                                  sharedQueryId,
                                                                                  upstreamPlacedDecomposedId));

            // To fulfill Cond-2:
            // Iterate over the upstream operators and identify the operators in the state Placed or ToBeRemoved
            for (const auto& node : upstreamOperator->getParents()) {
                auto downstreamOperator = node->as<LogicalOperator>();
                // Check the operator states
                if (downstreamOperator->getOperatorState() == OperatorState::PLACED
                    || downstreamOperator->getOperatorState() == OperatorState::TO_BE_REMOVED || downstreamOperator->getOperatorState() == OperatorState::TO_BE_REPLACED ) {

                    // Fetch the worker id where downstream logical operator is placed
                    const auto& downstreamLogicalOperatorPinnedWorkerID =
                        std::any_cast<WorkerId>(downstreamOperator->getProperty(PINNED_WORKER_ID));

                    // If the worker ids are not same then identify where connected "decomposed query plan" is placed.
                    if (downstreamLogicalOperatorPinnedWorkerID != upstreamLogicalOperatorPinnedWorkerId) {

                        // Find the downstream worker where the connected "downstream decomposed query plan" is located.
                        auto sysPlanMetaDataMap = std::any_cast<std::map<OperatorId, std::vector<SysPlanMetadata>>>(
                            upstreamOperator->getProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS));
                        for (auto [operatorId, sysPlanMetadataVec] : sysPlanMetaDataMap) {
                            auto upstreamId = upstreamLogicalOperatorPinnedWorkerId;
                            for (auto planIterator = sysPlanMetadataVec.begin(); planIterator != (sysPlanMetadataVec.end());
                                 ++planIterator) {
                                auto downstreamPlanMetaData = *planIterator;
                                auto downstreamId = downstreamPlanMetaData.workerId;

                                if (!topology->getCopyOfTopologyNodeWithId(downstreamId)
                                         ->containAsChild(topology->getCopyOfTopologyNodeWithId(upstreamId))) {
                                    auto downstreamGrpcAddress = topology->getGrpcAddress(downstreamId);
                                    if (!downstreamGrpcAddress.has_value()) {
                                        NES_ERROR("Unable to find the grpc address for the downstream worker {} ", downstreamId)
                                        throw std::runtime_error("Unable to find the grpc address for the downstream worker "
                                                                 + downstreamId.toString());
                                    }
                                    reconfigurationMarkerUnitComparator.emplace(downstreamGrpcAddress.value(),
                                                                                downstreamId,
                                                                                sharedQueryId,
                                                                                downstreamPlanMetaData.decomposedQueryId);
                                }
                                upstreamId = downstreamId;
                            }
                        }
                    }
                }
            }
        }
    }
}

void QueryPlacementAmendmentPhase::handlePlacementRemoval(NES::SharedQueryId sharedQueryId,
                                                          const std::set<LogicalOperatorPtr>& upstreamOperators,
                                                          const std::set<LogicalOperatorPtr>& downstreamOperators,
                                                          NES::DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
                                                          std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts) {

    //1. Fetch all upstream pinned operators that are not removed
    std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
    for (const auto& upstreamOperator : upstreamOperators) {
        if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
            pinnedUpstreamOperators.insert(upstreamOperator->as<LogicalOperator>());
        }
    };

    //2. Fetch all downstream pinned operators that are not removed
    std::set<LogicalOperatorPtr> pinnedDownStreamOperators;
    for (const auto& downstreamOperator : downstreamOperators) {
        if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
            pinnedDownStreamOperators.insert(downstreamOperator->as<LogicalOperator>());
        }
    };

    //3. Pin all sink operators
    pinAllSinkOperators(pinnedDownStreamOperators);

    //4. Check if all operators are pinned
    if (!containsOnlyPinnedOperators(pinnedDownStreamOperators) || !containsOnlyPinnedOperators(pinnedUpstreamOperators)) {
        throw Exceptions::QueryPlacementAdditionException(sharedQueryId,
                                                          "QueryPlacementAmendmentPhase: Found operators without pinning.");
    }

    //5. Call placement removal strategy
    if (containsOperatorsForRemoval(pinnedDownStreamOperators)) {
        auto placementRemovalStrategy =
            PlacementRemovalStrategy::create(globalExecutionPlan,
                                             topology,
                                             typeInferencePhase,
                                             coordinatorConfiguration->optimizer.placementAmendmentMode);
        auto placementRemovalDeploymentContexts =
            placementRemovalStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                pinnedUpstreamOperators,
                                                                pinnedDownStreamOperators,
                                                                nextDecomposedQueryPlanVersion);

        // Collect all deployment contexts returned by placement removal strategy
        for (const auto& [decomposedQueryId, deploymentContext] : placementRemovalDeploymentContexts) {
            deploymentContexts[decomposedQueryId] = deploymentContext;
        }
    } else {
        NES_WARNING("Skipping placement removal phase as no pinned downstream operator in the state TO_BE_REMOVED or "
                    "TO_BE_REPLACED state.");
    }
}

void QueryPlacementAmendmentPhase::handlePlacementAddition(
    Optimizer::PlacementStrategy placementStrategy,
    SharedQueryId sharedQueryId,
    const std::set<LogicalOperatorPtr>& upstreamOperators,
    const std::set<LogicalOperatorPtr>& downstreamOperators,
    DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
    std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts) {

    //1. Fetch all upstream pinned operators that are not removed
    std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
    for (const auto& upstreamOperator : upstreamOperators) {
        if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
            pinnedUpstreamOperators.insert(upstreamOperator->as<LogicalOperator>());
        }
    };

    //2. Fetch all downstream pinned operators that are not removed
    std::set<LogicalOperatorPtr> pinnedDownStreamOperators;
    for (const auto& downstreamOperator : downstreamOperators) {
        if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::REMOVED) {
            pinnedDownStreamOperators.insert(downstreamOperator->as<LogicalOperator>());
        }
    };

    //3. Call placement addition strategy
    if (containsOperatorsForPlacement(pinnedDownStreamOperators)) {
        auto placementAdditionStrategy = getStrategy(placementStrategy);
        auto placementAdditionResults = placementAdditionStrategy->updateGlobalExecutionPlan(sharedQueryId,
                                                                                             pinnedUpstreamOperators,
                                                                                             pinnedDownStreamOperators,
                                                                                             nextDecomposedQueryPlanVersion);

        // Collect all deployment contexts returned by placement removal strategy
        for (const auto& [decomposedQueryId, deploymentContext] : placementAdditionResults.deploymentContexts) {
            deploymentContexts[decomposedQueryId] = deploymentContext;
        }

        if (!placementAdditionResults.completedSuccessfully) {
            throw std::runtime_error("Placement addition phase unsuccessfully completed");
        }
    } else {
        NES_WARNING("Skipping placement addition phase as no pinned downstream operator in the state PLACED or "
                    "TO_BE_PLACED state.");
    }
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
    auto rootNodeId = topology->getRootWorkerNodeIds()[0];
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
            return ILPStrategy::create(globalExecutionPlan,
                                       topology,
                                       typeInferencePhase,
                                       coordinatorConfiguration->optimizer.placementAmendmentMode);
        case PlacementStrategy::BottomUp:
            return BottomUpStrategy::create(globalExecutionPlan,
                                            topology,
                                            typeInferencePhase,
                                            coordinatorConfiguration->optimizer.placementAmendmentMode);
        case PlacementStrategy::TopDown:
            return TopDownStrategy::create(globalExecutionPlan,
                                           topology,
                                           typeInferencePhase,
                                           coordinatorConfiguration->optimizer.placementAmendmentMode);
        case PlacementStrategy::ELEGANT_PERFORMANCE:
        case PlacementStrategy::ELEGANT_ENERGY:
        case PlacementStrategy::ELEGANT_BALANCED:
            return ElegantPlacementStrategy::create(plannerURL,
                                                    placementStrategy,
                                                    globalExecutionPlan,
                                                    topology,
                                                    typeInferencePhase,
                                                    coordinatorConfiguration->optimizer.placementAmendmentMode);
            // #2486        case PlacementStrategy::IFCOP:
            //            return IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
        case PlacementStrategy::MlHeuristic:
            return MlHeuristicStrategy::create(globalExecutionPlan,
                                               topology,
                                               typeInferencePhase,
                                               coordinatorConfiguration->optimizer.placementAmendmentMode);
        default:
            throw Exceptions::RuntimeException("Unknown placement strategy type "
                                               + std::string(magic_enum::enum_name(placementStrategy)));
    }
}
}// namespace NES::Optimizer
