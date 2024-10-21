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
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementRemovalException.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementRemoval/PlacementRemovalStrategy.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Util/SysPlanMetadata.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <queue>

namespace NES::Optimizer {

PlacementRemovalStrategyPtr PlacementRemovalStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                             const TopologyPtr& topology,
                                                             const TypeInferencePhasePtr& typeInferencePhase,
                                                             PlacementAmendmentMode placementAmendmentMode) {
    return std::make_shared<PlacementRemovalStrategy>(
        PlacementRemovalStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode));
}

PlacementRemovalStrategy::PlacementRemovalStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   const TopologyPtr& topology,
                                                   const TypeInferencePhasePtr& typeInferencePhase,
                                                   PlacementAmendmentMode placementAmendmentMode)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      placementAmendmentMode(placementAmendmentMode) {}

PlacementRemovalStrategy::~PlacementRemovalStrategy() {
    NES_INFO("Called ~PlacementRemovalStrategy()");
    if (placementAmendmentMode == PlacementAmendmentMode::PESSIMISTIC) {
        unlockTopologyNodesInSelectedPath();
    }
}

std::map<DecomposedQueryId, DeploymentContextPtr>
PlacementRemovalStrategy::updateGlobalExecutionPlan(NES::SharedQueryId sharedQueryId,
                                                    const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                    const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                    DecomposedQueryPlanVersion querySubPlanVersion) {

    try {
        NES_INFO("Placement removal strategy called for the shared query plan {}", sharedQueryId);

        // 1. Create the copy of the query plans
        auto copy =
            CopiedPinnedOperators::create(pinnedUpStreamOperators, pinnedDownStreamOperators, operatorIdToOriginalOperatorMap);

        std::vector<OperatorId> pinnedUpStreamOperatorIds;
        // Copy pinned upstream operator ids
        for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
            pinnedUpStreamOperatorIds.emplace_back(pinnedUpStreamOperator->getId());
        }

        std::vector<OperatorId> pinnedDownStreamOperatorIds;
        // Copy pinned downstream operator ids
        for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
            pinnedDownStreamOperatorIds.emplace_back(pinnedDownStreamOperator->getId());
        }

        // 2. Find all topology nodes used to place the operators in the state Placed, To-Be-Re-Placed, and To-Be-Removed.
        //      2.1. Iterate over the operators and collect the PINNED_WORKER_ID and information about the nodes where sys
        //           operators are placed using CONNECTED_SYS_SUB_PLAN_DETAILS.
        // 3. Lock all topology nodes.
        performPathSelection(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 4. If no operator in the state PLACED or TO_BE_REPLACED or TO_BE_REMOVED then skip rest of the process.
        if (workerIdsInBFS.empty()) {
            NES_WARNING("Skipping placement removal phase as nothing to be removed.");
            return {};
        }

        // 5. Fetch all placed query sub plans by analyzing the PLACED_SUB_PLAN_ID and CONNECTED_SYS_SUB_PLAN_DETAILS.
        updateDecomposedQueryPlans(sharedQueryId);

        // 6. Iterate over the identified topology nodes in a strict order and perform validation as follow:
        //      6.1. Check if after releasing the occupied resources, the new occupied resources are >= 0.
        //      6.2. The local query sub plan version and query sub plan version on the execution node are the same
        //           (If they are different then some parallel task has also manipulated the sub plan).
        // 7. If validation is successful, then lock the topology node and update the operator to Removed or To-Be-Placed
        //    state, either mark the place query sub plan as "Stopped" (if all operators to be removed)
        //    or "Updated" (if plan was modified by removing operators).
        return updateExecutionNodes(sharedQueryId, querySubPlanVersion, copy.copiedPinnedUpStreamOperators);
    } catch (std::exception& ex) {
        NES_ERROR("Exception occurred during placement removal phase: {}", ex.what());
        throw Exceptions::QueryPlacementRemovalException(sharedQueryId, ex.what());
    }
}

void PlacementRemovalStrategy::performPathSelection(const std::set<LogicalOperatorPtr>& upStreamPinnedOperators,
                                                    const std::set<LogicalOperatorPtr>& downStreamPinnedOperators) {
    try {
        // 1. We iterate over the query plan and record id of the operators to be processed
        std::queue<LogicalOperatorPtr> operatorsToProcessInBFSOrder;
        std::for_each(upStreamPinnedOperators.begin(), upStreamPinnedOperators.end(), [&](const auto& logicalOperator) {
            operatorsToProcessInBFSOrder.push(logicalOperator);
        });
        // 3. Record all selected topology nodes in BFS order
        while (!operatorsToProcessInBFSOrder.empty()) {
            auto operatorToProcess = operatorsToProcessInBFSOrder.front();
            operatorsToProcessInBFSOrder.pop();
            auto operatorId = operatorToProcess->getId();
            idsOfOperatorsToBeProcessed.emplace_back(operatorId);
            for (const auto& parent : operatorToProcess->getParents()) {
                operatorsToProcessInBFSOrder.emplace(parent->as<LogicalOperator>());
            }
        }

        // 2. Temp container for iteration
        // Iterate operator nodes in a true breadth first order
        // Initialize with all pinned upstream operators
        std::for_each(upStreamPinnedOperators.begin(), upStreamPinnedOperators.end(), [&](const auto& logicalOperator) {
            operatorsToProcessInBFSOrder.push(logicalOperator);
        });

        // 3. Record all selected topology nodes in BFS order
        while (!operatorsToProcessInBFSOrder.empty()) {

            // 4. Fetch the operator under consideration
            auto operatorToProcess = operatorsToProcessInBFSOrder.front();
            operatorsToProcessInBFSOrder.pop();

            auto operatorState = operatorToProcess->getOperatorState();
            auto operatorId = operatorToProcess->getId();

            // 5. If operator is in the state removed then raise an exception
            if (operatorState == OperatorState::REMOVED) {
                throw Exceptions::RuntimeException(
                    "Found an operator in the state removed. The sub query should not contain already removed operators.");
            }

            // 6. If operator is not in the state to be placed and has no pinned worker id property then raise an exception.
            if (operatorState != OperatorState::TO_BE_PLACED && !operatorToProcess->hasProperty(PINNED_WORKER_ID)) {
                throw Exceptions::RuntimeException("Found an operator without pinned worker id. The sub query should not contain "
                                                   "operators without pinned worker id.");
            }

            if (operatorState != OperatorState::TO_BE_PLACED) {
                // 7. Fetch the worker id where the operator is placed
                auto pinnedWorkerId = std::any_cast<WorkerId>(operatorToProcess->getProperty(PINNED_WORKER_ID));

                if (std::find(workerIdsInBFS.begin(), workerIdsInBFS.end(), pinnedWorkerId) == workerIdsInBFS.end()) {
                    workerIdsInBFS.emplace_back(pinnedWorkerId);
                }

                // 8. Fetch the decomposed query plan id that hosts the operator and record the sub query plan id
                auto decomposedQueryId =
                    std::any_cast<DecomposedQueryId>(operatorToProcess->getProperty(PLACED_DECOMPOSED_PLAN_ID));
                if (workerIdToDecomposedQueryPlanIds.contains(pinnedWorkerId)) {
                    auto subQueryPlanIds = workerIdToDecomposedQueryPlanIds[pinnedWorkerId];
                    subQueryPlanIds.emplace(decomposedQueryId);
                    workerIdToDecomposedQueryPlanIds[pinnedWorkerId] = subQueryPlanIds;
                } else {
                    workerIdToDecomposedQueryPlanIds[pinnedWorkerId] = {decomposedQueryId};
                }

                // 9. Record the operator id to be processed such that we can change its status later
                if (workerIdToOperatorIdMap.contains(pinnedWorkerId)) {
                    auto placedOperatorIds = workerIdToOperatorIdMap[pinnedWorkerId];
                    placedOperatorIds.emplace_back(operatorId);
                    workerIdToOperatorIdMap[pinnedWorkerId] = placedOperatorIds;
                } else {
                    workerIdToOperatorIdMap[pinnedWorkerId] = {operatorId};
                }
            }

            // 10. If the operator is one of the downstream pinned operator then continue
            auto found = std::find_if(downStreamPinnedOperators.begin(),
                                      downStreamPinnedOperators.end(),
                                      [&](LogicalOperatorPtr operatorPin) {
                                          return operatorPin->getId() == operatorToProcess->getId();
                                      });
            if (found != downStreamPinnedOperators.end()) {
                continue;
            }

            // 11. Fetch all connected upstream operators for further processing
            bool downStreamInToBePlacedState = true;
            for (const auto& parent : operatorToProcess->getParents()) {
                const auto& downStreamOperator = parent->as<LogicalOperator>();
                if (downStreamInToBePlacedState && downStreamOperator->getOperatorState() != OperatorState::TO_BE_PLACED) {
                    downStreamInToBePlacedState = false;
                }
                operatorsToProcessInBFSOrder.emplace(downStreamOperator);
            }

            // 12. If the operator is connected by system generated operators then record the worker ids and query sub plan ids
            if (operatorState != OperatorState::TO_BE_PLACED && !downStreamInToBePlacedState) {
                if (operatorToProcess->hasProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS)) {
                    auto downStreamOperatorToConnectedSysPlansMetaDataMap =
                        std::any_cast<std::map<OperatorId, std::vector<SysPlanMetadata>>>(
                            operatorToProcess->getProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS));

                    //Fetch data only for the operators listed for processing
                    for (const auto& idOfOperatorToProcess : idsOfOperatorsToBeProcessed) {
                        if (downStreamOperatorToConnectedSysPlansMetaDataMap.contains(idOfOperatorToProcess)) {
                            auto sysPlansMetaData = downStreamOperatorToConnectedSysPlansMetaDataMap[idOfOperatorToProcess];
                            for (const auto& sysPlanMetaData : sysPlansMetaData) {
                                if (std::find(workerIdsInBFS.begin(), workerIdsInBFS.end(), sysPlanMetaData.workerId)
                                    == workerIdsInBFS.end()) {
                                    workerIdsInBFS.emplace_back(sysPlanMetaData.workerId);
                                }
                                if (workerIdToDecomposedQueryPlanIds.contains(sysPlanMetaData.workerId)) {
                                    auto subQueryPlanIds = workerIdToDecomposedQueryPlanIds[sysPlanMetaData.workerId];
                                    subQueryPlanIds.emplace(sysPlanMetaData.decomposedQueryId);
                                    workerIdToDecomposedQueryPlanIds[sysPlanMetaData.workerId] = subQueryPlanIds;
                                } else {
                                    workerIdToDecomposedQueryPlanIds[sysPlanMetaData.workerId] = {
                                        sysPlanMetaData.decomposedQueryId};
                                }
                            }
                            downStreamOperatorToConnectedSysPlansMetaDataMap.erase(idOfOperatorToProcess);
                        }
                    }
                    //Save updated metadata
                    operatorToProcess->addProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS,
                                                   downStreamOperatorToConnectedSysPlansMetaDataMap);
                }
            }
        }

        for (const auto& workerId : workerIdsInBFS) {
            workerIdToTopologyNodeMap[workerId] = topology->getCopyOfTopologyNodeWithId(workerId);
        }

        // 13. try to lock all selected topology nodes
        if (!workerIdsInBFS.empty() && placementAmendmentMode == PlacementAmendmentMode::PESSIMISTIC) {
            // 14. Lock all identified worker ids before processing if the mode is pessimistic
            pessimisticPathSelection();
        }

        NES_INFO("Successfully selected path for placement removal");
    } catch (std::exception& ex) {
        NES_ERROR("Exception occurred during path selection: {}", ex.what());
        throw ex;
    }
}

void PlacementRemovalStrategy::pessimisticPathSelection() {
    NES_INFO("Pessimistically locking the selecting path for placement removal.");
    bool success = false;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success) {
        NES_INFO("Pessimistically locking selected path.");
        success = true;
        // 2. Perform path selection and if failure than use the exponential back-off and retry strategy
        for (const auto& workerId : workerIdsInBFS) {
            // 3. Try to acquire the lock2688422
            TopologyNodeWLock lock = topology->lockTopologyNode(workerId);
            if (!lock) {
                NES_WARNING("Unable to Lock the topology node {} selected in the path selection.", workerId);
                // 4. Release all the acquired locks as part of back-off and retry strategy.
                unlockTopologyNodesInSelectedPath();
                success = false;
                break;
            }
            lockedTopologyNodeMap[workerId] = std::move(lock);
        }
    }
    NES_INFO("Successfully locked selected path using pessimistic strategy.");
}

bool PlacementRemovalStrategy::unlockTopologyNodesInSelectedPath() {
    NES_INFO("Releasing locks for all locked topology nodes.");
    //1 Check if there are nodes on which locks are acquired
    if (workerIdsInBFS.empty()) {
        NES_WARNING("No topology node found for which the locks are to be release.")
        return false;
    }

    //2 Release the locks in the inverse order of their acquisition
    std::for_each(workerIdsInBFS.rbegin(), workerIdsInBFS.rend(), [&](const WorkerId& lockedTopologyNodeId) {
        //2.1 Release the lock on the locked topology node
        if (lockedTopologyNodeMap.contains(lockedTopologyNodeId)) {
            lockedTopologyNodeMap[lockedTopologyNodeId]->unlock();
            lockedTopologyNodeMap.erase(lockedTopologyNodeId);
        }
    });
    return true;
}

void PlacementRemovalStrategy::updateDecomposedQueryPlans(SharedQueryId sharedQueryId) {

    NES_INFO(
        "Update all decomposed query plans by removing the operators that are in the state To-Be-Removed or To-Be-RePlaced.");

    // 1. Iterate over the target decomposed query plans placed on different worker nodes to update them
    for (const auto& [workerId, decomposedQueryPlanIds] : workerIdToDecomposedQueryPlanIds) {

        std::vector<DecomposedQueryPlanPtr> updatedDecomposedQueryPlans;
        uint32_t releasedSlots = 0;
        // 2. Update the placed query sub plans on the execution node and record them
        for (const auto& decomposedQueryId : decomposedQueryPlanIds) {

            // 3. Fetch the copy of Decomposed query plan to modify
            auto decomposedQueryPlanToUpdate =
                globalExecutionPlan->getCopyOfDecomposedQueryPlan(workerId, sharedQueryId, decomposedQueryId);

            // 4. Check if plan is a sys generated query sub plan.
            // A Sys generated plan will contain only network source and sink operators.
            if (decomposedQueryPlanToUpdate->getRootOperators().size() == 1) {
                auto rootOperator = decomposedQueryPlanToUpdate->getRootOperators()[0];
                //Check if the root is of type network sink
                if (rootOperator->instanceOf<SinkLogicalOperator>()
                    && rootOperator->as_if<SinkLogicalOperator>()
                           ->getSinkDescriptor()
                           ->instanceOf<Network::NetworkSinkDescriptor>()) {
                    const auto& upstreamOperator = rootOperator->getChildren()[0];
                    //Check if the root is connected to a source of type network source
                    if (upstreamOperator->instanceOf<SourceLogicalOperator>()
                        && upstreamOperator->as_if<SourceLogicalOperator>()
                               ->getSourceDescriptor()
                               ->instanceOf<Network::NetworkSourceDescriptor>()) {
                        // 6. Mark the plan for migration to flush the in network tuples
                        decomposedQueryPlanToUpdate->setState(QueryState::MARKED_FOR_MIGRATION);
                        updatedDecomposedQueryPlans.emplace_back(decomposedQueryPlanToUpdate);
                        continue;
                    }
                }
            }

            // 7. Modify the query plan by removing the operators that are in to-be-Removed or in to-be-replaced state
            auto allOperators = decomposedQueryPlanToUpdate->getAllOperators();
            for (const auto& placedOperator : allOperators) {
                auto operatorId = placedOperator->getId();
                if (!operatorIdToOriginalOperatorMap.contains(operatorId)) {
                    NES_WARNING("Found the operator {} not in the submitted query plan.", placedOperator->toString());
                    continue;
                }

                auto operatorState = operatorIdToOriginalOperatorMap[operatorId]->getOperatorState();

                // 8. Remove all upstream and downstream operators of the operators in the state TO_BE_REMOVED or TO_BE_REPLACED
                if (operatorState == OperatorState::TO_BE_REMOVED || operatorState == OperatorState::TO_BE_REPLACED) {
                    // increment the released slot count
                    releasedSlots++;

                    // 9. If the operator is of type sink then also remove it as the root operator of the query sub plan
                    if (placedOperator->instanceOf<SinkLogicalOperator>()) {
                        decomposedQueryPlanToUpdate->removeAsRootOperator(operatorId);
                    }

                    // 10. Check all upstream operators of the considered operator and if any of the operator is of type
                    // network sink then remove it as the root of the query sub plan
                    for (auto placedDownStreamOperator : placedOperator->getParents()) {
                        if (placedDownStreamOperator->instanceOf<SinkLogicalOperator>()
                            && placedDownStreamOperator->as_if<SinkLogicalOperator>()
                                   ->getSinkDescriptor()
                                   ->instanceOf<Network::NetworkSinkDescriptor>()) {
                            decomposedQueryPlanToUpdate->removeAsRootOperator(
                                placedDownStreamOperator->as_if<Operator>()->getId());
                        }
                    }

                    // 11. Remove the operator from the plan by clearing the upstream and downstream operators
                    placedOperator->removeAllParent();
                    placedOperator->removeChildren();
                    placedOperator->clear();
                } else if (operatorState == OperatorState::PLACED) {

                    // 9. check if the placed pinned operator is connected to an operator o be processed via a
                    // network source then remove the connected network source
                    auto children = placedOperator->getChildren();
                    for (const auto& childOperator : children) {

                        // 10. Check if a network source operator
                        if (childOperator->instanceOf<SourceLogicalOperator>()
                            && childOperator->as_if<SourceLogicalOperator>()
                                   ->getSourceDescriptor()
                                   ->instanceOf<Network::NetworkSourceDescriptor>()) {

                            // 11. Fetch the id of the connected operator
                            auto connectedUpstreamOperatorId = std::any_cast<OperatorId>(
                                childOperator->as_if<LogicalOperator>()->getProperty(UPSTREAM_LOGICAL_OPERATOR_ID));

                            // 12. Check if connected to operator to be processed
                            bool connectedToOperatorToBeProcessed =
                                std::any_of(idsOfOperatorsToBeProcessed.begin(),
                                            idsOfOperatorsToBeProcessed.end(),
                                            [&](const OperatorId& operatorId) {
                                                return operatorId == connectedUpstreamOperatorId;
                                            });

                            // 13. remove the network source operator
                            if (connectedToOperatorToBeProcessed) {
                                placedOperator->removeChild(childOperator);
                            }
                        }
                    }
                }
            }

            // 12. Mark the plan for migration.
            // Plans that are updated are also marked for migration and the updated version is then deployed
            decomposedQueryPlanToUpdate->setState(QueryState::MARKED_FOR_MIGRATION);

            // 13. Add the updated query sub plan
            updatedDecomposedQueryPlans.emplace_back(decomposedQueryPlanToUpdate);
        }

        // 14. Store the updated query sub plans
        workerIdToUpdatedDecomposedQueryPlans[workerId] = updatedDecomposedQueryPlans;
        workerIdToReleasedSlotMap[workerId] = releasedSlots;
    }
}

std::map<DecomposedQueryId, DeploymentContextPtr>
PlacementRemovalStrategy::updateExecutionNodes(SharedQueryId sharedQueryId,
                                               DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                               std::set<LogicalOperatorPtr>& upStreamPinnedOperators) {

    std::map<DecomposedQueryId, DeploymentContextPtr> deploymentContexts;
    NES_INFO("Releasing locks for all locked topology nodes {}.", sharedQueryId);
    for (const auto& workerId : workerIdsInBFS) {

        if (!workerIdToUpdatedDecomposedQueryPlans.contains(workerId)) {
            NES_WARNING("No plan to updated query sub plan on the worker {}. Skipping remainder of the processing.", workerId);
            continue;
        }

        try {
            // 1. If using optimistic strategy then, lock the topology node with the workerId and perform the "validation" before continuing.
            auto releasedSlots = workerIdToReleasedSlotMap[workerId];
            if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
                //1.1. Step1:: Perform validation by checking if we can release the resources the operator placement algorithm reserved
                if (!topology->releaseSlots(workerId, releasedSlots)) {
                    NES_ERROR("Unable to release resources on the topology node {} to successfully remove operators.", workerId);
                    return deploymentContexts;
                }
            } else {
                auto lockedTopologyNode = lockedTopologyNodeMap[workerId];
                // 1.1. Step1:: Perform validation by checking if we can release the resources the operator placement algorithm reserved
                // for placing the operators.
                if (!lockedTopologyNode->operator*()->releaseSlots(releasedSlots)) {
                    NES_ERROR("Unable to release resources on the topology node {} to successfully remove operators.", workerId);
                    return deploymentContexts;
                }
            }

            // 2. Step1:: Perform validation by checking if the currently placed query sub plan has the same version as
            // the updated query sub plan or not
            auto updatedDecomposedQueryPlans = workerIdToUpdatedDecomposedQueryPlans[workerId];
            for (const auto& updatedDecomposedQueryPlan : updatedDecomposedQueryPlans) {
                auto decomposedQueryId = updatedDecomposedQueryPlan->getDecomposedQueryId();
                auto actualQuerySubPlan =
                    globalExecutionPlan->getCopyOfDecomposedQueryPlan(workerId, sharedQueryId, decomposedQueryId);

                // 3. Check is the updated and actual query sub plan has the same version number
                if (actualQuerySubPlan->getVersion() != updatedDecomposedQueryPlan->getVersion()) {
                    NES_ERROR("Query sub plan with id {} got updated. Current version {} Read version {}",
                              decomposedQueryId,
                              actualQuerySubPlan->getVersion(),
                              updatedDecomposedQueryPlan->getVersion());
                    return deploymentContexts;
                }

                // 4. Set the new version to the updated query sub plan
                updatedDecomposedQueryPlan->setVersion(decomposedQueryPlanVersion);
            }

            globalExecutionPlan->updateDecomposedQueryPlans(workerId, updatedDecomposedQueryPlans);

            // 5. Update the status of all operators
            auto updatedOperatorIds = workerIdToOperatorIdMap[workerId];
            for (const auto& updatedOperatorId : updatedOperatorIds) {
                auto originalOperator = operatorIdToOriginalOperatorMap[updatedOperatorId];
                const auto& currentOperatorState = originalOperator->getOperatorState();

                switch (currentOperatorState) {
                    case OperatorState::TO_BE_REPLACED: {
                        // Mark as to be placed
                        originalOperator->setOperatorState(OperatorState::TO_BE_PLACED);
                        originalOperator->removeProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS);
                        originalOperator->removeProperty(PLACED_DECOMPOSED_PLAN_ID);
                        originalOperator->removeProperty(PROCESSED);
                        //Remove pinning location if operator is not pinned upstream or sink or source operator
                        if (!originalOperator->instanceOf<SinkLogicalOperator>()
                            && !originalOperator->instanceOf<SourceLogicalOperator>()) {
                            auto isAPinnedUpstreamOperator =
                                std::any_of(upStreamPinnedOperators.begin(),
                                            upStreamPinnedOperators.end(),
                                            [&](const LogicalOperatorPtr& pinnedUpstreamOperator) {
                                                return pinnedUpstreamOperator->getId() == originalOperator->getId();
                                            });
                            if (!isAPinnedUpstreamOperator) {
                                originalOperator->removeProperty(PINNED_WORKER_ID);
                                originalOperator->removeProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS);
                                originalOperator->removeProperty(PLACED_DECOMPOSED_PLAN_ID);
                                originalOperator->removeProperty(PROCESSED);
                            }
                        }
                        break;
                    }
                    case OperatorState::TO_BE_REMOVED: {
                        // Mark as removed
                        originalOperator->setOperatorState(OperatorState::REMOVED);
                        break;
                    }
                    default:
                        NES_WARNING("Skip updating state for operator {} in current state {}",
                                    originalOperator->toString(),
                                    magic_enum::enum_name(currentOperatorState));
                }
            }

            auto topologyNode = workerIdToTopologyNodeMap[workerId];
            const std::string& grpcAddress = topologyNode->getGrpcAddress();
            // 6. compute deployment context
            for (const auto& decomposedQueryPlan : updatedDecomposedQueryPlans) {
                deploymentContexts[decomposedQueryPlan->getDecomposedQueryId()] =
                    DeploymentContext::create(grpcAddress, decomposedQueryPlan->copy());
            }
        } catch (std::exception& ex) {
            NES_ERROR("Exception occurred during pinned operator removal {}.", ex.what());
            throw ex;
        }
    }
    return deploymentContexts;
}

}// namespace NES::Optimizer
