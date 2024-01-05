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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementRemoval/PlacementRemovalStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Util/SysPlanMetaData.hpp>
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

bool PlacementRemovalStrategy::updateGlobalExecutionPlan(NES::SharedQueryId sharedQueryId,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_INFO("Placement removal strategy called for the shared query plan {}", sharedQueryId);

    // 1. Find all topology nodes used to place the operators in the state Placed, To-Be-Re-Placed, and To-Be-Removed.
    //      1.1. Iterate over the operators and collect the PINNED_WORKER_ID and information about the nodes where sys
    //           operators are placed using CONNECTED_SYS_SUB_PLAN_DETAILS.
    // 2. Lock all topology nodes.
    performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

    // 3. Create the copy of the query plans
    auto copiedPinnedOperators = createCopyOfQueryPlan(pinnedUpStreamOperators, pinnedDownStreamOperators);

    // 4. Fetch all placed query sub plans by analyzing the PLACED_SUB_PLAN_ID and CONNECTED_SYS_SUB_PLAN_DETAILS.
    updateQuerySubPlans(sharedQueryId,
                        copiedPinnedOperators.copiedPinnedUpStreamOperators,
                        copiedPinnedOperators.copiedPinnedDownStreamOperators);

    // 5. Iterate over the identified topology nodes in a strict order and perform validation as follow:
    //      5.1. Check if after releasing the occupied resources, the new occupied resources are >= 0.
    //      5.2. The local query sub plan version and query sub plan version on the execution node are the same
    //           (If they are different then some parallel task has also manipulated the sub plan).
    // 6. If validation is successful, then lock the topology node and update the operator to Removed or To-Be-Placed
    //    state, either mark the place query sub plan as "Stopped" (if all operators to be removed)
    //    or "Updated" (if plan was modified by removing operators).
    return updateExecutionNodes(sharedQueryId);
}

void PlacementRemovalStrategy::performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                                                    const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators) {

    // We iterate over the query plan in BFS order and record worker ids where operators are placed

    // 1. Add the selected topology nodes to the topology map
    // Temp container for iteration
    std::queue<LogicalOperatorNodePtr> operatorsToProcessInBFSOrder;
    // Iterate topology nodes in a true breadth first order
    // Initialize with the upstream nodes
    std::for_each(upStreamPinnedOperators.begin(), upStreamPinnedOperators.end(), [&](const auto& logicalOperator) {
        operatorsToProcessInBFSOrder.push(logicalOperator);
    });

    // 1.4. Record all selected topology nodes in BFS order
    while (!operatorsToProcessInBFSOrder.empty()) {
        auto operatorToProcess = operatorsToProcessInBFSOrder.front();
        operatorsToProcessInBFSOrder.pop();

        // If operator in the state To-Be-Placed then skip processing.
        auto operatorState = operatorToProcess->getOperatorState();
        auto operatorId = operatorToProcess->getId();

        if (operatorState == OperatorState::REMOVED) {
            //throw exception
        }

        if (!operatorToProcess->hasProperty(PINNED_WORKER_ID)) {
            //throw exception
        }

        // Fetch the worker id where the operator is placed
        auto pinnedWorkerId = std::any_cast<WorkerId>(operatorToProcess->getProperty(PINNED_WORKER_ID));
        workerIdsInBFS.emplace(pinnedWorkerId);

        // Fetch the query sub plan id that hosts the operator
        auto subQueryPlanId = std::any_cast<QuerySubPlanId>(operatorToProcess->getProperty(PLACED_SUB_PLAN_ID));
        if (workerIdToQuerySubPlanIds.contains(pinnedWorkerId)) {
            auto subQueryPlanIds = workerIdToQuerySubPlanIds[pinnedWorkerId];
            subQueryPlanIds.emplace(subQueryPlanId);
            workerIdToQuerySubPlanIds[pinnedWorkerId] = subQueryPlanIds;
        } else {
            workerIdToQuerySubPlanIds[pinnedWorkerId] = {subQueryPlanId};
        }

        // Record the operator id to be processed
        if (workerIdToOperatorIdMap.contains(pinnedWorkerId)) {
            auto placedOperatorIds = workerIdToOperatorIdMap[pinnedWorkerId];
            placedOperatorIds.emplace_back(operatorId);
            workerIdToOperatorIdMap[pinnedWorkerId] = placedOperatorIds;
        } else {
            workerIdToOperatorIdMap[pinnedWorkerId] = {operatorId};
        }

        //
        if (operatorToProcess->hasProperty(CONNECTED_SYS_SUB_PLAN_DETAILS)) {
            auto sysPlansMetaData =
                std::any_cast<std::vector<SysPlanMetaData>>(operatorToProcess->getProperty(CONNECTED_SYS_SUB_PLAN_DETAILS));
            for (const auto& sysPlanMetaData : sysPlansMetaData) {
                workerIdsInBFS.emplace(sysPlanMetaData.workerId);
                if (workerIdToQuerySubPlanIds.contains(pinnedWorkerId)) {
                    auto subQueryPlanIds = workerIdToQuerySubPlanIds[pinnedWorkerId];
                    subQueryPlanIds.emplace(sysPlanMetaData.querySubPlanId);
                    workerIdToQuerySubPlanIds[pinnedWorkerId] = subQueryPlanIds;
                } else {
                    workerIdToQuerySubPlanIds[pinnedWorkerId] = {sysPlanMetaData.querySubPlanId};
                }
            }
        }

        auto found = std::find_if(downStreamPinnedOperators.begin(),
                                  downStreamPinnedOperators.end(),
                                  [&](LogicalOperatorNodePtr operatorPin) {
                                      return operatorPin->getId() == operatorToProcess->getId();
                                  });
        if (found == downStreamPinnedOperators.end()) {
            continue;
        }

        for (const auto& downstreamOperator : operatorToProcess->getParents()) {
            operatorsToProcessInBFSOrder.emplace(downstreamOperator->as<LogicalOperatorNode>());
        }
    }

    bool success = workerIdsInBFS.empty();
    if (success) {
        // 3. Find the path for placement based on the selected placement mode
        if (placementAmendmentMode == PlacementAmendmentMode::PESSIMISTIC) {
            success = pessimisticPathSelection();
        }
    }

    // 4. Raise exception if unable to select and lock all topology nodes in the path
    if (!success) {
        throw Exceptions::RuntimeException("Unable to perform path selection.");
    }
}

bool PlacementRemovalStrategy::pessimisticPathSelection() {

    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {
        success = true;
        // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
        for (const auto& workerId : workerIdsInBFS) {
            if (topology->nodeWithWorkerIdExists(workerId)) {
                auto topologyNode = topology->getCopyOfTopologyNodeWithId(workerId);
                //Try to acquire the lock
                TopologyNodeWLock lock = topology->lockTopologyNode(workerId);
                if (!lock) {
                    NES_ERROR("Unable to Lock the topology node {} selected in the path selection.", workerId);
                    //Release all the acquired locks as part of back-off and retry strategy.
                    unlockTopologyNodesInSelectedPath();
                    success = false;
                    break;
                }
                lockedTopologyNodeMap[workerId] = std::move(lock);
            }
        }

        //1.3 If unable to lock the topology nodes then wait for other processes to release locks on the topology node
        if (!success) {
            NES_WARNING("Unable to lock topology nodes in the path. Waiting for the process to release locks and retry "
                        "path selection.");
            std::this_thread::sleep_for(backOffTime);
            retryCount++;
            backOffTime *= 2;
            backOffTime = std::min(MAX_PATH_SELECTION_RETRY_WAIT, backOffTime);
        }
    }
    return success;
}

CopiedPinnedOperators
PlacementRemovalStrategy::createCopyOfQueryPlan(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    std::set<LogicalOperatorNodePtr> copyOfPinnedUpStreamOperators;
    std::set<LogicalOperatorNodePtr> copyOfPinnedDownStreamOperators;

    // Temp container for iteration
    std::queue<LogicalOperatorNodePtr> operatorsToProcess;
    std::unordered_map<OperatorId, LogicalOperatorNodePtr> mapOfCopiedOperators;

    std::set<OperatorId> pinnedUpStreamOperatorId;
    for (auto pinnedUpStreamOperator : pinnedUpStreamOperators) {
        OperatorId operatorId = pinnedUpStreamOperator->getId();
        pinnedUpStreamOperatorId.emplace(operatorId);
        operatorsToProcess.emplace(pinnedUpStreamOperator);
        mapOfCopiedOperators[operatorId] = pinnedUpStreamOperator->copy()->as<LogicalOperatorNode>();
    }

    std::set<OperatorId> pinnedDownStreamOperatorId;
    for (auto pinnedDownStreamOperator : pinnedDownStreamOperators) {
        pinnedDownStreamOperatorId.emplace(pinnedDownStreamOperator->getId());
    }

    while (!operatorsToProcess.empty()) {
        auto operatorToProcess = operatorsToProcess.front();
        operatorsToProcess.pop();
        // Skip if the topology node was visited previously
        OperatorId operatorId = operatorToProcess->getId();
        operatorIdToOriginalOperatorMap[operatorId] = operatorToProcess;
        LogicalOperatorNodePtr operatorCopy = mapOfCopiedOperators[operatorId];

        //If the operator is a pinned upstream operator then add to the set of pinned upstream copy
        if (pinnedUpStreamOperatorId.contains(operatorId)) {
            copyOfPinnedUpStreamOperators.emplace(operatorCopy);
        }

        //If the operator is a downstream operator then add to the set of pinned downstream copy
        if (pinnedDownStreamOperatorId.contains(operatorId)) {
            copyOfPinnedDownStreamOperators.emplace(operatorCopy);
        }

        // Add to the list of topology nodes for which locks are acquired
        const auto& downstreamOperators = operatorToProcess->getParents();
        std::for_each(downstreamOperators.begin(), downstreamOperators.end(), [&](const NodePtr& operatorNode) {
            auto parentOperator = operatorNode->as<LogicalOperatorNode>();
            auto parentOperatorId = parentOperator->getId();
            LogicalOperatorNodePtr parentOperatorCopy;
            if (mapOfCopiedOperators.contains(parentOperatorId)) {
                parentOperatorCopy = mapOfCopiedOperators[parentOperatorId];
            } else {
                parentOperatorCopy = parentOperator->copy()->as<LogicalOperatorNode>();
                mapOfCopiedOperators[parentOperatorId] = parentOperatorCopy;
            }
            operatorCopy->addParent(parentOperatorCopy);
            operatorsToProcess.push(parentOperator);
        });
    }
    return {copyOfPinnedUpStreamOperators, copyOfPinnedDownStreamOperators};
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
        lockedTopologyNodeMap[lockedTopologyNodeId]->unlock();
        lockedTopologyNodeMap.erase(lockedTopologyNodeId);
    });
    workerIdsInBFS.clear();
    return true;
}

void PlacementRemovalStrategy::updateQuerySubPlans(SharedQueryId sharedQueryId,
                                                   const std::set<LogicalOperatorNodePtr>&,
                                                   const std::set<LogicalOperatorNodePtr>&) {

    NES_INFO("Releasing locks for all locked topology nodes.");
    for (const auto& [workerId, querySubPlanIds] : workerIdToQuerySubPlanIds) {
        auto executionNode = globalExecutionPlan->getExecutionNodeById(workerId);
        std::vector<QueryPlanPtr> modifiedQuerySubPlans;
        for (const auto& querySubPlanId : querySubPlanIds) {
            auto querySubPlanToModify = executionNode->getQuerySubPlan(sharedQueryId, querySubPlanId)->copy();

            //Modify the query plan by removing the operators that are in to-be-Removed or in to-be-replaced state
            auto allOperators = querySubPlanToModify->getAllOperators();

            modifiedQuerySubPlans.emplace_back(querySubPlanToModify);
        }
    }
}

bool PlacementRemovalStrategy::updateExecutionNodes(SharedQueryId sharedQueryId) {

    NES_INFO("Releasing locks for all locked topology nodes {}.", sharedQueryId);
    for (const auto& workerId : workerIdsInBFS) {

        TopologyNodeWLock lockedTopologyNode;
        // 1. If using optimistic strategy then, lock the topology node with the workerId and perform the "validation" before continuing.
        if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
            //1.1. wait till lock is acquired
            while (!(lockedTopologyNode = topology->lockTopologyNode(workerId))) {
                std::this_thread::sleep_for(PATH_SELECTION_RETRY_WAIT);
            };
        } else {
            lockedTopologyNode = lockedTopologyNodeMap[workerId];
        }

        try {
            // 1.2. Perform validation by checking if we can occupy the resources the operator placement algorithm reserved
            // for placing the operators.
            auto releasedSlots = workerIdToReleasedSlotMap[workerId];
            if (!lockedTopologyNode->operator*()->releaseSlots(releasedSlots)) {
                NES_ERROR("Unable to occupy resources on the topology node {} to successfully place operators.", workerId);
                return false;
            }

            auto updatedQuerySubPlans = workerIdToUpdatedQuerySubPlans[workerId];
            auto executionNode = globalExecutionPlan->getExecutionNodeById(workerId);

            // Perform validation
            bool planChanged = false;
            for (const auto& updatedQuerySubPlan : updatedQuerySubPlans) {
                auto querySubPlanId = updatedQuerySubPlan->getQuerySubPlanId();
                auto actualQuerySubPlan = executionNode->getQuerySubPlan(sharedQueryId, querySubPlanId);
                if (actualQuerySubPlan->getVersion() != updatedQuerySubPlan->getVersion()) {
                    planChanged = true;
                    break;
                }
                updatedQuerySubPlan->incrementVersion();
                if (updatedQuerySubPlan->getRootOperators().empty()) {
                    updatedQuerySubPlan->setQueryState(QueryState::MIGRATING);
                } else {
                    updatedQuerySubPlan->setQueryState(QueryState::RECONFIGURING);
                }
            }

            if (planChanged) {
                // throw exception
            }

            // Save Update query sub plans and execution node
            executionNode->updateQuerySubPlans(sharedQueryId, updatedQuerySubPlans);
            globalExecutionPlan->scheduleExecutionNode(executionNode);

            // Update the status of all operators
            auto updatedOperatorIds = workerIdToOperatorIdMap[workerId];
            for (const auto& updatedOperatorId : updatedOperatorIds) {
                auto originalOperator = operatorIdToOriginalOperatorMap[updatedOperatorId];
                const auto& currentOperatorState = originalOperator->getOperatorState();

                switch (currentOperatorState) {
                    case OperatorState::TO_BE_REPLACED: {
                        originalOperator->setOperatorState(OperatorState::TO_BE_PLACED);
                        break;
                    }
                    case OperatorState::TO_BE_REMOVED: {
                        originalOperator->setOperatorState(OperatorState::REMOVED);
                        break;
                    }
                    default:
                        NES_WARNING("Skip updating state for operator in current state {}",
                                    magic_enum::enum_name(currentOperatorState));
                }
            }

            // 1.8. Release lock on the topology node
            if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
                lockedTopologyNode->unlock();
            }
        } catch (std::exception& ex) {
            NES_ERROR("Exception occurred during pinned operator removal {}.", ex.what());
            throw ex;
        }
    }
    return true;
}

}// namespace NES::Optimizer
