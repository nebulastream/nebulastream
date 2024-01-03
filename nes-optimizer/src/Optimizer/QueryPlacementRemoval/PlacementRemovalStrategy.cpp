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
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <queue>

namespace NES::Optimizer {

PlacementRemovalStrategyPtr PlacementRemovalStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                             const TopologyPtr& topology,
                                                             const TypeInferencePhasePtr& typeInferencePhase,
                                                             PlacementAmenderMode placementAmenderMode) {
    return std::make_shared<PlacementRemovalStrategy>(
        PlacementRemovalStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmenderMode));
}

PlacementRemovalStrategy::PlacementRemovalStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   const TopologyPtr& topology,
                                                   const TypeInferencePhasePtr& typeInferencePhase,
                                                   PlacementAmenderMode placementAmenderMode)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      placementAmenderMode(placementAmenderMode) {}

bool PlacementRemovalStrategy::updateGlobalExecutionPlan(NES::SharedQueryId sharedQueryId,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_INFO("Placement removal strategy called for the shared query plan {}", sharedQueryId);

    // Pessimistic Approach:
    //1. Find all topology nodes used to place the operators in the state Placed, To-Be-Re-Placed, and To-Be-Removed.
    //      1.1. Iterate over the operators and collect the PINNED_WORKER_ID.
    //2. Lock all topology nodes.
    //3. Fetch all placed query sub plans by analyzing the PLACED_SUB_PLAN_ID and CONNECTED_SYS_SUB_PLAN_DETAILS.
    //4. Locally manipulate the placed query sub plans on the execution nodes and check if after releasing the
    //   occupied resources, the new occupied resources are >= 0.
    //5. Iterate over the identified topology nodes in a strict order and update the operator to Removed or To-Be-Placed
    //   state, either mark the place query sub plan as "Stopped" (if all operators to be removed)
    //   or "Updated" (if plan was modified by removing operators).


    // Optimistic Approach:
    //1. Find all topology nodes used to place the operators in the state Placed, To-Be-Re-Placed, and To-Be-Removed.
    //      1.1. Iterate over the operators and collect the PINNED_WORKER_ID.
    //2. Locally manipulate the placed query sub plans on the execution nodes.
    //3. Fetch all placed query sub plans by analyzing the PLACED_SUB_PLAN_ID and CONNECTED_SYS_SUB_PLAN_DETAILS.
    //4. Iterate over the identified topology nodes in a strict order and perform validation as follow:
    //      4.1. Check if after releasing the occupied resources, the new occupied resources are >= 0.
    //      4.2. The local query sub plan version and query sub plan version on the execution node are the same
    //           (If they are different then some parallel task has also manipulated the sub plan).
    //5. If validation is successful, then lock the topology node and update the operator to Removed or To-Be-Placed
    //   state, either mark the place query sub plan as "Stopped" (if all operators to be removed)
    //   or "Updated" (if plan was modified by removing operators).


    performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

    if (placementAmenderMode == PlacementAmenderMode::PESSIMISTIC) {
        return false;
    }

    return true;
}

void PlacementRemovalStrategy::performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                                                    const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators) {

    //1. Find the topology nodes that will host upstream operators
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_WORKER_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        WorkerId workerId = std::any_cast<uint64_t>(value);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical sources located on same physical node)
        pinnedUpStreamTopologyNodeIds.emplace(workerId);
    }

    //2. Find the topology nodes that will host downstream operators
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_WORKER_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        WorkerId workerId = std::any_cast<uint64_t>(value);
        pinnedDownStreamTopologyNodeIds.emplace(workerId);
    }

    bool success = false;
    // 3. Find the path for placement based on the selected placement mode
    switch (placementAmenderMode) {
        case PlacementAmenderMode::PESSIMISTIC: {
            success = pessimisticPathSelection(pinnedUpStreamTopologyNodeIds, pinnedDownStreamTopologyNodeIds);
            break;
        }
        case PlacementAmenderMode::OPTIMISTIC: {
            success = optimisticPathSelection(pinnedUpStreamTopologyNodeIds, pinnedDownStreamTopologyNodeIds);
            break;
        }
    }

    // 4. Raise exception if unable to select and lock all topology nodes in the path
    if (!success) {
        throw Exceptions::RuntimeException("Unable to perform path selection.");
    }
}

bool PlacementRemovalStrategy::optimisticPathSelection(const std::set<WorkerId>&,
                                                       const std::set<WorkerId>&) {

    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {

        // 1.1. Performs path selection
        std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath;
        /*std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
            findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);*/

        //1.2 Process the selected path else retry
        if (!sourceTopologyNodesInSelectedPath.empty()) {

            // 1.3. Add the selected topology nodes to the topology map
            // Temp container for iteration
            std::queue<TopologyNodePtr> topologyNodesInBFSOrder;
            // Iterate topology nodes in a true breadth first order
            // Initialize with the upstream nodes
            std::for_each(sourceTopologyNodesInSelectedPath.begin(),
                          sourceTopologyNodesInSelectedPath.end(),
                          [&](const TopologyNodePtr& topologyNode) {
                              topologyNodesInBFSOrder.push(topologyNode);
                          });

            // 1.4. Record all selected topology nodes in BFS order
            while (!topologyNodesInBFSOrder.empty()) {
                auto topologyNodeToLock = topologyNodesInBFSOrder.front();
                topologyNodesInBFSOrder.pop();
                // 1.4.1. Skip if the topology node was visited previously
                WorkerId idOfTopologyNodeToLock = topologyNodeToLock->getId();
                if (workerIdToTopologyNodeMap.contains(idOfTopologyNodeToLock)) {
                    continue;
                }

                // 1.4.2. Add to the list of topology nodes for which locks are acquired
                workerNodeIdsInBFS.emplace_back(idOfTopologyNodeToLock);
                workerIdToTopologyNodeMap[idOfTopologyNodeToLock] = topologyNodeToLock;
                const auto& downstreamTopologyNodes = topologyNodeToLock->getParents();
                std::for_each(downstreamTopologyNodes.begin(), downstreamTopologyNodes.end(), [&](const NodePtr& topologyNode) {
                    topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
                });
            }
            success = true;
        }

        // 1.5. If unable to lock the topology nodes then wait for other processes to release locks on the topology node
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

bool PlacementRemovalStrategy::pessimisticPathSelection(const std::set<WorkerId>&,
                                                        const std::set<WorkerId>&) {
    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {

        //1.1 Performs path selection
        std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath;
        /*std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
            findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);*/

        if (!sourceTopologyNodesInSelectedPath.empty()) {
            //1.2 Lock the selected topology nodes exclusively and create a topology map
            success = lockTopologyNodesInSelectedPath(sourceTopologyNodesInSelectedPath);
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

bool PlacementRemovalStrategy::lockTopologyNodesInSelectedPath(const std::vector<TopologyNodePtr>& sourceTopologyNodes) {
    workerIdToTopologyNodeMap.clear();
    // Temp container for iteration
    std::queue<TopologyNodePtr> topologyNodesInBFSOrder;
    // Iterate topology nodes in a true breadth first order
    // Initialize with the upstream nodes
    std::for_each(sourceTopologyNodes.begin(), sourceTopologyNodes.end(), [&](const TopologyNodePtr& topologyNode) {
        topologyNodesInBFSOrder.push(topologyNode);
    });

    while (!topologyNodesInBFSOrder.empty()) {
        auto topologyNodeToLock = topologyNodesInBFSOrder.front();
        topologyNodesInBFSOrder.pop();
        // Skip if the topology node was visited previously
        WorkerId idOfTopologyNodeToLock = topologyNodeToLock->getId();
        if (workerIdToTopologyNodeMap.contains(idOfTopologyNodeToLock)) {
            continue;
        }

        //Try to acquire the lock
        TopologyNodeWLock lock = topology->lockTopologyNode(idOfTopologyNodeToLock);
        if (!lock) {
            NES_ERROR("Unable to Lock the topology node {} selected in the path selection.", idOfTopologyNodeToLock);
            //Release all the acquired locks as part of back-off and retry strategy.
            unlockTopologyNodesInSelectedPath();
            return false;
        }
        lockedTopologyNodeMap[idOfTopologyNodeToLock] = std::move(lock);

        // Add to the list of topology nodes for which locks are acquired
        workerNodeIdsInBFS.emplace_back(idOfTopologyNodeToLock);
        workerIdToTopologyNodeMap[idOfTopologyNodeToLock] = topologyNodeToLock;
        const auto& downstreamTopologyNodes = topologyNodeToLock->getParents();
        std::for_each(downstreamTopologyNodes.begin(), downstreamTopologyNodes.end(), [&](const NodePtr& topologyNode) {
            topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
        });
    }
    return true;
}

bool PlacementRemovalStrategy::unlockTopologyNodesInSelectedPath() {
    NES_INFO("Releasing locks for all locked topology nodes.");
    //1 Check if there are nodes on which locks are acquired
    if (workerNodeIdsInBFS.empty()) {
        NES_WARNING("No topology node found for which the locks are to be release.")
        return false;
    }

    //2 Release the locks in the inverse order of their acquisition
    std::for_each(workerNodeIdsInBFS.rbegin(), workerNodeIdsInBFS.rend(), [&](const WorkerId& lockedTopologyNodeId) {
        //2.1 Release the lock on the locked topology node
        lockedTopologyNodeMap[lockedTopologyNodeId]->unlock();
        lockedTopologyNodeMap.erase(lockedTopologyNodeId);
    });
    workerNodeIdsInBFS.clear();
    return true;
}

}// namespace NES::Optimizer
