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
#include <Catalogs/Topology/PathFinder.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Util/SysPlanMetadata.hpp>
#include <algorithm>
#include <thread>
#include <unordered_set>
#include <utility>

namespace NES::Optimizer {

BasePlacementAdditionStrategy::BasePlacementAdditionStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                             const TopologyPtr& topology,
                                                             const TypeInferencePhasePtr& typeInferencePhase,
                                                             PlacementAmendmentMode placementAmendmentMode,
                                                             FaultToleranceType faultTolerance)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      placementAmendmentMode(placementAmendmentMode), faultTolerance(faultTolerance) {
    //Initialize path finder
    pathFinder = std::make_shared<PathFinder>((topology->getRootWorkerNodeIds()));
}

void BasePlacementAdditionStrategy::performPathSelection(const std::set<LogicalOperatorPtr>& upStreamPinnedOperators,
                                                         const std::set<LogicalOperatorPtr>& downStreamPinnedOperators) {

    if (faultTolerance == FaultToleranceType::M || faultTolerance == FaultToleranceType::AS) {
        workerToOffload = findOffloadOperator(upStreamPinnedOperators);
    }
    //1. Find the topology nodes that will host upstream operators
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_WORKER_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        WorkerId workerId = std::any_cast<WorkerId>(value);
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
        WorkerId workerId = std::any_cast<WorkerId>(value);
        pinnedDownStreamTopologyNodeIds.emplace(workerId);
    }

    bool success = false;
    // 3. Find the path for placement based on the selected placement mode
    switch (placementAmendmentMode) {
        case PlacementAmendmentMode::PESSIMISTIC: {
            success = pessimisticPathSelection(pinnedUpStreamTopologyNodeIds, pinnedDownStreamTopologyNodeIds);
            break;
        }
        case PlacementAmendmentMode::OPTIMISTIC: {
            success = optimisticPathSelection(pinnedUpStreamTopologyNodeIds, pinnedDownStreamTopologyNodeIds);
            break;
        }
    }

    // 4. Raise exception if unable to select and lock all topology nodes in the path
    if (!success) {
        throw Exceptions::RuntimeException("Unable to perform path selection.");
    }
    NES_INFO("Selected path for placement addition.");
}

bool BasePlacementAdditionStrategy::optimisticPathSelection(
    const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
    const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators) {
    uint64_t numberOfPath = 1;
    if (faultTolerance == FaultToleranceType::AS || faultTolerance == FaultToleranceType::M) {
        numberOfPath = 2;
    }

    bool success = false;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success) {

        // 1.1. Performs path selection
        std::vector<std::vector<TopologyNodePtr>> sourceTopologyNodesInSelectedPath;
        if (workerToOffload != INVALID_WORKER_NODE_ID) {
            sourceTopologyNodesInSelectedPath = {topology->findPathThatIncludesNode(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators, workerToOffload)};
        }
        else {
            sourceTopologyNodesInSelectedPath =
   findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);
        }


        for (uint64_t i = 0; i < numberOfPath; i++) {
            PathInfo pathInfo;

            //1.2 Process the selected path else retry
            if (!sourceTopologyNodesInSelectedPath[i].empty()) {

                // 1.3. Add the selected topology nodes to the topology map
                // Temp container for iteration
                std::queue<TopologyNodePtr> topologyNodesInBFSOrder;
                // Iterate topology nodes in a true breadth first order
                // Initialize with the upstream nodes
                std::for_each(sourceTopologyNodesInSelectedPath[i].begin(),
                              sourceTopologyNodesInSelectedPath[i].end(),
                              [&](const TopologyNodePtr& topologyNode) {
                                  topologyNodesInBFSOrder.push(topologyNode);
                              });

                // 1.4. Record all selected topology nodes in BFS order
                while (!topologyNodesInBFSOrder.empty()) {
                    auto topologyNodeToLock = topologyNodesInBFSOrder.front();
                    topologyNodesInBFSOrder.pop();
                    // 1.4.1. Skip if the topology node was visited previously
                    WorkerId idOfTopologyNodeToLock = topologyNodeToLock->getId();
                    if (pathInfo.workerIdToTopologyNodeMap.contains(idOfTopologyNodeToLock)) {
                        continue;
                    }

                    // 1.4.2. Add to the list of topology nodes for which locks are acquired
                    pathInfo.workerNodeIdsInBFS.emplace_back(idOfTopologyNodeToLock);
                    pathInfo.workerIdToTopologyNodeMap[idOfTopologyNodeToLock] = topologyNodeToLock;
                    const auto& downstreamTopologyNodes = topologyNodeToLock->getParents();
                    std::for_each(downstreamTopologyNodes.begin(),
                                  downstreamTopologyNodes.end(),
                                  [&](const NodePtr& topologyNode) {
                                      topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
                                  });
                }
                success = true;
            }
        }
    }
    return success;
}


bool BasePlacementAdditionStrategy::pessimisticPathSelection(
    const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
    const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators) {

    bool success = false;
    if (faultTolerance == FaultToleranceType::M) {
        topology->assignLevelsByBFS(topologyNodesWithUpStreamPinnedOperators);
        topology->assignAlternativeNodes(topologyNodesWithUpStreamPinnedOperators, pinnedDownStreamTopologyNodeIds);
    }

    while (!success) {
        // 1. Perform path selection
        std::vector<std::vector<TopologyNodePtr>> allSourcesAllPaths;

        if (workerToOffload != INVALID_WORKER_NODE_ID) {
            auto pathIncludingOffload = topology->findPathThatIncludesNode(
                topologyNodesWithUpStreamPinnedOperators,
                topologyNodesWithDownStreamPinnedOperators,
                workerToOffload
            );
            if (!pathIncludingOffload.empty()) {
                allSourcesAllPaths.push_back(std::move(pathIncludingOffload));
            }
        } else {
            for (auto sourceId : topologyNodesWithUpStreamPinnedOperators) {
                auto perSourcePaths = findPath({sourceId}, topologyNodesWithDownStreamPinnedOperators);
                for (auto& p : perSourcePaths) {
                    allSourcesAllPaths.push_back(std::move(p));
                }
            }
        }

        // 2. Process all found paths
        std::set<WorkerId> nodesToLock;
        pathsFound.clear();
        for (auto& singlePath : allSourcesAllPaths) {
            if (!singlePath.empty()) {
                PathInfo pathInfo;
                for (auto& node : singlePath) {
                    WorkerId id = node->getId();
                    nodesToLock.insert(id);
                    pathInfo.workerNodeIdsInBFS.push_back(id);
                    pathInfo.workerIdToTopologyNodeMap[id] = node;
                }
                pathsFound.push_back(std::move(pathInfo));
            }
        }

        // 3. Attempt to lock the topology nodes.
        success = lockTopologyNodes(nodesToLock);
    }
    return success;
}




WorkerId BasePlacementAdditionStrategy::findOffloadOperator(const std::set<LogicalOperatorPtr>& upStreamPinnedOperators) {


    if (upStreamPinnedOperators.empty()) {
        return INVALID_WORKER_NODE_ID;
    }

    std::unordered_set<OperatorId> visited;
    std::queue<LogicalOperatorPtr> toExplore;

    for (auto& pinnedOp : upStreamPinnedOperators) {
        toExplore.push(pinnedOp);
    }

    while (!toExplore.empty()) {
        auto currentOp = toExplore.front();
        toExplore.pop();

        if (!visited.insert(currentOp->getId()).second) {
            continue;
        }

        if (currentOp->hasProperty("WORKER_ID_TO_OFFLOAD")) {
            auto offloadId = std::any_cast<WorkerId>(currentOp->getProperty("WORKER_ID_TO_OFFLOAD"));
            return offloadId;
        }

        for (auto& parent : currentOp->getParents()) {
            auto parentLogOp = parent->as_if<LogicalOperator>();
            if (parentLogOp) {
                toExplore.push(parentLogOp);
            }
        }
    }

    return INVALID_WORKER_NODE_ID;
}

bool BasePlacementAdditionStrategy::lockTopologyNodes(const std::set<WorkerId>& nodesToLock) {
    for (const auto& workerId : nodesToLock) {
        // Try to acquire the lock
        TopologyNodeWLock lock = topology->lockTopologyNode(workerId);
        if (!lock) {
            NES_ERROR("Unable to Lock the topology node {} selected in the path selection.", workerId);
            // Release all the acquired locks as part of back-off and retry strategy.
            unlockAllLockedTopologyNodes();
            return false;
        }
        lockedTopologyNodeMap[workerId] = std::move(lock);
    }
    return true;
}

bool BasePlacementAdditionStrategy::lockTopologyNodesInSelectedPath(PathInfo path) {
    for (const auto& idOfTopologyNodeToLock : path.workerNodeIdsInBFS) {
        //Try to acquire the lock
        TopologyNodeWLock lock = topology->lockTopologyNode(idOfTopologyNodeToLock);
        if (!lock) {
            NES_ERROR("Unable to Lock the topology node {} selected in the path selection.", idOfTopologyNodeToLock);
            //Release all the acquired locks as part of back-off and retry strategy.
            unlockTopologyNodesInSelectedPath(path);
            return false;
        }
        lockedTopologyNodeMap[idOfTopologyNodeToLock] = std::move(lock);
    }
    return true;
}

void BasePlacementAdditionStrategy::unlockAllLockedTopologyNodes() {
    for (auto& [workerId, lock] : lockedTopologyNodeMap) {
        // Release the lock
        lock.reset();
    }
    lockedTopologyNodeMap.clear();
}

bool BasePlacementAdditionStrategy::unlockTopologyNodesInSelectedPath(PathInfo path) {
    NES_INFO("Releasing locks for all locked topology nodes.");
    //1 Check if there are nodes on which locks are acquired
    if (path.workerNodeIdsInBFS.empty()) {
        NES_WARNING("No topology node found for which the locks are to be release.")
        return false;
    }

    //2 Release the locks in the inverse order of their acquisition
    std::for_each(path.workerNodeIdsInBFS.rbegin(), path.workerNodeIdsInBFS.rend(), [&](const WorkerId& lockedTopologyNodeId) {
        //2.1 Release the lock on the locked topology node
        if (lockedTopologyNodeMap.contains(lockedTopologyNodeId)) {
            lockedTopologyNodeMap[lockedTopologyNodeId]->unlock();
            lockedTopologyNodeMap.erase(lockedTopologyNodeId);
        }
    });
    path.workerNodeIdsInBFS.clear();
    return true;
}

std::vector<std::vector<TopologyNodePtr>>
BasePlacementAdditionStrategy::findPath(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                        const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators) {

    //1 Create the pinned upstream and downstream topology node vector
    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());

    //2 Do the path selection
    // NOTE: this call can be replaced with any arbitrary path selection algorithm
    return topology->findAllPathsBetween(upstreamTopologyNodes, downstreamTopologyNodes);
}

ComputedDecomposedQueryPlans
BasePlacementAdditionStrategy::computeDecomposedQueryPlans(SharedQueryId sharedQueryId,
                                                           const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                           const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {

    // The trailing logic iterates over the completely pinned query plan in strict BFS order.
    // Starting from the upstream to downstream operators.
    // For example, a shared query plan with id 1:
    //
    // OP1[N1] --> OP2[N2]                         --> OP7[N2] --> OP8[N3] --> OP9[N3]
    //                     \                     /
    //                      -> OP5[N2] -> OP6[N2]
    //                     /                     \
    // OP3[N1] --> OP4[N2]                         --> OP10[N3]
    //
    // The strict BFS iteration order will be: OP1, OP3, OP2, OP4, OP5, OP6, OP7, OP10, OP8, OP9
    //
    // During the BFS iteration it computes query sub plans that need to be placed on the execution nodes.
    // It starts with query sub plans containing only the upstream operators and then append to them the next downstream operator.
    // In the above example following will be the output after each iteration.
    //
    // 1: { N1: {(1, INVALID, OP1)} } // INVALID id as pinned operator
    // 2: { N1: {(1, INVALID, OP1), (1, INVALID, OP3)}}
    // 3: { N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 1, OP2)}
    //    }
    // 4: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 1, OP2), (1, 2, OP4)}
    //    }
    // 5: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5)} //Note: two plans got merged
    //    }
    // 6: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5->OP6)}
    //    }
    // 7: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5->OP6->OP7)}
    //    }
    // 8: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5->OP6->OP7)}
    //      N3: {(1, INVALID, OP10)}
    //    }
    // 9: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5->OP6->OP7)}
    //      N3: {(1, INVALID, OP10), (1, 4, OP8)}
    //    }
    // 10: {
    //      N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //      N2: {(1, 3, {OP2,OP4}->OP5->OP6->OP7)}
    //      N3: {(1, INVALID, OP10), (1, INVALID, OP8->OP9)} //Note: shared query plan id was changed to INVALID id as pinned operator found
    //    }

    // Create a map of computed query sub plans
    ComputedDecomposedQueryPlans computedDecomposedQueryPlans;

    //1. Prepare for iterating over operators in strict BFS order
    // Initialize with the pinned upstream operators
    std::queue<LogicalOperatorPtr> pinnedOperators;
    std::for_each(pinnedUpStreamOperators.begin(), pinnedUpStreamOperators.end(), [&](const auto& operatorNode) {
        pinnedOperators.push(operatorNode);
    });

    //2. Iterate over all placed operators in strict BFS order
    while (!pinnedOperators.empty()) {

        // 2.1. Fetch the pinned operator and the worker Id where it is pinned
        auto pinnedOperator = pinnedOperators.front();
        pinnedOperators.pop();
        // Skip further processing if operator was processed previously
        OperatorId operatorId = pinnedOperator->getId();
        if (operatorIdToCopiedOperatorMap.contains(operatorId)) {
            continue;
        }
        //Add operator to processed operator set
        operatorIdToCopiedOperatorMap[operatorId] = pinnedOperator;
        auto copyOfPinnedOperator = pinnedOperator->copy()->as<LogicalOperator>();// Make a copy of the operator
        auto pinnedWorkerId = std::any_cast<WorkerId>(pinnedOperator->getProperty(PINNED_WORKER_ID));
        if (pinnedOperator->getOperatorState() == OperatorState::TO_BE_PLACED
            || pinnedOperator->getOperatorState() == OperatorState::TO_BE_REPLACED) {
            workerIdToResourceConsumedMap[pinnedWorkerId]++;
        }

        // 2.2. If the pinnedWorkerId has already placed query sub plans then compute an updated list of query sub plans
        if (computedDecomposedQueryPlans.contains(pinnedWorkerId)) {
            // 2.2.1. Prepare an updated list of query sub plans and a new placed query plan for the operator under
            // consideration
            std::vector<DecomposedQueryPlanPtr> updatedDecomposedQueryPlans;
            DecomposedQueryPlanPtr newDecomposedQueryPlan;

            // 2.2.2. Iterate over all already placed query sub plans to check if the iterated operator
            // contains the root operator of already placed query sub plan as its upstream operator
            auto existingDecomposedQueryPlans = computedDecomposedQueryPlans[pinnedWorkerId];
            for (const auto& existingDecomposedQueryPlan : existingDecomposedQueryPlans) {

                //2.2.2.1. For all upstream operators of the pinned operator check if they are in the placed query sub plan
                bool foundConnectedUpstreamPlacedOperator = false;
                for (const auto& childOperator : pinnedOperator->getChildren()) {
                    OperatorId childOperatorId = childOperator->as<Operator>()->getId();
                    if (existingDecomposedQueryPlan->hasOperatorWithId(childOperatorId)) {
                        auto placedOperator = existingDecomposedQueryPlan->getOperatorWithOperatorId(childOperatorId);
                        //Remove the placed operator as the root
                        existingDecomposedQueryPlan->removeAsRootOperator(placedOperator->getId());
                        // Add the copy as the parent of the iterated root operator and update the root of the iterated
                        placedOperator->addParent(copyOfPinnedOperator);
                        foundConnectedUpstreamPlacedOperator = true;
                    }
                }

                // 2.2.2.2. For all downstream operators of the pinned operator check if they are in the placed query sub plan
                bool foundConnectedDownstreamPlacedOperator = false;
                for (const auto& parentOperator : pinnedOperator->getParents()) {
                    OperatorId parentOperatorId = parentOperator->as<Operator>()->getId();
                    if (existingDecomposedQueryPlan->hasOperatorWithId(parentOperatorId)) {
                        auto placedOperator = existingDecomposedQueryPlan->getOperatorWithOperatorId(parentOperatorId);
                        // Add the copy as the parent of the iterated root operator and update the root of the iterated
                        placedOperator->addChild(copyOfPinnedOperator);

                        // Check if all upstream operators of the considered pinnedOperator are co-located on same node
                        // We will use this information to compute network source and sink operators
                        auto actualPlacedOperator = operatorIdToCopiedOperatorMap[placedOperator->getId()];
                        if (actualPlacedOperator->getChildren().size() == placedOperator->getChildren().size()) {
                            placedOperator->addProperty(CO_LOCATED_UPSTREAM_OPERATORS, true);
                        }
                        foundConnectedDownstreamPlacedOperator = true;
                    }
                }

                if (foundConnectedUpstreamPlacedOperator) {// If an upstream operator found in the placed query sub
                                                           // plan then create a new or merge the placed query plan with
                                                           // the new query sub plan.
                    if (!newDecomposedQueryPlan) {
                        newDecomposedQueryPlan = DecomposedQueryPlan::create(existingDecomposedQueryPlan->getDecomposedQueryId(),
                                                                             sharedQueryId,
                                                                             pinnedWorkerId,
                                                                             existingDecomposedQueryPlan->getRootOperators(),
                                                                             existingDecomposedQueryPlan->getFaultToleranceType());
                        newDecomposedQueryPlan->addRootOperator(copyOfPinnedOperator);
                    } else {
                        const auto& rootOperators = existingDecomposedQueryPlan->getRootOperators();
                        std::for_each(rootOperators.begin(), rootOperators.end(), [&](const auto& rootOperator) {
                            newDecomposedQueryPlan->addRootOperator(rootOperator);
                        });
                    }
                } else if (foundConnectedDownstreamPlacedOperator) {// If a downstream operator found in the placed query sub
                                                                    // plan then create a new or merge the placed
                                                                    // query plan \with the previous new query sub plan.
                    if (!newDecomposedQueryPlan) {
                        //Add root of existing plan as the root of new plan
                        newDecomposedQueryPlan = DecomposedQueryPlan::create(existingDecomposedQueryPlan->getDecomposedQueryId(),
                                                                             sharedQueryId,
                                                                             pinnedWorkerId,
                                                                             existingDecomposedQueryPlan->getRootOperators(),
                                                                             existingDecomposedQueryPlan->getFaultToleranceType());
                    } else {
                        const auto& rootOperators = existingDecomposedQueryPlan->getRootOperators();
                        //NOTE: Remove the copy Of pinnedOperator as the root operator
                        newDecomposedQueryPlan->removeAsRootOperator(copyOfPinnedOperator->getId());
                        std::for_each(rootOperators.begin(), rootOperators.end(), [&](const auto& rootOperator) {
                            newDecomposedQueryPlan->addRootOperator(rootOperator);
                        });
                    }
                } else {// Else add the disjoint query sub plan to the updated query plan list
                    updatedDecomposedQueryPlans.emplace_back(existingDecomposedQueryPlan);
                }
            }

            //2.2.3. Create a new query sub plan for the operator under consideration if all plans are disjoint.
            if (!newDecomposedQueryPlan) {
                if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                    // Create a temporary query sub plans for the operator as it might need to be merged with another query
                    // sub plan that is already placed on the execution node. Thus, we assign it an invalid sub query plan id.
                    newDecomposedQueryPlan = DecomposedQueryPlan::create(INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                                         sharedQueryId,
                                                                         pinnedWorkerId,
                                                                         {copyOfPinnedOperator},
                                                                         faultTolerance);
                } else {
                    newDecomposedQueryPlan = DecomposedQueryPlan::create(PlanIdGenerator::getNextDecomposedQueryPlanId(),
                                                                         sharedQueryId,
                                                                         pinnedWorkerId,
                                                                         {copyOfPinnedOperator},
                                                                         faultTolerance);
                }
            } else if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                newDecomposedQueryPlan->setDecomposedQueryPlanId(
                    INVALID_DECOMPOSED_QUERY_PLAN_ID);// set invalid query sub plan id
            }

            //2.2.4. Add the new query sub plan to the list
            updatedDecomposedQueryPlans.emplace_back(newDecomposedQueryPlan);
            computedDecomposedQueryPlans[pinnedWorkerId] = updatedDecomposedQueryPlans;
        } else {// 2.2.1. If no query sub plan placed on the pinned worker node
            // Create a new placed query sub plan
            DecomposedQueryPlanPtr newDecomposedQueryPlan;
            if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                // Create a temporary query sub plans for the operator as it might need to be merged with another query
                // sub plan that is already placed on the execution node. Thus, we assign it an invalid sub query plan id.
                newDecomposedQueryPlan = DecomposedQueryPlan::create(INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                                     sharedQueryId,
                                                                     pinnedWorkerId,
                                                                     {copyOfPinnedOperator},
                                                                     faultTolerance);
            } else {
                newDecomposedQueryPlan = DecomposedQueryPlan::create(PlanIdGenerator::getNextDecomposedQueryPlanId(),
                                                                     sharedQueryId,
                                                                     pinnedWorkerId,
                                                                     {copyOfPinnedOperator},
                                                                     faultTolerance);
            }
            //2.2.2. Add the new query sub plan to the list
            computedDecomposedQueryPlans[pinnedWorkerId] = {newDecomposedQueryPlan};
        }

        // Check if all upstream operators of the considered pinnedOperator are co-located on same node
        // We will use this information to compute network source and sink operators
        if (copyOfPinnedOperator->getChildren().size() == pinnedOperator->getChildren().size()) {
            copyOfPinnedOperator->addProperty(CO_LOCATED_UPSTREAM_OPERATORS, true);
        }

        //2.3. If the operator is in the state placed then check if it is one of the pinned downstream operators
        if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
            //2.3.1. Check if this operator in the pinned downstream operator list.
            const auto& isPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                                  pinnedDownStreamOperators.end(),
                                                                  [operatorId](const auto& pinnedDownStreamOperator) {
                                                                      return pinnedDownStreamOperator->getId() == operatorId;
                                                                  });

            //2.3.2. If reached the pinned downstream operator then skip further traversal
            if (isPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
                continue;
            }
        }

        // 2.4. Add next downstream operators for the traversal
        auto downStreamOperators = pinnedOperator->getParents();
        std::for_each(downStreamOperators.begin(), downStreamOperators.end(), [&](const NodePtr& operatorNode) {
            // 2.5. Only add those operators that are to be processed as part of the given query plan.
            // NOTE: It can happen that an operator is connected to multiple downstream operators but some of them are not to be considered during the placement.
            const auto& downStreamOperator = operatorNode->as<LogicalOperator>();
            if (operatorIdToOriginalOperatorMap.contains(downStreamOperator->getId())) {
                pinnedOperators.push(downStreamOperator);
            }
        });
    }
    return computedDecomposedQueryPlans;
}

void BasePlacementAdditionStrategy::addNetworkOperators(ComputedDecomposedQueryPlans& computedDecomposedQueryPlans) {

    // Iterate over all computed query sub plans and then add network source and sink operators
    //
    // Input:
    // {
    //    N1: {(1, INVALID, OP1-->OP2), (1, INVALID, OP3)}
    //    N2: {(1, 3, {OP2,OP4}->OP5->OP6->OP7)}
    //    N3: {(1, INVALID, OP9), (1, INVALID, OP8->OP9)} //Note: shared query plan id was changed to INVALID id as pinned operator found
    // }
    //
    // Updated Map:
    // {
    //    N1: {(1, INVALID, OP1-->OP2-->NSnk), (1, INVALID, OP3-->NSnk)}
    //    N2: {(1, 3, {NSrc-->OP2, NSrc-->OP4}-->OP5-->OP6-->{NSnk, OP7-->NSnk})}
    //    N3: {(1, INVALID, NSrc-->OP9), (1, INVALID, NSrc-->OP8-->OP9)}
    // }
    //
    // Iterate over all computed decomposed query plans and add network source and sink operators.
    for (const auto& [workerId, decomposedQueryPlans] : computedDecomposedQueryPlans) {
        auto downstreamTopologyNode = getTopologyNode(workerId);

        // 1. Iterate over all query sub plans
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {

            // shared query id
            auto sharedQueryId = decomposedQueryPlan->getSharedQueryId();

            // 2. Fetch all logical operators whose upstream logical operators are missing.
            // In the previous call, we stored this information by setting the property
            // CO_LOCATED_UPSTREAM_OPERATORS for all operators with co-located upstream operators
            std::vector<OperatorPtr> candidateOperators;
            for (const auto& operatorNode : decomposedQueryPlan->getAllOperators()) {
                if (!operatorNode->hasProperty(CO_LOCATED_UPSTREAM_OPERATORS)) {
                    candidateOperators.emplace_back(operatorNode);
                }
            }

            // 3. Iterate over all candidate operators and add network source sink operators
            for (const auto& candidateOperator : candidateOperators) {

                // 4. Check if candidate operator in the state "Placed" or is of type Network Source or Sink then skip
                // the operation
                if ((candidateOperator->instanceOf<SourceLogicalOperator>()
                     && candidateOperator->as_if<SourceLogicalOperator>()
                            ->getSourceDescriptor()
                            ->instanceOf<Network::NetworkSourceDescriptor>())
                    || (candidateOperator->instanceOf<SinkLogicalOperator>()
                        && candidateOperator->as_if<SinkLogicalOperator>()
                               ->getSinkDescriptor()
                               ->instanceOf<Network::NetworkSinkDescriptor>())) {
                    continue;
                }

                auto originalCopiedOperator = operatorIdToCopiedOperatorMap[candidateOperator->getId()];
                auto downStreamNonSystemOperatorId = candidateOperator->getId();

                // 5. For each candidate operator not in the state "Placed" find the topology node hosting the immediate
                // upstream logical operators.
                for (const auto& upstreamOperator : originalCopiedOperator->getChildren()) {

                    NES_DEBUG("Connected upstream operator {}", upstreamOperator->toString())

                    // 6. Fetch the id of the topology node hosting the upstream operator to connect.
                    const auto& upstreamOperatorToConnect = upstreamOperator->as<LogicalOperator>();
                    auto upstreamWorkerId = std::any_cast<WorkerId>(upstreamOperatorToConnect->getProperty(PINNED_WORKER_ID));
                    auto upstreamNonSystemOperatorId = upstreamOperatorToConnect->getId();

                    // 7. If both operators are co-located we do not need to add network source or sink
                    if (workerId == upstreamWorkerId) {
                        continue;
                    }

                    // 8. Get the upstream topology node.
                    auto upstreamTopologyNode = getTopologyNode(upstreamWorkerId);

                    // 9. Find topology nodes connecting the iterated workerId and pinnedUpstreamWorkerId.
                    std::vector<TopologyNodePtr> topologyNodesBetween =
                        pathFinder->findNodesBetween(upstreamTopologyNode, downstreamTopologyNode);

                    NES_DEBUG("Num of nodes between topology node {} and {} is {}",
                              upstreamTopologyNode->toString(),
                              downstreamTopologyNode->toString(),
                              topologyNodesBetween.size())

                    DecomposedQueryPlanPtr querySubPlanWithUpstreamOperator;

                    // 10. Fetch all query sub plans placed on the upstream node and find the query sub plan hosting the
                    // upstream operator to connect.
                    for (const auto& upstreamQuerySubPlan : computedDecomposedQueryPlans[upstreamWorkerId]) {
                        if (upstreamQuerySubPlan->hasOperatorWithId(upstreamOperatorToConnect->getId())) {
                            querySubPlanWithUpstreamOperator = upstreamQuerySubPlan;
                            break;
                        }
                    }

                    if (!querySubPlanWithUpstreamOperator) {
                        NES_ERROR("Unable to find a query sub plan hosting the upstream operator to connect.");
                        throw Exceptions::RuntimeException(
                            "Unable to find a query sub plan hosting the upstream operator to connect.");
                    }

                    // 11. Get the schema and operator id for the source network operators
                    auto sourceSchema = upstreamOperatorToConnect->as<LogicalOperator>()->getOutputSchema();
                    auto networkSourceOperatorId = getNextOperatorId();

                    // compute a vector of sub plan id and worker node id where the system generated plans are placed
                    std::vector<SysPlanMetadata> connectedSysDecomposedPlanDetails{};
                    OperatorPtr operatorToConnectInMatchedPlan;
                    // 12. Starting from the upstream to downstream topology node and add network sink source pairs.
                    for (uint16_t pathIndex = 0; pathIndex < topologyNodesBetween.size(); pathIndex++) {

                        WorkerId currentWorkerId = topologyNodesBetween[pathIndex]->getId();

                        if (currentWorkerId == upstreamWorkerId) {
                            // 13. Add a network sink operator as new root operator to the upstream operator to connect

                            // 14. create network sink operator to attach.
                            auto networkSinkOperator = createNetworkSinkOperator(sharedQueryId,
                                                                                 networkSourceOperatorId,
                                                                                 topologyNodesBetween[pathIndex + 1]);

                            // 14. Find the upstream operator to connect in the matched query sub plan
                            //set properties on new sink
                            networkSinkOperator->addProperty(UPSTREAM_LOGICAL_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSinkOperator->addProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID, downStreamNonSystemOperatorId);
                            operatorToConnectInMatchedPlan =
                                querySubPlanWithUpstreamOperator->getOperatorWithOperatorId(upstreamOperatorToConnect->getId());
                            operatorToConnectInMatchedPlan->addParent(networkSinkOperator);
                            querySubPlanWithUpstreamOperator->removeAsRootOperator(operatorToConnectInMatchedPlan->getId());
                            querySubPlanWithUpstreamOperator->addRootOperator(networkSinkOperator);

                        } else if (currentWorkerId == workerId) {
                            // 12. Add a network source operator to the leaf operator.

                            // 13. Record information about the query plan and worker id
                            DecomposedQueryId decomposedPlanId = decomposedQueryPlan->getDecomposedQueryId();

                            if (candidateOperator->as<LogicalOperator>()->getOperatorState() == OperatorState::PLACED) {
                                decomposedPlanId =
                                    std::any_cast<DecomposedQueryId>(candidateOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
                            } else if (decomposedPlanId == INVALID_DECOMPOSED_QUERY_PLAN_ID) {
                                // The candidate operator is not in the placed state but it is connected to a placed downstream operator
                                // therefore connected downstream operator prop should be explored
                                for (const auto& placedOperator : decomposedQueryPlan->getAllOperators()) {
                                    if (placedOperator->as_if<LogicalOperator>()->getOperatorState() == OperatorState::PLACED) {
                                        decomposedPlanId = std::any_cast<DecomposedQueryId>(
                                            placedOperator->getProperty(PLACED_DECOMPOSED_PLAN_ID));
                                        break;
                                    }
                                }

                                // Check if found a decomposed query plan
                                if (decomposedPlanId == INVALID_DECOMPOSED_QUERY_PLAN_ID) {
                                    NES_ERROR("No placed operator found to retrieve original decomposed query plan.")
                                    throw Exceptions::RuntimeException(
                                        "No placed operator found to retrieve original decomposed query plan.");
                                }
                            }

                            connectedSysDecomposedPlanDetails.emplace_back(SysPlanMetadata(decomposedPlanId, currentWorkerId));
                            // 14. create network source operator
                            auto networkSourceOperator = createNetworkSourceOperator(sharedQueryId,
                                                                                     sourceSchema,
                                                                                     networkSourceOperatorId,
                                                                                     topologyNodesBetween[pathIndex - 1]);
                            //set properties on new source
                            networkSourceOperator->addProperty(UPSTREAM_LOGICAL_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSourceOperator->addProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID, downStreamNonSystemOperatorId);

                            candidateOperator->addChild(networkSourceOperator);
                            break;
                        } else {
                            // 12. Compute a network source sink plan.

                            // 13. create network source operator
                            auto networkSourceOperator = createNetworkSourceOperator(sharedQueryId,
                                                                                     sourceSchema,
                                                                                     networkSourceOperatorId,
                                                                                     topologyNodesBetween[pathIndex - 1]);

                            // 14. Generate id for next network source
                            networkSourceOperatorId = getNextOperatorId();

                            // 15. create network sink operator to attach.
                            auto networkSinkOperator = createNetworkSinkOperator(sharedQueryId,
                                                                                 networkSourceOperatorId,
                                                                                 topologyNodesBetween[pathIndex + 1]);

                            // 16. add network source as the child
                            networkSinkOperator->addChild(networkSourceOperator);

                            // 17. add upstream and downstream operator id
                            networkSinkOperator->addProperty(UPSTREAM_LOGICAL_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSinkOperator->addProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID, downStreamNonSystemOperatorId);

                            // 18. create the query sub plan
                            auto newDecomposedQueryPlan =
                                DecomposedQueryPlan::create(PlanIdGenerator::getNextDecomposedQueryPlanId(),
                                                            sharedQueryId,
                                                            currentWorkerId,
                                                            {networkSinkOperator},
                                                            faultTolerance);


                            // 19. Record information about the query plan and worker id
                            connectedSysDecomposedPlanDetails.emplace_back(
                                SysPlanMetadata(newDecomposedQueryPlan->getDecomposedQueryId(), currentWorkerId));

                            // 20. add the new query plan
                            if (computedDecomposedQueryPlans.contains(currentWorkerId)) {
                                computedDecomposedQueryPlans[currentWorkerId].emplace_back(newDecomposedQueryPlan);
                            } else {
                                computedDecomposedQueryPlans[currentWorkerId] = {newDecomposedQueryPlan};
                            }
                            networkSourceOperator->addProperty(UPSTREAM_LOGICAL_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSourceOperator->addProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID, downStreamNonSystemOperatorId);
                        }
                    }

                    std::map<OperatorId, std::vector<SysPlanMetadata>> downStreamOperatorToConnectedSysPlansMetaDataMap;
                    // 15. Add metadata about the plans and topology nodes hosting the system generated operators.
                    if (operatorToConnectInMatchedPlan->hasProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS)) {
                        downStreamOperatorToConnectedSysPlansMetaDataMap =
                            std::any_cast<std::map<OperatorId, std::vector<SysPlanMetadata>>>(
                                operatorToConnectInMatchedPlan->getProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS));
                    }
                    downStreamOperatorToConnectedSysPlansMetaDataMap[downStreamNonSystemOperatorId] =
                        connectedSysDecomposedPlanDetails;
                    operatorToConnectInMatchedPlan->addProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS,
                                                                downStreamOperatorToConnectedSysPlansMetaDataMap);
                }
            }
        }
    }
}

PlacementAdditionResult
BasePlacementAdditionStrategy::updateExecutionNodes(SharedQueryId sharedQueryId,
                                                    ComputedDecomposedQueryPlans& computedSubQueryPlans,
                                                    DecomposedQueryPlanVersion decomposedQueryPlanVersion) {

    std::unordered_map<DecomposedQueryId, DeploymentContextPtr> deploymentContexts;
    for (const auto& path : pathsFound) {
        for (const auto& workerNodeId : path.workerNodeIdsInBFS) {

            // Used for computing the deployment context
            auto copiedTopologyNode = path.workerIdToTopologyNodeMap.find(workerNodeId);
            const std::string& grpcAddress = copiedTopologyNode->second->getGrpcAddress();

            try {
                // 1. If using optimistic strategy then, directly use the topology node with the workerId and perform the "validation" before continuing.
                // 1.2. Perform validation by checking if we can occupy the resources the operator placement algorithm reserved
                // for placing the operators.
                auto consumedResources = workerIdToResourceConsumedMap[workerNodeId];
                if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
                    if (!topology->occupySlots(workerNodeId, consumedResources)) {
                        NES_ERROR("Unable to occupy resources on the topology node {} to successfully place operators.",
                                  workerNodeId);
                        return PlacementAdditionResult(false, deploymentContexts);
                    }
                    //Create execution node if doe not exists
                    TopologyNodeWLock lockedTopologyNode;
                    while (!(lockedTopologyNode = topology->lockTopologyNode(workerNodeId))) {
                    };
                    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
                } else {
                    auto lockedTopologyNode = lockedTopologyNodeMap[workerNodeId];
                    if (!lockedTopologyNode->operator*()->occupySlots(consumedResources)) {
                        NES_ERROR("Unable to occupy resources on the topology node {} to successfully place operators.",
                                  workerNodeId);
                        return PlacementAdditionResult(false, deploymentContexts);
                    }
                    //Create execution node if doe not exists
                    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
                }

                // 1.3. Check if the worker node contains pinned upstream operators
                bool containPinnedUpstreamOperator = false;
                bool containPinnedDownstreamOperator = false;
                if (pinnedUpStreamTopologyNodeIds.contains(workerNodeId)) {
                    containPinnedUpstreamOperator = true;
                } else if (pinnedDownStreamTopologyNodeIds.contains(workerNodeId)) {
                    containPinnedDownstreamOperator = true;
                }

                // 1.5. Iterate over the placed query sub plans and update execution node
                auto computedDecomposedQueryPlans = computedSubQueryPlans[workerNodeId];
                for (const auto& computedDecomposedQueryPlan : computedDecomposedQueryPlans) {

                    // 1.5.1. Perform the type inference phase on the updated query sub plan and update execution node.
                    if (computedDecomposedQueryPlan->getDecomposedQueryId() == INVALID_DECOMPOSED_QUERY_PLAN_ID) {
                        if (containPinnedUpstreamOperator) {

                            // Record all placed query decomposed query plans that host pinned leaf operators
                            auto decomposedQueryPlanComparator = [](const DecomposedQueryPlanPtr& first,
                                                                    const DecomposedQueryPlanPtr& second) {
                                return first->getDecomposedQueryId() > second->getDecomposedQueryId();
                            };
                            std::set<DecomposedQueryPlanPtr, decltype(decomposedQueryPlanComparator)> hostDecomposedQueryPlans;

                            auto placedDecomposedQueryPlans =
                                globalExecutionPlan->getCopyOfAllDecomposedQueryPlans(workerNodeId, sharedQueryId);

                            // Iterate over all pinned leaf operators and find a host placed query sub plan that hosts
                            auto computedOperators = computedDecomposedQueryPlan->getAllOperators();
                            for (const auto& computedOperator : computedOperators) {

                                // If the pinned leaf operator is not of type logical source and was already placed.
                                // Then find and merge the operator with query sub plan containing the placed leaf operator
                                if (computedOperator->as<LogicalOperator>()->getOperatorState() == OperatorState::PLACED) {

                                    // Flag indicating, if we found a placed decomposed query plan that contains pinned operator
                                    bool found = false;

                                    // Find and merge with the placed decomposed query plan
                                    for (const auto& placedDecomposedQueryPlan : placedDecomposedQueryPlans) {

                                        // Check if the placed decomposed query plan contains the pinned upstream operator
                                        auto matchingPlacedLeafOperator =
                                            placedDecomposedQueryPlan->getOperatorWithOperatorId(computedOperator->getId());
                                        if (matchingPlacedLeafOperator) {
                                            if (computedOperator->hasProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS)) {
                                                auto connectedSysSubPlanDetails =
                                                    computedOperator->getProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS);
                                                matchingPlacedLeafOperator->addProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS,
                                                                                        connectedSysSubPlanDetails);
                                            } else {
                                                NES_WARNING("connected sys sub plan details not found");
                                            }
                                            // Add all newly computed pinned downstream operators to the matching placed leaf operator
                                            auto pinnedDownstreamOperators = computedOperator->getParents();
                                            for (const auto& pinnedDownstreamOperator : pinnedDownstreamOperators) {
                                                bool foundOperatorWithEqualId = false;
                                                //if the operator matches a placed operator that was not system generated,
                                                //do not replace anything and leave the placed operator where it is
                                                for (const auto& placedParent : matchingPlacedLeafOperator->getParents()) {
                                                    if (placedParent->as<Operator>()->getId()
                                                        == pinnedDownstreamOperator->as<Operator>()->getId()) {
                                                        NES_DEBUG("Not modifying operator because it is a non system operator");
                                                        foundOperatorWithEqualId = true;
                                                        }
                                                }
                                                if (foundOperatorWithEqualId) {
                                                    continue;
                                                }

                                                bool mergedOperator = false;
                                                if (pinnedDownstreamOperator->instanceOf<SinkLogicalOperator>()) {
                                                    mergedOperator =
                                                        tryMergingNetworkSink(decomposedQueryPlanVersion,
                                                                              computedDecomposedQueryPlan,
                                                                              matchingPlacedLeafOperator,
                                                                              pinnedDownstreamOperator->as<SinkLogicalOperator>());
                                                }
                                                if (!mergedOperator) {
                                                    pinnedDownstreamOperator->removeChild(computedOperator);
                                                    pinnedDownstreamOperator->addChild(matchingPlacedLeafOperator);
                                                }
                                            }
                                            hostDecomposedQueryPlans.emplace(placedDecomposedQueryPlan);
                                            found = true;
                                            break;
                                        }
                                    }

                                    // If no query sub plan found that hosts the placed leaf operator
                                    if (!found) {
                                        NES_ERROR("Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                        throw Exceptions::RuntimeException(
                                            "Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                    }
                                }
                            }

                            DecomposedQueryPlanPtr updatedDecomposedQueryPlan;
                            // Merge the two host query sub plans
                            if (!hostDecomposedQueryPlans.empty()) {
                                for (const auto& hostQuerySubPlan : hostDecomposedQueryPlans) {
                                    if (!updatedDecomposedQueryPlan) {
                                        updatedDecomposedQueryPlan = hostQuerySubPlan;
                                    } else {
                                        for (const auto& hostRootOperators : hostQuerySubPlan->getRootOperators()) {
                                            updatedDecomposedQueryPlan->addRootOperator(hostRootOperators);
                                        }
                                    }
                                }
                            }

                            //In the end, add root operators of computed plan as well.
                            for (const auto& rootOperator : computedDecomposedQueryPlan->getRootOperators()) {
                                updatedDecomposedQueryPlan->addRootOperator(rootOperator);
                            }

                            //As we are updating an existing query sub plan we mark the plan for re-deployment
                            updatedDecomposedQueryPlan->setState(QueryState::MARKED_FOR_REDEPLOYMENT);
                            updatedDecomposedQueryPlan = typeInferencePhase->execute(updatedDecomposedQueryPlan, faultTolerance);
                            updatedDecomposedQueryPlan->setVersion(decomposedQueryPlanVersion);
                            //Add decomposed query plan to the global execution plan
                            globalExecutionPlan->addDecomposedQueryPlan(workerNodeId, updatedDecomposedQueryPlan);
                            // 1.6. Update state and properties of all operators placed on the execution node
                            markOperatorsAsPlaced(workerNodeId, updatedDecomposedQueryPlan);
                            // 1.7. Compute deployment context
                            deploymentContexts[updatedDecomposedQueryPlan->getDecomposedQueryId()] =
                                DeploymentContext::create(grpcAddress, updatedDecomposedQueryPlan->copy());
                        } else if (containPinnedDownstreamOperator) {

                            // Record all placed query decomposed query plans that host pinned leaf operators
                            auto decomposedQueryPlanComparator = [](const DecomposedQueryPlanPtr& first,
                                                                    const DecomposedQueryPlanPtr& second) {
                                return first->getDecomposedQueryId() < second->getDecomposedQueryId();
                            };
                            std::set<DecomposedQueryPlanPtr, decltype(decomposedQueryPlanComparator)> hostDecomposedQueryPlans;

                            auto placedDecomposedQueryPlans =
                                globalExecutionPlan->getCopyOfAllDecomposedQueryPlans(workerNodeId, sharedQueryId);

                            auto computedOperators = computedDecomposedQueryPlan->getAllOperators();
                            for (const auto& computedOperator : computedOperators) {
                                // If the pinned leaf operator is not of type logical source and was already placed.
                                // Then find and merge the operator with query sub plan containing the placed leaf operator
                                if (computedOperator->as<LogicalOperator>()->getOperatorState() == OperatorState::PLACED) {
                                    // Flag indicating, if we found a placed decomposed query plan that contains pinned operator
                                    bool found = false;

                                    // Find and merge with the shared query plan
                                    for (const auto& placedDecomposedQueryPlan : placedDecomposedQueryPlans) {
                                        // If the placed query sub plan contains the pinned upstream operator
                                        auto matchingPinnedRootOperator =
                                            placedDecomposedQueryPlan->getOperatorWithOperatorId(computedOperator->getId());
                                        if (matchingPinnedRootOperator) {
                                            // Add all newly computed downstream operators to matching placed leaf operator
                                            for (const auto& upstreamOperator : computedOperator->getChildren()) {
                                                bool foundOperatorWithEqualId = false;
                                                //if the operator matches a placed operator that was not system generated,
                                                //do not replace anything and leave the placed operator where it is
                                                for (const auto& placedChild : matchingPinnedRootOperator->getChildren()) {
                                                    if (placedChild->as<Operator>()->getId()
                                                        == upstreamOperator->as<Operator>()->getId()) {
                                                        NES_DEBUG("Not modifying operator because it is a non system operator");
                                                        foundOperatorWithEqualId = true;
                                                        }
                                                }
                                                if (foundOperatorWithEqualId) {
                                                    continue;
                                                }

                                                matchingPinnedRootOperator->addChild(upstreamOperator);
                                            }

                                            /*reassign operator ids to avoid worker side operator id collisions between the two
                                             * versions of the plan*/
                                            for (const auto& sinkOperator : placedDecomposedQueryPlan->getSinkOperators()) {
                                                auto sinkDescriptor = sinkOperator->getSinkDescriptor();
                                                if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
                                                    auto newNetworkSinkDescriptor =
                                                        sinkDescriptor->as<Network::NetworkSinkDescriptor>();
                                                    auto newId = getNextOperatorId();
                                                    auto mergedNetworkSinkDescriptor = Network::NetworkSinkDescriptor::create(
                                                        newNetworkSinkDescriptor->getNodeLocation(),
                                                        newNetworkSinkDescriptor->getNesPartition(),
                                                        SINK_RETRY_WAIT,
                                                        SINK_RETRIES,
                                                        decomposedQueryPlanVersion,
                                                        newNetworkSinkDescriptor->getNumberOfOrigins(),
                                                        newId);
                                                    sinkOperator->setSinkDescriptor(mergedNetworkSinkDescriptor);
                                                    sinkOperator->setId(getNextOperatorId());
                                                }
                                            }

                                            hostDecomposedQueryPlans.emplace(placedDecomposedQueryPlan);
                                            found = true;
                                            break;
                                        }
                                    }

                                    // If no query sub plan found that hosts the placed leaf operator
                                    if (!found) {
                                        NES_ERROR("Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                        throw Exceptions::RuntimeException(
                                            "Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                    }
                                }
                            }

                            DecomposedQueryPlanPtr updatedDecomposedQueryPlan;
                            // Merge the two host query sub plans
                            if (!hostDecomposedQueryPlans.empty()) {
                                for (const auto& hostQuerySubPlan : hostDecomposedQueryPlans) {
                                    if (!updatedDecomposedQueryPlan) {
                                        updatedDecomposedQueryPlan = hostQuerySubPlan;
                                    } else {
                                        for (const auto& hostRootOperators : hostQuerySubPlan->getRootOperators()) {
                                            updatedDecomposedQueryPlan->addRootOperator(hostRootOperators);
                                        }
                                    }
                                }
                            }

                            //As we are updating an existing query sub plan we mark the plan for re-deployment
                            //todo #5158: check if the plan was actually changed and omit redeploymentif not
                            updatedDecomposedQueryPlan->setState(QueryState::MARKED_FOR_DEPLOYMENT);
                            updatedDecomposedQueryPlan = typeInferencePhase->execute(updatedDecomposedQueryPlan);
                            updatedDecomposedQueryPlan->setVersion(decomposedQueryPlanVersion);
                            updatedDecomposedQueryPlan->setDecomposedQueryPlanId(PlanIdGenerator::getNextDecomposedQueryPlanId());
                            globalExecutionPlan->addDecomposedQueryPlan(workerNodeId, updatedDecomposedQueryPlan);
                            // 1.6. Update state and properties of all operators placed on the execution node
                            markOperatorsAsPlaced(workerNodeId, updatedDecomposedQueryPlan);
                            // 1.7. Compute deployment context
                            deploymentContexts[updatedDecomposedQueryPlan->getDecomposedQueryId()] =
                                DeploymentContext::create(grpcAddress, updatedDecomposedQueryPlan->copy());
                        } else {
                            NES_ERROR("A decomposed query plan {} with invalid decomposed query plan id that has no pinned upstream "
                                      "or downstream operator.",
                                      computedDecomposedQueryPlan->toString());
                            throw Exceptions::RuntimeException("A decomposed plan with invalid decomposed query plan id that has no "
                                                               "pinned upstream or downstream operator.");
                        }
                    } else {
                        auto updatedDecomposedQueryPlan = typeInferencePhase->execute(computedDecomposedQueryPlan, faultTolerance);
                        updatedDecomposedQueryPlan->setState(QueryState::MARKED_FOR_DEPLOYMENT);
                        updatedDecomposedQueryPlan->setVersion(decomposedQueryPlanVersion);
                        globalExecutionPlan->addDecomposedQueryPlan(workerNodeId, updatedDecomposedQueryPlan);
                        // 1.6. Update state and properties of all operators placed on the execution node
                        markOperatorsAsPlaced(workerNodeId, updatedDecomposedQueryPlan);
                        // 1.7. Compute deployment context
                        deploymentContexts[updatedDecomposedQueryPlan->getDecomposedQueryId()] =
                            DeploymentContext::create(grpcAddress, updatedDecomposedQueryPlan->copy());
                    }
                }
            } catch (std::exception& ex) {
                NES_ERROR("Exception occurred during pinned operator placement {}.", ex.what());
                throw ex;
            }
        }
    }
    return PlacementAdditionResult(true, deploymentContexts);
}

void BasePlacementAdditionStrategy::markOperatorsAsPlaced(WorkerId workerId, DecomposedQueryPlanPtr decomposedQueryPlan) {

    DecomposedQueryId decomposedQueryId = decomposedQueryPlan->getDecomposedQueryId();
    auto allPlacedOperators = decomposedQueryPlan->getAllOperators();
    for (const auto& placedOperator : allPlacedOperators) {
        OperatorId operatorId = placedOperator->getId();
        //Set status to Placed and copy over metadata properties for all operators considered in this placement
        if (operatorIdToOriginalOperatorMap.contains(operatorId)) {
            auto originalOperator = operatorIdToOriginalOperatorMap[operatorId];
            originalOperator->setOperatorState(OperatorState::PLACED);
            originalOperator->addProperty(PINNED_WORKER_ID, workerId);
            originalOperator->addProperty(PLACED_DECOMPOSED_PLAN_ID, decomposedQueryId);
            if (placedOperator->hasProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS)) {
                originalOperator->addProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS,
                                              placedOperator->getProperty(CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS));
            }
        }
    }
}

bool BasePlacementAdditionStrategy::tryMergingNetworkSource(DecomposedQueryPlanVersion querySubPlanVersion,
                                                            const NodePtr& placedDownstreamOperator,
                                                            const SourceLogicalOperatorPtr& newNetworkSourceOperator) {

    //check if the new source is a network source
    auto newSourceDescriptor = newNetworkSourceOperator->getSourceDescriptor();
    if (!newSourceDescriptor) {
        return false;
    }
    auto newNetworkSourceDescriptor = newSourceDescriptor->as<Network::NetworkSourceDescriptor>();

    NES_ASSERT(newNetworkSourceOperator->hasProperty(UPSTREAM_LOGICAL_OPERATOR_ID),
               "Network source does not have the UPSTREAM_LOGICAL_OPERATOR_ID property set");

    bool replacedOperator = false;
    for (const auto& existingChild : placedDownstreamOperator->getChildren()) {

        //check if the placed operator is a network source
        if (!existingChild->instanceOf<SourceLogicalOperator>()) {
            continue;
        }
        auto existingSource = existingChild->as<SourceLogicalOperator>();
        auto existingSourceDescriptor = existingSource->getSourceDescriptor();
        if (!existingSourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
            continue;
        }
        auto existingNetworkSourceDescriptor = existingSourceDescriptor->as<Network::NetworkSourceDescriptor>();

        NES_ASSERT(existingSource->hasProperty(UPSTREAM_LOGICAL_OPERATOR_ID),
                   "Network source does not have the UPSTREAM_LOGICAL_OPERATOR_ID property set");

        if (std::any_cast<OperatorId>(existingSource->getProperty(UPSTREAM_LOGICAL_OPERATOR_ID))
            == std::any_cast<OperatorId>(newNetworkSourceOperator->getProperty(UPSTREAM_LOGICAL_OPERATOR_ID))) {
            auto mergedNetworkSourceDescriptor =
                Network::NetworkSourceDescriptor::create(newNetworkSourceDescriptor->getSchema(),
                                                         newNetworkSourceDescriptor->getNesPartition(),
                                                         newNetworkSourceDescriptor->getNodeLocation(),
                                                         SOURCE_RETRY_WAIT,
                                                         SOURCE_RETRIES,
                                                         querySubPlanVersion,
                                                         existingNetworkSourceDescriptor->getUniqueId());
            existingSource->setSourceDescriptor(mergedNetworkSourceDescriptor);
            auto computedParent = newNetworkSourceOperator->getParents().front();
            replacedOperator = true;
            break;
        }
    }
    return replacedOperator;
}

bool BasePlacementAdditionStrategy::tryMergingNetworkSink(DecomposedQueryPlanVersion querySubPlanVersion,
                                                          const DecomposedQueryPlanPtr& computedQuerySubPlan,
                                                          const NodePtr& upstreamOperatorOfPlacedSinksToCheck,
                                                          const SinkLogicalOperatorPtr& newNetworkSinkOperator) {
    //check if the new sink is a network sink
    auto newSinkDescriptor = newNetworkSinkOperator->getSinkDescriptor();
    if (!newSinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        return false;
    }
    auto newNetworkSinkDescriptor = newSinkDescriptor->as<Network::NetworkSinkDescriptor>();

    NES_ASSERT(newNetworkSinkOperator->hasProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID),
               "Network sink does not have the DOWNSTREAM_LOGICAL_OPERATOR_ID property set");

    bool replacedOperator = false;
    for (const auto& existingParent : upstreamOperatorOfPlacedSinksToCheck->getParents()) {

        //check if the placed operator is a network sink
        if (!existingParent->instanceOf<SinkLogicalOperator>()) {
            continue;
        }
        auto existingSink = existingParent->as<SinkLogicalOperator>();
        auto existingSinkDescriptor = existingSink->getSinkDescriptor();
        if (!existingSinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
            continue;
        }
        auto existingNetworkSinkDescriptor = existingSinkDescriptor->as<Network::NetworkSinkDescriptor>();

        NES_ASSERT(existingSink->hasProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID),
                   "Network sink does not have the DOWNSTREAM_LOGICAL_OPERATOR_ID property set");

        //check if the new sink corresponds to a pleced sink that needs to be reconfigured
        if (std::any_cast<OperatorId>(existingSink->getProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID))
            == std::any_cast<OperatorId>(newNetworkSinkOperator->getProperty(DOWNSTREAM_LOGICAL_OPERATOR_ID))) {
            auto mergedNetworkSinkDescriptor =
                Network::NetworkSinkDescriptor::create(newNetworkSinkDescriptor->getNodeLocation(),
                                                       newNetworkSinkDescriptor->getNesPartition(),
                                                       SINK_RETRY_WAIT,
                                                       SINK_RETRIES,
                                                       querySubPlanVersion,
                                                       newNetworkSinkDescriptor->getNumberOfOrigins(),
                                                       existingNetworkSinkDescriptor->getUniqueId());
            existingSink->setSinkDescriptor(mergedNetworkSinkDescriptor);
            computedQuerySubPlan->removeAsRootOperator(newNetworkSinkOperator->getId());
            replacedOperator = true;
            break;
        }
    }
    return replacedOperator;
}

TopologyNodePtr BasePlacementAdditionStrategy::getTopologyNode(WorkerId workerId) {

    NES_TRACE("Get the topology node {}", workerId);
    if (!lockedTopologyNodeMap.contains(workerId)) {
        NES_ERROR("Topology node with id {} not considered during the path selection phase.", workerId);
        throw Exceptions::RuntimeException("Topology node with id " + workerId.toString()
                                           + " not considered during the path selection phase.");
    }

    return lockedTopologyNodeMap.find(workerId)->second->operator*();
}

LogicalOperatorPtr BasePlacementAdditionStrategy::createNetworkSinkOperator(SharedQueryId sharedQueryId,
                                                                            OperatorId sourceOperatorId,
                                                                            const TopologyNodePtr& sourceTopologyNode) {

    NES_TRACE("create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(sharedQueryId,
                                       sourceOperatorId,
                                       Network::DEFAULT_PARTITION_ID,
                                       Network::DEFAULT_SUBPARTITION_ID);
    DecomposedQueryPlanVersion sinkVersion = 0;
    OperatorId id = getNextOperatorId();
    auto numberOfOrigins = 0;
    return LogicalOperatorFactory::createSinkOperator(Network::NetworkSinkDescriptor::create(nodeLocation,
                                                                                             nesPartition,
                                                                                             SINK_RETRY_WAIT,
                                                                                             SINK_RETRIES,
                                                                                             sinkVersion,
                                                                                             numberOfOrigins,
                                                                                             id),
                                                      INVALID_WORKER_NODE_ID,
                                                      id);
}

LogicalOperatorPtr BasePlacementAdditionStrategy::createNetworkSourceOperator(SharedQueryId sharedQueryId,
                                                                              SchemaPtr inputSchema,
                                                                              OperatorId operatorId,
                                                                              const TopologyNodePtr& sinkTopologyNode) {
    NES_TRACE("create Network Source operator");
    NES_ASSERT2_FMT(sinkTopologyNode, "Invalid sink node while placing query " << sharedQueryId);
    Network::NodeLocation upstreamNodeLocation(sinkTopologyNode->getId(),
                                               sinkTopologyNode->getIpAddress(),
                                               sinkTopologyNode->getDataPort());
    const Network::NesPartition nesPartition =
        Network::NesPartition(sharedQueryId, operatorId, Network::DEFAULT_PARTITION_ID, Network::DEFAULT_SUBPARTITION_ID);
    const SourceDescriptorPtr& networkSourceDescriptor = Network::NetworkSourceDescriptor::create(std::move(inputSchema),
                                                                                                  nesPartition,
                                                                                                  upstreamNodeLocation,
                                                                                                  SOURCE_RETRY_WAIT,
                                                                                                  SOURCE_RETRIES,
                                                                                                  0,
                                                                                                  operatorId);
    return LogicalOperatorFactory::createSourceOperator(networkSourceDescriptor, operatorId);
}

std::vector<TopologyNodePtr>
BasePlacementAdditionStrategy::getTopologyNodesForChildrenOperators(const LogicalOperatorPtr& operatorNode) {
    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        if (!child->as_if<LogicalOperator>()->hasProperty(PINNED_WORKER_ID)) {
            NES_WARNING("unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode =
            lockedTopologyNodeMap[std::any_cast<WorkerId>(child->as_if<LogicalOperator>()->getProperty(PINNED_WORKER_ID))]->
            operator*();

        auto existingNode =
            std::find_if(childTopologyNodes.begin(), childTopologyNodes.end(), [&childTopologyNode](const auto& node) {
                return node->getId() == childTopologyNode->getId();
            });
        if (existingNode == childTopologyNodes.end()) {
            childTopologyNodes.emplace_back(childTopologyNode);
        }
    }
    NES_DEBUG("returning list of topology nodes where children operators are placed");
    return childTopologyNodes;
}

BasePlacementAdditionStrategy::~BasePlacementAdditionStrategy() {
    NES_INFO("~BasePlacementStrategy()");
    //Release the lock for pessimistic placement mode
    if (placementAmendmentMode == PlacementAmendmentMode::PESSIMISTIC) {
        for (const auto& path : pathsFound) {
            for (const auto& path : pathsFound)
                unlockTopologyNodesInSelectedPath(path);
        }
    }
}
}// namespace NES::Optimizer
