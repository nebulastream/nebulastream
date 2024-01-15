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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SysPlanMetaData.hpp>
#include <algorithm>
#include <thread>
#include <unordered_set>
#include <utility>

namespace NES::Optimizer {

BasePlacementAdditionStrategy::BasePlacementAdditionStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                             const TopologyPtr& topology,
                                                             const TypeInferencePhasePtr& typeInferencePhase,
                                                             PlacementAmendmentMode placementAmendmentMode)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      placementAmendmentMode(placementAmendmentMode) {
    //Initialize path finder
    pathFinder = std::make_shared<PathFinder>((topology->getRootTopologyNodeId()));
}

bool BasePlacementAdditionStrategy::updateGlobalExecutionPlan(QueryPlanPtr /*queryPlan*/) { NES_NOT_IMPLEMENTED(); }

void BasePlacementAdditionStrategy::performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
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

    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {

        // 1.1. Performs path selection
        std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
            findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);

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

bool BasePlacementAdditionStrategy::pessimisticPathSelection(
    const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
    const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators) {
    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {

        //1.1 Performs path selection
        std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
            findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);

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

bool BasePlacementAdditionStrategy::lockTopologyNodesInSelectedPath(const std::vector<TopologyNodePtr>& sourceTopologyNodes) {
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

bool BasePlacementAdditionStrategy::unlockTopologyNodesInSelectedPath() {
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

std::vector<TopologyNodePtr>
BasePlacementAdditionStrategy::findPath(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                        const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators) {

    //1 Create the pinned upstream and downstream topology node vector
    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());

    //2 Do the path selection
    // NOTE: this call can be replaced with any arbitrary path selection algorithm
    return topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
}

ComputedSubQueryPlans
BasePlacementAdditionStrategy::computeQuerySubPlans(SharedQueryId sharedQueryId,
                                                    const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                    const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

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
    ComputedSubQueryPlans computedSubQueryPlans;

    //1. Prepare for iterating over operators in strict BFS order
    // Initialize with the pinned upstream operators
    std::queue<LogicalOperatorNodePtr> pinnedOperators;
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
        auto copyOfPinnedOperator = pinnedOperator->copy()->as<LogicalOperatorNode>();// Make a copy of the operator
        auto pinnedWorkerId = std::any_cast<uint64_t>(pinnedOperator->getProperty(PINNED_WORKER_ID));
        if (pinnedOperator->getOperatorState() == OperatorState::TO_BE_PLACED
            || pinnedOperator->getOperatorState() == OperatorState::TO_BE_REPLACED) {
            workerIdToResourceConsumedMap[pinnedWorkerId]++;
        }

        // 2.2. If the pinnedWorkerId has already placed query sub plans then compute an updated list of query sub plans
        if (computedSubQueryPlans.contains(pinnedWorkerId)) {
            // 2.2.1. Prepare an updated list of query sub plans and a new placed query plan for the operator under
            // considersation
            std::vector<QueryPlanPtr> updatedQuerySubPlans;
            QueryPlanPtr newPlacedQuerySubPlan;

            // 2.2.2. Iterate over all already placed query sub plans to check if the iterated operator
            // contains the root operator of already placed query sub plan as its upstream operator
            auto alreadyPlacedQuerySubPlans = computedSubQueryPlans[pinnedWorkerId];
            for (const auto& placedQuerySubPlan : alreadyPlacedQuerySubPlans) {

                //2.2.2.1. For all upstream operators of the pinned operator check if they are in the placed query sub plan
                bool foundConnectedUpstreamPlacedOperator = false;
                for (const auto& childOperator : pinnedOperator->getChildren()) {
                    OperatorId childOperatorId = childOperator->as<OperatorNode>()->getId();
                    if (placedQuerySubPlan->hasOperatorWithId(childOperatorId)) {
                        auto placedOperator = placedQuerySubPlan->getOperatorWithId(childOperatorId);
                        //Remove the placed operator as the root
                        placedQuerySubPlan->removeAsRootOperator(placedOperator);
                        // Add the copy as the parent of the iterated root operator and update the root of the iterated
                        placedOperator->addParent(copyOfPinnedOperator);
                        foundConnectedUpstreamPlacedOperator = true;
                    }
                }

                // 2.2.2.2. For all downstream operators of the pinned operator check if they are in the placed query sub plan
                bool foundConnectedDownstreamPlacedOperator = false;
                for (const auto& parentOperator : pinnedOperator->getParents()) {
                    OperatorId parentOperatorId = parentOperator->as<OperatorNode>()->getId();
                    if (placedQuerySubPlan->hasOperatorWithId(parentOperatorId)) {
                        auto placedOperator = placedQuerySubPlan->getOperatorWithId(parentOperatorId);
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
                    if (!newPlacedQuerySubPlan) {
                        newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId,
                                                                  placedQuerySubPlan->getQuerySubPlanId(),
                                                                  placedQuerySubPlan->getRootOperators());
                        newPlacedQuerySubPlan->addRootOperator(copyOfPinnedOperator);
                    } else {
                        const auto& rootOperators = placedQuerySubPlan->getRootOperators();
                        std::for_each(rootOperators.begin(), rootOperators.end(), [&](const auto& rootOperator) {
                            newPlacedQuerySubPlan->addRootOperator(rootOperator);
                        });
                    }
                } else if (foundConnectedDownstreamPlacedOperator) {// If a downstream operator found in the placed query sub
                                                                    // plan then create a new or merge the placed
                                                                    // query plan \with the previous new query sub plan.
                    if (!newPlacedQuerySubPlan) {
                        newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId,
                                                                  placedQuerySubPlan->getQuerySubPlanId(),
                                                                  placedQuerySubPlan->getRootOperators());
                    } else {
                        const auto& rootOperators = placedQuerySubPlan->getRootOperators();
                        //NOTE: Remove the copy Of pinnedOperator as the root operator
                        newPlacedQuerySubPlan->removeAsRootOperator(copyOfPinnedOperator);
                        std::for_each(rootOperators.begin(), rootOperators.end(), [&](const auto& rootOperator) {
                            newPlacedQuerySubPlan->addRootOperator(rootOperator);
                        });
                    }
                } else {// Else add the disjoint query sub plan to the updated query plan list
                    updatedQuerySubPlans.emplace_back(placedQuerySubPlan);
                }
            }

            //2.2.3. Create a new query sub plan for the operator under consideration if all plans are disjoint.
            if (!newPlacedQuerySubPlan) {
                if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                    // Create a temporary query sub plans for the operator as it might need to be merged with another query
                    // sub plan that is already placed on the execution node. Thus we assign it an invalid sub query plan id.
                    newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId, INVALID_QUERY_SUB_PLAN_ID, {copyOfPinnedOperator});
                } else {
                    newPlacedQuerySubPlan =
                        QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {copyOfPinnedOperator});
                }
            } else if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                newPlacedQuerySubPlan->setQuerySubPlanId(INVALID_QUERY_SUB_PLAN_ID);// set invalid query sub plan id
            }

            //2.2.4. Add the new query sub plan to the list
            updatedQuerySubPlans.emplace_back(newPlacedQuerySubPlan);
            computedSubQueryPlans[pinnedWorkerId] = updatedQuerySubPlans;
        } else {// 2.2.1. If no query sub plan placed on the pinned worker node
            // Create a new placed query sub plan
            QueryPlanPtr newPlacedQuerySubPlan;
            if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
                // Create a temporary query sub plans for the operator as it might need to be merged with another query
                // sub plan that is already placed on the execution node. Thus we assign it an invalid sub query plan id.
                newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId, INVALID_QUERY_SUB_PLAN_ID, {copyOfPinnedOperator});
            } else {
                newPlacedQuerySubPlan =
                    QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {copyOfPinnedOperator});
            }

            //2.2.2. Add the new query sub plan to the list
            computedSubQueryPlans[pinnedWorkerId] = {newPlacedQuerySubPlan};
        }

        // Check if all upstream operators of the considered pinnedOperator are co-located on same node
        // We will use this information to compute network source and sink operators
        if (copyOfPinnedOperator->getChildren().size() == pinnedOperator->getChildren().size()) {
            copyOfPinnedOperator->addProperty(CO_LOCATED_UPSTREAM_OPERATORS, true);
        }

        //2.3. If the operator is in the state placed then check if it is one of the pinned downstream operator
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
            const auto& downStreamOperator = operatorNode->as<LogicalOperatorNode>();
            if (operatorIdToOriginalOperatorMap.contains(downStreamOperator->getId())) {
                pinnedOperators.push(downStreamOperator);
            }
        });
    }
    return computedSubQueryPlans;
}

void BasePlacementAdditionStrategy::addNetworkOperators(ComputedSubQueryPlans& computedSubQueryPlans) {

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

    // Iterate over all computed sub query plans and add network source and sink operators.
    for (const auto& [workerId, querySubPlans] : computedSubQueryPlans) {
        auto downstreamTopologyNode = getTopologyNode(workerId);

        // 1. Iterate over all query sub plans
        for (const auto& querySubPlan : querySubPlans) {

            // shared query id
            auto sharedQueryId = querySubPlan->getQueryId();

            // 2. Fetch all logical operators whose upstream logical operators are missing.
            // In the previous call, we stored this information by setting the property
            // CO_LOCATED_UPSTREAM_OPERATORS for all operators with co-located upstream operators
            std::vector<OperatorNodePtr> candidateOperators;
            for (const auto& operatorNode : querySubPlan->getAllOperators()) {
                if (!operatorNode->hasProperty(CO_LOCATED_UPSTREAM_OPERATORS)) {
                    candidateOperators.emplace_back(operatorNode);
                }
            }

            // 3. Iterate over all candidate operators and add network source sink operators
            for (const auto& candidateOperator : candidateOperators) {

                // 4. Check if candidate operator in the state "Placed" or is of type Network Source or Sink then skip
                // the operation
                if ((candidateOperator->instanceOf<SourceLogicalOperatorNode>()
                     && candidateOperator->as_if<SourceLogicalOperatorNode>()
                            ->getSourceDescriptor()
                            ->instanceOf<Network::NetworkSourceDescriptor>())
                    || (candidateOperator->instanceOf<SinkLogicalOperatorNode>()
                        && candidateOperator->as_if<SinkLogicalOperatorNode>()
                               ->getSinkDescriptor()
                               ->instanceOf<Network::NetworkSinkDescriptor>())) {
                    continue;
                }

                auto originalCopiedOperator = operatorIdToCopiedOperatorMap[candidateOperator->getId()];
                auto downStreamNonSystemOperatorId = candidateOperator->getId();

                // 5. For each candidate operator not in the state "Placed" find the topology node hosting the immediate
                // upstream logical operators.
                for (const auto& upstreamOperator : originalCopiedOperator->getChildren()) {

                    // 6. Fetch the id of the topology node hosting the upstream operator to connect.
                    const auto& upstreamOperatorToConnect = upstreamOperator->as<LogicalOperatorNode>();
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

                    QueryPlanPtr querySubPlanWithUpstreamOperator;

                    // 10. Fetch all query sub plans placed on the upstream node and find the query sub plan hosting the
                    // upstream operator to connect.
                    for (const auto& upstreamQuerySubPlan : computedSubQueryPlans[upstreamWorkerId]) {
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
                    auto sourceSchema = upstreamOperatorToConnect->as<LogicalOperatorNode>()->getOutputSchema();
                    auto networkSourceOperatorId = getNextOperatorId();

                    // compute a vector of sub plan id and worker node id where the system generated plans are placed
                    std::vector<SysPlanMetaData> connectedSysSubPlanDetails;
                    OperatorNodePtr operatorToConnectInMatchedPlan;
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
                            networkSinkOperator->addProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSinkOperator->addProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID, downStreamNonSystemOperatorId);
                            operatorToConnectInMatchedPlan =
                                querySubPlanWithUpstreamOperator->getOperatorWithId(upstreamOperatorToConnect->getId());
                            operatorToConnectInMatchedPlan->addParent(networkSinkOperator);
                            querySubPlanWithUpstreamOperator->removeAsRootOperator(operatorToConnectInMatchedPlan);
                            querySubPlanWithUpstreamOperator->addRootOperator(networkSinkOperator);

                        } else if (currentWorkerId == workerId) {
                            // 12. Add a network source operator to the leaf operator.

                            // 13. Record information about the query plan and worker id
                            connectedSysSubPlanDetails.emplace_back(
                                SysPlanMetaData(querySubPlan->getQuerySubPlanId(), currentWorkerId));
                            // 14. create network source operator
                            auto networkSourceOperator = createNetworkSourceOperator(sharedQueryId,
                                                                                     sourceSchema,
                                                                                     networkSourceOperatorId,
                                                                                     topologyNodesBetween[pathIndex - 1]);
                            candidateOperator->addChild(networkSourceOperator);

                            //set properties on new source
                            networkSourceOperator->addProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID, upstreamNonSystemOperatorId);
                            networkSourceOperator->addProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID, downStreamNonSystemOperatorId);
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

                            // 17. create the query sub plan
                            auto newQuerySubPlan =
                                QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {networkSinkOperator});

                            // 18. Record information about the query plan and worker id
                            connectedSysSubPlanDetails.emplace_back(
                                SysPlanMetaData(newQuerySubPlan->getQuerySubPlanId(), currentWorkerId));

                            // 19. add the new query plan
                            if (computedSubQueryPlans.contains(currentWorkerId)) {
                                computedSubQueryPlans[currentWorkerId].emplace_back(newQuerySubPlan);
                            } else {
                                computedSubQueryPlans[currentWorkerId] = {newQuerySubPlan};
                            }
                        }
                    }

                    // 15. Add metadata about the plans and topology nodes hosting the system generated operators.
                    operatorToConnectInMatchedPlan->addProperty(CONNECTED_SYS_SUB_PLAN_DETAILS, connectedSysSubPlanDetails);
                }
            }
        }
    }
}

bool BasePlacementAdditionStrategy::updateExecutionNodes(SharedQueryId sharedQueryId,
                                                         ComputedSubQueryPlans& computedSubQueryPlans,
                                                         QuerySubPlanVersion querySubPlanVersion) {

    for (const auto& workerNodeId : workerNodeIdsInBFS) {

        TopologyNodeWLock lockedTopologyNode;
        // 1. If using optimistic strategy then, lock the topology node with the workerId and perform the "validation" before continuing.
        if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
            //1.1. wait till lock is acquired
            while (!(lockedTopologyNode = topology->lockTopologyNode(workerNodeId))) {
                std::this_thread::sleep_for(PATH_SELECTION_RETRY_WAIT);
            };
        } else {
            lockedTopologyNode = lockedTopologyNodeMap[workerNodeId];
        }

        try {
            // 1.2. Perform validation by checking if we can occupy the resources the operator placement algorithm reserved
            // for placing the operators.
            auto consumedResources = workerIdToResourceConsumedMap[workerNodeId];
            if (!lockedTopologyNode->operator*()->occupySlots(consumedResources)) {
                NES_ERROR("Unable to occupy resources on the topology node {} to successfully place operators.", workerNodeId);
                return false;
            }

            // 1.3. Check if the worker node contains pinned upstream operators
            bool containPinnedUpstreamOperator = false;
            bool containPinnedDownstreamOperator = false;
            if (pinnedUpStreamTopologyNodeIds.contains(workerNodeId)) {
                containPinnedUpstreamOperator = true;
            } else if (pinnedDownStreamTopologyNodeIds.contains(workerNodeId)) {
                containPinnedDownstreamOperator = true;
            }

            // 1.4. Fetch the execution node to update and placed query sub plans
            auto topologyNode = workerIdToTopologyNodeMap[workerNodeId];
            auto executionNode = getExecutionNode(topologyNode);
            auto placedQuerySubPlans = executionNode->getQuerySubPlans(sharedQueryId);

            // 1.5. Iterate over the placed query sub plans and update execution node
            auto computedQuerySubPlans = computedSubQueryPlans[workerNodeId];
            for (const auto& computedQuerySubPlan : computedQuerySubPlans) {

                // 1.5.1. Perform the type inference phase on the updated query sub plan and update execution node.
                if (computedQuerySubPlan->getQuerySubPlanId() == INVALID_QUERY_SUB_PLAN_ID) {
                    if (containPinnedUpstreamOperator) {

                        // Record all placed query sub plans that host pinned leaf operators
                        std::set<QueryPlanPtr> hostQuerySubPlans;

                        // Iterate over all pinned leaf operators and find a host placed query sub plan that hosts
                        auto computedOperators = computedQuerySubPlan->getAllOperators();
                        for (const auto& computedOperator : computedOperators) {

                            // If the pinned leaf operator is not of type logical source and was already placed.
                            // Then find and merge the operator with query sub plan containing the placed leaf operator
                            if (computedOperator->as<LogicalOperatorNode>()->getOperatorState() == OperatorState::PLACED) {

                                bool foundHostQuerySubPlan = false;

                                // Find and merge with the shared query plan
                                for (const auto& placedQuerySubPlan : placedQuerySubPlans) {

                                    // If the placed query sub plan contains the pinned upstream operator
                                    auto matchingPlacedLeafOperator =
                                        placedQuerySubPlan->getOperatorWithId(computedOperator->getId());
                                    if (matchingPlacedLeafOperator) {
                                        if (!computedOperator->hasProperty(CONNECTED_SYS_SUB_PLAN_DETAILS)) {
                                            NES_WARNING("connected sys sub plan details not found");
                                        }
                                        auto connectedSysSubPlanDetails =
                                            computedOperator->getProperty(CONNECTED_SYS_SUB_PLAN_DETAILS);
                                        matchingPlacedLeafOperator->addProperty(CONNECTED_SYS_SUB_PLAN_DETAILS,
                                                                                connectedSysSubPlanDetails);
                                        // Add all newly computed pinned downstream operators to the matching placed leaf operator
                                        auto pinnedDownstreamOperators = computedOperator->getParents();
                                        for (const auto& pinnedDownstreamOperator : pinnedDownstreamOperators) {
                                            bool foundOperatorWithEqualId = false;
                                            for (const auto& placedParent : matchingPlacedLeafOperator->getParents()) {
                                                if (placedParent->as<OperatorNode>()->getId()
                                                    == pinnedDownstreamOperator->as<OperatorNode>()->getId()) {
                                                    NES_DEBUG("Not modifying operator because it is a non system operator");
                                                    foundOperatorWithEqualId = true;
                                                }
                                            }
                                            if (foundOperatorWithEqualId) {
                                                continue;
                                            }
                                            bool mergedOperator = tryMergingSink(querySubPlanVersion,
                                                                                 computedQuerySubPlan,
                                                                                 matchingPlacedLeafOperator,
                                                                                 pinnedDownstreamOperator);
                                            if (!mergedOperator) {
                                                pinnedDownstreamOperator->removeChild(computedOperator);
                                                pinnedDownstreamOperator->addChild(matchingPlacedLeafOperator);
                                            }
                                        }
                                        hostQuerySubPlans.emplace(placedQuerySubPlan);
                                        foundHostQuerySubPlan = true;
                                        break;
                                    }
                                }

                                // If no query sub plan found that hosts the placed leaf operator
                                if (!foundHostQuerySubPlan) {
                                    NES_ERROR("Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                    throw Exceptions::RuntimeException(
                                        "Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                }
                            }
                        }

                        QueryPlanPtr updatedQuerySubPlan;
                        // Merge the two host query sub plans
                        if (!hostQuerySubPlans.empty()) {
                            for (const auto& hostQuerySubPlan : hostQuerySubPlans) {
                                if (!updatedQuerySubPlan) {
                                    updatedQuerySubPlan = hostQuerySubPlan;
                                } else {
                                    for (const auto& hostRootOperators : hostQuerySubPlan->getRootOperators()) {
                                        updatedQuerySubPlan->addRootOperator(hostRootOperators);
                                    }
                                }
                            }
                        }

                        //In the end, add root operators of computed plan as well.
                        for (const auto& rootOperator : computedQuerySubPlan->getRootOperators()) {
                            updatedQuerySubPlan->addRootOperator(rootOperator);
                        }

                        //As we are updating an existing query sub plan we mark the plan for re-deployment
                        updatedQuerySubPlan->setQueryState(QueryState::MARKED_FOR_REDEPLOYMENT);
                        updatedQuerySubPlan = typeInferencePhase->execute(updatedQuerySubPlan);
                        updatedQuerySubPlan->setVersion(querySubPlanVersion);
                        executionNode->addNewQuerySubPlan(updatedQuerySubPlan->getQueryId(), updatedQuerySubPlan);

                    } else if (containPinnedDownstreamOperator) {

                        auto computedOperators = computedQuerySubPlan->getAllOperators();
                        std::set<QueryPlanPtr> hostQuerySubPlans;
                        for (const auto& computedOperator : computedOperators) {

                            // If the pinned leaf operator is not of type logical source and was already placed.
                            // Then find and merge the operator with query sub plan containing the placed leaf operator
                            if (computedOperator->as<LogicalOperatorNode>()->getOperatorState() == OperatorState::PLACED) {

                                bool foundHostQuerySubPlan = false;

                                // Find and merge with the shared query plan
                                for (const auto& placedQuerySubPlan : placedQuerySubPlans) {

                                    // If the placed query sub plan contains the pinned upstream operator
                                    auto matchingPinnedRootOperator =
                                        placedQuerySubPlan->getOperatorWithId(computedOperator->getId());
                                    if (matchingPinnedRootOperator) {
                                        // Add all newly computed downstream operators to matching placed leaf operator
                                        for (const auto& upstreamOperator : computedOperator->getChildren()) {
                                            bool foundOperatorWithEqualId = false;
                                            for (const auto& placedChild : matchingPinnedRootOperator->getChildren()) {
                                                if (placedChild->as<OperatorNode>()->getId()
                                                    == upstreamOperator->as<OperatorNode>()->getId()) {
                                                    NES_DEBUG("Not modifying operator because it is a non system operator");
                                                    foundOperatorWithEqualId = true;
                                                }
                                            }
                                            if (foundOperatorWithEqualId) {
                                                continue;
                                            }
                                            bool mergedSource = tryMergingSource(querySubPlanVersion,
                                                                                 matchingPinnedRootOperator,
                                                                                 upstreamOperator);
                                            if (!mergedSource) {
                                                matchingPinnedRootOperator->addChild(upstreamOperator);
                                            }
                                        }
                                        hostQuerySubPlans.emplace(placedQuerySubPlan);
                                        foundHostQuerySubPlan = true;
                                        break;
                                    }
                                }

                                // If no query sub plan found that hosts the placed leaf operator
                                if (!foundHostQuerySubPlan) {
                                    NES_ERROR("Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                    throw Exceptions::RuntimeException(
                                        "Unable to find query sub plan hosting the placed and pinned upstream operator.");
                                }
                            }
                        }

                        QueryPlanPtr updatedQuerySubPlan;
                        // Merge the two host query sub plans
                        if (!hostQuerySubPlans.empty()) {
                            for (const auto& hostQuerySubPlan : hostQuerySubPlans) {
                                if (!updatedQuerySubPlan) {
                                    updatedQuerySubPlan = hostQuerySubPlan;
                                } else {
                                    for (const auto& hostRootOperators : hostQuerySubPlan->getRootOperators()) {
                                        updatedQuerySubPlan->addRootOperator(hostRootOperators);
                                    }
                                }
                            }
                        }

                        //As we are updating an existing query sub plan we mark the plan for re-deployment
                        updatedQuerySubPlan->setQueryState(QueryState::MARKED_FOR_REDEPLOYMENT);
                        updatedQuerySubPlan = typeInferencePhase->execute(updatedQuerySubPlan);
                        updatedQuerySubPlan->setVersion(querySubPlanVersion);
                        executionNode->addNewQuerySubPlan(updatedQuerySubPlan->getQueryId(), updatedQuerySubPlan);
                    } else {
                        NES_ERROR(
                            "A query sub plan {} with invalid query sub plan found that has no pinned upstream or downstream "
                            "operator.",
                            computedQuerySubPlan->toString());
                        throw Exceptions::RuntimeException("A query sub plan with invalid query sub plan found that has no "
                                                           "pinned upstream or downstream operator.");
                    }
                } else {
                    auto updatedQuerySubPlan = typeInferencePhase->execute(computedQuerySubPlan);
                    updatedQuerySubPlan->setQueryState(QueryState::MARKED_FOR_DEPLOYMENT);
                    updatedQuerySubPlan->setVersion(querySubPlanVersion);
                    executionNode->addNewQuerySubPlan(updatedQuerySubPlan->getQueryId(), updatedQuerySubPlan);
                }
            }

            // 1.6. Add the execution node to the global execution plan
            if (!globalExecutionPlan->checkIfExecutionNodeExists(workerNodeId)) {
                globalExecutionPlan->addExecutionNode(executionNode);
            } else {
                globalExecutionPlan->scheduleExecutionNode(executionNode);
            }

            // 1.7. Update state and properties of all operators placed on the execution node
            placedQuerySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
            for (auto placedQuerySubPlan : placedQuerySubPlans) {
                QuerySubPlanId querySubPlanId = placedQuerySubPlan->getQuerySubPlanId();
                auto allPlacedOperators = placedQuerySubPlan->getAllOperators();
                for (const auto& placedOperator : allPlacedOperators) {
                    OperatorId operatorId = placedOperator->getId();
                    //Set status to Placed and copy over metadata properties for
                    if (operatorIdToOriginalOperatorMap.contains(operatorId)) {
                        auto originalOperator = operatorIdToOriginalOperatorMap[operatorId];
                        originalOperator->setOperatorState(OperatorState::PLACED);
                        originalOperator->addProperty(PINNED_WORKER_ID, workerNodeId);
                        originalOperator->addProperty(PLACED_SUB_PLAN_ID, querySubPlanId);
                        if (placedOperator->hasProperty(CONNECTED_SYS_SUB_PLAN_DETAILS)) {
                            originalOperator->addProperty(CONNECTED_SYS_SUB_PLAN_DETAILS,
                                                          placedOperator->getProperty(CONNECTED_SYS_SUB_PLAN_DETAILS));
                        }
                    }
                }
            }

            // 1.8. Release lock on the topology node
            if (placementAmendmentMode == PlacementAmendmentMode::OPTIMISTIC) {
                lockedTopologyNode->unlock();
            }
        } catch (std::exception& ex) {
            NES_ERROR("Exception occurred during pinned operator placement {}.", ex.what());
            throw ex;
        }
    }
    return true;
}
bool BasePlacementAdditionStrategy::tryMergingSource(QuerySubPlanVersion querySubPlanVersion,
                                                     const NodePtr& placedDownstreamOperator,
                                                     const NodePtr& newOperator) {

    //check if the new operator is a network source
    auto newSource = newOperator->as_if<SourceLogicalOperatorNode>();
    if (!newSource) {
        return false;
    }
    auto newNetworkSourceDescriptor = newSource->getSourceDescriptor()->as_if<Network::NetworkSourceDescriptor>();
    if (!newNetworkSourceDescriptor) {
        return false;
    }

    NES_ASSERT(newSource->hasProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID),
               "Network source does not have the UPSTREAM_NON_SYSTEM_OPERATOR_ID property set");

    bool replacedOperator = false;
    for (const auto& existingChild : placedDownstreamOperator->getChildren()) {

        //check if the placed operator is a network source
        auto existingNetworkSource = existingChild->as_if<SourceLogicalOperatorNode>();
        if (!existingChild) {
            continue;
        }
        auto existingNetworkSourceDescriptor =
            existingNetworkSource->getSourceDescriptor()->as_if<Network::NetworkSourceDescriptor>();
        if (!existingNetworkSourceDescriptor) {
            continue;
        }

        NES_ASSERT(existingNetworkSource->hasProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID),
                   "Network source does not have the UPSTREAM_NON_SYSTEM_OPERATOR_ID property set");

        if (std::any_cast<OperatorId>(existingNetworkSource->getProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID))
            == std::any_cast<OperatorId>(newSource->getProperty(UPSTREAM_NON_SYSTEM_OPERATOR_ID))) {
            auto mergedNetworkSourceDescriptor = Network::NetworkSourceDescriptor::create(
                newNetworkSourceDescriptor->getSchema(),
                newNetworkSourceDescriptor->getNesPartition(),
                newNetworkSourceDescriptor->getNodeLocation(),
                SOURCE_RETRY_WAIT,
                SOURCE_RETRIES,
                querySubPlanVersion,
                existingNetworkSourceDescriptor->getUniqueId());
            existingNetworkSource->setSourceDescriptor(mergedNetworkSourceDescriptor);
            auto computedParent = newSource->getParents().front();
            computedParent->removeChild(newSource);
            replacedOperator = true;
            break;
        }
    }
    return replacedOperator;
}

bool BasePlacementAdditionStrategy::tryMergingSink(QuerySubPlanVersion querySubPlanVersion,
                                                   const QueryPlanPtr& computedQuerySubPlan,
                                                   const NodePtr& upstreamOperatorOfPlacedSinksToCheck,
                                                   const NodePtr& newOperator) {
    //check if the new operator is a network sink
    auto newSink = newOperator->as_if<SinkLogicalOperatorNode>();
    if (!newSink) {
        return false;
    }
    auto newNetworkSinkDescriptor = newSink->getSinkDescriptor()->as_if<Network::NetworkSinkDescriptor>();
    if (!newNetworkSinkDescriptor) {
        return false;
    }

    NES_ASSERT(newSink->hasProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID),
               "Network sink does not have the DOWNSTREAM_NON_SYSTEM_OPERATOR_ID property set");

    bool replacedOperator = false;
    for (const auto& existingParent : upstreamOperatorOfPlacedSinksToCheck->getParents()) {

        //check if the placed operator is a network sink
        auto existingNetworkSink = existingParent->as_if<SinkLogicalOperatorNode>();
        if (!existingNetworkSink) {
            continue;
        }
        auto existingNetworkSinkDescriptor = existingNetworkSink->getSinkDescriptor()->as_if<Network::NetworkSinkDescriptor>();
        if (!existingNetworkSinkDescriptor) {
            continue;
        }

        NES_ASSERT(existingNetworkSink->hasProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID),
                   "Network sink does not have the DOWNSTREAM_NON_SYSTEM_OPERATOR_ID property set");

        //check if the new sink corresponds to a pleced sink that needs to be reconfigured
        if (std::any_cast<OperatorId>(existingNetworkSink->getProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID))
            == std::any_cast<OperatorId>(newSink->getProperty(DOWNSTREAM_NON_SYSTEM_OPERATOR_ID))) {
            auto mergedNetworkSinkDescriptor =
                Network::NetworkSinkDescriptor::create(newNetworkSinkDescriptor->getNodeLocation(),
                                                       newNetworkSinkDescriptor->getNesPartition(),
                                                       SINK_RETRY_WAIT,
                                                       SINK_RETRIES,
                                                       querySubPlanVersion,
                                                       newNetworkSinkDescriptor->getNumberOfOrigins(),
                                                       existingNetworkSinkDescriptor->getUniqueId());
            existingNetworkSink->setSinkDescriptor(mergedNetworkSinkDescriptor);
            computedQuerySubPlan->removeAsRootOperator(newSink);
            replacedOperator = true;
            break;
        }
    }
    return replacedOperator;
}

ExecutionNodePtr BasePlacementAdditionStrategy::getExecutionNode(const TopologyNodePtr& candidateTopologyNode) {

    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode->getId())) {
        NES_TRACE("node {} was already used by other deployment", candidateTopologyNode->toString());
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeById(candidateTopologyNode->getId());
    } else {
        NES_TRACE("create new execution node with id: {}", candidateTopologyNode->getId());
        candidateExecutionNode = ExecutionNode::createExecutionNode(candidateTopologyNode);
    }
    return candidateExecutionNode;
}

TopologyNodePtr BasePlacementAdditionStrategy::getTopologyNode(WorkerId workerId) {

    NES_TRACE("Get the topology node {}", workerId);
    if (!workerIdToTopologyNodeMap.contains(workerId)) {
        NES_ERROR("Topology node with id {} not considered during the path selection phase.", workerId);
        throw Exceptions::RuntimeException("Topology node with id " + std::to_string(workerId)
                                           + " not considered during the path selection phase.");
    }

    return workerIdToTopologyNodeMap[workerId];
}

LogicalOperatorNodePtr BasePlacementAdditionStrategy::createNetworkSinkOperator(QueryId queryId,
                                                                                OperatorId sourceOperatorId,
                                                                                const TopologyNodePtr& sourceTopologyNode) {

    NES_TRACE("create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    QuerySubPlanVersion sinkVersion = 0;
    OperatorId id = getNextOperatorId();
    auto numberOfOrigins = 0;
    return LogicalOperatorFactory::createSinkOperator(Network::NetworkSinkDescriptor::create(nodeLocation,
                                                                                             nesPartition,
                                                                                             SINK_RETRY_WAIT,
                                                                                             SINK_RETRIES,
                                                                                             sinkVersion,
                                                                                             numberOfOrigins,
                                                                                             id),
                                                      id);
}

LogicalOperatorNodePtr BasePlacementAdditionStrategy::createNetworkSourceOperator(QueryId queryId,
                                                                                  SchemaPtr inputSchema,
                                                                                  OperatorId operatorId,
                                                                                  const TopologyNodePtr& sinkTopologyNode) {
    NES_TRACE("create Network Source operator");
    NES_ASSERT2_FMT(sinkTopologyNode, "Invalid sink node while placing query " << queryId);
    Network::NodeLocation upstreamNodeLocation(sinkTopologyNode->getId(),
                                               sinkTopologyNode->getIpAddress(),
                                               sinkTopologyNode->getDataPort());
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
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
BasePlacementAdditionStrategy::getTopologyNodesForChildrenOperators(const LogicalOperatorNodePtr& operatorNode) {
    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        if (!child->as_if<LogicalOperatorNode>()->hasProperty(PINNED_WORKER_ID)) {
            NES_WARNING("unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode = workerIdToTopologyNodeMap[std::any_cast<uint64_t>(
            child->as_if<LogicalOperatorNode>()->getProperty(PINNED_WORKER_ID))];

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
        unlockTopologyNodesInSelectedPath();
    }
}
}// namespace NES::Optimizer
