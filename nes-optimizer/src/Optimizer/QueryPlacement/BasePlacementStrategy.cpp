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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <thread>
#include <unordered_set>
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                             const TopologyPtr& topology,
                                             const TypeInferencePhasePtr& typeInferencePhase,
                                             PlacementMode placementMode)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      placementMode(placementMode) {}

bool BasePlacementStrategy::updateGlobalExecutionPlan(QueryPlanPtr /*queryPlan*/) { NES_NOT_IMPLEMENTED(); }

void BasePlacementStrategy::pinOperators(QueryPlanPtr queryPlan,
                                         const TopologyPtr& topology,
                                         NES::Optimizer::PlacementMatrix& matrix) {
    std::vector<TopologyNodePtr> topologyNodes;
    auto topologyIterator = NES::BreadthFirstNodeIterator(topology->getRoot());
    for (auto itr = topologyIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
        topologyNodes.emplace_back((*itr)->as<TopologyNode>());
    }

    auto operators = QueryPlanIterator(std::move(queryPlan)).snapshot();

    for (uint64_t i = 0; i < topologyNodes.size(); i++) {
        // Set the Pinned operator property
        auto currentRow = matrix[i];
        for (uint64_t j = 0; j < operators.size(); j++) {
            if (currentRow[j]) {
                // if the the value of the matrix at (i,j) is 1, then add a PINNED_NODE_ID of the topologyNodes[i] to operators[j]
                operators[j]->as<LogicalOperatorNode>()->addProperty(PINNED_WORKER_ID, topologyNodes[i]->getId());
            }
        }
    }
}

void BasePlacementStrategy::performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                                                 const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators) {

    //1. Find the topology nodes that will host upstream operators
    std::set<TopologyNodePtr> topologyNodesWithUpStreamPinnedOperators;
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_WORKER_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        WorkerId workerId = std::any_cast<uint64_t>(value);
        pinnedUpStreamTopologyNodeIds.emplace(workerId);
        auto topologyNode = topology->findWorkerWithId(workerId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical sources located on same physical node)
        topologyNodesWithUpStreamPinnedOperators.insert(topologyNode);
    }

    //2. Find the topology nodes that will host downstream operators
    std::set<TopologyNodePtr> topologyNodesWithDownStreamPinnedOperators;
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_WORKER_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        WorkerId workerId = std::any_cast<uint64_t>(value);
        pinnedDownStreamTopologyNodeIds.emplace(workerId);
        auto nodeWithPhysicalSource = topology->findWorkerWithId(workerId);
        topologyNodesWithDownStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    //3. Find the path for placement based on the selected placement mode
    switch (placementMode) {
        case PlacementMode::Pessimistic: {
            pessimisticPathSelection(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);
            break;
        }
        case PlacementMode::Optimistic: {
            optimisticPathSelection(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);
            break;
        }
    }
}

void BasePlacementStrategy::optimisticPathSelection(const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
                                                    const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators) {

    //1 Performs path selection
    std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
        findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);

    //2 Add the selected topology nodes to the topology map
    topologyMap.clear();

    // Temp container for iteration
    std::queue<TopologyNodePtr> topologyNodesInBFSOrder;
    // Iterate topology nodes in a true breadth first order
    // Initialize with the upstream nodes
    std::for_each(sourceTopologyNodesInSelectedPath.begin(),
                  sourceTopologyNodesInSelectedPath.end(),
                  [&](const TopologyNodePtr& topologyNode) {
                      topologyNodesInBFSOrder.push(topologyNode);
                  });

    while (!topologyNodesInBFSOrder.empty()) {
        auto topologyNodeToLock = topologyNodesInBFSOrder.front();
        topologyNodesInBFSOrder.pop();
        // Skip if the topology node was visited previously
        WorkerId idOfTopologyNodeToLock = topologyNodeToLock->getId();
        if (topologyMap.contains(idOfTopologyNodeToLock)) {
            continue;
        }

        // Add to the list of topology nodes for which locks are acquired
        workerNodeIdsInBFS.emplace_back(idOfTopologyNodeToLock);
        topologyMap[idOfTopologyNodeToLock] = topologyNodeToLock;
        const auto& downstreamTopologyNodes = topologyNodeToLock->getParents();
        std::for_each(downstreamTopologyNodes.begin(), downstreamTopologyNodes.end(), [&](const NodePtr& topologyNode) {
            topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
        });
    }
}

void BasePlacementStrategy::pessimisticPathSelection(
    const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
    const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators) {
    bool success = false;
    uint8_t retryCount = 0;
    std::chrono::milliseconds backOffTime = PATH_SELECTION_RETRY_WAIT;
    // 1. Perform path selection and if failure than use the exponential back-off and retry strategy
    while (!success && retryCount < MAX_PATH_SELECTION_RETRIES) {

        //1.1 Performs path selection
        std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
            findPath(topologyNodesWithUpStreamPinnedOperators, topologyNodesWithDownStreamPinnedOperators);

        //1.2 Lock the selected topology nodes exclusively and create a topology map
        success = lockTopologyNodesInSelectedPath(sourceTopologyNodesInSelectedPath);

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

    // 2. Raise exception if unable to select and lock all topology nodes in the path
    if (!success) {
        throw Exceptions::RuntimeException("Unable to perform path selection.");
    }
}

bool BasePlacementStrategy::lockTopologyNodesInSelectedPath(const std::vector<TopologyNodePtr>& sourceTopologyNodes) {
    topologyMap.clear();
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
        if (topologyMap.contains(idOfTopologyNodeToLock)) {
            continue;
        }

        //Try to acquire the lock
        if (!topology->acquireLockOnTopologyNode(idOfTopologyNodeToLock)) {
            NES_ERROR("Unable to Lock the topology node {} selected in the path selection.", idOfTopologyNodeToLock);
            //Release all the acquired locks as part of back-off and retry strategy.
            unlockTopologyNodes();
            return false;
        }

        // Add to the list of topology nodes for which locks are acquired
        workerNodeIdsInBFS.emplace_back(idOfTopologyNodeToLock);
        topologyMap[idOfTopologyNodeToLock] = topologyNodeToLock;
        const auto& downstreamTopologyNodes = topologyNodeToLock->getParents();
        std::for_each(downstreamTopologyNodes.begin(), downstreamTopologyNodes.end(), [&](const NodePtr& topologyNode) {
            topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
        });
    }
    return true;
}

std::vector<TopologyNodePtr>
BasePlacementStrategy::findPath(const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
                                const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators) {

    //1 Create the pinned upstream and downstream topology node vector
    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());

    //2 Do the path selection
    std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
        topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
    if (sourceTopologyNodesInSelectedPath.empty()) {
        throw Exceptions::RuntimeException("Could not find the path for placement.");
    }
    return sourceTopologyNodesInSelectedPath;
}

bool BasePlacementStrategy::unlockTopologyNodes() {
    NES_INFO("Releasing locks for all locked topology nodes.");
    //1 Check if there are nodes on which locks are acquired
    if (workerNodeIdsInBFS.empty()) {
        NES_WARNING("No topology node found for which the locks are to be release.")
        return false;
    }

    //2 Release the locks in the inverse order of their acquisition
    std::for_each(workerNodeIdsInBFS.rbegin(), workerNodeIdsInBFS.rend(), [&](const WorkerId& lockedTopologyNodeId) {
        //2.1 Release the lock on the locked topology node
        if (!topology->releaseLockOnTopologyNode(lockedTopologyNodeId)) {
            //2.1.1 Raise an exception if the lock was not released
            NES_ERROR("Unable to release lock on the topology node {}.", lockedTopologyNodeId);
            throw Exceptions::RuntimeException("Unable to release lock on the topology node "
                                               + std::to_string(lockedTopologyNodeId));
        }
    });
    return true;
}

ComputedSubQueryPlans
BasePlacementStrategy::computeQuerySubPlans(SharedQueryId sharedQueryId,
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
    //      N3: {(1, INVALID, OP9), (1, INVALID, OP8->OP9)} //Note: shared query plan id was changed to INVALID id as pinned operator found
    //    }

    // Create a map of computed query sub plans
    ComputedSubQueryPlans computedSubQueryPlans;

    //1. Prepare for iterating over operators in strict BFS order
    // Initialize with the pinned upstream operators
    std::queue<LogicalOperatorNodePtr> placedOperators;
    std::for_each(pinnedUpStreamOperators.begin(), pinnedUpStreamOperators.end(), [&](const auto& operatorNode) {
        placedOperators.push(operatorNode);
    });

    //2. Iterate over all placed operators in strict BFS order
    while (!placedOperators.empty()) {

        // 2.1. Iterate of the operator and fetch the location where the operator is pinned to be placed
        auto placedOperator = placedOperators.front();
        operatorIdToOperatorMap[placedOperator->getId()] = placedOperator;
        auto copyOfPlacedOperator = placedOperator->copy()->as<LogicalOperatorNode>();// Make a copy of the operator
        placedOperators.pop();
        auto pinnedWorkerId = std::any_cast<uint64_t>(placedOperator->getProperty(PINNED_WORKER_ID));

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

                bool foundAnUpstreamPlacedOperator = false;
                for (const auto& rootOperator : placedQuerySubPlan->getRootOperators()) {
                    if (placedOperator->getChildWithOperatorId(rootOperator->getId())) {
                        //Add the copy as the parent of the iterated root operator and update the root of the iterated
                        rootOperator->addParent(copyOfPlacedOperator);
                        placedQuerySubPlan->removeAsRootOperator(rootOperator);
                        foundAnUpstreamPlacedOperator = true;
                    }
                }

                if (foundAnUpstreamPlacedOperator) {// If an upstream operator found as the root operator of query sub
                                                    // plan then create a new or merge it with the new query sub plan.
                    if (!newPlacedQuerySubPlan) {
                        newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId,
                                                                  placedQuerySubPlan->getQuerySubPlanId(),
                                                                  placedQuerySubPlan->getRootOperators());
                        newPlacedQuerySubPlan->addRootOperator(copyOfPlacedOperator);
                    } else {
                        const auto& rootOperators = placedQuerySubPlan->getRootOperators();
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
                if (copyOfPlacedOperator->getOperatorState() == OperatorState::PLACED) {
                    // Create a temporary query sub plans for the operator as it might need to be merged with another query
                    // sub plan that is already placed on the execution node. Thus we assign it an invalid sub query plan id.
                    newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId, INVALID_QUERY_SUB_PLAN_ID, {copyOfPlacedOperator});
                } else {
                    newPlacedQuerySubPlan =
                        QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {copyOfPlacedOperator});
                }
            } else if (copyOfPlacedOperator->getOperatorState() == OperatorState::PLACED) {
                newPlacedQuerySubPlan->setQuerySubPlanId(INVALID_QUERY_SUB_PLAN_ID);// set invalid query sub plan id
            }

            //2.2.4. Add the new query sub plan to the list
            updatedQuerySubPlans.emplace_back(newPlacedQuerySubPlan);
            computedSubQueryPlans[pinnedWorkerId] = updatedQuerySubPlans;
        } else {// 2.2.1. If no query sub plan placed on the pinned worker node
            QueryPlanPtr newPlacedQuerySubPlan;
            if (copyOfPlacedOperator->getOperatorState() == OperatorState::PLACED) {
                // Create a temporary query sub plans for the operator as it might need to be merged with another query
                // sub plan that is already placed on the execution node. Thus we assign it an invalid sub query plan id.
                newPlacedQuerySubPlan = QueryPlan::create(sharedQueryId, INVALID_QUERY_SUB_PLAN_ID, {copyOfPlacedOperator});
            } else {
                newPlacedQuerySubPlan =
                    QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {copyOfPlacedOperator});
            }
            computedSubQueryPlans[pinnedWorkerId] = {newPlacedQuerySubPlan};
        }

        //2.3. If the operator is in the state placed then check if it is one of the pinned downstream operator
        if (placedOperator->getOperatorState() == OperatorState::PLACED) {
            //2.3.1. Check if this operator in the pinned downstream operator list.
            const auto& isPinnedDownStreamOperator =
                std::find_if(pinnedDownStreamOperators.begin(),
                             pinnedDownStreamOperators.end(),
                             [placedOperator](const auto& pinnedDownStreamOperator) {
                                 return pinnedDownStreamOperator->getId() == placedOperator->getId();
                             });

            //2.3.2. If reached the pinned downstream operator then skip further traversal
            if (isPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
                continue;
            }
        }

        // 2.4. Add next downstream operators for the traversal
        auto downStreamOperators = placedOperator->getParents();
        std::for_each(downStreamOperators.begin(), downStreamOperators.end(), [&](const NodePtr& operatorNode) {
            placedOperators.push(operatorNode->as<LogicalOperatorNode>());
        });
    }

    return computedSubQueryPlans;
}

void BasePlacementStrategy::addNetworkOperators(ComputedSubQueryPlans& computedSubQueryPlans) {

    // Iterate over all computed sub query plans and add network source and sink operators.
    for (const auto& [workerId, querySubPlans] : computedSubQueryPlans) {
        auto downstreamTopologyNode = getTopologyNode(workerId);

        // 1. Iterate over all query sub plans
        for (const auto& querySubPlan : querySubPlans) {

            // shared query id
            auto sharedQueryId = querySubPlan->getQueryId();

            // 2. Iterate over all leaf operators
            auto leafOperators = querySubPlan->getLeafOperators();
            for (const auto& leafOperator : leafOperators) {

                // 3. Check if leaf operator in the state "Placed" then skip the operation
                if (leafOperator->as<LogicalOperatorNode>()->getOperatorState() == OperatorState::PLACED) {
                    continue;
                }

                auto originalLeafOperator = operatorIdToOperatorMap[leafOperator->getId()];

                // 4. For each leaf operator not in the state "Placed" find the topology node hosting the immediate
                // upstream logical operators.
                for (const auto& upstreamOperator : originalLeafOperator->getChildren()) {

                    // 5. Fetch the id of the topology node hosting the upstream operator to connect.
                    const auto& upstreamOperatorToConnect = upstreamOperator->as<LogicalOperatorNode>();
                    auto upstreamWorkerId = std::any_cast<WorkerId>(upstreamOperatorToConnect->getProperty(PINNED_WORKER_ID));

                    // 6. Get the upstream topology node.
                    auto upstreamTopologyNode = getTopologyNode(upstreamWorkerId);

                    // 7. Find topology nodes connecting the iterated workerId and pinnedUpstreamWorkerId.
                    std::vector<TopologyNodePtr> topologyNodesBetween =
                        topology->findNodesBetween(upstreamTopologyNode, downstreamTopologyNode);

                    bool placedNetworkOperator = false;

                    QueryPlanPtr querySubPlanWithUpstreamOperator;

                    // 8. Fetch all query sub plans placed on the upstream node and find the query sub plan hosting the
                    // upstream operator to connect.
                    for (const auto& upstreamQuerySubPlan : computedSubQueryPlans[upstreamWorkerId]) {
                        if (upstreamQuerySubPlan->hasOperatorWithId(upstreamOperatorToConnect->getId())) {
                            querySubPlanWithUpstreamOperator = upstreamQuerySubPlan;
                            break;
                        }
                    }

                    if (!querySubPlanWithUpstreamOperator) {
                        //throw exception
                    }

                    auto sourceSchema = upstreamOperatorToConnect->as<LogicalOperatorNode>()->getOutputSchema();

                    auto networkSourceOperatorId = getNextOperatorId();

                    // 9. Starting from the upstream to downstream topology node and add network sink source pairs.
                    for (uint16_t pathIndex = 0; pathIndex < topologyNodesBetween.size(); pathIndex++) {

                        WorkerId currentWorkerId = topologyNodesBetween[pathIndex]->getId();
                        if (currentWorkerId == upstreamWorkerId) {
                            // 10. Add a network sink operator as new root operator to the upstream operator to connect

                            // 11. create network sink operator to attach.
                            auto networkSinkOperator = createNetworkSinkOperator(sharedQueryId,
                                                                                 networkSourceOperatorId,
                                                                                 topologyNodesBetween[pathIndex + 1]);

                            // 12. Find the upstream operator to connect in the matched query sub plan
                            auto operatorToConnectInMatchedPlan =
                                querySubPlanWithUpstreamOperator->getOperatorWithId(upstreamOperatorToConnect->getId());
                            operatorToConnectInMatchedPlan->addParent(networkSinkOperator);
                            querySubPlanWithUpstreamOperator->removeAsRootOperator(operatorToConnectInMatchedPlan);
                            querySubPlanWithUpstreamOperator->addRootOperator(networkSinkOperator);

                        } else if (currentWorkerId == workerId) {
                            // 10. Add a network source operator to the leaf operator.

                            // 11. create network source operator
                            auto networkSourceOperator = createNetworkSourceOperator(sharedQueryId,
                                                                                     sourceSchema,
                                                                                     networkSourceOperatorId,
                                                                                     topologyNodesBetween[pathIndex - 1]);
                            leafOperator->addChild(networkSourceOperator);
                            break;
                        } else {
                            // 10. Compute a network source sink plan.

                            // 11. create network source operator
                            auto networkSourceOperator = createNetworkSourceOperator(sharedQueryId,
                                                                                     sourceSchema,
                                                                                     networkSourceOperatorId,
                                                                                     topologyNodesBetween[pathIndex]);

                            // 13. Generate id for next network source
                            networkSourceOperatorId = getNextOperatorId();

                            // 12. create network sink operator to attach.
                            auto networkSinkOperator = createNetworkSinkOperator(sharedQueryId,
                                                                                 networkSourceOperatorId,
                                                                                 topologyNodesBetween[pathIndex + 1]);

                            // 13. add network source as the child
                            networkSinkOperator->addChild(networkSourceOperator);

                            // 14. create the query sub plan
                            auto newQuerySubPlan =
                                QueryPlan::create(sharedQueryId, PlanIdGenerator::getNextQuerySubPlanId(), {networkSinkOperator});

                            // 15. add the new query plan
                            if (computedSubQueryPlans.contains(currentWorkerId)) {
                                auto placedQuerySubPlans = computedSubQueryPlans[currentWorkerId];
                                placedQuerySubPlans.emplace_back(newQuerySubPlan);
                            } else {
                                computedSubQueryPlans[currentWorkerId] = {newQuerySubPlan};
                            }
                        }
                    }
                }
            }
        }
    }
}

bool BasePlacementStrategy::updateExecutionNodes(ComputedSubQueryPlans& computedSubQueryPlans) {

    for (const auto& workerNodeId : workerNodeIdsInBFS) {

        // 1. If using optimistic strategy then, lock the topology node with the workerId and perform the "validation" before continuing.
        if (placementMode == PlacementMode::Optimistic) {
            //wait till lock is acquired
            while (!topology->acquireLockOnTopologyNode(workerNodeId)) {
                std::this_thread::sleep_for(PATH_SELECTION_RETRY_WAIT);
            };
        }

        // 2. Perform validation
        auto globalTopologyNodeCopy = topology->findWorkerWithId(workerNodeId);
        auto localTopologyNodeCopy = topologyMap[workerNodeId];

        globalTopologyNodeCopy->getAvailableResources();
        localTopologyNodeCopy->getAvailableResources();


        // 3. Check if the worker node contains pinned upstream operators
        if (pinnedUpStreamTopologyNodeIds.contains(workerNodeId)) {

        }

        auto computedQuerySubPlans = computedSubQueryPlans[workerNodeId];
    }

    for (const auto& [workerId, querySubPlans] : computedSubQueryPlans) {

        if (placementMode == PlacementMode::Optimistic) {
        }

        // 2. Fetch the execution node with id = workerId.
        // 3. Iterate over all query sub plans:
        //      3.1. Check if "all leaf operators" are in the state "Placed".
        //              When True:
        //                      3.1.1. Get from the execution node the query sub plan hosting the operators.
        //                      3.1.2. Replace the placed operator in the iterated query sub plan with the operators from the fetched query plan.
        //                      3.1.3. Compute new root operators for the query sub plan.
        //              When False:
        //                      3.1.1. Create a new sub query plan.
        //      3.2. Perform the type inference phase on the updated query sub plan.
        // 4. Update the topology with new resource values, the execution node with new query sub plan, and operators with state "Placed".
        // 5. If using optimistic strategy then, release the lock.
    }

    if (placementMode == PlacementMode::Pessimistic) {
        return unlockTopologyNodes();
    }

    return true;
}

ExecutionNodePtr BasePlacementStrategy::getExecutionNode(const TopologyNodePtr& candidateTopologyNode) {

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

TopologyNodePtr BasePlacementStrategy::getTopologyNode(WorkerId workerId) {

    NES_TRACE("Get the topology node {}", workerId);
    if (!topologyMap.contains(workerId)) {
        NES_ERROR("Topology node with id {} not considered during the path selection phase.", workerId);
        throw Exceptions::RuntimeException("Topology node with id " + std::to_string(workerId)
                                           + " not considered during the path selection phase.");
    }

    auto topologyNode = topologyMap[workerId];
    if (topologyNode->getAvailableResources() == 0) {
        NES_ERROR("Unable to find resources on the physical node for placement of source operator");
        throw Exceptions::RuntimeException("Unable to find resources on the physical node for placement of source operator");
    }
    return topologyNode;
}

LogicalOperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId,
                                                                        OperatorId sourceOperatorId,
                                                                        const TopologyNodePtr& sourceTopologyNode) {

    NES_TRACE("create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    Version sinkVersion = 0;
    OperatorId id = getNextOperatorId();
    auto numberOfOrigins = 0;
    return LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, SINK_RETRY_WAIT, SINK_RETRIES, sinkVersion, numberOfOrigins, id), id);
}

LogicalOperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId,
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
                                                                                                  0);
    return LogicalOperatorFactory::createSourceOperator(networkSourceDescriptor, operatorId);
}

bool BasePlacementStrategy::runTypeInferencePhase(QueryId queryId) {
    NES_DEBUG("Run type inference phase for all the query sub plans to be deployed.");
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (const auto& executionNode : executionNodes) {
        NES_TRACE("Get all query sub plans on the execution node for the query with id {}", queryId);
        const std::vector<QueryPlanPtr>& querySubPlans = executionNode->getQuerySubPlans(queryId);
        for (const auto& querySubPlan : querySubPlans) {
            auto sinks = querySubPlan->getOperatorByType<SinkLogicalOperatorNode>();
            for (const auto& sink : sinks) {
                auto sinkDescriptor = sink->getSinkDescriptor()->as<SinkDescriptor>();
                sink->setSinkDescriptor(sinkDescriptor);
            }
            typeInferencePhase->execute(querySubPlan);
        }
    }
    return true;
}

void BasePlacementStrategy::addExecutionNodeAsRoot(ExecutionNodePtr& executionNode) {
    NES_TRACE("Adding new execution node with id: {}", executionNode->getTopologyNode()->getId());
    //1. Check if the candidateTopologyNode is a root node of the topology
    if (executionNode->getTopologyNode()->getParents().empty()) {
        //2. Check if the candidateExecutionNode is a root node
        if (!globalExecutionPlan->checkIfExecutionNodeIsARoot(executionNode->getId())) {
            if (!globalExecutionPlan->addExecutionNodeAsRoot(executionNode)) {
                NES_ERROR("failed to add execution node as root");
                throw Exceptions::RuntimeException("failed to add execution node as root");
            }
        }
    }
}

std::vector<TopologyNodePtr>
BasePlacementStrategy::getTopologyNodesForChildrenOperators(const LogicalOperatorNodePtr& operatorNode) {
    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        if (!child->as_if<LogicalOperatorNode>()->hasProperty(PINNED_WORKER_ID)) {
            NES_WARNING("unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode =
            topologyMap[std::any_cast<uint64_t>(child->as_if<LogicalOperatorNode>()->getProperty(PINNED_WORKER_ID))];

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

QueryPlanPtr BasePlacementStrategy::getCandidateQueryPlanForOperator(QueryId queryId,
                                                                     const LogicalOperatorNodePtr& operatorNode,
                                                                     const ExecutionNodePtr& executionNode) {

    NES_DEBUG("Get candidate query plan for the operator {} on execution node with id {}",
              operatorNode->toString(),
              executionNode->getId());

    // Get all query sub plans for the query id on the execution node
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("BottomUpStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
        return candidateQueryPlan;
    }

    //Check if a query plan already contains the operator
    for (const auto& querySubPlan : querySubPlans) {
        if (querySubPlan->hasOperatorWithId(operatorNode->getId())) {
            return querySubPlan;// return the query sub plan that contains it
        }
    }

    // Otherwise find query plans containing the upstream operator
    std::vector<QueryPlanPtr> queryPlansWithChildren;
    NES_TRACE("Find query plans with child operators for the input logical operator.");
    std::vector<NodePtr> children = operatorNode->getChildren();
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(child->as<LogicalOperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("Found query plan with child operator {}", child->toString());
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        // if there are more than 1 query plans containing the child operator, the create a new query plan, add root operators on
        // it, and return the created query plan
        if (queryPlansWithChildren.size() > 1) {
            NES_TRACE("Found more than 1 query plan with the child operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            NES_TRACE("return the updated query plan.");
            return candidateQueryPlan;
        }
        // if there is only 1 plan containing the child operator, then return that query plan
        if (queryPlansWithChildren.size() == 1) {
            NES_TRACE("Found only 1 query plan with the child operator of the input logical operator. "
                      "Returning the query plan.");
            return queryPlansWithChildren[0];
        }
    }
    NES_TRACE("No query plan exists with the child operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
    return candidateQueryPlan;
}

}// namespace NES::Optimizer
