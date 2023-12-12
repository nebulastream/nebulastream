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
#include <unordered_set>
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                             const TopologyPtr& topology,
                                             const TypeInferencePhasePtr& typeInferencePhase)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase) {}

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
                operators[j]->as<LogicalOperatorNode>()->addProperty(PINNED_NODE_ID, topologyNodes[i]->getId());
            }
        }
    }
}

void BasePlacementStrategy::performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                                                 const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators) {

    //1. Find the topology nodes that will host upstream operators

    std::set<TopologyNodePtr> topologyNodesWithUpStreamPinnedOperators;
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findWorkerWithId(nodeId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical sources located on same physical node)
        topologyNodesWithUpStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    //2. Find the topology nodes that will host downstream operators

    std::set<TopologyNodePtr> topologyNodesWithDownStreamPinnedOperators;
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exceptions::RuntimeException(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findWorkerWithId(nodeId);
        topologyNodesWithDownStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    //3. Performs path selection

    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());
    std::vector<TopologyNodePtr> sourceTopologyNodesInSelectedPath =
        topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
    if (sourceTopologyNodesInSelectedPath.empty()) {
        throw Exceptions::RuntimeException("Could not find the path for placement.");
    }

    //4. Lock the selected topology nodes exclusively and create a topology map
    lockTopologyNodesInSelectedPath(sourceTopologyNodesInSelectedPath);
}

bool BasePlacementStrategy::lockTopologyNodesInSelectedPath(const std::vector<TopologyNodePtr>& sourceTopologyNodes) {
    topologyMap.clear();
    //Temp container for iteration
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
            NES_WARNING("Unable to acquire locks on the topology node {}", topologyNodeToLock->toString())
            //Release all the acquired locks as part of back-off and retry strategy.
            unlockTopologyNodes();
            NES_ERROR("Unable to Lock the topology node {}.", idOfTopologyNodeToLock);
            //Raise an exception
            throw Exceptions::RuntimeException(" Failed to acquire locks for the node " + std::to_string(idOfTopologyNodeToLock)
                                               + " selected in the path selection.");
        }

        // Add to the list of topology nodes for which locks are acquired
        lockedTopologyNodeIds.emplace_back(idOfTopologyNodeToLock);
        topologyMap[idOfTopologyNodeToLock] = topologyNodeToLock;
        std::for_each(topologyNodeToLock->getParents().begin(),
                      topologyNodeToLock->getParents().end(),
                      [&](const NodePtr& topologyNode) {
                          topologyNodesInBFSOrder.push(topologyNode->as<TopologyNode>());
                      });
    }
    return true;
}

bool BasePlacementStrategy::unlockTopologyNodes() {
    // Check if there are nodes on which locks are acquired
    if (lockedTopologyNodeIds.empty()) {
        NES_WARNING("No topology node found for which the locks are to be release.")
        return false;
    }

    //Release the locks in the inverse order of their acquisition
    std::for_each(lockedTopologyNodeIds.rbegin(), lockedTopologyNodeIds.rend(), [&](const WorkerId& lockedTopologyNodeId) {
        // Release the lock on the locked topology node
        if (!topology->releaseLockOnTopologyNode(lockedTopologyNodeId)) {
            NES_ERROR("Unable to release lock on the topology node {}.", lockedTopologyNodeId);
            throw Exceptions::RuntimeException("Unable to release lock on the topology node "
                                               + std::to_string(lockedTopologyNodeId));
        }
    });
    return true;
}

void BasePlacementStrategy::placePinnedOperators(QueryId queryId,
                                                 const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("Place all pinned upstream operators.");
    //0. Iterate over all pinned upstream operators and place them
    for (auto& pinnedOperator : pinnedUpStreamOperators) {
        NES_TRACE("Place operator {}", pinnedOperator->toString());
        //1. Fetch the node where operator is to be placed
        auto pinnedNodeId = std::any_cast<uint64_t>(pinnedOperator->getProperty(PINNED_NODE_ID));
        NES_TRACE("Get the topology node for logical operator with id {}", pinnedNodeId);
        if (topologyMap.find(pinnedNodeId) == topologyMap.end()) {
            NES_ERROR("Topology node with id {} not considered for the placement.", pinnedNodeId);
            throw Exceptions::RuntimeException("Topology node with id " + std::to_string(pinnedNodeId)
                                               + " not considered for the placement.");
        }
        auto pinnedNode = topologyMap[pinnedNodeId];
        // 2. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedOperator->getOperatorState() == OperatorState::PLACED) {
            //2.1 Fetch the execution node storing the operator
            const auto& candidateExecutionNode = globalExecutionPlan->getExecutionNodeById(pinnedNodeId);
            operatorToExecutionNodeMap[pinnedOperator->getId()] = candidateExecutionNode;
            //2.2 Fetch candidate query plan where operator was added
            QueryPlanPtr candidateQueryPlan = getCandidateQueryPlanForOperator(queryId, pinnedOperator, candidateExecutionNode);
            //2.3 Record to which subquery plan the operator was added
            operatorToSubPlan[pinnedOperator->getId()] = candidateQueryPlan;
        } else {// 3. If pinned operator is not placed then start by placing the operator
            //3.1 Find if the operator has multiple upstream or downstream operators and is not of source operator type
            if ((pinnedOperator->hasMultipleChildrenOrParents() && !pinnedOperator->instanceOf<SourceLogicalOperatorNode>())
                || pinnedOperator->instanceOf<SinkLogicalOperatorNode>()) {
                //3.1.1 Check if all upstream operators of this operator are placed
                bool allUpstreamOperatorsPlaced = true;
                for (auto& upstreamOperator : pinnedOperator->getChildren()) {
                    if (upstreamOperator->as_if<LogicalOperatorNode>()->getOperatorState() != OperatorState::PLACED) {
                        allUpstreamOperatorsPlaced = false;
                        break;
                    }
                }
                //3.1.2 If not all upstream operators are placed then skip placement of this operator
                if (!allUpstreamOperatorsPlaced) {
                    NES_WARNING("Upstream operators are not placed yet. Skipping the placement.");
                    continue;
                }
            }
            //3.2 Fetch Execution node with id same as the pinned node id
            auto candidateExecutionNode = getExecutionNode(pinnedNode);
            //3.3 Fetch candidate query plan where operator is to be added
            NES_TRACE("Get the candidate query plan where operator is to be appended.");
            QueryPlanPtr candidateQueryPlan = getCandidateQueryPlanForOperator(queryId, pinnedOperator, candidateExecutionNode);
            //Record to which subquery plan the operator was added
            operatorToSubPlan[pinnedOperator->getId()] = candidateQueryPlan;

            //3.4 Create copy of the operator before insertion into query plan
            pinnedOperator->setOperatorState(OperatorState::PLACED);
            auto pinnedOperatorCopy = pinnedOperator->copy();

            //3.5 Add pinned operator to the candidate query plan
            if (candidateQueryPlan->getRootOperators().empty()) {//3.5.1 if candidate query plan
                                                                 // is empty then set the operator as root of the query plan
                candidateQueryPlan->appendOperatorAsNewRoot(pinnedOperatorCopy);
            } else {//3.5.2 if candidate query plan is non-empty then set the operator as downstream operator of all its upstream operators
                //Loop over all upstream operators of pinned operator
                auto upstreamOperators = pinnedOperator->getChildren();
                for (const auto& upstreamOperator : upstreamOperators) {
                    //3.5.2.1 If candidate query plan has the upstream operator then add to it pinned operator as downstream operator
                    if (candidateQueryPlan->hasOperatorWithId(upstreamOperator->as<LogicalOperatorNode>()->getId())) {
                        //add downstream operator
                        candidateQueryPlan->getOperatorWithId(upstreamOperator->as<LogicalOperatorNode>()->getId())
                            ->addParent(pinnedOperatorCopy);
                    }
                    //3.5.2.2 Find if the upstream operator of the pinned operator is one of the root operator of the query plan.
                    auto rootOperators = candidateQueryPlan->getRootOperators();
                    auto found =
                        std::find_if(rootOperators.begin(),
                                     rootOperators.end(),
                                     [upstreamOperator](const OperatorNodePtr& rootOperator) {
                                         return rootOperator->getId() == upstreamOperator->as<LogicalOperatorNode>()->getId();
                                     });
                    if (found != rootOperators.end()) {
                        //Remove the upstream operator as root
                        candidateQueryPlan->removeAsRootOperator(*(found));
                    }
                    //add pinned operator as the root after validation
                    //validation
                    auto updatedRootOperators = candidateQueryPlan->getRootOperators();
                    auto operatorAlreadyExistsAsRoot =
                        std::find_if(updatedRootOperators.begin(),
                                     updatedRootOperators.end(),
                                     [pinnedOperatorCopy](const OperatorNodePtr& rootOperator) {
                                         return rootOperator->getId() == pinnedOperatorCopy->as<LogicalOperatorNode>()->getId();
                                     });
                    if (operatorAlreadyExistsAsRoot == updatedRootOperators.end()) {
                        candidateQueryPlan->addRootOperator(pinnedOperatorCopy);
                    }
                }
            }

            NES_TRACE("Add the query plan to the candidate execution node.");
            if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
                NES_ERROR("failed to create a new QuerySubPlan execution node for query.");
                throw Exceptions::RuntimeException("failed to create a new QuerySubPlan execution node for query.");
            }
            NES_TRACE("Update the global execution plan with candidate execution node");
            globalExecutionPlan->addExecutionNode(candidateExecutionNode);

            NES_TRACE("Place the information about the candidate execution plan and operator id in "
                      "the map.");
            operatorToExecutionNodeMap[pinnedOperator->getId()] = candidateExecutionNode;
            NES_DEBUG("Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            pinnedNode->reduceResources(1);
            topology->reduceResources(pinnedNode->getId(), 1);
        }

        //3. Check if this operator in the pinned downstream operator list.
        auto isOperatorAPinnedDownStreamOperator =
            std::find_if(pinnedDownStreamOperators.begin(),
                         pinnedDownStreamOperators.end(),
                         [pinnedOperator](const OperatorNodePtr& pinnedDownStreamOperator) {
                             return pinnedDownStreamOperator->getId() == pinnedOperator->getId();
                         });

        //4. Check if this operator is not in the list of pinned downstream operators then recursively call this function for its downstream operators.
        if (isOperatorAPinnedDownStreamOperator == pinnedDownStreamOperators.end()) {
            //4.1 Prepare next set of pinned upstream operators to place.
            std::set<LogicalOperatorNodePtr> nextPinnedUpstreamOperators;
            for (const auto& downStreamOperator : pinnedOperator->getParents()) {
                //4.1.1 Only select the operators that are not placed.
                if (downStreamOperator->as<LogicalOperatorNode>()->getOperatorState() != OperatorState::PLACED) {
                    nextPinnedUpstreamOperators.insert(downStreamOperator->as<LogicalOperatorNode>());
                }
            }
            //4.2 Recursively call this function for next set of pinned upstream operators.
            placePinnedOperators(queryId, nextPinnedUpstreamOperators, pinnedDownStreamOperators);
        }
    }
    NES_DEBUG("Finished placing query operators into the global execution plan");
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
    if (topologyNode->getAvailableResources() == 0 && !operatorToExecutionNodeMap.contains(workerId)) {
        NES_ERROR("Unable to find resources on the physical node for placement of source operator");
        throw Exceptions::RuntimeException("Unable to find resources on the physical node for placement of source operator");
    }
    return topologyNode;
}

LogicalOperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId,
                                                                        uint64_t sourceOperatorId,
                                                                        const TopologyNodePtr& sourceTopologyNode) {

    NES_TRACE("create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    Version sinkVersion = 0;
    return LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, SINK_RETRY_WAIT, SINK_RETRIES, sinkVersion));
}

LogicalOperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId,
                                                                          SchemaPtr inputSchema,
                                                                          uint64_t operatorId,
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

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(QueryId queryId,
                                                             const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                             const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_INFO("{}", globalExecutionPlan->getAsString());

    NES_TRACE("Add system generated operators for the query with id {}", queryId);
    for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        placeNetworkOperator(queryId, pinnedUpStreamOperator, pinnedDownStreamOperators);
    }
    operatorToSubPlan.clear();
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId,
                                                 const LogicalOperatorNodePtr& upStreamOperator,
                                                 const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Get execution node where operator is placed");
    // 1. Fetch the execution node containing the input operator
    ExecutionNodePtr upStreamExecutionNode = operatorToExecutionNodeMap[upStreamOperator->getId()];

    // 2. add execution node as root of global execution plan if no root exists
    // Note: this logic will change when we will support more than one sink node
    if (globalExecutionPlan->getRootNodes().empty()) {
        addExecutionNodeAsRoot(upStreamExecutionNode);
    }

    // 3. Iterate over all direct downstream operators and find if network sink and source pair need to be added
    for (const auto& parent : upStreamOperator->getParents()) {

        // 4. Check if the downstream operator was provided for placement
        LogicalOperatorNodePtr downStreamOperator = parent->as<LogicalOperatorNode>();
        auto found = operatorToExecutionNodeMap.find(downStreamOperator->getId());

        // 5. Skip the step if the downstream operator was not provided for placement
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("BasePlacementStrategy::placeNetworkOperator: Skipping ");
            continue;
        }

        // 6. Fetch execution node for the downstream operator
        NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Get execution node where parent operator is placed");
        ExecutionNodePtr downStreamExecutionNode = operatorToExecutionNodeMap[downStreamOperator->getId()];
        bool allUpStreamOperatorsProcessed = true;

        // 7. if the upstream and downstream execution nodes are different and the upstream and downstream operators are not already
        // connected using network sink and source pairs then enter the if condition.
        if (upStreamExecutionNode->getId() != downStreamExecutionNode->getId()
            && !isSourceAndDestinationConnected(upStreamOperator, downStreamOperator)) {

            // 7.1. Find nodes between the upstream and downstream topology nodes for placing the network source and sink pairs
            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Find the nodes between the topology node (inclusive) for "
                      "child and parent operators.");
            TopologyNodePtr upstreamTopologyNode = upStreamExecutionNode->getTopologyNode();
            TopologyNodePtr downstreamTopologyNode = downStreamExecutionNode->getTopologyNode();
            std::vector<TopologyNodePtr> nodesBetween = topology->findNodesBetween(upstreamTopologyNode, downstreamTopologyNode);
            TopologyNodePtr previousParent = nullptr;

            // 7.2. Add network source and sinks for the identified topology nodes
            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: For all topology nodes between the upstream topology node "
                      "add network source or sink operator.");
            const SchemaPtr& inputSchema = upStreamOperator->getOutputSchema();
            uint64_t sourceOperatorId = getNextOperatorId();
            // hint to understand the for loop below:
            // give a path from node0 (source) to nodeN (root)
            // base case: place network sink on node0 to send data to node1
            // base case: place network source on nodeN
            // now consider the (i-1)-th, i-th, and (i+1)-th nodes: assume that the (i-1)-th node has a sink that sends to the i-th
            // node, add a net source on the i-th node to receive from the network sink on the (i-1)-th node. Next, add a network
            // sink on the i-th node to send data to the (i+1)-th node.
            for (auto i = static_cast<std::size_t>(0UL); i < nodesBetween.size(); ++i) {

                NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Find the execution node for the topology node.");
                ExecutionNodePtr candidateExecutionNode = getExecutionNode(nodesBetween[i]);
                NES_ASSERT2_FMT(candidateExecutionNode, "Invalid candidate execution node while placing query " << queryId);
                if (i == 0) {
                    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Find the query plan with child operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetUpStreamOperator = querySubPlan->getOperatorWithId(upStreamOperator->getId());
                        if (targetUpStreamOperator) {
                            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Add network sink operator as root of the "
                                      "query plan with child "
                                      "operator.");
                            OperatorNodePtr networkSink =
                                createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                            targetUpStreamOperator->addParent(networkSink);
                            querySubPlan->removeAsRootOperator(targetUpStreamOperator);
                            querySubPlan->addRootOperator(networkSink);
                            operatorToSubPlan[networkSink->getId()] = querySubPlan;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_ERROR("BasePlacementStrategy::placeNetworkOperator: unable to place network sink operator for the "
                                  "child operator");
                        throw Exceptions::RuntimeException(
                            "BasePlacementStrategy::placeNetworkOperator: unable to place network sink operator for "
                            "the child operator");
                    }
                } else if (i == nodesBetween.size() - 1) {
                    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Find the query plan with parent operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    auto sinkNode = previousParent = nodesBetween[i - 1];
                    OperatorNodePtr sourceOperator =
                        createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId, sinkNode);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetDownstreamOperator = querySubPlan->getOperatorWithId(downStreamOperator->getId());
                        if (targetDownstreamOperator) {
                            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: add network source operator as child to the "
                                      "parent operator.");
                            targetDownstreamOperator->addChild(sourceOperator);
                            operatorToSubPlan[sourceOperator->getId()] = querySubPlan;
                            allUpStreamOperatorsProcessed =
                                (downStreamOperator->getChildren().size() == targetDownstreamOperator->getChildren().size());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_WARNING("BasePlacementStrategy::placeNetworkOperator: unable to place network source operator for "
                                    "the parent operator");
                        throw Exceptions::RuntimeException(
                            "BasePlacementStrategy::placeNetworkOperator: unable to place network source operator "
                            "for the parent operator");
                    }
                } else {

                    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Create a new query plan and add pair of network "
                              "source and network sink "
                              "operators.");
                    QueryPlanPtr querySubPlan = QueryPlan::create();
                    querySubPlan->setQueryId(queryId);
                    querySubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: add network source operator");
                    auto sinkNode = previousParent = nodesBetween[i - 1];
                    NES_ASSERT2_FMT(sinkNode, "Invalid sink node while placing query " << queryId);
                    const OperatorNodePtr networkSource =
                        createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId, sinkNode);
                    querySubPlan->appendOperatorAsNewRoot(networkSource);
                    operatorToSubPlan[networkSource->getId()] = querySubPlan;

                    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: add network sink operator");
                    sourceOperatorId = getNextOperatorId();
                    OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSink);
                    operatorToSubPlan[networkSink->getId()] = querySubPlan;

                    NES_TRACE("add query plan to execution node and update the global execution plan");
                    candidateExecutionNode->addNewQuerySubPlan(queryId, querySubPlan);
                    globalExecutionPlan->addExecutionNode(candidateExecutionNode);
                }
                // Add the parent-child relation
                if (previousParent) {
                    globalExecutionPlan->addExecutionNodeAsParentTo(previousParent->getId(), candidateExecutionNode);
                }
            }
        }

        // 8. If both upstream and downstream execution nodes are same then enter the if condition
        if (upStreamExecutionNode->getId() == downStreamExecutionNode->getId()) {
            auto queryPlanForDownStreamOperator = operatorToSubPlan[downStreamOperator->getId()];
            auto downStreamOperatorInQueryPlan = queryPlanForDownStreamOperator->getOperatorWithId(downStreamOperator->getId());
            auto queryPlanForUpStreamOperator = operatorToSubPlan[upStreamOperator->getId()];
            // 8.1. Check if the upstream and downstream operators are in different query plans and downstream operator has no
            // further downstream operators.
            QuerySubPlanId downStreamQuerySubPlanId = queryPlanForDownStreamOperator->getQuerySubPlanId();
            QuerySubPlanId upStreamQuerySubPlanId = queryPlanForUpStreamOperator->getQuerySubPlanId();
            if (upStreamQuerySubPlanId != downStreamQuerySubPlanId && downStreamOperatorInQueryPlan->getChildren().empty()) {
                // 8.1.1. combine the two plans to construct a unified query plan as the two operators should be in the same query plan
                NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Combining parent and child as they are in different "
                          "plans but have same execution plan.");
                //Construct a unified query plan
                auto downStreamOperatorCopy = downStreamOperator->copy()->as<LogicalOperatorNode>();
                for (const auto& upstreamOptr : downStreamOperator->getChildren()) {
                    auto upstreamOperatorId = upstreamOptr->as<LogicalOperatorNode>()->getId();
                    if (queryPlanForUpStreamOperator->hasOperatorWithId(upstreamOperatorId)) {
                        const OperatorNodePtr& childOpInSubPlan =
                            queryPlanForUpStreamOperator->getOperatorWithId(upstreamOperatorId);
                        childOpInSubPlan->addParent(downStreamOperatorCopy);
                        queryPlanForUpStreamOperator->removeAsRootOperator(childOpInSubPlan);
                    }
                }
                queryPlanForUpStreamOperator->addRootOperator(downStreamOperatorCopy);

                // 8.1.2. Assign the updated query plan to the downstream operator
                operatorToSubPlan[downStreamOperatorCopy->getId()] = queryPlanForUpStreamOperator;

                // 8.1.3. empty the downstream query plan
                downStreamOperatorInQueryPlan->removeAllParent();
                if (downStreamOperatorInQueryPlan->getChildren().empty()) {
                    queryPlanForDownStreamOperator->removeAsRootOperator(downStreamOperator);
                }

                // 8.1.4. remove the empty downstream query plan from the execution node
                if (queryPlanForDownStreamOperator->getRootOperators().empty()) {
                    auto querySubPlans = downStreamExecutionNode->getQuerySubPlans(queryId);
                    auto found = std::find_if(querySubPlans.begin(),
                                              querySubPlans.end(),
                                              [downStreamQuerySubPlanId](const QueryPlanPtr& querySubPlan) {
                                                  return downStreamQuerySubPlanId == querySubPlan->getQuerySubPlanId();
                                              });
                    if (found == querySubPlans.end()) {
                        throw Exceptions::RuntimeException(
                            "BasePlacementStrategy::placeNetworkOperator: Parent plan not found in execution node.");
                    }
                    querySubPlans.erase(found);
                    downStreamExecutionNode->updateQuerySubPlans(queryId, querySubPlans);
                }
            }
        }

        // 9. Process next upstream operator only when:
        // a) All upstream operators are processed.
        // b) Current operator is not in the list of pinned upstream operators.
        if (allUpStreamOperatorsProcessed && !operatorPresentInCollection(upStreamOperator, pinnedDownStreamOperators)) {
            NES_TRACE("add network source and sink operator for the parent operator");
            placeNetworkOperator(queryId, downStreamOperator, pinnedDownStreamOperators);
        } else {
            NES_TRACE("Skipping network source and sink operator for the parent operator as all children "
                      "operators are not processed");
        }
    }
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

bool BasePlacementStrategy::isSourceAndDestinationConnected(const LogicalOperatorNodePtr& upStreamOperator,
                                                            const LogicalOperatorNodePtr& downStreamOperator) {
    // 1. Fetch execution nodes for both operators
    auto upStreamExecutionNode = operatorToExecutionNodeMap[upStreamOperator->getId()];
    auto downStreamExecutionNode = operatorToExecutionNodeMap[downStreamOperator->getId()];

    // 2. Fetch the sub query plan containing both the operators
    auto subPlanWithUpstreamOperator = operatorToSubPlan[upStreamOperator->getId()];
    auto subPlanWithDownStreamOperator = operatorToSubPlan[downStreamOperator->getId()];

    // 3. Fetch the sink operators from the sub plan containing upstream operator
    // and source operators from the sub plan containing downstream operator
    auto sinkOperatorsFromUpstreamSubPlan =
        subPlanWithUpstreamOperator
            ->getOperatorByType<SinkLogicalOperatorNode>();//NOTE: we may have sub query plan without any sink
    auto sourcesOperatorsFromDownStreamSubPlan = subPlanWithDownStreamOperator->getSourceOperators();

    // 4. validate that they contain non empty sink and source operators
    if (sinkOperatorsFromUpstreamSubPlan.empty() || sourcesOperatorsFromDownStreamSubPlan.empty()) {
        return false;
    }

    // 5. Initialize a collection of sink operators using sink operators connected to the upstream operator
    // and iterate over the collection to check if they are connected to a downstream query plan that contains
    // the input upstream operator
    std::vector<SinkLogicalOperatorNodePtr> sinkOperatorsToCheck =
        std::vector<SinkLogicalOperatorNodePtr>{sinkOperatorsFromUpstreamSubPlan.begin(), sinkOperatorsFromUpstreamSubPlan.end()};
    while (!sinkOperatorsToCheck.empty()) {
        auto sink = sinkOperatorsToCheck.back();
        sinkOperatorsToCheck.pop_back();
        // 5.1. Extract descriptor of network sink type
        if (sink->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
            //5.2. Fetch downstream query plan for the connected network source using information from the descriptor
            auto networkSinkDescriptor = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
            auto nextDownStreamSubPlan = operatorToSubPlan[networkSinkDescriptor->getNesPartition().getOperatorId()];
            //5.3. if no sub query plan found for the downstream operator then the operator is not participating in the placement
            if (!nextDownStreamSubPlan) {
                NES_WARNING("Skipping connectivity check as encountered a downstream operator not "
                            "participating in the placement.");
                continue;// skip and continue
            }
            //5.4. Check if the downstream operator is present in the query plan
            if (nextDownStreamSubPlan->hasOperatorWithId(downStreamOperator->getId())) {
                return true;// return true as connection found
            }
            //5.5. Fetch sink operators from the next downstream sub plan to check if two input operators are connected transitively
            // and add the sink operator to the collection of sink operators to check.
            auto downStreamSinkOperators =
                nextDownStreamSubPlan
                    ->getOperatorByType<SinkLogicalOperatorNode>();//NOTE: we may have sub query plan without any sink
            for (const auto& downStreamSinkOperator : downStreamSinkOperators) {
                sinkOperatorsToCheck.emplace_back(downStreamSinkOperator);
            }
        }
    }
    return false;// return false as connection not found
}

bool BasePlacementStrategy::operatorPresentInCollection(const LogicalOperatorNodePtr& operatorToSearch,
                                                        const std::set<LogicalOperatorNodePtr>& operatorCollection) {

    auto isPresent = std::find_if(operatorCollection.begin(),
                                  operatorCollection.end(),
                                  [operatorToSearch](OperatorNodePtr operatorFromCollection) {
                                      return operatorToSearch->getId() == operatorFromCollection->getId();
                                  });
    return isPresent != operatorCollection.end();
}

std::vector<TopologyNodePtr>
BasePlacementStrategy::getTopologyNodesForChildrenOperators(const LogicalOperatorNodePtr& operatorNode) {
    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        if (!child->as_if<LogicalOperatorNode>()->hasProperty(PINNED_NODE_ID)) {
            NES_WARNING("unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode =
            topologyMap[std::any_cast<uint64_t>(child->as_if<LogicalOperatorNode>()->getProperty(PINNED_NODE_ID))];

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

    // Otherwise find query plans containing the child operator
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
