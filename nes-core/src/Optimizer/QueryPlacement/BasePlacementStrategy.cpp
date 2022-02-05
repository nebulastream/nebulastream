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
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <stack>
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                             TopologyPtr topologyPtr,
                                             TypeInferencePhasePtr typeInferencePhase)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topologyPtr)),
      typeInferencePhase(std::move(typeInferencePhase)) {}

bool BasePlacementStrategy::updateGlobalExecutionPlan(QueryPlanPtr /*queryPlan*/) { NES_NOT_IMPLEMENTED(); }

void BasePlacementStrategy::performPathSelection(std::vector<OperatorNodePtr> upStreamPinnedOperators,
                                                 std::vector<OperatorNodePtr> downStreamPinnedOperators) {

    //1. Find the topology nodes that will host upstream operators

    std::set<TopologyNodePtr> topologyNodesWithUpStreamPinnedOperators;
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exception("LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                            + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalStream = topology->findNodeWithId(nodeId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical streams located on same physical node)
        topologyNodesWithUpStreamPinnedOperators.insert(nodeWithPhysicalStream);
    }

    //2. Find the topology nodes that will host downstream operators

    std::set<TopologyNodePtr> topologyNodesWithDownStreamPinnedOperators;
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exception("LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                            + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalStream = topology->findNodeWithId(nodeId);
        topologyNodesWithDownStreamPinnedOperators.insert(nodeWithPhysicalStream);
    }

    //3. Performs path selection

    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());
    std::vector<TopologyNodePtr> selectedTopologyForPlacement =
        topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
    if (selectedTopologyForPlacement.empty()) {
        throw Exception("BasePlacementStrategy: Could not find the path for placement.");
    }

    //4. Map nodes in the selected topology by their ids.

    nodeIdToTopologyNodeMap.clear();
    // fetch root node from the identified path
    auto rootNode = selectedTopologyForPlacement[0]->getAllRootNodes()[0];
    auto topologyIterator = NES::DepthFirstNodeIterator(rootNode).begin();
    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        nodeIdToTopologyNodeMap[currentTopologyNode->getId()] = currentTopologyNode;
        ++topologyIterator;
    }
}

ExecutionNodePtr BasePlacementStrategy::getExecutionNode(const TopologyNodePtr& candidateTopologyNode) {

    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode->getId())) {
        NES_TRACE("BasePlacementStrategy: node " << candidateTopologyNode->toString() << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateTopologyNode->getId());
    } else {
        NES_TRACE("BasePlacementStrategy: create new execution node with id: " << candidateTopologyNode->getId());
        candidateExecutionNode = ExecutionNode::createExecutionNode(candidateTopologyNode);
    }
    return candidateExecutionNode;
}

TopologyNodePtr BasePlacementStrategy::getTopologyNode(uint64_t nodeId) {

    NES_TRACE("BasePlacementStrategy: Get the topology node for logical operator with id " << nodeId);
    auto found = nodeIdToTopologyNodeMap.find(nodeId);

    if (found == nodeIdToTopologyNodeMap.end()) {
        NES_ERROR("BasePlacementStrategy: Topology node with id " << nodeId << " not considered for the placement.");
        throw Exception("BasePlacementStrategy: Topology node with id " + std::to_string(nodeId)
                        + " not considered for the placement.");
    }

    if (found->second->getAvailableResources() == 0 && !operatorToExecutionNodeMap.contains(nodeId)) {
        NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        throw Exception("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
    }
    return found->second;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(QueryId queryId,
                                                                 uint64_t sourceOperatorId,
                                                                 const TopologyNodePtr& sourceTopologyNode) {

    NES_TRACE("BasePlacementStrategy: create Network Sink operator");
    Network::NodeLocation nodeLocation(sourceTopologyNode->getId(),
                                       sourceTopologyNode->getIpAddress(),
                                       sourceTopologyNode->getDataPort());
    Network::NesPartition nesPartition(queryId, sourceOperatorId, 0, 0);
    return LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, SINK_RETRY_WAIT, SINK_RETRIES));
}

OperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(QueryId queryId,
                                                                   SchemaPtr inputSchema,
                                                                   uint64_t operatorId,
                                                                   const TopologyNodePtr& sinkTopologyNode) {
    NES_TRACE("BasePlacementStrategy: create Network Source operator");
    NES_ASSERT2_FMT(sinkTopologyNode, "Invalid sink node while placing query " << queryId);
    Network::NodeLocation upstreamNodeLocation(sinkTopologyNode->getId(),
                                               sinkTopologyNode->getIpAddress(),
                                               sinkTopologyNode->getDataPort());
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
    const SourceDescriptorPtr& networkSourceDescriptor = Network::NetworkSourceDescriptor::create(std::move(inputSchema),
                                                                                                  nesPartition,
                                                                                                  upstreamNodeLocation,
                                                                                                  SOURCE_RETRY_WAIT,
                                                                                                  SOURCE_RETRIES);
    return LogicalOperatorFactory::createSourceOperator(networkSourceDescriptor, operatorId);
}

bool BasePlacementStrategy::runTypeInferencePhase(QueryId queryId,
                                                  FaultToleranceType faultToleranceType,
                                                  LineageType lineageType) {
    NES_DEBUG("BasePlacementStrategy: Run type inference phase for all the query sub plans to be deployed.");
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (const auto& executionNode : executionNodes) {
        NES_TRACE("BasePlacementStrategy: Get all query sub plans on the execution node for the query with id " << queryId);
        const std::vector<QueryPlanPtr>& querySubPlans = executionNode->getQuerySubPlans(queryId);
        for (const auto& querySubPlan : querySubPlans) {
            typeInferencePhase->execute(querySubPlan);
            querySubPlan->setFaultToleranceType(faultToleranceType);
            querySubPlan->setLineageType(lineageType);
        }
    }
    return true;
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(QueryId queryId,
                                                             const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                             const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    //Map operator id to the query plan that contains the operator
    // NOTE: I think we can move this logic to placement logic when operator is added to the query plan
    for (const auto& [operatorId, executionNode] : operatorToExecutionNodeMap) {
        auto queryPlans = executionNode->getQuerySubPlans(queryId);
        for (const auto& queryPlan : queryPlans) {
            auto logicalOperator = queryPlan->getOperatorWithId(operatorId);
            if (logicalOperator) {
                operatorToSubPlan[operatorId] = queryPlan;
                break;
            }
        }
    }

    NES_TRACE("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        placeNetworkOperator(queryId, pinnedUpStreamOperator, pinnedDownStreamOperators);
    }
    operatorToSubPlan.clear();
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId,
                                                 const OperatorNodePtr& upStreamOperator,
                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

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
        OperatorNodePtr downStreamOperator = parent->as<OperatorNode>();
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
            uint64_t sourceOperatorId = Util::getNextOperatorId();
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
                        throw Exception("BasePlacementStrategy::placeNetworkOperator: unable to place network sink operator for "
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
                        throw Exception("BasePlacementStrategy::placeNetworkOperator: unable to place network source operator "
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
                    sourceOperatorId = Util::getNextOperatorId();
                    OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSink);
                    operatorToSubPlan[networkSink->getId()] = querySubPlan;

                    NES_TRACE("BasePlacementStrategy: add query plan to execution node and update the global execution plan");
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
                auto downStreamOperatorCopy = downStreamOperator->copy();
                for (const auto& upstreamOptr : downStreamOperator->getChildren()) {
                    auto upstreamOperatorId = upstreamOptr->as<OperatorNode>()->getId();
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
                        throw Exception("BasePlacementStrategy::placeNetworkOperator: Parent plan not found in execution node.");
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
            NES_TRACE("BasePlacementStrategy: add network source and sink operator for the parent operator");
            placeNetworkOperator(queryId, downStreamOperator, pinnedDownStreamOperators);
        } else {
            NES_TRACE("BasePlacementStrategy: Skipping network source and sink operator for the parent operator as all children "
                      "operators are not processed");
        }
    }
}

void BasePlacementStrategy::addExecutionNodeAsRoot(ExecutionNodePtr& executionNode) {
    NES_TRACE("BasePlacementStrategy: Adding new execution node with id: " << executionNode->getTopologyNode()->getId());
    //1. Check if the candidateTopologyNode is a root node of the topology
    if (executionNode->getTopologyNode()->getParents().empty()) {
        //2. Check if the candidateExecutionNode is a root node
        if (!globalExecutionPlan->checkIfExecutionNodeIsARoot(executionNode->getId())) {
            if (!globalExecutionPlan->addExecutionNodeAsRoot(executionNode)) {
                NES_ERROR("BasePlacementStrategy: failed to add execution node as root");
                throw Exception("BasePlacementStrategy: failed to add execution node as root");
            }
        }
    }
}

bool BasePlacementStrategy::assignMappingToTopology(const NES::TopologyPtr topologyPtr,
                                                    const NES::QueryPlanPtr queryPlan,
                                                    const PlacementMatrix mapping) {

    auto topologyIterator = NES::DepthFirstNodeIterator(topologyPtr->getRoot()).begin();
    auto mappingIterator = mapping.begin();

    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        auto currentExecutionNode = BasePlacementStrategy::getExecutionNode(currentTopologyNode);

        // create an empty query sub plan
        QueryPlanPtr candidateQueryPlan;

        // get operators to place in the current topology nodes
        auto queryMappingIterator = mappingIterator->begin();

        // iterate to all operator in current query plan
        auto queryPlanIterator = NES::QueryPlanIterator(queryPlan);
        for (auto&& op : queryPlanIterator) {
            if (*queryMappingIterator) {
                // place the operator in the current node
                auto querySubPlans = currentExecutionNode->getQuerySubPlans(queryPlan->getQueryId());

                // If there is no existing query sub plan for this query, create a new one
                if (querySubPlans.empty()) {
                    candidateQueryPlan = QueryPlan::create();
                    candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
                    candidateQueryPlan->addRootOperator(op->as<OperatorNode>()->copy());
                } else {
                    // Check if existing operator is the parent of current operator to place

                    // 1. get the parent of the current operator to place
                    std::vector<NodePtr> parents = op->as<OperatorNode>()->getParents();

                    // 2. find all query plan in the current execution node that are parents to the current operator to place
                    std::vector<QueryPlanPtr> queryPlansWithParent;
                    for (auto& parent : parents) {
                        auto found =
                            std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
                                return querySubPlan->hasOperatorWithId(parent->as<OperatorNode>()->getId());
                            });

                        if (found != querySubPlans.end()) {
                            queryPlansWithParent.push_back(*found);
                            querySubPlans.erase(found);
                        }
                    }

                    // 3. If we have parents for the current operator to place, update the existing query sub plan
                    if (!queryPlansWithParent.empty()) {
                        currentExecutionNode->updateQuerySubPlans(queryPlan->getQueryId(), querySubPlans);
                        if (queryPlansWithParent.size() > 1) {
                            candidateQueryPlan = QueryPlan::create();
                            candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                            for (auto& queryPlanWithChildren : queryPlansWithParent) {
                                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                                    candidateQueryPlan->addRootOperator(root);
                                }
                            }
                        } else if (queryPlansWithParent.size() == 1) {
                            // if there is only one parent, then use this
                            candidateQueryPlan = queryPlansWithParent[0];
                        }

                        if (candidateQueryPlan) {
                            for (const auto& candidateParent : parents) {
                                auto parentOperator =
                                    candidateQueryPlan->getOperatorWithId(candidateParent->as<OperatorNode>()->getId());
                                if (parentOperator) {
                                    parentOperator->addChild(op->as<OperatorNode>()->copy());
                                }
                            }
                        }
                    } else {
                        // if queryPlansWithParent is empty, create a new candidateQueryPlan
                        candidateQueryPlan = QueryPlan::create();
                        candidateQueryPlan->setQueryId(queryPlan->getQueryId());
                        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
                        candidateQueryPlan->addRootOperator(op->as<OperatorNode>()->copy());
                    }
                }
                currentExecutionNode->addNewQuerySubPlan(queryPlan->getQueryId(), candidateQueryPlan);
                globalExecutionPlan->addExecutionNode(currentExecutionNode);
                operatorToExecutionNodeMap[op->as<OperatorNode>()->getId()] = currentExecutionNode;
            }
            ++queryMappingIterator;
        }
        ++topologyIterator;
        ++mappingIterator;
    }
    return true;
}

bool BasePlacementStrategy::isSourceAndDestinationConnected(const OperatorNodePtr& upStreamOperator,
                                                            const OperatorNodePtr& downStreamOperator) {
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
                NES_WARNING("BasePlacementStrategy: Skipping connectivity check as encountered a downstream operator not "
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

bool BasePlacementStrategy::operatorPresentInCollection(const OperatorNodePtr& operatorToSearch,
                                                        const std::vector<OperatorNodePtr>& operatorCollection) {

    auto isPresent = std::find_if(operatorCollection.begin(),
                                  operatorCollection.end(),
                                  [operatorToSearch](OperatorNodePtr operatorFromCollection) {
                                      return operatorToSearch->getId() == operatorFromCollection->getId();
                                  });
    return isPresent != operatorCollection.end();
}
}// namespace NES::Optimizer
