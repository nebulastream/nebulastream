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
                                             TypeInferencePhasePtr typeInferencePhase,
                                             SourceCatalogPtr streamCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topologyPtr)),
      typeInferencePhase(std::move(typeInferencePhase)), streamCatalog(std::move(streamCatalog)) {}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(const QueryPlanPtr& queryPlan) {

    auto queryId = queryPlan->getQueryId();

    //1. Find the section of topology that will be used to place the operators

    std::set<TopologyNodePtr> nodesWithPhysicalStreams;
    //Fetch all topology node contributing the physical streams involved in the query plan
    auto sourceOperators = queryPlan->getSourceOperators();
    for (const auto& sourceOperator : sourceOperators) {
        auto value = sourceOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw Exception("LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical source operator.");
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalStream = topology->findNodeWithId(nodeId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical streams located on same physical node)
        nodesWithPhysicalStreams.insert(nodeWithPhysicalStream);
    }
    std::vector sourceNodes(nodesWithPhysicalStreams.begin(), nodesWithPhysicalStreams.end());
    auto sinkNode = topology->getRoot();
    //NOTE: This step performs path selection
    std::vector<TopologyNodePtr> selectedTopologyForPlacement = topology->findPathBetween(sourceNodes, {sinkNode});

    //2. Now we perform pinning of the non-source operators

    //Find the root node of the topology to place sink operators
    NES_TRACE("BasePlacementStrategy: Locating node to pin sink operator.");
    std::vector<NodePtr> rootNodes = selectedTopologyForPlacement[0]->getAllRootNodes();
    //TODO: change here if the topology can have more than one root node
    // NOTE: this is not supported currently
    if (rootNodes.size() > 1) {
        NES_NOT_IMPLEMENTED();
    }
    if (rootNodes.empty()) {
        NES_ERROR("BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
        throw QueryPlacementException(
            queryId,
            "BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
    }
    TopologyNodePtr rootNode = rootNodes[0]->as<TopologyNode>();

    //NOTE: This step pins only the sink operators
    auto sinkOperators = queryPlan->getSinkOperators();
    //Fetch one root node and pin all sink operators to the node
    NES_TRACE("BasePlacementStrategy: Adding location for sink operator.");
    for (const auto& sinkOperator : sinkOperators) {
        auto value = sinkOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            sinkOperator->addProperty(PINNED_NODE_ID, rootNode->getId());
        }
    }

    //3. Map nodes in the selected topology by their ids.

    nodeIdToTopologyNodeMap.clear();
    auto topologyIterator = NES::DepthFirstNodeIterator(rootNode).begin();
    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        nodeIdToTopologyNodeMap[currentTopologyNode->getId()] = currentTopologyNode;
        ++topologyIterator;
    }
}

void BasePlacementStrategy::performPathSelection(std::vector<OperatorNodePtr> upStreamPinnedOperators, std::vector<OperatorNodePtr> downStreamPinnedOperators) {

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
        topologyNodesWithUpStreamPinnedOperators.insert(nodeWithPhysicalStream);
    }

    //3. Performs path selection
    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());
    std::vector<TopologyNodePtr> selectedTopologyForPlacement =
        topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
    if (selectedTopologyForPlacement.empty()) {
        throw Exception("Could not find the path for placement.");
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

    NES_DEBUG("BasePlacementStrategy: Get the topology node for logical operator with id " << nodeId);
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

    NES_DEBUG("BasePlacementStrategy: create Network Sink operator");
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
    NES_DEBUG("BasePlacementStrategy: create Network Source operator");
    NES_ASSERT2_FMT(sinkTopologyNode, "Invalid sink node while placing query " << queryId);
    Network::NodeLocation upstreamNodeLocation(sinkTopologyNode->getId(),
                                               sinkTopologyNode->getIpAddress(),
                                               sinkTopologyNode->getDataPort());
    const Network::NesPartition nesPartition = Network::NesPartition(queryId, operatorId, 0, 0);
    return LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(std::move(inputSchema),
                                                                                                 nesPartition,
                                                                                                 upstreamNodeLocation,
                                                                                                 SOURCE_RETRY_WAIT,
                                                                                                 SOURCE_RETRIES),
                                                        operatorId);
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    auto queryExecutionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (const auto& queryExecutionNode : queryExecutionNodes) {
        for (const auto& querySubPlan : queryExecutionNode->getQuerySubPlans(queryId)) {
            auto nodes = QueryPlanIterator(querySubPlan).snapshot();
            for (const auto& node : nodes) {
                const std::shared_ptr<LogicalOperatorNode>& asOperatorNode = node->as<LogicalOperatorNode>();
                operatorToSubPlan[asOperatorNode->getId()] = querySubPlan;
            }
        }
    }
    NES_DEBUG("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    NES_TRACE("BasePlacementStrategy: For each source operator check for the assignment of network source and sink operators");
    for (auto& sourceOperator : sourceOperators) {
        placeNetworkOperator(queryId, sourceOperator);
    }
    operatorToSubPlan.clear();
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Get execution node where operator is placed");
    ExecutionNodePtr executionNode = operatorToExecutionNodeMap[operatorNode->getId()];
    addExecutionNodeAsRoot(executionNode);

    for (const auto& parent : operatorNode->getParents()) {

        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Get execution node where parent operator is placed");
        ExecutionNodePtr parentExecutionNode = operatorToExecutionNodeMap[parentOperator->getId()];
        bool allChildrenPlaced = true;
        if (executionNode->getId() != parentExecutionNode->getId()
            && !isSourceAndDestinationConnected(operatorNode, parentOperator)) {

            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Parent and its child operator are placed on different "
                      "physical node.");

            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Find the nodes between the topology node (inclusive) for "
                      "child and parent operators.");
            TopologyNodePtr sourceNode = executionNode->getTopologyNode();
            TopologyNodePtr destinationNode = parentExecutionNode->getTopologyNode();
            std::vector<TopologyNodePtr> nodesBetween = topology->findNodesBetween(sourceNode, destinationNode);
            TopologyNodePtr previousParent = nullptr;

            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: For all the nodes between the topology node for child and "
                      "parent operators add "
                      "respective source or sink operator.");
            const SchemaPtr& inputSchema = operatorNode->getOutputSchema();
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
                        OperatorNodePtr targetUpStreamOperator = querySubPlan->getOperatorWithId(operatorNode->getId());
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
                        OperatorNodePtr targetDownstreamOperator = querySubPlan->getOperatorWithId(parentOperator->getId());
                        if (targetDownstreamOperator) {
                            NES_TRACE("BasePlacementStrategy::placeNetworkOperator: add network source operator as child to the "
                                      "parent operator.");
                            targetDownstreamOperator->addChild(sourceOperator);
                            operatorToSubPlan[sourceOperator->getId()] = querySubPlan;
                            allChildrenPlaced =
                                (parentOperator->getChildren().size() == targetDownstreamOperator->getChildren().size());
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
        if (executionNode->getId() == parentExecutionNode->getId()) {
            auto childPlan = operatorToSubPlan[operatorNode->getId()];
            auto parentPlan = operatorToSubPlan[parentOperator->getId()];
            auto parentOperatorInSubPlan = parentPlan->getOperatorWithId(parentOperator->getId());
            if (childPlan->getQuerySubPlanId() != parentPlan->getQuerySubPlanId()
                && parentOperatorInSubPlan->getChildren().empty()) {
                NES_TRACE("BasePlacementStrategy::placeNetworkOperator: Combining parent and child as they are in different "
                          "plans but same execution plan.");
                auto parentCopy = parentOperator->copy();
                for (const auto& child : parentOperator->getChildren()) {
                    const std::shared_ptr<OperatorNode>& childOp = child->as<OperatorNode>();
                    if (childPlan->hasOperatorWithId(childOp->getId())) {
                        const OperatorNodePtr& childOpInSubPlan = childPlan->getOperatorWithId(childOp->getId());
                        childOpInSubPlan->addParent(parentCopy);
                        childPlan->removeAsRootOperator(childOpInSubPlan);
                    }
                }
                childPlan->addRootOperator(parentCopy);
                operatorToSubPlan[parentCopy->getId()] = childPlan;
                parentOperatorInSubPlan->removeAllParent();
                if (parentOperatorInSubPlan->getChildren().empty()) {
                    parentPlan->removeAsRootOperator(parentOperator);
                }
                if (parentPlan->getRootOperators().empty()) {
                    auto parentExecutionPlans = parentExecutionNode->getQuerySubPlans(queryId);
                    auto parentRef = std::find_if(parentExecutionPlans.begin(),
                                                  parentExecutionPlans.end(),
                                                  [parentPlan](const QueryPlanPtr& querySubPlan) {
                                                      return parentPlan->getQuerySubPlanId() == querySubPlan->getQuerySubPlanId();
                                                  });
                    if (parentRef == parentExecutionPlans.end()) {
                        throw Exception("BasePlacementStrategy::placeNetworkOperator: Parent plan not found in execution node.");
                    }
                    parentExecutionPlans.erase(parentRef);
                    parentExecutionNode->updateQuerySubPlans(queryId, parentExecutionPlans);
                }
            }
        }

        if (allChildrenPlaced) {
            NES_TRACE("BasePlacementStrategy: add network source and sink operator for the parent operator");
            placeNetworkOperator(queryId, parentOperator);
        } else {
            NES_TRACE("BasePlacementStrategy: Skipping network source and sink operator for the parent operator as all children "
                      "operators are not processed");
        }
    }
}

void BasePlacementStrategy::addExecutionNodeAsRoot(ExecutionNodePtr& executionNode) {
    // check if the topology of the execution node is a root node
    NES_TRACE("BasePlacementStrategy: Adding new execution node with id: " << executionNode->getTopologyNode()->getId());
    // Check if the candidateTopologyNode is a root node of the topology
    if (executionNode->getTopologyNode()->getParents().empty()) {
        // Check if the candidateExecutionNode is a root node
        if (!globalExecutionPlan->checkIfExecutionNodeIsARoot(executionNode->getId())) {
            if (!globalExecutionPlan->addExecutionNodeAsRoot(executionNode)) {
                NES_ERROR("BasePlacementStrategy: failed to add execution node as root");
                throw Exception("BasePlacementStrategy: failed to add execution node as root");
            }
        }
    }
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

bool BasePlacementStrategy::isSourceAndDestinationConnected(const OperatorNodePtr& source, const OperatorNodePtr& destination) {
    auto sourceNode = operatorToExecutionNodeMap[source->getId()];
    auto targetNode = operatorToExecutionNodeMap[destination->getId()];
    auto sourceSubPlan = operatorToSubPlan[source->getId()];
    auto destinationSubPlan = operatorToSubPlan[destination->getId()];

    auto sourceSinks = sourceSubPlan->getOperatorByType<SinkLogicalOperatorNode>();
    auto destinationSources = destinationSubPlan->getSourceOperators();

    if (sourceSinks.empty() || destinationSources.empty()) {
        return false;
    }
    std::vector<SinkLogicalOperatorNodePtr> sinks =
        std::vector<SinkLogicalOperatorNodePtr>{sourceSinks.begin(), sourceSinks.end()};

    while (!sinks.empty()) {
        auto sink = sinks.back();
        sinks.pop_back();
        if (sink->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
            auto networkSinkDescriptor = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
            auto nextDestinationPlan = operatorToSubPlan[networkSinkDescriptor->getNesPartition().getOperatorId()];
            if (nextDestinationPlan->hasOperatorWithId(destination->getId())) {
                return true;
            }
            auto destinationSinks = nextDestinationPlan->getOperatorByType<SinkLogicalOperatorNode>();
            for (const auto& destinationSink : destinationSinks) {
                sinks.emplace_back(destinationSink);
            }
        }
    }
    return false;
}
}// namespace NES::Optimizer
