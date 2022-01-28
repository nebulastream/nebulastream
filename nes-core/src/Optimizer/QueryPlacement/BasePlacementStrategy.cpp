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
#include <utility>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                             TopologyPtr topologyPtr,
                                             TypeInferencePhasePtr typeInferencePhase,
                                             SourceCatalogPtr streamCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topologyPtr)),
      typeInferencePhase(std::move(typeInferencePhase)), streamCatalog(std::move(streamCatalog)) {}

void BasePlacementStrategy::mapPinnedOperatorToTopologyNodes(const QueryPlanPtr& queryPlan) {

    NES_DEBUG("BasePlacementStrategy: Prepare a map of source to physical nodes");

    QueryId queryId = queryPlan->getQueryId();
    //Clear the previous pinned operator mapping
    pinnedOperatorLocationMap.clear();
    //Fetch all source and sink operators for pinning
    auto sourceOperators = queryPlan->getSourceOperators();
    auto sinkOperators = queryPlan->getSinkOperators();

    TopologyNodePtr sinkNode = topology->getRoot();
    std::set<TopologyNodePtr> nodesWithPhysicalStreams;
    //Fetch all topology node contributing the physical streams involved in the query plan
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

    //NOTE: This step performs path selection
    std::vector<TopologyNodePtr> selectedTopologyForPlacement = topology->findPathBetween(sourceNodes, {sinkNode});

    //NOTE: This step pins the sink operators
    NES_TRACE("BasePlacementStrategy: Locating node to pin sink operator.");
    std::vector<NodePtr> rootNodes = selectedTopologyForPlacement[0]->getAllRootNodes();
    //TODO: change here if the topology can have more than one root node (this is not supported currently)
    if (rootNodes.size() > 1) {
        NES_NOT_IMPLEMENTED();
    }

    if (rootNodes.empty()) {
        NES_ERROR("BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
        throw QueryPlacementException(
            queryId,
            "BasePlacementStrategy: Found no root nodes in the topology plan. Please check the topology graph.");
    }
    //Fetch one root node and pin all sink operators to the node
    TopologyNodePtr rootNode = rootNodes[0]->as<TopologyNode>();
    NES_TRACE("BasePlacementStrategy: Adding location for sink operator.");
    for (const auto& sinkOperator : sinkOperators) {
        pinnedOperatorLocationMap[sinkOperator->getId()] = rootNode;
    }

    //NOTE: This step pins all source nodes
    for (const auto& sourceOperator : sourceOperators) {
        auto value = sourceOperator->getProperty(PINNED_NODE_ID);
        auto nodeId = std::any_cast<uint64_t>(value);
        auto topologyNode = Topology::findTopologyNodeInSubgraphById(nodeId, {rootNode});
        if (!topologyNode) {
            NES_ERROR("BasePlacementStrategy: unable to locate a node in the selected topology for placing the source operator.");
            throw QueryPlacementException(
                queryId,
                "BasePlacementStrategy: unable to locate a node in the selected topology for placing the source operator.");
        }
        pinnedOperatorLocationMap[sourceOperator->getId()] = topologyNode;
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

TopologyNodePtr BasePlacementStrategy::getTopologyNodeForPinnedOperator(uint64_t operatorId) {

    NES_DEBUG("BasePlacementStrategy: Get the topology node for logical operator with id " << operatorId);
    auto found = pinnedOperatorLocationMap.find(operatorId);

    if (found == pinnedOperatorLocationMap.end()) {
        NES_ERROR("BasePlacementStrategy: unable to find location for the pinned operator");
        throw Exception("BasePlacementStrategy: unable to find location for the pinned operator");
    }

    TopologyNodePtr candidateTopologyNode = pinnedOperatorLocationMap[operatorId];
    if (candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        throw Exception("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
    }
    return candidateTopologyNode;
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
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
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
                                                                                                 NSOURCE_RETRY_WAIT,
                                                                                                 NSOURCE_RETRIES),
                                                        operatorId);
}

void BasePlacementStrategy::addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BasePlacementStrategy: Add system generated operators for the query with id " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    NES_TRACE("BasePlacementStrategy: For each source operator check for the assignment of network source and sink operators");
    for (auto& sourceOperator : sourceOperators) {
        placeNetworkOperator(queryId, sourceOperator);
    }
}

void BasePlacementStrategy::placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_TRACE("BasePlacementStrategy: Get execution node where operator is placed");
    ExecutionNodePtr executionNode = operatorToExecutionNodeMap[operatorNode->getId()];
    addExecutionNodeAsRoot(executionNode);

    for (const auto& parent : operatorNode->getParents()) {

        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        NES_TRACE("BasePlacementStrategy: Get execution node where parent operator is placed");
        ExecutionNodePtr parentExecutionNode = operatorToExecutionNodeMap[parentOperator->getId()];
        bool allChildrenPlaced = true;
        if (executionNode->getId() != parentExecutionNode->getId()) {
            NES_TRACE("BasePlacementStrategy: Parent and its child operator are placed on different physical node.");

            NES_TRACE(
                "BasePlacementStrategy: Find the nodes between the topology node (inclusive) for child and parent operators.");
            TopologyNodePtr sourceNode = executionNode->getTopologyNode();
            TopologyNodePtr destinationNode = parentExecutionNode->getTopologyNode();
            std::vector<TopologyNodePtr> nodesBetween = topology->findNodesBetween(sourceNode, destinationNode);
            TopologyNodePtr previousParent = nullptr;

            NES_TRACE("BasePlacementStrategy: For all the nodes between the topology node for child and parent operators add "
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
            for (std::size_t i = static_cast<std::size_t>(0ul); i < nodesBetween.size(); ++i) {

                NES_TRACE("BasePlacementStrategy: Find the execution node for the topology node.");
                ExecutionNodePtr candidateExecutionNode = getExecutionNode(nodesBetween[i]);
                NES_ASSERT2_FMT(candidateExecutionNode, "Invalid candidate execution node while placing query " << queryId);
                if (i == 0) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with child operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetUpStreamOperator = querySubPlan->getOperatorWithId(operatorNode->getId());
                        if (targetUpStreamOperator) {
                            NES_TRACE("BasePlacementStrategy: Add network sink operator as root of the query plan with child "
                                      "operator.");
                            OperatorNodePtr networkSink =
                                createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                            targetUpStreamOperator->addParent(networkSink);
                            querySubPlan->removeAsRootOperator(targetUpStreamOperator);
                            querySubPlan->addRootOperator(networkSink);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_ERROR("BasePlacementStrategy: unable to place network sink operator for the child operator");
                        throw Exception("BasePlacementStrategy: unable to place network sink operator for the child operator");
                    }
                } else if (i == nodesBetween.size() - 1) {
                    NES_TRACE("BasePlacementStrategy: Find the query plan with parent operator.");
                    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
                    auto sinkNode = previousParent = nodesBetween[i - 1];
                    OperatorNodePtr sourceOperator =
                        createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId, sinkNode);
                    bool found = false;
                    for (auto& querySubPlan : querySubPlans) {
                        OperatorNodePtr targetDownstreamOperator = querySubPlan->getOperatorWithId(parentOperator->getId());
                        if (targetDownstreamOperator) {
                            NES_TRACE("BasePlacementStrategy: add network source operator as child to the parent operator.");
                            targetDownstreamOperator->addChild(sourceOperator);
                            allChildrenPlaced =
                                (parentOperator->getChildren().size() == targetDownstreamOperator->getChildren().size());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        NES_WARNING("BasePlacementStrategy: unable to place network source operator for the parent operator");
                        throw Exception("BasePlacementStrategy: unable to place network source operator for the parent operator");
                    }
                } else {

                    NES_TRACE("BasePlacementStrategy: Create a new query plan and add pair of network source and network sink "
                              "operators.");
                    QueryPlanPtr querySubPlan = QueryPlan::create();
                    querySubPlan->setQueryId(queryId);
                    querySubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());

                    NES_TRACE("BasePlacementStrategy: add network source operator");
                    auto sinkNode = previousParent = nodesBetween[i - 1];
                    NES_ASSERT2_FMT(sinkNode, "Invalid sink node while placing query " << queryId);
                    const OperatorNodePtr networkSource =
                        createNetworkSourceOperator(queryId, inputSchema, sourceOperatorId, sinkNode);
                    querySubPlan->appendOperatorAsNewRoot(networkSource);

                    NES_TRACE("BasePlacementStrategy: add network sink operator");
                    sourceOperatorId = Util::getNextOperatorId();
                    OperatorNodePtr networkSink = createNetworkSinkOperator(queryId, sourceOperatorId, nodesBetween[i + 1]);
                    querySubPlan->appendOperatorAsNewRoot(networkSink);

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

}// namespace NES::Optimizer
