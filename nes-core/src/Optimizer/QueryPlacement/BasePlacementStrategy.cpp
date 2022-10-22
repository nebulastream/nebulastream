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

#include "GRPC/WorkerRPCClient.hpp"
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
#include <Util/Logger/Logger.hpp>
#include <log4cxx/helpers/exception.h>
#include <stack>
#include <utility>
#include <numeric>
#include <FaultTolerance/FaultToleranceConfiguration.hpp>

namespace NES::Optimizer {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                             TopologyPtr topologyPtr,
                                             TypeInferencePhasePtr typeInferencePhase)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topologyPtr)),
      typeInferencePhase(std::move(typeInferencePhase)) {}

bool BasePlacementStrategy::updateGlobalExecutionPlan(QueryPlanPtr /*queryPlan*/) { NES_NOT_IMPLEMENTED(); }

void BasePlacementStrategy::pinOperators(QueryPlanPtr queryPlan, TopologyPtr topology, NES::Optimizer::PlacementMatrix& matrix) {
    matrix.size();
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
                // if the value of the matrix at (i,j) is 1, then add a PINNED_NODE_ID of the topologyNodes[i] to operators[j]
                operators[j]->as<OperatorNode>()->addProperty("PINNED_NODE_ID", topologyNodes[i]->getId());
            }
        }
    }
}

void BasePlacementStrategy::performPathSelection(std::vector<OperatorNodePtr> upStreamPinnedOperators,
                                                 std::vector<OperatorNodePtr> downStreamPinnedOperators) {

    //1. Find the topology nodes that will host upstream operators

    std::set<TopologyNodePtr> topologyNodesWithUpStreamPinnedOperators;
    for (const auto& pinnedOperator : upStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw log4cxx::helpers::Exception(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findNodeWithId(nodeId);
        //NOTE: Add the physical node to the set (we used set here to prevent inserting duplicate physical node in-case of self join or
        // two physical sources located on same physical node)
        topologyNodesWithUpStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    //2. Find the topology nodes that will host downstream operators

    std::set<TopologyNodePtr> topologyNodesWithDownStreamPinnedOperators;
    for (const auto& pinnedOperator : downStreamPinnedOperators) {
        auto value = pinnedOperator->getProperty(PINNED_NODE_ID);
        if (!value.has_value()) {
            throw log4cxx::helpers::Exception(
                "LogicalSourceExpansionRule: Unable to find pinned node identifier for the logical operator "
                + pinnedOperator->toString());
        }
        auto nodeId = std::any_cast<uint64_t>(value);
        auto nodeWithPhysicalSource = topology->findNodeWithId(nodeId);
        topologyNodesWithDownStreamPinnedOperators.insert(nodeWithPhysicalSource);
    }

    //3. Performs path selection

    std::vector upstreamTopologyNodes(topologyNodesWithUpStreamPinnedOperators.begin(),
                                      topologyNodesWithUpStreamPinnedOperators.end());
    std::vector downstreamTopologyNodes(topologyNodesWithDownStreamPinnedOperators.begin(),
                                        topologyNodesWithDownStreamPinnedOperators.end());
    std::vector<TopologyNodePtr> selectedTopologyForPlacement =
        topology->findPathBetween(upstreamTopologyNodes, downstreamTopologyNodes);
    if (selectedTopologyForPlacement.empty()) {
        throw log4cxx::helpers::Exception("BasePlacementStrategy: Could not find the path for placement.");
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
        throw log4cxx::helpers::Exception("BasePlacementStrategy: Topology node with id " + std::to_string(nodeId)
                                          + " not considered for the placement.");
    }

    if (found->second->getAvailableResources() == 0 && !operatorToExecutionNodeMap.contains(nodeId)) {
        NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        throw log4cxx::helpers::Exception(
            "BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
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
            auto sinks = querySubPlan->getOperatorByType<SinkLogicalOperatorNode>();
            for (const auto& sink : sinks) {
                auto sinkDescriptor = sink->getSinkDescriptor()->as<SinkDescriptor>();
                sinkDescriptor->setFaultToleranceType(faultToleranceType);
                sink->setSinkDescriptor(sinkDescriptor);
            }
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
                        throw log4cxx::helpers::Exception(
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
                        throw log4cxx::helpers::Exception(
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
                        throw log4cxx::helpers::Exception(
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
                throw log4cxx::helpers::Exception("BasePlacementStrategy: failed to add execution node as root");
            }
        }
    }
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

std::vector<TopologyNodePtr> BasePlacementStrategy::getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode) {
    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("BasePlacementStrategy: Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("BasePlacementStrategy: unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        childTopologyNodes.push_back(childTopologyNode);
    }
    NES_DEBUG("BasePlacementStrategy: returning list of topology nodes where children operators are placed");
    return childTopologyNodes;
}

QueryPlanPtr BasePlacementStrategy::getCandidateQueryPlanForOperator(QueryId queryId,
                                                                     const OperatorNodePtr& operatorNode,
                                                                     const ExecutionNodePtr& executionNode) {

    NES_DEBUG("BasePlacementStrategy: Get candidate query plan for the operator " << operatorNode << " on execution node with id "
                                                                                  << executionNode->getId());
    NES_TRACE("BasePlacementStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("BottomUpStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
        return candidateQueryPlan;
    }

    // Find query plans containing the child operator
    std::vector<QueryPlanPtr> queryPlansWithChildren;
    NES_TRACE("BasePlacementStrategy: Find query plans with child operators for the input logical operator.");
    std::vector<NodePtr> children = operatorNode->getChildren();
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(child->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("BasePlacementStrategy: Found query plan with child operator " << child);
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        // if there are more than 1 query plans containing the child operator, the create a new query plan, add root operators on
        // it, and return the created query plan
        if (queryPlansWithChildren.size() > 1) {
            NES_TRACE(
                "BasePlacementStrategy: Found more than 1 query plan with the child operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE(
                "BasePlacementStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            NES_TRACE("BasePlacementStrategy: return the updated query plan.");
            return candidateQueryPlan;
        }
        // if there is only 1 plan containing the child operator, then return that query plan
        if (queryPlansWithChildren.size() == 1) {
            NES_TRACE("BasePlacementStrategy: Found only 1 query plan with the child operator of the input logical operator. "
                      "Returning the query plan.");
            return queryPlansWithChildren[0];
        }
    }
    NES_TRACE(
        "BasePlacementStrategy: no query plan exists with the child operator of the input logical operator. Returning an empty "
        "query plan.");
    candidateQueryPlan = QueryPlan::create(queryId, PlanIdGenerator::getNextQuerySubPlanId());
    return candidateQueryPlan;
}

std::vector<ExecutionNodePtr> BasePlacementStrategy::getNeighborNodes(const ExecutionNodePtr& executionNode, int levelsLower, int targetDepth){

    std::vector<ExecutionNodePtr> answer;

    if(targetDepth - levelsLower == 0){
            answer.push_back(executionNode);
    }else{
        for(auto& child : executionNode->getChildren()){
            std::vector<ExecutionNodePtr> answer2 = getNeighborNodes(child->as<ExecutionNode>(), levelsLower + 1, targetDepth);
            answer.insert(answer.end(), answer2.begin(), answer2.end());
        }
    }

    return answer;

}

int BasePlacementStrategy::getDepth(const GlobalExecutionPlanPtr& globalExecutionPlan, const ExecutionNodePtr& executionNode, int counter){

    std::vector<ExecutionNodePtr> rootNodes = globalExecutionPlan->getRootNodes();

    for(auto& parent : executionNode->getParents()){


        counter++;
        ExecutionNodePtr exParent = parent->as<ExecutionNode>();
        if(std::find(rootNodes.begin(), rootNodes.end(), exParent)!= rootNodes.end()){
            return counter;
        }else{
            for(auto& thisParent : executionNode->getParents()){

                ExecutionNodePtr thisExParent = thisParent->as<ExecutionNode>();
                return getDepth(globalExecutionPlan, thisExParent, counter);
            }
        }
    }

    return counter;
}

/*void BasePlacementStrategy::initDepths(ExecutionNodePtr executionNode, QueryId queryId){

    if(executionNode->getParents().empty()){
        executionNode->setQueryDepth(queryId, 0);
        return;
    }
    NodePtr *min = std::max_element( executionNode->getParents().begin(), executionNode->getParents().end(),
                                [&queryId]( const NodePtr &a, const NodePtr &b)
                                {
                                    return a->as<ExecutionNode>()->getQueryDepth(queryId)
                                        > b->as<ExecutionNode>()->getQueryDepth(queryId);
                                } );

    executionNode->setQueryDepth(queryId, min.);
}*/


/*float BasePlacementStrategy::calcDownstreamLinkWeights(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, ExecutionNodePtr executionNode, QueryId queryId){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    float downstreamLinkWeights = 0;

    for(auto& parent : topNode->getParents()){
        TopologyNodePtr topParent = parent->as<TopologyNode>();
        ExecutionNodePtr exParent = globalExecutionPlan->getExecutionNodeByNodeId(topParent->getId());
        if(globalExecutionPlan->checkIfExecutionNodeExists(topParent->getId()) && exParent->hasQuerySubPlans(queryId)){
            downstreamLinkWeights += calcLinkWeight(topology, executionNode, exParent);
        }
    }

    return downstreamLinkWeights;
}*/

float BasePlacementStrategy::calcLinkWeight(const TopologyPtr& topology, ExecutionNodePtr upstreamNode, const ExecutionNodePtr& downstreamNode){
    TopologyNodePtr topUpstreamNode = topology->findNodeWithId(upstreamNode->getId());
    TopologyNodePtr topDownstreamNode = topology->findNodeWithId(downstreamNode->getId());

    float queueSizeUpstream = calcOutputQueue(topology, upstreamNode);
    float queueSizeDownstream = calcOutputQueue(topology, downstreamNode);
    float networkConnectivity = getNetworkConnectivity(topology, upstreamNode, downstreamNode);
    float processingCapacity = topDownstreamNode->getResourceCapacity();
    float formula = (queueSizeUpstream - queueSizeDownstream) * networkConnectivity * processingCapacity;

    float weight = std::max(static_cast<float>(0), formula);

    return weight;
}


float BasePlacementStrategy::calcOutputQueue(const TopologyPtr& topology, const ExecutionNodePtr& executionNode){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    //TODO get queue-trimming / checkpointing interval
    //TODO get number of stateful operators within one ExecutionNode



    float interval = (rand() % 3 + 0.8);
    float statefulOperators = 0;

    for(auto& subPlanMap : executionNode->getAllQuerySubPlans()){
        for (auto& subPlan : subPlanMap.second){
            for(auto& op : subPlan->getRootOperators()){
                statefulOperators += 1;
            }
        }
    }

    //TODO some nodes have no available buffers?
    if(topNode->getAvailableBuffers() == 0){
        topNode->setAvailableBuffers(rand() % 200000 + 50000);
    }

    float queueSize = (topNode->getAvailableBuffers() / (statefulOperators + 1)) / interval;
    return queueSize;
}

float BasePlacementStrategy::getNetworkConnectivity(const TopologyPtr& topology, const ExecutionNodePtr& upstreamNode, ExecutionNodePtr downstreamNode){
    TopologyNodePtr topUpstreamNode = topology->findNodeWithId(upstreamNode->getId());
    TopologyNodePtr topDownstreamNode = topology->findNodeWithId(downstreamNode->getId());

    return topUpstreamNode->connectivities.find(topDownstreamNode->getId())->second;
}

void BasePlacementStrategy::initNetworkConnectivities(const TopologyPtr& topology, const GlobalExecutionPlanPtr& globalExecutionPlan, QueryId queryId){

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();

    for(auto& exNode : globalExecutionPlan->getExecutionNodesByQueryId(queryId)){

        TopologyNodePtr topNode = topology->findNodeWithId(exNode->getId());
        for(auto& exNodeParent : topNode->getParents()){
            TopologyNodePtr topNodeParent = topology->findNodeWithId(exNodeParent->as<TopologyNode>()->getId());
            if(globalExecutionPlan->checkIfExecutionNodeExists(topNodeParent->getId()) &&
                globalExecutionPlan->getExecutionNodeByNodeId(topNodeParent->getId())->hasQuerySubPlans(queryId)){
                TopologyNodePtr thisTopNodeParent = exNodeParent->as<TopologyNode>();
                auto startIpAddress = topNode->getIpAddress();
                auto startGrpcPort = topNode->getGrpcPort();
                std::string startRpcAddress = startIpAddress + ":" + std::to_string(startGrpcPort);

                auto destIpAddress = topNodeParent->getIpAddress();
                auto destGrpcPort = topNodeParent->getGrpcPort();
                std::string destRpcAddress = destIpAddress + ":" + std::to_string(destGrpcPort);

                int conn = workerRpcClient->getConnectivity(startRpcAddress,destRpcAddress);

                //TODO connection does not work correctly in this setup. should work in regular environments like with
                //the setup in UpstreamBackupTest
                if(conn == -1){
                    conn = (rand() % 100 + 10);
                }

                NES_INFO("NODE#" + std::to_string(exNode->getId()) + " TO NODE#" + std::to_string(exNodeParent->as<TopologyNode>()->getId())
                         + " HAS CONN OF: " + std::to_string(conn));

                topNode->addConnectivity(thisTopNodeParent->getId(), conn);

            }
        }



    }
}

int BasePlacementStrategy::getDelayToRoot(ExecutionNodePtr executionNode, TopologyPtr topology, int delay){

    if(!executionNode->getParents().empty()){
        TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());
        TopologyNodePtr topNodeParent = topology->findNodeWithId(executionNode->getParents()[0]->as<ExecutionNode>()->getId());
        auto workerRpcClient = std::make_shared<WorkerRPCClient>();
        auto startIpAddress = topNode->getIpAddress();
        auto startGrpcPort = topNode->getGrpcPort();
        std::string startRpcAddress = startIpAddress + ":" + std::to_string(startGrpcPort);

        auto destIpAddress = topNodeParent->getIpAddress();
        auto destGrpcPort = topNodeParent->getGrpcPort();
        std::string destRpcAddress = destIpAddress + ":" + std::to_string(destGrpcPort);

        int conn = workerRpcClient->getConnectivity(startRpcAddress,destRpcAddress);

        if(conn == -1){
            conn = (rand() % 100 + 10);
        }

        return delay + getDelayToRoot(executionNode->getParents()[0]->as<ExecutionNode>(), topology, conn);
    }else{
        return delay;
    }
}


void BasePlacementStrategy::initAdjustedCosts(const TopologyPtr& topology, GlobalExecutionPlanPtr globalExecutionPlan, ExecutionNodePtr rootNode, QueryId queryId){

    rootNode->setAdjustedCosts(queryId, rootNode->getOccupiedResources(queryId));



    for(auto& node : getUpstreamTree(topology->findNodeWithId(rootNode->getId()),false)){
        ExecutionNodePtr nodeEx = globalExecutionPlan->getExecutionNodeByNodeId(node->getId());


        for(auto& child : node->getChildren()){
            TopologyNodePtr childTop = child->as<TopologyNode>();

            if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){
                ExecutionNodePtr childEx = globalExecutionPlan->getExecutionNodeByNodeId(child->as<TopologyNode>()->getId());

                float childExOccupiedResources = childEx->getOccupiedResources(queryId);
                float nodeResourceCapacity = node->getResourceCapacity();
                float childExResourceCapacity = childEx->getTopologyNode()->getResourceCapacity();
                float resourceCapacityRatio =  nodeResourceCapacity / childExResourceCapacity;

                float childEffectiveCost = childExOccupiedResources * resourceCapacityRatio;

                if(resourceCapacityRatio > childEx->getDownstreamRatio()){
                    childEx->setDownstreamRatio(resourceCapacityRatio);
                }

                if(childEffectiveCost > childEx->getAdjustedCostsByQueryId(queryId)){
                    childEx->setAdjustedCosts(queryId, childEffectiveCost);
                }
            }
        }
    }

}



/*void BasePlacementStrategy::calcEffectiveValues(const TopologyPtr& topology, GlobalExecutionPlanPtr globalExecutionPlan, std::vector<TopologyNodePtr> topologyNodes, QueryId queryId){









    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();



    for(auto& topNode : topologyNodes){


        auto ipAddress = topNode->getIpAddress();
        auto grpcPort = topNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

        int localLatency = workerRpcClient->getRTT(rpcAddress);

        if(localLatency != (-1)){
            topNode->setLatency(localLatency);
        }else{
            topNode->setLatency(rand() % 500 + 1);
        }
    }

    //prev working: for(auto& enode: executionNodes){
    for(auto& enode: globalExecutionPlan->getExecutionNodesByQueryId(queryId)){

        TopologyNodePtr node = topology->findNodeWithId(enode->getId());

        auto ipAddress = node->getIpAddress();
        auto grpcPort = node->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

 int localAvailableBuffers = workerRpcClient->sayHi(queryId, rpcAddress);

        if(localAvailableBuffers != (-1)){
            node->setAvailableBuffers(localAvailableBuffers);
        }else{
            node->setAvailableBuffers(rand() % 2000 + 500);
        }
    }

    for(auto&root : globalExecutionPlan->getRootNodes()){
        std::vector<TopologyNodePtr> downstreamTree = getUpstreamTree(topology->findNodeWithId(root->getId()),false);

        float latestOccupiedResources = 0;

        for(auto& node : getUpstreamTree(topology->findNodeWithId(root->getId()),false)){

            ExecutionNodePtr nodeEx = globalExecutionPlan->getExecutionNodeByNodeId(node->getId());



            if(node->getEffectiveRessources() != 0){
                float latestOccupiedResources = node->getEffectiveRessources();
            }

            if(globalExecutionPlan->checkIfExecutionNodeExists(node->getId())){

                if(!node->getChildren().empty()){

                    for(auto& child : node->getChildren()){

                        TopologyNodePtr childTop = child->as<TopologyNode>();

                        if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){

                            ExecutionNodePtr childEx = globalExecutionPlan->getExecutionNodeByNodeId(childTop->getId());

                            float childExOccupiedResources = childEx->getOccupiedResources(queryId);
                            float nodeResourceCapacity = node->getResourceCapacity();
                            float childExResourceCapacity = childEx->getTopologyNode()->getResourceCapacity();
                            float resourceCapacityRatio =  nodeResourceCapacity / childExResourceCapacity;

                            if(nodeEx->getOccupiedResources(queryId) != 0){
                                float childEffectiveCost = nodeEx->getOccupiedResources(queryId) * resourceCapacityRatio;
                            }else{
                                float childEffectiveCost = latestOccupiedResources * resourceCapacityRatio;
                            }

                            float childEffectiveCost = nodeEx->getOccupiedResources(queryId) * resourceCapacityRatio;

                            if(childEffectiveCost > childTop->getEffectiveRessources()){
                                childTop->setEffectiveRessources(childEffectiveCost);
                                childTop->addNodeProperty("ressources",std::tuple<int,float>{childEx->getOccupiedResources(queryId),
                                                                                               childEffectiveCost});
                            }
                        }
                    }
                }
                if(node->getParents().empty()){
                    node->addNodeProperty("ressources",std::tuple<int,float>{nodeEx->getOccupiedResources(queryId),
                                                                               nodeEx->getOccupiedResources(queryId)});
                    node->setEffectiveRessources(nodeEx->getOccupiedResources(queryId));
                }
            }
        }
        for (auto& node : getUpstreamTree(topology->findNodeWithId(root->getId()),true)){

            TopologyNodePtr topNode = node->as<TopologyNode>();

            if(node->getChildren().empty() && globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
                topNode->setEffectiveLatency(node->as<TopologyNode>()->getLatency());
                topNode->addNodeProperty(std::to_string(queryId),std::tuple<float,float>{topNode->getLatency(),topNode->getLatency()});
            }

            if(!node->getParents().empty()){
                for (auto& parent : node->getParents()){

                    bool hasExNodeChild = false;

                    for(auto& child : parent->getChildren()){
                        TopologyNodePtr childTop = child->as<TopologyNode>();
                        if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){
                            hasExNodeChild = true;
                            break;
                        }
                    }

                    TopologyNodePtr parentTop = parent->as<TopologyNode>();

                    float parentTopLatency = static_cast<float>(parentTop->getLatency());
                    float floatSize = static_cast<float>(globalExecutionPlan->getAllExecutionNodes().size());
                    float latencyRatio = static_cast<float>(1.0 / (floatSize));
                    float newEffectiveLatency = parentTopLatency * latencyRatio;

                    if(globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
                        if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){


                            if(newEffectiveLatency > parentTop->getEffectiveLatency()){
                                parentTop->setEffectiveLatency(newEffectiveLatency);
                                parentTop->removeNodeProperty("latency");
                                parentTop->addNodeProperty("latency",std::tuple<float,float>{parentTop->getLatency(),newEffectiveLatency});
                            }
                        }
                    }
                    else{
                        if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){
                            if(!hasExNodeChild){
                                parentTop->setEffectiveLatency(parentTop->getLatency());
                                parentTop->removeNodeProperty("latency");
                                parentTop->addNodeProperty("latency",std::tuple<float,float>{parentTop->getLatency(),parentTop->getLatency()});
                            }
                        }
                    }
                }
            }
        }
    }


    for(auto& exNode : executionNodes){
        TopologyNodePtr topNode = topology->findNodeWithId(exNode->getId());
        for(auto& exNodeParent : topNode->getParents()){
            if(globalExecutionPlan->getExecutionNodeByNodeId(topology->findNodeWithId(exNodeParent->as<TopologyNode>()->getId())->getId())->hasQuerySubPlans(queryId)){
                TopologyNodePtr topNodeParent = exNodeParent->as<TopologyNode>();
                auto startIpAddress = topNode->getIpAddress();
                auto startGrpcPort = topNode->getGrpcPort();
                std::string startRpcAddress = startIpAddress + ":" + std::to_string(startGrpcPort);

                auto destIpAddress = topNodeParent->getIpAddress();
                auto destGrpcPort = topNodeParent->getGrpcPort();
                std::string destRpcAddress = destIpAddress + ":" + std::to_string(destGrpcPort);

                int conn = workerRpcClient->getConnectivity(startRpcAddress,destRpcAddress);

                if(conn == -1){
                    conn = (rand() % 100 + 10);
                }

                NES_INFO("NODE#" + std::to_string(exNode->getId()) + " TO NODE#" + std::to_string(exNodeParent->as<TopologyNode>()->getId())
                         + " HAS CONN OF: " + std::to_string(conn));

                if(topNode->hasNodeProperty("connectivity")){
                    std::map<int,int> linkConnecitivites = std::any_cast<std::map<int,int>>(topNode->getNodeProperty("connectivity"));

                    linkConnecitivites.insert({topNodeParent->getId(),conn});
                    topNode->removeNodeProperty("connectivity");
                    topNode->addNodeProperty("connectivity",(topNodeParent->getId(),linkConnecitivites));
                }else{
                    std::map<int,int> map = {{topNodeParent->getId(),conn}};
                    topNode->addNodeProperty("connectivity",map);
                }




            }
        }



    }



}*/

std::vector<TopologyNodePtr> BasePlacementStrategy::getDownstreamTree(const TopologyNodePtr& topNode, bool reversed){

    std::queue<TopologyNodePtr> q;
    std::vector<TopologyNodePtr> answer;

    q.push(topNode);

    while(!q.empty()){
        TopologyNodePtr first = q.front();
        answer.push_back(first);
        q.pop();

        for(auto& parent : first->getParents()){
            q.push(parent->as<TopologyNode>());
        }
    }

    if(reversed){
        std::reverse(begin(answer),end(answer));
    }
    return answer;
}

std::vector<TopologyNodePtr> BasePlacementStrategy::getUpstreamTree(const TopologyNodePtr& topNode, bool reversed){

    std::queue<TopologyNodePtr> q;
    std::vector<TopologyNodePtr> answer;

    q.push(topNode);


    while(!q.empty()){
        TopologyNodePtr first = q.front();
        answer.push_back(first);
        q.pop();

        for(auto& child : first->getChildren()){
            q.push(child->as<TopologyNode>());
        }
    }

    if(reversed){
        std::reverse(begin(answer),end(answer));
    }
    return answer;
}



//TODO solve all cases of random values

std::tuple<float,float,float> BasePlacementStrategy::calcActiveStandby(TopologyPtr topology, const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                         const ExecutionNodePtr& executionNode, int replicas, QueryId queryId){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    std::vector<ExecutionNodePtr> downstreamNodes;
    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        std::vector<ExecutionNodePtr> x = getNeighborNodes(rootNode,0, getDepth(globalExecutionPlan, executionNode, 0) - 1);
        downstreamNodes.insert(downstreamNodes.end(), x.begin(), x.end());
    }


    if(downstreamNodes.size() < static_cast<size_t>(replicas)){
        return {-1,-1,-1};
    }

    auto maxCost= std::max_element( downstreamNodes.begin(), downstreamNodes.end(),
                                [&queryId]( const ExecutionNodePtr &a, const ExecutionNodePtr &b )
                                {
                                    return a->getOccupiedResources(queryId) < b->getOccupiedResources(queryId);
                                } );

    ExecutionNodePtr primary = downstreamNodes[std::distance(downstreamNodes.begin(), maxCost)];
    float downstreamCost = primary->getOccupiedResources(queryId);


    std::map<float,int> ranks;
    std::vector<float> linkWeights(downstreamNodes.size());

    //Get all nodes that are one level downstream. Has to be looped because the globalExecutionPlan may have multiple roots
    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        std::vector<ExecutionNodePtr> x = getNeighborNodes(rootNode,0, getDepth(globalExecutionPlan, executionNode, 0) - 1);
        downstreamNodes.insert(downstreamNodes.end(), x.begin(), x.end());
    }

    //Abort if there are less downstream nodes than required replicas
    if(downstreamNodes.size() < static_cast<size_t>(replicas)){
        return {-1,-1,-1};
    }else{
        //Index linkWeights to nodeIds so that they can be sorted afterwards
        for(auto& downstreamNode : downstreamNodes){
            float linkWeight = calcLinkWeight(topology, executionNode, downstreamNode);
            ranks.insert({linkWeight, downstreamNode->getId()});
            linkWeights.push_back(linkWeight);
        }
    }

    //Sort linkWeights to downstream nodes in descending order so that the first n can be used as replicas.
    std::sort(linkWeights.begin(), linkWeights.end(), std::greater<float>());


    float procCost = 0;
    std::vector<ExecutionNodePtr> downstreamNodesToUse;

    //Get n nodes with the highest linkWeight to the executionNode
    for (int i = 0; i < replicas; ++i) {
        downstreamNodesToUse.push_back(globalExecutionPlan->getExecutionNodeByNodeId(ranks.find(linkWeights[i])->second));
    }

    //Add up the adjustedCosts of placing the operator replicas onto every selected node
        for(auto& replicatedNode : downstreamNodesToUse){
            procCost += (replicatedNode->getDownstreamRatio() * downstreamCost);
        }


    float networkCost = 0;

    float memoryCost = calcOutputQueue(topology, executionNode);

    for(auto& replica : downstreamNodesToUse){
        networkCost += calcLinkWeight(topology, executionNode, globalExecutionPlan->getExecutionNodeByNodeId(replica->getId()));
        memoryCost += calcOutputQueue(topology, globalExecutionPlan->getExecutionNodeByNodeId(replica->getId()));
    }

    return {procCost,networkCost,memoryCost};
}

std::tuple<float,float,float> BasePlacementStrategy::calcCheckpointing(TopologyPtr topology, const GlobalExecutionPlanPtr& globalExecutionPlan, ExecutionNodePtr executionNode, QueryId queryId){

    std::vector<ExecutionNodePtr> downstreamNodes;
    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        std::vector<ExecutionNodePtr> x = getNeighborNodes(rootNode,0, getDepth(globalExecutionPlan, executionNode, 0) - 1);
        downstreamNodes.insert(downstreamNodes.end(), x.begin(), x.end());
    }

    if(downstreamNodes.size() < 2){
        return {-1,-1,-1};
    }

    auto max = std::max_element( downstreamNodes.begin(), downstreamNodes.end(),
                                [&queryId]( const ExecutionNodePtr &a, const ExecutionNodePtr &b )
                                {
                                    return a->getOccupiedResources(queryId) < b->getOccupiedResources(queryId);
                                } );

    auto min = std::min_element( downstreamNodes.begin(), downstreamNodes.end(),
                                [&queryId]( const ExecutionNodePtr &a, const ExecutionNodePtr &b )
                                {
                                    return a->getOccupiedResources(queryId) > b->getOccupiedResources(queryId);
                                } );

    ExecutionNodePtr primary = downstreamNodes[std::distance(downstreamNodes.begin(), max)];
    TopologyNodePtr topPrimary = topology->findNodeWithId(primary->getId());
    ExecutionNodePtr secondary = downstreamNodes[std::distance(downstreamNodes.begin(), min)];
    TopologyNodePtr topSecondary = topology->findNodeWithId(secondary->getId());

    /*
    //TODO get correct operators
    int numOfOperatorsOnPrimary = 0;
    for(auto& subPlan : primary->getQuerySubPlans(queryId)){
        numOfOperatorsOnPrimary += subPlan->getRootOperators().size();
    }

    float procCost = (numOfOperatorsOnPrimary * secondary->getDownstreamRatio());

    //TODO get interval
    float interval = (rand() % 3 + 0.8);

    //TODO some nodes have no available buffers?
    if(topPrimary->getAvailableBuffers() == 0){
        topPrimary->setAvailableBuffers(rand() % 200000 + 50000);
    }

    float networkCost = (calcLinkWeight(topology,executionNode,secondary) / interval);

    //float memoryCost = (topPrimary->getAvailableBuffers() / interval);
    float memoryCost = topPrimary->getAvailableBuffers();*/

    float procCost = -1;
    float networkCost = -1;
    float memoryCost = -1;

    return {procCost, networkCost, memoryCost};

}

std::tuple<float,float,float> BasePlacementStrategy::calcUpstreamBackup(const TopologyPtr& topology, GlobalExecutionPlanPtr globalExecutionPlan, ExecutionNodePtr executionNode, QueryId queryId,
                                                                          FaultToleranceConfigurationPtr ftConfig){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    //TODO get number of operators that are not sink or source
    int numOfOperators = 0;
    for(auto&subPlan : executionNode->getQuerySubPlans(queryId)){
        numOfOperators += subPlan->getRootOperators().size();
    }
    /*
    float procCost = numOfOperators;

    float networkCost = 0;*/

    std::vector<ExecutionNodePtr> downstreamNodes;
    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        std::vector<ExecutionNodePtr> x = getNeighborNodes(rootNode,0, getDepth(globalExecutionPlan, executionNode, 0) - 1);
        downstreamNodes.insert(downstreamNodes.end(), x.begin(), x.end());
    }

    if(downstreamNodes.size() < 2){
        return {-1,-1,-1};
    }

    auto max = std::max_element( downstreamNodes.begin(), downstreamNodes.end(),
                                [&queryId]( const ExecutionNodePtr &a, const ExecutionNodePtr &b )
                                {
                                    return a->getOccupiedResources(queryId) < b->getOccupiedResources(queryId);
                                } );

    auto min = std::max_element( downstreamNodes.begin(), downstreamNodes.end(),
                                [&queryId]( const ExecutionNodePtr &a, const ExecutionNodePtr &b )
                                {
                                    return a->getOccupiedResources(queryId) > b->getOccupiedResources(queryId);
                                } );

    ExecutionNodePtr primary = downstreamNodes[std::distance(downstreamNodes.begin(), max)];
    TopologyNodePtr topPrimary = topology->findNodeWithId(primary->getId());
    ExecutionNodePtr secondary = downstreamNodes[std::distance(downstreamNodes.begin(), min)];
    TopologyNodePtr topSecondary = topology->findNodeWithId(secondary->getId());

    /*
    //Get all nodes that are one level downstream. Has to be looped because the globalExecutionPlan may have multiple roots
    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        std::vector<ExecutionNodePtr> x = getNeighborNodes(rootNode,0, getDepth(globalExecutionPlan, executionNode, 0) - 1);
        downstreamNodes.insert(downstreamNodes.end(), x.begin(), x.end());
    }

    //TODO get interval
    float interval = (rand() % 3 + 0.8);


    networkCost += calcLinkWeight(topology, executionNode, primary) / interval;
    networkCost += calcLinkWeight(topology, executionNode, secondary) / interval;
    for(auto& upstreamNode : getDownstreamTree(topPrimary, false)){
        networkCost += calcDownstreamLinkWeights(topology, globalExecutionPlan, executionNode, queryId) / interval;
    }
    for(auto& upstreamNode : getDownstreamTree(topSecondary, false)){
        networkCost += calcDownstreamLinkWeights(topology, globalExecutionPlan, executionNode, queryId) / interval;
    }*/

    /**
     * New processing formula
     * (ingestion_rate[t/s] / ack_rate[t]) * stateful_operators[t] => [t/s]
     */

    /**
     * New network formula
     * (ingestion_rate[t/s] / ack_rate[t]) * ack_size[b] => [b/s]
     */

    /*
    networkCost += (2 * (calcLinkWeight(topology, executionNode, primary) / interval));

    float memoryCost = calcOutputQueue(topology, executionNode);*/

    /**
     * New memory formula
     * n * tuple_size + ingestion_rate * delay_to_root * tuple_size
     */

    float ackInterval = static_cast<float>(ftConfig->getAckRate()) / static_cast<float>(ftConfig->getIngestionRate());
    float acksSizePerSecond = ftConfig->getAckRate() * ftConfig->getAckSize();

    float procCost = (static_cast<float>(ftConfig->getIngestionRate() * ftConfig->getTupleSize()) + static_cast<float>(acksSizePerSecond / ackInterval)) * executionNode->getDownstreamRatio();

    float networkCost = static_cast<float>(ftConfig->getIngestionRate() / ftConfig->getAckRate()) * static_cast<float>(ftConfig->getAckSize());

    float memoryCost = static_cast<float>(ftConfig->getIngestionRate() * getDelayToRoot(executionNode, topology, 0) * ftConfig->getTupleSize());

    return {procCost,networkCost,memoryCost};

}

FaultToleranceType BasePlacementStrategy::bestApproach(std::vector<float> activeStandbyCosts, std::vector<float> checkpointingCosts, std::vector<float> upstreamBackupCosts){

    std::vector<float> allProcessingCosts = {activeStandbyCosts[0], checkpointingCosts[0], upstreamBackupCosts[0]};
    std::vector<float> allNetworkingCosts = {activeStandbyCosts[1], checkpointingCosts[1], upstreamBackupCosts[1]};
    std::vector<float> allMemoryCosts = {activeStandbyCosts[2], checkpointingCosts[2], upstreamBackupCosts[2]};

    bool activeStandbyPossible = (std::reduce(activeStandbyCosts.begin(), activeStandbyCosts.end()) != -3);
    bool checkpointingPossible = (std::reduce(checkpointingCosts.begin(), checkpointingCosts.end()) != -3);
    bool upstreamBackupPossible = (std::reduce(upstreamBackupCosts.begin(), upstreamBackupCosts.end()) != -3);

    if(!activeStandbyPossible && !checkpointingPossible && !upstreamBackupPossible){
        return FaultToleranceType::NONE;
    }

    if(!activeStandbyPossible){
        allProcessingCosts.erase(allProcessingCosts.begin() + 0);
        allNetworkingCosts.erase(allNetworkingCosts.begin() + 0);
        allMemoryCosts.erase(allMemoryCosts.begin() + 0);
    }
    if(!checkpointingPossible){
        allProcessingCosts.erase(allProcessingCosts.begin() + 1);
        allNetworkingCosts.erase(allNetworkingCosts.begin() + 1);
        allMemoryCosts.erase(allMemoryCosts.begin() + 1);
    }
    if(!upstreamBackupPossible){
        allProcessingCosts.erase(allProcessingCosts.begin() + 2);
        allNetworkingCosts.erase(allNetworkingCosts.begin() + 2);
        allMemoryCosts.erase(allMemoryCosts.begin() + 2);
    }

    float maxProcessingCost = *max_element(std::begin(allProcessingCosts), std::end(allProcessingCosts));
    float maxNetworkingCost = *max_element(std::begin(allNetworkingCosts), std::end(allNetworkingCosts));
    float maxMemoryCost = *max_element(std::begin(allMemoryCosts), std::end(allMemoryCosts));

    float minProcessingCost = *min_element(std::begin(allProcessingCosts), std::end(allProcessingCosts));
    float minNetworkingCost = *min_element(std::begin(allNetworkingCosts), std::end(allNetworkingCosts));
    float minMemoryCost = *min_element(std::begin(allMemoryCosts), std::end(allMemoryCosts));

    std::map<FaultToleranceType,float> faultToleranceScores;

    if(activeStandbyPossible){
        float activeStandbyScore = ((activeStandbyCosts[0] - minProcessingCost) / (maxProcessingCost - minProcessingCost))
            + ((activeStandbyCosts[1] - minNetworkingCost) / (maxNetworkingCost - minNetworkingCost))
            + ((activeStandbyCosts[2] - minMemoryCost) / (maxMemoryCost - minMemoryCost));

        faultToleranceScores.insert({FaultToleranceType::ACTIVE_STANDBY, activeStandbyScore});
    }
    if(checkpointingPossible){
        float checkpointingScore = ((checkpointingCosts[0] - minProcessingCost) / (maxProcessingCost - minProcessingCost))
            + ((checkpointingCosts[1] - minNetworkingCost) / (maxNetworkingCost - minNetworkingCost))
            + ((checkpointingCosts[2] - minMemoryCost) / (maxMemoryCost - minMemoryCost));

        faultToleranceScores.insert({FaultToleranceType::CHECKPOINTING, checkpointingScore});
    }
    if(upstreamBackupPossible){
        float upstreamBackupScore = ((upstreamBackupCosts[0] - minProcessingCost) / (maxProcessingCost - minProcessingCost))
            + ((upstreamBackupCosts[1] - minNetworkingCost) / (maxNetworkingCost - minNetworkingCost))
            + ((upstreamBackupCosts[2] - minMemoryCost) / (maxMemoryCost - minMemoryCost));

        faultToleranceScores.insert({FaultToleranceType::UPSTREAM_BACKUP, upstreamBackupScore});
    }

    //Changed it so that the lowest value is selected
    auto x = std::max_element(faultToleranceScores.begin(), faultToleranceScores.end(),
                                  [](const std::pair<FaultToleranceType, float>& p1, const std::pair<FaultToleranceType, float>& p2) {
                                      return p1.second > p2.second; });

        return x->first;
    



    //float faultToleranceScores[3] = {activeStandbyScore, checkpointingScore, upstreamBackupScore};
    /*const int N = sizeof(faultToleranceScores) / sizeof(int);
    int indexOfBestScore = std::distance(faultToleranceScores, std::max_element(faultToleranceScores, faultToleranceScores + N));

    switch(indexOfBestScore) {
        case 0:
            NES_INFO("FT TO CHOSE: ACTIVE STANDBY");
            return FaultToleranceType::ACTIVE_STANDBY;
        case 1:
            NES_INFO("FT TO CHOSE: CHECKPOINTING");
            return FaultToleranceType::CHECKPOINTING;
        case 2:
            NES_INFO("FT TO CHOSE: UPSTREAM BACKUP");
            return FaultToleranceType::UPSTREAM_BACKUP;
        default:
            return FaultToleranceType::NONE;
    }*/

}

}// namespace NES::Optimizer

//TODO missing info: queue-trimming / checkpointing interval, how to obtain stateful operators, sometimes nodes have no available buffers

