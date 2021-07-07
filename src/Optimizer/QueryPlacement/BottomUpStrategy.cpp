/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<BottomUpStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(std::move(globalExecutionPlan),
                                                               std::move(topology),
                                                               std::move(typeInferencePhase),
                                                               std::move(streamCatalog)));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO("BottomUpStrategy: Performing placement of the input query plan with id " << queryId);
        NES_INFO("BottomUpStrategy: And query plan \n" << queryPlan->toString());

        NES_DEBUG("BottomUpStrategy: Get all source operators");
        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.empty()) {
            NES_ERROR("BottomUpStrategy: No source operators found in the query plan wih id: " << queryId);
            throw Exception("BottomUpStrategy: No source operators found in the query plan wih id: "
                            + std::to_string(queryId));
        }

        NES_DEBUG("BottomUpStrategy: map pinned operators to the physical location");
        mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);

        NES_DEBUG("BottomUpStrategy: Get all sink operators");
        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        if (sinkOperators.empty()) {
            NES_ERROR("BottomUpStrategy: No sink operators found in the query plan wih id: " << queryId);
            throw Exception("BottomUpStrategy: No sink operators found in the query plan wih id: " + std::to_string(queryId));
        }

        NES_DEBUG("BottomUpStrategy: place query plan with id : " << queryId);
        placeQueryPlanOnTopology(queryPlan);
        NES_DEBUG("BottomUpStrategy: Add system generated operators for query with id : " << queryId);
        addNetworkSourceAndSinkOperators(queryPlan);
        NES_DEBUG("BottomUpStrategy: clear the temporary map : " << queryId);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();
        NES_DEBUG("BottomUpStrategy: Run type inference phase for query plans in global execution plan for query with id : "
                  << queryId);

        NES_DEBUG("BottomUpStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
        return runTypeInferencePhase(queryId);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

std::unordered_set<ExecutionNodePtr> BottomUpStrategy::updateGlobalExecutionPlanPartial(QueryPlanPtr queryPlan){

    executionNodesCreatedDuringPartialPlacement.clear();
    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO("BottomUpStrategy: Performing partial placement of the input query plan with id " << queryId);
        NES_INFO("BottomUpStrategy: And query plan \n" << queryPlan->toString());

        NES_DEBUG("BottomUpStrategy: Get all source operators");
        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        std::vector<TopologyNodePtr> sourceNodes;

        if (sourceOperators.empty()) {
            NES_ERROR("BottomUpStrategy: No source operators found in the query plan wih id: " << queryId);
            throw Exception("BottomUpStrategy: No source operators found in the query plan wih id: " + std::to_string(queryId));
        }
        for(auto& sourceOperator : sourceOperators){
            if(!sourceOperator->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()){
                throw Exception("BottomUpStrategy: Partial placement on QueryPlan requires all Source Operators to be NetworkSources " + std::to_string(queryId));
            }
            sourceNodes.push_back(topology->findNodeWithId(sourceOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getSinkTopologyNode()));
        }

        NES_DEBUG("BottomUpStrategy: Get all sink operators");
        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        std::vector<TopologyNodePtr> sinkNodes;
        if (sinkOperators.empty()) {
            NES_ERROR("BottomUpStrategy: No sink operators found in the query plan wih id: " << queryId);
            throw Exception("BottomUpStrategy: No sink operators found in the query plan wih id: " + std::to_string(queryId));
        }

        for(auto& sinkOperator : sinkOperators){
            if(!sinkOperator->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()){
                throw Exception("BottomUpStrategy: Partial placement on QueryPlan requires all Sink Operators to be NetworkSinks " + std::to_string(queryId));
            }
           sinkNodes.push_back(topology->findNodeWithId(sinkOperator->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>()->getNodeLocation().getNodeId()));
        }
        if(sinkNodes.empty())
            throw Exception("BottomUpStrategy: Node root node found for partial placement");
        if(sinkNodes.size()< 1){
            auto id = sinkNodes[0]->getId();
            for(auto const& sinkNode : sinkNodes){
                if(sinkNode->getId() != id)
                    throw Exception("BottomUpStrategy: Partial placement requires all sinks to be placed on same node");
            }
        }

        //TODO: change if more than one sink node is allowed
        auto rootNode = sinkNodes[0];
        NES_DEBUG("BottomUpStrategy: map pinned operators to the physical location");
        mapPinnedOperatorToTopologyNodes(queryId, queryPlan, sourceNodes, rootNode);

        NES_DEBUG("BottomUpStrategy: place query plan with id : " << queryId);
        placeQueryPlanOnTopology(queryPlan,true);
        NES_DEBUG("BottomUpStrategy: Add system generated operators for query with id : " << queryId);
        addNetworkSourceAndSinkOperators(queryPlan, true);
        NES_DEBUG("BottomUpStrategy: clear the temporary map : " << queryId);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();
        NES_DEBUG("BottomUpStrategy: Run type inference phase for query plans in global execution plan for query with id : "
                      << queryId);

        NES_DEBUG("BottomUpStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
        return executionNodesCreatedDuringPartialPlacement;
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}


void BottomUpStrategy::placeQueryPlanOnTopology(const QueryPlanPtr& queryPlan, bool partialPlacement) {

    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    for (auto& sourceOperator : sourceOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sourceOperator->getId());
        if (candidateTopologyNode->getAvailableResources() == 0 && !partialPlacement) {
            NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            throw Exception(

                "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator"
                + std::to_string(queryId));
        }
        placeOperatorOnTopologyNode(queryId, sourceOperator, candidateTopologyNode, partialPlacement);
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::placeOperatorOnTopologyNode(QueryId queryId,
                                                   const OperatorNodePtr& operatorNode,
                                                   TopologyNodePtr candidateTopologyNode, bool partialPlacement) {

    bool isNetworkSource = false;
    bool isSourceLogicalOperator = operatorNode->instanceOf<SourceLogicalOperatorNode>();
    if(isSourceLogicalOperator){
        isNetworkSource = operatorNode->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>();
    }

    bool isNetworkSink = false;
    bool isSinkLogicalOperator = operatorNode->instanceOf<SinkLogicalOperatorNode>();
    if(isSinkLogicalOperator){
        isNetworkSink = operatorNode->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>();
    }

    //if OP = networksource-> only do operatorToExecutionNodeMap

    //if OP = networkSnk -> only do opertorToExecutionNodeMap, where exec Node = root top node
    if(!isNetworkSource && !isNetworkSink) {

        NES_DEBUG("BottomUpStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("BottomUpStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("BottomUpStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
            if (childTopologyNodes.empty()) {
                NES_WARNING("BottomUpStrategy: No topology node found where child operators are placed.");
                return;
            }

            NES_TRACE("BottomUpStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR(
                    "BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
                    << operatorNode->getId());
                topology->print();
                throw Exception("BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("BottomUpStrategy: Received Sink operator for placement.");
                auto pinnedSinkOperatorLocation = getTopologyNodeForPinnedOperator(operatorNode->getId());

                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR("BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be placed.");
                    throw Exception(

                        "BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw Exception("BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {

            NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : "
                              << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
            throw Exception("BottomUpStrategy: No node available for further placement of operators");
        }

        if(candidateTopologyNode->getParents().empty() && partialPlacement){
            NES_DEBUG("BottomUpStrategy: During partial placement we do not allow placement on the root node");
            throw Exception("BottomUpStrategy: During partial placement we do not allow placement on the root node");
        }

        NES_TRACE("BottomUpStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("BottomUpStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlan(queryId, operatorNode, candidateExecutionNode);
        candidateQueryPlan->appendOperatorAsNewRoot(operatorNode->copy());

        NES_TRACE("BottomUpStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
            throw Exception("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query "
                            + std::to_string(queryId));
        }
        NES_TRACE("BottomUpStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("BottomUpStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        if(partialPlacement)
            executionNodesCreatedDuringPartialPlacement.insert(candidateExecutionNode);
        NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    }
    else {
        if(isNetworkSource) {
            operatorToExecutionNodeMap[operatorNode->getId()] = getExecutionNode(candidateTopologyNode);
            //FIXME: we are considering only one root node currently
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
        }
        if(isNetworkSink) {
            operatorToExecutionNodeMap[operatorNode->getId()] = getExecutionNode(candidateTopologyNode->getAllRootNodes()[0]->as<TopologyNode>());
        }
    }

    NES_TRACE("BottomUpStrategy: Place the parent operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperatorOnTopologyNode(queryId, parent->as<OperatorNode>(), candidateTopologyNode);
    }
}

QueryPlanPtr BottomUpStrategy::getCandidateQueryPlan(QueryId queryId,
                                                     const OperatorNodePtr& operatorNode,
                                                     const ExecutionNodePtr& executionNode) {

    NES_DEBUG("BottomUpStrategy: Get candidate query plan for the operator " << operatorNode << " on execution node with id "
                                                                             << executionNode->getId());
    NES_TRACE("BottomUpStrategy: Get all query sub plans for the query id on the execution node.");
    std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
    QueryPlanPtr candidateQueryPlan;
    if (querySubPlans.empty()) {
        NES_TRACE("BottomUpStrategy: no query plan exists for this query on the executionNode. Returning an empty query plan.");
        candidateQueryPlan = QueryPlan::create();
        candidateQueryPlan->setQueryId(queryId);
        candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
        return candidateQueryPlan;
    }

    std::vector<QueryPlanPtr> queryPlansWithChildren;
    NES_TRACE("BottomUpStrategy: Find query plans with child operators for the input logical operator.");
    std::vector<NodePtr> children = operatorNode->getChildren();
    //NOTE: we do not check for parent operators as we are performing bottom up placement.
    for (auto& child : children) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](const QueryPlanPtr& querySubPlan) {
            return querySubPlan->hasOperatorWithId(child->as<OperatorNode>()->getId());
        });

        if (found != querySubPlans.end()) {
            NES_TRACE("BottomUpStrategy: Found query plan with child operator " << child);
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    if (!queryPlansWithChildren.empty()) {
        executionNode->updateQuerySubPlans(queryId, querySubPlans);
        if (queryPlansWithChildren.size() > 1) {
            NES_TRACE("BottomUpStrategy: Found more than 1 query plan with the child operators of the input logical operator.");
            candidateQueryPlan = QueryPlan::create();
            candidateQueryPlan->setQueryId(queryId);
            candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
            NES_TRACE("BottomUpStrategy: Prepare a new query plan and add the root of the query plans with parent operators as "
                      "the root of the new query plan.");
            for (auto& queryPlanWithChildren : queryPlansWithChildren) {
                for (auto& root : queryPlanWithChildren->getRootOperators()) {
                    candidateQueryPlan->addRootOperator(root);
                }
            }
            NES_TRACE("BottomUpStrategy: return the updated query plan.");
            return candidateQueryPlan;
        }
        if (queryPlansWithChildren.size() == 1) {
            NES_TRACE("BottomUpStrategy: Found only 1 query plan with the child operator of the input logical operator. "
                      "Returning the query plan.");
            return queryPlansWithChildren[0];
        }
    }
    NES_TRACE("BottomUpStrategy: no query plan exists with the child operator of the input logical operator. Returning an empty "
              "query plan.");
    candidateQueryPlan = QueryPlan::create();
    candidateQueryPlan->setQueryId(queryId);
    candidateQueryPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    return candidateQueryPlan;
}

std::vector<TopologyNodePtr> BottomUpStrategy::getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode) {

    std::vector<TopologyNodePtr> childTopologyNodes;
    NES_DEBUG("BottomUpStrategy: Get topology nodes with children operators");
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (auto& child : children) {
        const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
        if (found == operatorToExecutionNodeMap.end()) {
            NES_WARNING("BottomUpStrategy: unable to find topology for child operator.");
            return {};
        }
        TopologyNodePtr childTopologyNode = found->second->getTopologyNode();
        childTopologyNodes.push_back(childTopologyNode);
    }
    NES_DEBUG("BottomUpStrategy: returning list of topology nodes where children operators are placed");
    return childTopologyNodes;
}

}// namespace NES::Optimizer
