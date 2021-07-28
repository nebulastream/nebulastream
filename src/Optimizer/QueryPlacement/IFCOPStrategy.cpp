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
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<IFCOPStrategy> IFCOPStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                     NES::TopologyPtr topology,
                                                     NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                     NES::StreamCatalogPtr streamCatalog) {
    return std::make_unique<IFCOPStrategy>(IFCOPStrategy(std::move(globalExecutionPlan),
                                                         std::move(topology),
                                                         std::move(typeInferencePhase),
                                                         std::move(streamCatalog)));
}

IFCOPStrategy::IFCOPStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                             NES::TopologyPtr topology,
                             NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                             NES::StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool IFCOPStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    // initiate operatorIdToNodePlacementMap
    initiateTopologyNodeIdToIndexMap();

    // Search for an operator placement candidate with the lowest cost
    // 1. get a placement candidate
    auto currentCandidate = getPlacementCandidate(queryPlan);

    // 2. compute the cost of the current candidate
    auto currentCost = getCost(currentCandidate, queryPlan, 1);

    // 3. prepare variables to store information about the best candidate we obtain so far
    auto bestCandidate = currentCandidate;
    auto bestCost = currentCost;

    // 4. perform iteration to search for the candidate with the lowest cost
    const uint32_t maxIter = 100;// maximum iteration to search for an optimal placement candidate
    for (uint32_t i = 1; i < maxIter; i++) {
        // 4.1. get a new candidate
        currentCandidate = getPlacementCandidate(queryPlan);
        // 4.2. compute the cost of the current candidate
        currentCost = getCost(currentCandidate, queryPlan, 1);

        // 4.3. ceck if the current candidate has the lowest cost then the current best candidate
        if (currentCost < bestCost) {
            // 4.3.1 update the current best candidate if that is the case
            bestCandidate = currentCandidate;
            bestCost = currentCost;
        }

        NES_TRACE("IFCOP: currentCost: " << currentCost);
    }

    // 5. assign the PlacementMatrix of the current candidate to the actual execution plan
    assignMappingToTopology(topology, queryPlan, bestCandidate);

    // 6. complete the placement with system-generated network sources and sinks
    addNetworkSourceAndSinkOperators(queryPlan);

    // 7. run the type inference phase
    return runTypeInferencePhase(queryPlan->getQueryId());
}

PlacementMatrix IFCOPStrategy::getPlacementCandidate(NES::QueryPlanPtr queryPlan) {

    PlacementMatrix placementCandidate;

    std::map<std::pair<OperatorId, uint64_t>, std::pair<uint64_t, uint64_t>> matrixMapping;

    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());

    uint64_t topoIdx = 0;

    // prepare the mapping
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        // add a new entry in the binary mapping for the current node
        std::vector<bool> currentTopologyNodeMapping;
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);

        uint64_t opIdx = 0;
        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            auto currentEntry =
                std::make_pair(std::make_pair((*topoItr)->as<TopologyNode>()->getId(), (*qPlanItr)->as<OperatorNode>()->getId()),
                               std::make_pair(topoIdx, opIdx));
            matrixMapping.insert(currentEntry);
            currentTopologyNodeMapping.push_back(false);
            ++opIdx;
        }
        topoIdx++;
        placementCandidate.push_back(currentTopologyNodeMapping);
    }

    // perform the assignment
    std::map<TopologyNodePtr, std::vector<std::string>> topologyNodeToStreamName;
    std::vector<OperatorId> placedOperatorIds;// bookkeeping: each operator should be placed once
    // loop over all logical stream
    for (auto srcOp : queryPlan->getSourceOperators()) {
        LogicalOperatorNodePtr currentOperator = srcOp;
        for (auto topologyNode : streamCatalog->getSourceNodesForLogicalStream(srcOp->getSourceDescriptor()->getStreamName())) {
            TopologyNodePtr currentTopologyNodePtr = topologyNode;

            topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].first;
            auto opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].second;

            // place the current source operator here if no source operator for the same logical source is placed
            if (std::find(placedOperatorIds.begin(), placedOperatorIds.end(), srcOp->getId()) == placedOperatorIds.end()
                && std::find(topologyNodeToStreamName[topologyNode].begin(),
                             topologyNodeToStreamName[topologyNode].end(),
                             srcOp->getSourceDescriptor()->getStreamName())
                    == topologyNodeToStreamName[topologyNode].end()) {
                placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
                placedOperatorIds.push_back(currentOperator->getId());

                // bookkeeping the assignment of source operators
                if (topologyNodeToStreamName.find(topologyNode) == topologyNodeToStreamName.end()) {
                    std::vector<std::string> placedLogicalStreams = {srcOp->getSourceDescriptor()->getStreamName()};
                    topologyNodeToStreamName.insert(std::make_pair(topologyNode, placedLogicalStreams));
                } else {
                    topologyNodeToStreamName[topologyNode].push_back(srcOp->getSourceDescriptor()->getStreamName());
                }

                // placing the rest of the operator except the sink
                // prepare a random generator with a uniform distriution
                std::random_device rd;
                std::mt19937 mt(rd());
                std::uniform_real_distribution<double> dist(0.0, 1.0);

                // we have 50% chance of continuing placing the next operator in the current topology node
                const double stopChance = 0.5;

                // traverse from the current topology node (i.e., source node) to the last node before the sink node
                while (currentTopologyNodePtr != topology->getRoot()) {
                    // draw a random decission whether to stop or continue placing the current operator
                    auto stop = dist(mt) > stopChance;
                    // while not stop and the current operator is not a sink operator, place the next parent operator in the query plan
                    while (!stop
                           && !currentOperator->getParents()[0]
                                   ->instanceOf<SinkLogicalOperatorNode>()) {// assuming one sink operator
                        currentOperator =
                            currentOperator->getParents()[0]->as<LogicalOperatorNode>();// assuming one parent per operator

                        // get the index of current topology node and operator in the PlacementCandidate
                        topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].first;
                        opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].second;

                        // set the Placement decision in the current topology and operator index to true
                        placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
                        placedOperatorIds.push_back(currentOperator->getId());

                        // draw a random decision if we should stop or continue after this placement
                        stop = dist(mt) > stopChance;
                    }

                    // traverse to the parent of the current topology node
                    currentTopologyNodePtr =
                        currentTopologyNodePtr->getParents()[0]->as<TopologyNode>();// assuming one parent per operator
                }
            }
        }
    }

    auto currentTopologyNodePtr = topology->getRoot();
    // check all un-assigned operators, including the sink
    QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto qPlanIter = queryPlanIterator.begin(); qPlanIter != NES::QueryPlanIterator::end(); ++qPlanIter) {
        auto currentOpId = (*qPlanIter)->as<LogicalOperatorNode>()->getId();
        if (std::find(placedOperatorIds.begin(), placedOperatorIds.end(), currentOpId) == placedOperatorIds.end()) {
            // if the current operator id is not in placedOperatorIds, then place the current operator at the sink
            topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOpId)].first;
            auto opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOpId)].second;

            placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
            placedOperatorIds.push_back(currentOpId);
        }
    }
    return placementCandidate;
}

double IFCOPStrategy::getCost(const PlacementMatrix& placementCandidate, NES::QueryPlanPtr queryPlan, double costRatio) {
    double totalCost = 0.0;

    // compute over-utilization cost
    uint32_t nodeIndex = 0;
    uint32_t overutilizationCost = 0;
    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        auto currentTopologyNode = (*topoItr)->as<TopologyNode>();
        auto totalAssignedOperators =
            std::count_if(placementCandidate[nodeIndex].begin(), placementCandidate[nodeIndex].end(), [](bool item) {
                return item;
            });
        if (totalAssignedOperators > currentTopologyNode->getAvailableResources()) {
            overutilizationCost += (totalAssignedOperators - currentTopologyNode->getAvailableResources());
        }
        nodeIndex++;
    }

    totalCost += costRatio * getNetworkCost(topology->getRoot(), placementCandidate, std::move(queryPlan)) + (1-costRatio) * overutilizationCost;
    return totalCost;
}

double IFCOPStrategy::getLocalCost(const std::vector<bool>& nodePlacement, NES::QueryPlanPtr queryPlan) {
    QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);
    uint32_t opIdx = 0;

    double cost = 1.0;
    for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
        auto currentOperator = (*qPlanItr)->as<OperatorNode>();

        double dmf = 1; // fallback if the DMF property does not exist in the current operator
        if (currentOperator->checkIfPropertyExist("DMF")) {
            dmf = std::any_cast<double>(currentOperator->getProperty("DMF"));
        }
        cost = cost * (nodePlacement[opIdx] * dmf + (1 - nodePlacement[opIdx]));
    }
    return cost;
}

double IFCOPStrategy::getNetworkCost(const TopologyNodePtr currentNode,
                                     const PlacementMatrix& placementCandidate,
                                     NES::QueryPlanPtr queryPlan) {

    auto currentNodeCost = getLocalCost(placementCandidate[topologyNodeIdToIndexMap[currentNode->getId()]], queryPlan);

    if (!currentNode->getChildren().empty()) {
        double childCost = 0;

        for (const auto& node : currentNode->getChildren()) {
            auto childNode = node->as<TopologyNode>();
            childCost += getNetworkCost(childNode, placementCandidate, queryPlan);
        }
        double cost = childCost * currentNodeCost;
        return cost;
    }

    return currentNodeCost;
}

void IFCOPStrategy::initiateTopologyNodeIdToIndexMap() {
    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());
    uint32_t topoIdx = 0;
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        NES_DEBUG("IFCOP::DEBUG:: topoid=" << (*topoItr)->as<TopologyNode>()->getId());
        topologyNodeIdToIndexMap.insert({(*topoItr)->as<TopologyNode>()->getId(), topoIdx});
        topoIdx++;
    }
}

}// namespace NES::Optimizer
