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
    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());
    uint32_t topoIdx = 0;
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        NES_DEBUG("IFCOP::DEBUG:: topoid=" << (*topoItr)->as<TopologyNode>()->getId());
        topologyNodeIdToIndexMap.insert({(*topoItr)->as<TopologyNode>()->getId(), topoIdx});
        topoIdx++;
    }

    auto currentCandidate = getPlacementCandidate(queryPlan);
    auto currentCost = getCost(currentCandidate, queryPlan);
    auto bestCandidate = currentCandidate;
    auto bestCost = currentCost;

    const uint32_t maxIter = 100;// maximum iteration to search for an optimal placement candidate
    for (uint32_t i = 1; i < maxIter; i++) {
        currentCandidate = getPlacementCandidate(queryPlan);
        currentCost = getCost(currentCandidate, queryPlan);

        if (currentCost < bestCost) {
            bestCandidate = currentCandidate;
            bestCost = currentCost;
        }

        NES_DEBUG("IFCOP: currentCost: " << currentCost);
    }

    assignMappingToTopology(topology, queryPlan, bestCandidate);

    addNetworkSourceAndSinkOperators(queryPlan);

    return runTypeInferencePhase(queryPlan->getQueryId());
}
std::vector<std::vector<bool>> IFCOPStrategy::getPlacementCandidate(NES::QueryPlanPtr queryPlan) {

    std::vector<std::vector<bool>> placementCandidate;

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
                std::random_device rd;
                std::mt19937 mt(rd());
                std::uniform_real_distribution<double> dist(0.0, 1.0);

                const double stopChance = 0.5;

                while (currentTopologyNodePtr != topology->getRoot()) {
                    auto stop = dist(mt) > stopChance;
                    while (!stop
                           && !currentOperator->getParents()[0]
                                   ->instanceOf<SinkLogicalOperatorNode>()) {// assuming one sink operator
                        currentOperator =
                            currentOperator->getParents()[0]->as<LogicalOperatorNode>();// assuming one parent per operator

                        topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].first;
                        opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].second;
                        placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
                        placedOperatorIds.push_back(currentOperator->getId());
                        stop = dist(mt) > stopChance;
                    }
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

double IFCOPStrategy::getCost(std::vector<std::vector<bool>> placementCandidate, NES::QueryPlanPtr queryPlan) {
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

    totalCost += getNetworkCost(topology->getRoot(), placementCandidate, std::move(queryPlan)) + overutilizationCost;
    return totalCost;
}
double IFCOPStrategy::getLocalCost(std::vector<bool> nodePlacement, NES::QueryPlanPtr queryPlan) {
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

double IFCOPStrategy::getNetworkCost(const TopologyNodePtr& currentNode,
                                     std::vector<std::vector<bool>> placementCandidate,
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

}// namespace NES::Optimizer
