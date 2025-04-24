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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/QueryPlacementAddition/IFCOPStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <random>
#include <utility>

namespace NES::Optimizer {

//TODO: This is a broken strategy and need to be fixed as part of the issue #2486
BasePlacementStrategyPtr IFCOPStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                               const TopologyPtr& topology,
                                               const TypeInferencePhasePtr& typeInferencePhase,
                                               PlacementAmendmentMode placementAmendmentMode) {
    return std::make_unique<IFCOPStrategy>(
        IFCOPStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode));
}

IFCOPStrategy::IFCOPStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                             const TopologyPtr& topology,
                             const TypeInferencePhasePtr& typeInferencePhase,
                             PlacementAmendmentMode placementAmendmentMode)
    : BasePlacementAdditionStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode) {}

// TODO: fix as part of #2486
/*bool IFCOPStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    // initiate operatorIdToNodePlacementMap
    initiateWorkerIdToIndexMap();

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

        // 4.3. check if the current candidate has the lowest cost then the current best candidate
        if (currentCost < bestCost) {
            // 4.3.1 update the current best candidate if that is the case
            bestCandidate = currentCandidate;
            bestCost = currentCost;
        }

        NES_TRACE("IFCOP: currentCost: {}", currentCost);
    }

    // 5. assign the PlacementMatrix of the current candidate to the actual execution plan
    // TODO 2486: use the new interface
    //assignMappingToTopology(topology, queryPlan, bestCandidate);

    // 6. complete the placement with system-generated network sources and sinks
    //addNetworkSourceAndSinkOperators(queryPlan);

    // 7. run the type inference phase
    //    runTypeInferencePhase(queryPlan->getQueryId());

    return false;
}*/

PlacementMatrix IFCOPStrategy::getPlacementCandidate(NES::QueryPlanPtr) {

    PlacementMatrix placementCandidate;

    // mapping between pair of topologyId and operator id to pair of topology node and operator index in the iterator
    // this helps to determine the index of a topology and operator in their iterator based on their ids
    /*IdToIteratorIndexMapping matrixMapping;

    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());

    uint64_t topoIdx = 0;

    // prepare the mapping
    // TODO: we should avoid iterating the whole topology each time we generate a new placement candidate (#2082)
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        // add a new entry in the binary mapping for the current node
        std::vector<bool> currentTopologyNodeMapping;
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);

        uint64_t opIdx = 0;
        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            auto currentEntry =
                std::make_pair(std::make_pair((*topoItr)->as<TopologyNode>()->getId(), (*qPlanItr)->as<Operator>()->getId()),
                               std::make_pair(topoIdx, opIdx));
            matrixMapping.insert(currentEntry);
            currentTopologyNodeMapping.push_back(false);
            ++opIdx;
        }
        topoIdx++;
        placementCandidate.push_back(currentTopologyNodeMapping);
    }*/

    // perform the assignment
    std::map<TopologyNodePtr, std::vector<std::string>> topologyNodeToSourceName;
    std::vector<OperatorId> placedOperatorIds;// bookkeeping: each operator should be placed once
    // loop over all logical source
    // FIXME: #2486 Dwi: I think we will get this information from source operator's properties
    //    for (auto srcOp : queryPlan->getSourceOperators()) {
    //        LogicalOperatorPtr currentOperator = srcOp;
    //        for (auto topologyNode :
    //             sourceCatalog->getSourceNodesForLogicalSource(srcOp->getSourceDescriptor()->getLogicalSourceName())) {
    //            TopologyNodePtr currentTopologyNodePtr = topologyNode;
    //
    //            topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].first;
    //            auto opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].second;
    //
    //            // place the current source operator here if no source operator for the same logical source is placed
    //            if (std::find(placedOperatorIds.begin(), placedOperatorIds.end(), srcOp->getId()) == placedOperatorIds.end()
    //                && std::find(topologyNodeToSourceName[topologyNode].begin(),
    //                             topologyNodeToSourceName[topologyNode].end(),
    //                             srcOp->getSourceDescriptor()->getLogicalSourceName())
    //                    == topologyNodeToSourceName[topologyNode].end()) {
    //                placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
    //                placedOperatorIds.push_back(currentOperator->getId());
    //
    //                // bookkeeping the assignment of source operators
    //                if (topologyNodeToSourceName.find(topologyNode) == topologyNodeToSourceName.end()) {
    //                    std::vector<std::string> placedLogicalSources = {srcOp->getSourceDescriptor()->getLogicalSourceName()};
    //                    topologyNodeToSourceName.insert(std::make_pair(topologyNode, placedLogicalSources));
    //                } else {
    //                    topologyNodeToSourceName[topologyNode].push_back(srcOp->getSourceDescriptor()->getLogicalSourceName());
    //                }
    //
    //                // placing the rest of the operator except the sink
    //                // prepare a random generator with a uniform distribution
    //                std::random_device rd;
    //                std::mt19937 mt(rd());
    //                std::uniform_real_distribution<double> dist(0.0, 1.0);
    //
    //                // we have 50% chance of continuing placing the next operator in the current topology node
    //                const double stopChance = 0.5;
    //
    //                // traverse from the current topology node (i.e., source node) to the last node before the sink node
    //                while (currentTopologyNodePtr != topology->getRoot()) {
    //                    // draw a random decission whether to stop or continue placing the current operator
    //                    auto stop = dist(mt) > stopChance;
    //                    // while not stop and the current operator is not a sink operator, place the next parent operator in the query plan
    //                    while (!stop
    //                           && !currentOperator->getParents()[0]
    //                                   ->instanceOf<SinkLogicalOperator>()) {// assuming one sink operator
    //                        currentOperator =
    //                            currentOperator->getParents()[0]->as<LogicalOperator>();// assuming one parent per operator
    //
    //                        // get the index of current topology node and operator in the PlacementCandidate
    //                        topoIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].first;
    //                        opIdx = matrixMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOperator->getId())].second;
    //
    //                        // set the Placement decision in the current topology and operator index to true
    //                        placementCandidate[topoIdx][opIdx] = true;// the assignment is done here
    //                        placedOperatorIds.push_back(currentOperator->getId());
    //
    //                        // draw a random decision if we should stop or continue after this placement
    //                        stop = dist(mt) > stopChance;
    //                    }
    //
    //                    // traverse to the parent of the current topology node
    //                    currentTopologyNodePtr =
    //                        currentTopologyNodePtr->getParents()[0]->as<TopologyNode>();// assuming one parent per operator
    //                }
    //            }
    //        }
    //    }

    //    assignRemainingOperator(queryPlan, topoIdx, matrixMapping, placedOperatorIds, placementCandidate);

    return placementCandidate;
}

double IFCOPStrategy::getCost(const PlacementMatrix&, QueryPlanPtr, double) {
    double totalCost = 0.0;

    /*// compute over-utilization cost
    uint32_t nodeIndex = 0;
    uint32_t overutilizationCost = 0;

    // iterate the topology using DFS iterator
    auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        auto currentTopologyNode = (*topoItr)->as<TopologyNode>();

        // count the total number of operator assigned to the current topology node (i.e., which has the placement decision = true
        auto totalAssignedOperators =
            std::count_if(placementCandidate[nodeIndex].begin(), placementCandidate[nodeIndex].end(), [](bool item) {
                return item;
            });

        // check if the number of assigned operator exceed the capacity
        if (totalAssignedOperators > currentTopologyNode->getAvailableResources()) {
            // accumulate the overutilization
            overutilizationCost += (totalAssignedOperators - currentTopologyNode->getAvailableResources());
        }

        // continue with the next node
        nodeIndex++;
    }

    totalCost += costRatio * getNetworkCost(topology->getRoot(), placementCandidate, std::move(queryPlan))
        + (1 - costRatio) * overutilizationCost;*/
    return totalCost;
}

double IFCOPStrategy::getLocalCost(const std::vector<bool>&, NES::QueryPlanPtr) {

    double cost = 1.0;// initialize to 1 as we perform a product operation
    /*// initial value for operator index and cost
    uint32_t opIdx = 0;
    QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);
    // loop over operators in the query plan and check the placement decision for each operator in the current topology node
    for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
        auto currentOperator = (*qPlanItr)->as<Operator>();

        double dmf = 1;// fallback if the DMF property does not exist in the current operator
        // check if the current operator has the data modification factor (DMF) property, otherwise fallback to 1
        if (currentOperator->hasProperty("DMF")) {
            // obtain the dmf property
            dmf = std::any_cast<double>(currentOperator->getProperty("DMF"));
        }
        // running product of dmf multiplied by the placement decision (either 1 or 0)
        // the third term is used to avoid multiplication by 0 when the placement decision is 0
        cost = cost * (nodePlacement[opIdx] * dmf + (1 - nodePlacement[opIdx]));
    }*/
    return cost;
}

double IFCOPStrategy::getNetworkCost(const TopologyNodePtr currentNode,
                                     const PlacementMatrix& placementCandidate,
                                     NES::QueryPlanPtr queryPlan) {

    // get the local cost for the current topology node
    auto currentNodeCost = getLocalCost(placementCandidate[topologyNodeIdToIndexMap[currentNode->getId()]], queryPlan);

    // check if the current node has children or if it is a source node
    if (!currentNode->getChildren().empty()) {
        double childCost = 0;

        // loop over the child node of the current topology node and compute to cost of each node
        for (const auto& node : currentNode->getChildren()) {
            auto childNode = node->as<TopologyNode>();
            // compute the network cost of the current child and then sum it to childCost
            childCost += getNetworkCost(childNode, placementCandidate, queryPlan);
        }

        // multiply the summed childCost with the cost of the current node o quantify how the placement from source node up to the
        // current topology node affect the cost of placement in the downstream topology nodes
        double cost = childCost * currentNodeCost;
        return cost;
    }

    // if the current topology node is a source node, return only the local cost of the node
    return currentNodeCost;
}

void IFCOPStrategy::initiateWorkerIdToIndexMap() {
    /*auto topologyIterator = DepthFirstNodeIterator(topology->getRoot());
    uint32_t topoIdx = 0;
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::DepthFirstNodeIterator::end(); ++topoItr) {
        NES_DEBUG("IFCOP::DEBUG:: topoid= {}", (*topoItr)->as<TopologyNode>()->getId());
        topologyNodeIdToIndexMap.insert({(*topoItr)->as<TopologyNode>()->getId(), topoIdx});
        topoIdx++;
    }*/
}

void IFCOPStrategy::assignRemainingOperator(NES::QueryPlanPtr,
                                            uint32_t,
                                            IdToIteratorIndexMapping&,
                                            std::vector<OperatorId>&,
                                            PlacementMatrix&) {
    /*auto currentTopologyNodePtr = topology->getRoot();

    // iterate to all operator in the query to check for un-assinged operator
    QueryPlanIterator queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto qPlanIter = queryPlanIterator.begin(); qPlanIter != NES::QueryPlanIterator::end(); ++qPlanIter) {
        auto currentOpId = (*qPlanIter)->as<LogicalOperator>()->getId();

        // check if the current operator has been placed before
        if (std::find(placedOperatorIds.begin(), placedOperatorIds.end(), currentOpId) == placedOperatorIds.end()) {
            // if the current operator id is not in placedOperatorIds, then place the current operator at the sink

            // obtain the index of the topology id and operator id
            topoIdx = idToIteratorIndexMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOpId)].first;
            auto opIdx = idToIteratorIndexMapping[std::make_pair(currentTopologyNodePtr->getId(), currentOpId)].second;

            placementCandidate[topoIdx][opIdx] = true;// the assignment is done here

            // register the current operator as placed
            placedOperatorIds.push_back(currentOpId);
        }
    }*/
}

PlacementAdditionResult IFCOPStrategy::updateGlobalExecutionPlan(SharedQueryId /*queryId*/,
                                                                 const std::set<LogicalOperatorPtr>& /*pinnedUpStreamNodes*/,
                                                                 const std::set<LogicalOperatorPtr>& /*pinnedDownStreamNodes*/,
                                                                 DecomposedQueryPlanVersion /*querySubPlanVersion*/,
                                                                 FaultToleranceType /*faultTolerance*/,
                                                                 CheckpointStorageType /*checkpointStorage*/) {
    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Optimizer
