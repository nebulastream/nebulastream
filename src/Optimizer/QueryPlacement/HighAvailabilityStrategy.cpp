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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/QueryPlacement/HighAvailabilityStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>
namespace NES {

HighAvailabilityStrategy::HighAvailabilityStrategy(NESTopologyPlanPtr nesTopologyPlan) : BasePlacementStrategy(nesTopologyPlan) {}

NESExecutionPlanPtr HighAvailabilityStrategy::initializeExecutionPlan(QueryPlanPtr queryPlan, NESTopologyPlanPtr nesTopologyPlan,
                                                                      StreamCatalogPtr streamCatalog) {
    this->nesTopologyPlan = nesTopologyPlan;
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string streamName = queryPlan->getSourceStreamName();

    if (!sourceOperator) {
        NES_THROW_RUNTIME_ERROR("HighAvailabilityStrategy: Unable to find the source operator.");
    }

    const std::vector<NESTopologyEntryPtr> sourceNodePtrs = streamCatalog->getSourceNodesForLogicalStream(streamName);

    if (sourceNodePtrs.empty()) {
        NES_ERROR("HighAvailabilityStrategy: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("HighAvailabilityStrategy: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperator, sourceNodePtrs);

    NES_INFO("HighAvailabilityStrategy: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = sourceOperator->getSourceDescriptor()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void HighAvailabilityStrategy::placeOperators(NESExecutionPlanPtr nesExecutionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                                              LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes) {

    TranslateToLegacyPlanPhasePtr translator = TranslateToLegacyPlanPhase::create();
    NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();
    uint64_t linkRedundency = 2;

    NES_INFO("HighAvailabilityStrategy: Find paths between source nodes and sink node such that the nodes on the paths are"
             "connected with "
             << linkRedundency << " number of redundant links.");
    vector<vector<NESTopologyEntryPtr>> placementPaths;

    for (NESTopologyEntryPtr sourceNode : sourceNodes) {

        NES_DEBUG("HighAvailabilityStrategy: For each source find all paths between source and sink nodes.");
        const auto listOfPaths = pathFinder->findAllPathsBetween(sourceNode, sinkNode);

        //Find the most common path among the list of paths
        vector<NESTopologyEntryPtr> pathForPlacement;
        uint64_t maxPathWeight = 0;
        NES_DEBUG("HighAvailabilityStrategy: Find a path such that the path nodes are shared with most of the remaining paths.");
        for (uint64_t i = 0; i < listOfPaths.size(); i++) {

            vector<NESTopologyEntryPtr> path_i = listOfPaths[i];
            map<NESTopologyEntryPtr, uint64_t> nodeCountMap;
            for (uint64_t j = 0; j < listOfPaths.size(); j++) {

                NES_DEBUG("HighAvailabilityStrategy: Skip comparision with itself.");
                if (i == j) {
                    continue;
                }
                vector<NESTopologyEntryPtr> path_j = listOfPaths[j];
                NES_DEBUG("HighAvailabilityStrategy: Fast Forward the path to find first non common node.");
                uint64_t pathINodeIndex = 0;
                while (path_i[pathINodeIndex]->getId() == path_j[pathINodeIndex]->getId() && pathINodeIndex <= path_i.size()
                       && pathINodeIndex <= path_j.size()) {

                    if (nodeCountMap.find(path_i[pathINodeIndex]) == nodeCountMap.end()) {
                        nodeCountMap[path_i[pathINodeIndex]] = 0;
                    }
                    pathINodeIndex++;
                }

                NES_DEBUG("HighAvailabilityStrategy: If no non-common node exists with current path then skip to compare with "
                          "next path.");
                if (pathINodeIndex > path_i.size() || pathINodeIndex > path_j.size()) {
                    continue;
                }

                NES_DEBUG("HighAvailabilityStrategy: Construct a map with key as the nodes of the current path and value as the "
                          "number of times the node occurred in other paths");
                for (uint64_t idx = pathINodeIndex; idx < path_i.size(); idx++) {
                    auto node_i = path_i[idx];
                    const auto itr = find_if(path_j.begin(), path_j.end(), [node_i](NESTopologyEntryPtr node_j) {
                        return node_i->getId() == node_j->getId();
                    });

                    if (itr != path_j.end()) {
                        if (nodeCountMap.find(node_i) != nodeCountMap.end()) {
                            nodeCountMap[node_i] = nodeCountMap[node_i] + 1;
                        } else {
                            nodeCountMap[node_i] = 1;
                        }
                    } else if (nodeCountMap.find(node_i) == nodeCountMap.end()) {
                        nodeCountMap[node_i] = 0;
                    }
                }
            }

            uint64_t totalWeight = 0;
            vector<NESTopologyEntryPtr> commonPath = {sourceNode};
            NES_DEBUG("HighAvailabilityStrategy: Iterate over the computed map and identify the nodes with sufficient number of"
                      " occurrence in other paths");
            for (auto itr = nodeCountMap.rbegin(); itr != nodeCountMap.rend(); itr++) {
                NES_DEBUG("HighAvailabilityStrategy: Check if the node on the path is shared by atleast " << linkRedundency
                                                                                                          << " paths.");
                if (itr->second >= linkRedundency - 1) {
                    commonPath.push_back(itr->first);
                    totalWeight++;
                }
            }

            if (maxPathWeight < totalWeight) {
                pathForPlacement = commonPath;
            }
        }
        NES_DEBUG("HighAvailabilityStrategy: Place the path with nodes that are shared with most of the remaining paths.");
        placementPaths.push_back(pathForPlacement);
    }

    NES_INFO("HighAvailabilityStrategy: Sort the paths based on available compute resources.");

    //Sort all the paths with increased aggregated compute capacity
    vector<std::pair<uint64_t, int>> computeCostList;

    //Calculate total compute cost for each path
    for (uint64_t i = 0; i < placementPaths.size(); i++) {
        vector<NESTopologyEntryPtr> path = placementPaths[i];
        uint64_t totalComputeForPath = 0;
        for (NESTopologyEntryPtr node : path) {
            totalComputeForPath = totalComputeForPath + node->getCpuCapacity();
        }
        computeCostList.push_back(make_pair(totalComputeForPath, i));
    }

    sort(computeCostList.begin(), computeCostList.end());

    vector<vector<NESTopologyEntryPtr>> sortedListOfPaths;
    for (auto pair : computeCostList) {
        sortedListOfPaths.push_back(placementPaths[pair.second]);
    }

    NES_INFO("HighAvailabilityStrategy: Perform placement of operators on each path.");

    for (vector<NESTopologyEntryPtr> pathForPlacement : sortedListOfPaths) {

        LogicalOperatorNodePtr targetOperator = sourceOperator;
        //Perform Bottom-Up placement
        for (uint64_t i = 0; i < pathForPlacement.size(); i++) {
            NESTopologyEntryPtr node = pathForPlacement[i];
            while (node->getRemainingCpuCapacity() > 0 && targetOperator) {

                if (targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                    node = sinkNode;
                }

                NES_DEBUG("TopDown: Transforming New Operator into legacy operator");
                OperatorPtr legacyOperator = translator->transform(targetOperator);

                if (!nesExecutionPlanPtr->hasVertex(node->getId())) {
                    NES_DEBUG("HighThroughput: Create new execution node.");
                    stringstream operatorName;
                    operatorName << targetOperator->toString() << "(OP-" << std::to_string(targetOperator->getId()) << ")";
                    const ExecutionNodePtr newExecutionNode = nesExecutionPlanPtr->createExecutionNode(
                        operatorName.str(), to_string(node->getId()), node, legacyOperator->copy());
                    newExecutionNode->addOperatorId(targetOperator->getId());
                } else {

                    const ExecutionNodePtr existingExecutionNode = nesExecutionPlanPtr->getExecutionNode(node->getId());
                    uint64_t operatorId = targetOperator->getId();
                    vector<uint64_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                    const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);
                    if (exists != residentOperatorIds.end()) {
                        //skip adding rest of the operator chains as they already exists.
                        NES_DEBUG("HighThroughput: skip adding rest of the operator chains as they already exists.");
                        targetOperator = nullptr;
                        break;
                    } else {

                        NES_DEBUG("HighThroughput: adding target operator to already existing operator chain.");
                        stringstream operatorName;
                        operatorName << existingExecutionNode->getOperatorName() << "=>" << targetOperator->toString() << "(OP-"
                                     << std::to_string(targetOperator->getId()) << ")";
                        existingExecutionNode->addOperator(legacyOperator->copy());
                        existingExecutionNode->setOperatorName(operatorName.str());
                        existingExecutionNode->addOperatorId(targetOperator->getId());
                    }
                }

                targetOperator = targetOperator->getParents()[0]->as<LogicalOperatorNode>();
                node->reduceCpuCapacity(1);
            }

            if (!targetOperator) {
                break;
            }

            NES_INFO("HighAvailabilityStrategy: Find if next target operator already placed on one of the nodes of current path");

            //find if the next target operator already placed
            bool isAlreadyPlaced = false;
            for (uint64_t j = i + 1; j < pathForPlacement.size(); j++) {
                if (nesExecutionPlanPtr->hasVertex(pathForPlacement[j]->getId())) {
                    vector<uint64_t> placedOperators =
                        nesExecutionPlanPtr->getExecutionNode(pathForPlacement[j]->getId())->getChildOperatorIds();
                    uint64_t operatorId = targetOperator->getId();
                    auto found = find_if(placedOperators.begin(), placedOperators.end(), [operatorId](uint64_t opId) {
                        return operatorId == opId;
                    });

                    if (found != placedOperators.end()) {
                        isAlreadyPlaced = true;
                    }
                }
            }

            if (isAlreadyPlaced) {
                break;
            }
        }

        NES_INFO("HighAvailabilityStrategy: Add forward operators to the remaining nodes on current path");
        addForwardOperators(pathForPlacement, nesExecutionPlanPtr);
    }
}

void HighAvailabilityStrategy::addForwardOperators(vector<NESTopologyEntryPtr> pathForPlacement,
                                                   NES::NESExecutionPlanPtr nesExecutionPlanPtr) const {

    // We iterate over the nodes used for the placement in the path and find all paths between two consecutive nodes.
    // Since, we want to stop before the last node the loop terminates before last node.
    // This loop is done to avoid placing forward operators on a path not selected for the placement after being considered
    // for initial path selection.
    for (uint64_t i = 0; i < pathForPlacement.size() - 1; i++) {

        NES_DEBUG(
            "HighAvailabilityStrategy: Find all paths between two consecutive nodes of used for performing operator placement");
        auto paths = pathFinder->findAllPathsBetween(pathForPlacement[i], pathForPlacement[i + 1]);

        for (vector<NESTopologyEntryPtr> path : paths) {
            for (NESTopologyEntryPtr node : path) {

                NES_DEBUG("HighAvailabilityStrategy: Add FWD operator on the node where no query operator has been placed");
                if (node->getCpuCapacity() == node->getRemainingCpuCapacity()) {
                    nesExecutionPlanPtr->createExecutionNode("FWD", to_string(node->getId()), node,
                                                             /**executableOperator**/ nullptr);
                    node->reduceCpuCapacity(1);
                }
            }
        }
    }
}

}// namespace NES
