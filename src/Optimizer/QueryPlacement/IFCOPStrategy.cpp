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
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
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

void IFCOPStrategy::getPathForPlacement(NES::QueryPlanPtr queryPlan) {
    // 1. get source-to-sink path for all sources
    std::vector<TopologyNodePtr> allSourceNodes;
    // 1a. list all logical stream used in the query
    for (const auto& sourceOperator : queryPlan->getSourceOperators()) {
        const auto& streamName = sourceOperator->getSourceDescriptor()->getStreamName();

        // 1b. for each logical stream, get topology nodes containing the logical stream
        const auto sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        allSourceNodes.insert(allSourceNodes.end(), sourceNodes.begin(), sourceNodes.end());
    }
    // 1c. get the sink node
    auto sinkNode = topology->getRoot();

    // 2. get source to sink path for each source node
    std::vector<std::optional<TopologyNodePtr>> allSourceToSinkPath;
    for (const auto& sourceNode : allSourceNodes) {
        auto sourceToSink = topology->findAllPathBetween(sourceNode, sinkNode);
        allSourceToSinkPath.push_back(sourceToSink);
    }

    // 3. merge all source to sink path
    NES_TRACE("IFCOPStrategy::getPathForPlacement: merging all source to sink paths");
    TopologyPtr selectedTopology;
    TopologyNodePtr root = topology->getRoot()->copy();
    root->removeChildren();

    for (auto path : allSourceToSinkPath) {
        NES_TRACE("IFCOPStrategy::getPathForPlacement: iterate throuugh allSourceToSinkPath");
    }
}

bool IFCOPStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    auto placementCandidate = getPlacementCandidate(queryPlan);
    assignMappingToTopology(topology, queryPlan, placementCandidate);

    addNetworkSourceAndSinkOperators(queryPlan);
    return runTypeInferencePhase(queryPlan->getQueryId());
}
std::vector<std::vector<bool>> IFCOPStrategy::getPlacementCandidate(NES::QueryPlanPtr queryPlan) {

    std::vector<std::vector<bool>> placementCandidate;

    std::map<std::pair<OperatorId, uint64_t>, std::pair<uint64_t, uint64_t>> matrixMapping;

    auto topologyIterator = BreadthFirstNodeIterator(topology->getRoot());

    uint64_t topoIdx = 0;

    // prepare the mapping
    for (auto topoItr = topologyIterator.begin(); topoItr != NES::BreadthFirstNodeIterator::end(); ++topoItr) {
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
        auto currentOperator = srcOp;
        for (auto topologyNode : streamCatalog->getSourceNodesForLogicalStream(srcOp->getSourceDescriptor()->getStreamName())) {
            auto topoIdx = matrixMapping[std::make_pair(topologyNode->getId(), currentOperator->getId())].first;
            auto opIdx = matrixMapping[std::make_pair(topologyNode->getId(), currentOperator->getId())].second;

            // place the current source operator here if no source operator for the same logical source is placed
            if (std::find(placedOperatorIds.begin(), placedOperatorIds.end(), srcOp->getId()) == placedOperatorIds.end()
                && std::find(topologyNodeToStreamName[topologyNode].begin(),
                             topologyNodeToStreamName[topologyNode].end(),
                             srcOp->getSourceDescriptor()->getStreamName())
                    == topologyNodeToStreamName[topologyNode].end()) {
                placementCandidate[topoIdx][opIdx] = true;
                placedOperatorIds.push_back(srcOp->getId());

                if (topologyNodeToStreamName.find(topologyNode) == topologyNodeToStreamName.end()) {
                    std::vector<std::string> placedLogicalStreams = {srcOp->getSourceDescriptor()->getStreamName()};
                    topologyNodeToStreamName.insert(std::make_pair(topologyNode, placedLogicalStreams));
                } else {
                    topologyNodeToStreamName[topologyNode].push_back(srcOp->getSourceDescriptor()->getStreamName());
                }
            }
        }
    }

    return placementCandidate;
}

}// namespace NES::Optimizer
