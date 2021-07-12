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

    // FIXME 1018: assuming a single logical source
    std::vector<TopologyNodePtr> allSourceNodes;
    for (const auto& sourceOperator : queryPlan->getSourceOperators()) {
        const auto& streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        const auto sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        allSourceNodes.insert(allSourceNodes.end(), sourceNodes.begin(), sourceNodes.end());
    }
    auto topologyIterator = BreadthFirstNodeIterator(topology->getRoot());

    int placedOperator = 0;
    int totalNumberOfOperators = QueryPlanIterator(queryPlan).snapshot().size();

    for (auto itr = topologyIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
        // add a new entry in the binary mapping for the current node
        std::vector<bool> currentTopologyNodeMapping;

        auto currentTopologyNode = (*itr)->as<TopologyNode>();
        auto isSourceNode =
            std::find(allSourceNodes.begin(), allSourceNodes.end(), currentTopologyNode) != allSourceNodes.end();
        auto isSinkNode = currentTopologyNode->equal(topology->getRoot());

        int lowerBound = 0;
        if (isSinkNode) {
            // if the current node is a sink node, place at least 1 operator (i.e., the sink)
            lowerBound = 1;
        }

        int upperBound = totalNumberOfOperators - placedOperator;
        if (!isSourceNode) {
            // if the current node is NOT a source node, avoid placing the last operator (i.e., the source)
            upperBound = totalNumberOfOperators - placedOperator - 1;
        }

        // define the number of operator to place in the current node
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int> uniform_dist(lowerBound, upperBound);
        int numOperatorToPlace = uniform_dist(e1);

        // if we are reaching the source node but still have operators to place, place them all in the source node
        if (isSourceNode && (placedOperator < totalNumberOfOperators)) {
            numOperatorToPlace = totalNumberOfOperators - placedOperator;
        }

        auto queryPlanIteror = QueryPlanIterator(queryPlan);
        auto qPlanItr = queryPlanIteror.begin();

        // skip already placed operators
        for (int i=0; i<placedOperator; i++) {
            currentTopologyNodeMapping.push_back(false);
            ++qPlanItr;
        }

        // place the rest of the operators
        while (qPlanItr != QueryPlanIterator::end()) {
            ++qPlanItr;
            if (numOperatorToPlace > 0) {
                currentTopologyNodeMapping.push_back(true);
                numOperatorToPlace--;
                placedOperator++;
            } else {
                currentTopologyNodeMapping.push_back(false);
            }
        }

        placementCandidate.push_back(currentTopologyNodeMapping);
    }
    return placementCandidate;
}

}// namespace NES::Optimizer
