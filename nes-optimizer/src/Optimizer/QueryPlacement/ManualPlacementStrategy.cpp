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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <utility>

namespace NES::Optimizer {

BasePlacementStrategyPtr ManualPlacementStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                         const TopologyPtr& topology,
                                                         const TypeInferencePhasePtr& typeInferencePhase,
                                                         PlacementMode placementMode) {
    return std::make_unique<ManualPlacementStrategy>(
        ManualPlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, placementMode));
}

ManualPlacementStrategy::ManualPlacementStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const TypeInferencePhasePtr& typeInferencePhase,
                                                 PlacementMode placementMode)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, placementMode) {}

bool ManualPlacementStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                        const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                        const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    try {
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Compute query sub plans
        auto computedQuerySubPlans = computeQuerySubPlans(sharedQueryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 4. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementException(sharedQueryId, ex.what());
    }
};
}// namespace NES::Optimizer
