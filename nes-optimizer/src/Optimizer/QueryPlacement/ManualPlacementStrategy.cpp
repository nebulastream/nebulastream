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

#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <utility>

namespace NES::Optimizer {

std::unique_ptr<ManualPlacementStrategy>
ManualPlacementStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                NES::TopologyPtr topology,
                                NES::Optimizer::TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<ManualPlacementStrategy>(
        ManualPlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

ManualPlacementStrategy::ManualPlacementStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                 NES::TopologyPtr topology,
                                                 NES::Optimizer::TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

bool ManualPlacementStrategy::updateGlobalExecutionPlan(
    QueryId queryId /*queryId*/,
    FaultToleranceType faultToleranceType /*faultToleranceType*/,
    LineageType lineageType /*lineageType*/,
    const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    try {
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place the operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementException(queryId, ex.what());
    }
};
}// namespace NES::Optimizer
