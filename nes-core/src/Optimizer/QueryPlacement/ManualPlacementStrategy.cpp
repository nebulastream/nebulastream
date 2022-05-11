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

#include "Exceptions/QueryPlacementException.hpp"
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
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

void ManualPlacementStrategy::setBinaryMapping(PlacementMatrix userDefinedBinaryMapping) {
    this->binaryMapping = std::move(userDefinedBinaryMapping);
}

bool ManualPlacementStrategy::updateGlobalExecutionPlan(
    QueryId queryId /*queryId*/,
    FaultToleranceType faultToleranceType /*faultToleranceType*/,
    LineageType lineageType /*lineageType*/,
    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::vector<OperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    auto bottomUpStrategy = BottomUpStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
    return bottomUpStrategy->updateGlobalExecutionPlan(queryId, faultToleranceType, lineageType, pinnedUpStreamOperators, pinnedDownStreamOperators);
};
}// namespace NES::Optimizer
