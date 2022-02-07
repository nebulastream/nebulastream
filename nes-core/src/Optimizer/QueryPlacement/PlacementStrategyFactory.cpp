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

#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Optimizer/QueryPlacement/ILPStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Util/PlacementStrategy.hpp>

namespace NES::Optimizer {

BasePlacementStrategyPtr PlacementStrategyFactory::getStrategy(PlacementStrategy::Value placementStrategy,
                                                               const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                               const TopologyPtr& topology,
                                                               const TypeInferencePhasePtr& typeInferencePhase,
                                                               const z3::ContextPtr& z3Context) {
    switch (placementStrategy) {
// #2485        case PlacementStrategy::ILP:
//            return ILPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, z3Context);
        case PlacementStrategy::BottomUp:
            return BottomUpStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
        case PlacementStrategy::TopDown:
            return TopDownStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
// #2486        case PlacementStrategy::IFCOP:
//            return IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase);
        // FIXME: enable them with issue #755
        //        case LowLatency: return LowLatencyStrategy::create(nesTopologyPlan);
        //        case HighThroughput: return HighThroughputStrategy::create(nesTopologyPlan);
        //        case MinimumResourceConsumption: return MinimumResourceConsumptionStrategy::create(nesTopologyPlan);
        //        case MinimumEnergyConsumption: return MinimumEnergyConsumptionStrategy::create(nesTopologyPlan);
        //        case HighAvailability: return HighAvailabilityStrategy::create(nesTopologyPlan);
        default: throw Exception("Unknown placement strategy type " + std::to_string(placementStrategy));
    }
}
}// namespace NES::Optimizer
