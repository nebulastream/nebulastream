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

#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>

namespace NES {

std::unique_ptr<BasePlacementStrategy> PlacementStrategyFactory::getStrategy(std::string strategyName, GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                                             TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    switch (stringToPlacementStrategyType[strategyName]) {
        case BottomUp:
            return BottomUpStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
        case TopDown:
            return TopDownStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
            // FIXME: enable them with issue #755
            //        case LowLatency: return LowLatencyStrategy::create(nesTopologyPlan);
            //        case HighThroughput: return HighThroughputStrategy::create(nesTopologyPlan);
            //        case MinimumResourceConsumption: return MinimumResourceConsumptionStrategy::create(nesTopologyPlan);
            //        case MinimumEnergyConsumption: return MinimumEnergyConsumptionStrategy::create(nesTopologyPlan);
            //        case HighAvailability: return HighAvailabilityStrategy::create(nesTopologyPlan);
        default:
            return nullptr;
    }
}

}// namespace NES
