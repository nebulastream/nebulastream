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

#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<ManualPlacementStrategy> ManualPlacementStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                                         NES::TopologyPtr topology,
                                                                         NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                                         NES::StreamCatalogPtr streamCatalog) {
    return std::make_unique<ManualPlacementStrategy>(ManualPlacementStrategy(std::move(globalExecutionPlan),
                                                                             std::move(topology),
                                                                             std::move(typeInferencePhase),
                                                                             std::move(streamCatalog)));
}

ManualPlacementStrategy::ManualPlacementStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                 NES::TopologyPtr topology,
                                                 NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                 NES::StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool ManualPlacementStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    // check if user has specify binary mapping for placement
    if (binaryMapping.empty()) {
        NES_ERROR("ManualPlacementStrategy::updateGlobalExecutionPlan: binary mapping is not set");
        return false;
    }

    // apply the placement from the specified binary mapping
    assignMappingToTopology(topology, queryPlan, this->binaryMapping);
    addNetworkSourceAndSinkOperators(queryPlan);
    return runTypeInferencePhase(queryPlan->getQueryId());
}

bool ManualPlacementStrategy::partiallyUpdateGlobalExecutionPlan(const QueryPlanPtr& /*queryPlan*/) {
    NES_NOT_IMPLEMENTED();
    return false;
}

void ManualPlacementStrategy::setBinaryMapping(std::vector<std::vector<bool>> userDefinedBinaryMapping) {
    this->binaryMapping = std::move(userDefinedBinaryMapping);
}

}// namespace NES::Optimizer
