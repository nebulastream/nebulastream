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

#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Plans/Query/QueryPlan.hpp>
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

bool IFCOPStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    // TODO: update the binary mapping

    // TODO assign the binary mapping
//    assignMappingToTopology(topology, queryPlan, this->binaryMapping);

    addNetworkSourceAndSinkOperators(queryPlan);
    return runTypeInferencePhase(queryPlan->getQueryId());
}

}// namespace NES::Optimizer
