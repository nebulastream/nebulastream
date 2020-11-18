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

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryPlacementPhase::QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), typeInferencePhase(typeInferencePhase),
      streamCatalog(streamCatalog) {
    NES_DEBUG("QueryPlacementPhase()");
}

QueryPlacementPhase::~QueryPlacementPhase() { NES_DEBUG("~QueryPlacementPhase()"); }

QueryPlacementPhasePtr QueryPlacementPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                   TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryPlacementPhase>(
        QueryPlacementPhase(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

bool QueryPlacementPhase::execute(std::string placementStrategy, QueryPlanPtr queryPlan) {
    NES_INFO("NESOptimizer: Placing input Query Plan on Global Execution Plan");
    NES_INFO("NESOptimizer: Get the placement strategy");
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    auto placementStrategyPtr = PlacementStrategyFactory::getStrategy(placementStrategy, globalExecutionPlan, topology,
                                                                      typeInferencePhase, streamCatalog);
    if (!placementStrategyPtr) {
        NES_ERROR("NESOptimizer: unable to find placement strategy for " + placementStrategy);
        return false;
    }
    return placementStrategyPtr->updateGlobalExecutionPlan(queryPlan);
}

}// namespace NES