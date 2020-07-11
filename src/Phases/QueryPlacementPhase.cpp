#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryPlacementPhase::QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, TypeInferencePhasePtr typeInferencePhase,
                                         StreamCatalogPtr streamCatalog) : globalExecutionPlan(globalExecutionPlan), nesTopologyPlan(nesTopologyPlan),
                                                                           typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog) {}

QueryPlacementPhasePtr QueryPlacementPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, TypeInferencePhasePtr typeInferencePhase,
                                                   StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryPlacementPhase>(QueryPlacementPhase(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog));
}

bool QueryPlacementPhase::execute(std::string placementStrategy, QueryPlanPtr queryPlan) {
    NES_INFO("NESOptimizer: Placing input Query Plan on Global Execution Plan");
    NES_INFO("NESOptimizer: Get the placement strategy");
    //TODO: At the time of placement we have to make sure that there are no changes done on nesTopologyPlan (how to handle the case of dynamic topology?)
    // one solution could be: 1.) Take the snapshot of the topology and perform the placement 2.) If the topology changed meanwhile, repeat step 1.
    auto placementStrategyPtr = PlacementStrategyFactory::getStrategy(placementStrategy, globalExecutionPlan, nesTopologyPlan,
                                                                      typeInferencePhase, streamCatalog);
    if (!placementStrategyPtr) {
        NES_ERROR("NESOptimizer: unable to find placement strategy for " + placementStrategy);
        return false;
    }
    return placementStrategyPtr->updateGlobalExecutionPlan(queryPlan);
}

}// namespace NES