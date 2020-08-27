#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryPlacementPhase::QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                         StreamCatalogPtr streamCatalog) : globalExecutionPlan(globalExecutionPlan), topology(topology),
                                                                           typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog) {
    NES_DEBUG("QueryPlacementPhase()");
}

QueryPlacementPhase::~QueryPlacementPhase() {
    NES_DEBUG("~QueryPlacementPhase()");
}

QueryPlacementPhasePtr QueryPlacementPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                                   StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryPlacementPhase>(QueryPlacementPhase(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
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