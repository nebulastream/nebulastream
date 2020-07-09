#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryPlacementPhasePtr QueryPlacementPhase::create() {
    return std::make_shared<QueryPlacementPhase>(QueryPlacementPhase());
}

bool QueryPlacementPhase::execute(std::string placementStrategy, QueryPlanPtr queryPlan) {
    NES_INFO("NESOptimizer: Preparing execution graph for input query");
    NES_INFO("NESOptimizer: Initializing Placement strategy");
    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(placementStrategy, nesTopologyPlan, globalExecutionPlan);
    if (!placementStrategyPtr) {
        NES_ERROR("NESOptimizer: unable to find placement strategy for " + placementStrategy);
        return nullptr;
    }
    GlobalExecutionPlanPtr globalExecution = placementStrategyPtr->updateGlobalExecutionPlan(queryPlan, streamCatalog);
    return globalExecution;
}


}