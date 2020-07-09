#include <API/Query.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace NES;

NESOptimizer::NESOptimizer() {
    typeInferencePhasePtr = TypeInferencePhase::create();
}

GlobalExecutionPlanPtr NESOptimizer::updateExecutionGraph(std::string strategy, QueryPlanPtr queryPlan, NESTopologyPlanPtr nesTopologyPlan,
                                                          StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan) {

    NES_INFO("NESOptimizer: Preparing execution graph for input query");
    NES_INFO("NESOptimizer: Initializing Placement strategy");
    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(strategy, nesTopologyPlan, globalExecutionPlan);
    if (!placementStrategyPtr) {
        NES_ERROR("NESOptimizer: unable to find placement strategy for " + strategy);
        return nullptr;
    }

    NES_INFO("NESOptimizer: Building Execution plan for the input query");
    queryPlan = typeInferencePhasePtr->execute(queryPlan);
    GlobalExecutionPlanPtr globalExecution = placementStrategyPtr->updateGlobalExecutionPlan(queryPlan, streamCatalog);
    return globalExecution;
};
