#include <API/Query.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace NES;

NESOptimizer::NESOptimizer() {
    translateFromLegacyPlanPhase = TranslateFromLegacyPlanPhase::create();
}

GlobalExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, QueryPlanPtr queryPlan,
                                                        NESTopologyPlanPtr nesTopologyPlan, StreamCatalogPtr streamCatalog) {

    NES_INFO("NESOptimizer: Preparing execution graph for input query");

    NES_INFO("NESOptimizer: Initializing Placement strategy");
    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(nesTopologyPlan, strategy);

    NES_INFO("NESOptimizer: Building Execution plan for the input query");
    TypeInferencePhasePtr typeInferencePhasePtr = TypeInferencePhase::create();
    queryPlan = typeInferencePhasePtr->transform(queryPlan);
    GlobalExecutionPlanPtr globalExecution = placementStrategyPtr->initializeExecutionPlan(queryPlan, streamCatalog);
    return globalExecution;
};
