#include <API/Query.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace NES;

NESOptimizer::NESOptimizer() {
    translateFromLegacyPlanPhase = TranslateFromLegacyPlanPhase::create();
}

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, QueryPtr queryPtr,
                                                        NESTopologyPlanPtr nesTopologyPlan, StreamCatalogPtr streamCatalog) {

    NES_INFO("NESOptimizer: Preparing execution graph for input query");

    NES_INFO("NESOptimizer: Initializing Placement strategy");
    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(strategy);
    
    NES_INFO("NESOptimizer: Building Execution plan for the input query");
    NESExecutionPlanPtr nesExecutionPlanPtr = placementStrategyPtr->initializeExecutionPlan(queryPtr, nesTopologyPlan, streamCatalog);
    return nesExecutionPlanPtr;
};
