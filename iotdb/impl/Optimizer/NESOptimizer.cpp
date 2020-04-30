#include <Optimizer/NESOptimizer.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Topology/NESTopologyPlan.hpp>

using namespace NES;

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, InputQueryPtr inputQuery,
                                                        NESTopologyPlanPtr nesTopologyPlan) {

    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(strategy);
    QueryPtr query;
    NESExecutionPlanPtr nesExecutionPlanPtr = placementStrategyPtr->initializeExecutionPlan(query, nesTopologyPlan);
    return nesExecutionPlanPtr;
};
