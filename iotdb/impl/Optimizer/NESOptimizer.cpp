#include "Optimizer/NESOptimizer.hpp"
#include "Optimizer/BasePlacementStrategy.hpp"
#include "Topology/NESTopologyPlan.hpp"

using namespace NES;

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, InputQueryPtr inputQuery,
                                                        NESTopologyPlanPtr nesTopologyPlan) {

    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(strategy);
    NESExecutionPlanPtr nesExecutionPlanPtr = placementStrategyPtr->initializeExecutionPlan(inputQuery, nesTopologyPlan);
    return nesExecutionPlanPtr;
};
