#include "../../include/Optimizer/NESOptimizer.hpp"

#include "../../include/Optimizer/NESPlacementOptimizer.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
#include "../../include/Topology/NESTopologyPlan.hpp"

using namespace NES;

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, InputQueryPtr inputQuery,
                                                        NESTopologyPlanPtr nesTopologyPlan) {

    shared_ptr<NESPlacementOptimizer> optimizerPtr = NESPlacementOptimizer::getOptimizer(strategy);
    NESExecutionPlanPtr nesExecutionPlanPtr = optimizerPtr->initializeExecutionPlan(inputQuery, nesTopologyPlan);
    return nesExecutionPlanPtr;
};
