#include "../../include/Optimizer/NESOptimizer.hpp"

#include "../../include/Optimizer/NESPlacementOptimizer.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
#include "../../include/Topology/NESTopologyPlan.hpp"

using namespace NES;

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(const std::string strategy, const InputQueryPtr inputQuery,
                                                        const NESTopologyPlanPtr nesTopologyPlan) {

    const shared_ptr<NESPlacementOptimizer> optimizerPtr = NESPlacementOptimizer::getOptimizer(strategy);
    const NESExecutionPlanPtr nesExecutionPlanPtr = optimizerPtr->initializeExecutionPlan(inputQuery, nesTopologyPlan);
    return nesExecutionPlanPtr;
};
