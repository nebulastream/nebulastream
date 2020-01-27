#include "../../include/Optimizer/NESOptimizer.hpp"

#include "../../include/Optimizer/NESPlacementOptimizer.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
#include "../../include/Topology/NESTopologyPlan.hpp"

using namespace NES;

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(
    const std::string strategy, const InputQueryPtr inputQuery,
    const NESTopologyPlanPtr nesTopologyPlan) {

  const shared_ptr<NESPlacementOptimizer> optimizerPtr =
      NESPlacementOptimizer::getOptimizer(strategy);

  const NESExecutionPlan executionGraph = optimizerPtr->initializeExecutionPlan(
      inputQuery, nesTopologyPlan);

  return make_shared<NESExecutionPlan>(executionGraph);
}
;
