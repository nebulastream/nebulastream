#include "../../include/Optimizer/NESOptimizer.hpp"

#include "../../include/Optimizer/NESPlacementOptimizer.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
#include "../../include/Topology/NESTopologyPlan.hpp"

using namespace iotdb;

NESExecutionPlan NESOptimizer::prepareExecutionGraph(const std::string strategy, const InputQueryPtr inputQuery,
                                                     const NESTopologyPlanPtr fogTopologyPlan) {

  const shared_ptr<NESPlacementOptimizer> optimizerPtr = NESPlacementOptimizer::getOptimizer(strategy);

  const NESExecutionPlan executionGraph = optimizerPtr->initializeExecutionPlan(inputQuery, fogTopologyPlan);

  return executionGraph;

};
