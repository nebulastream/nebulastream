#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

FogExecutionPlan FogOptimizer::prepareExecutionGraph(const std::string strategy, const InputQueryPtr inputQuery,
                                                     const FogTopologyPlanPtr fogTopologyPlan) {

  const shared_ptr<FogPlacementOptimizer> optimizerPtr = FogPlacementOptimizer::getOptimizer(strategy);

  const FogExecutionPlan executionGraph = optimizerPtr->initializeExecutionPlan(inputQuery, fogTopologyPlan);

  return executionGraph;

};
