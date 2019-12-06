#include "Services/OptimizerService.hpp"
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>

using namespace iotdb;
using namespace std;

FogExecutionPlan OptimizerService::getExecutionPlan(InputQueryPtr inputQuery, string optimizationStrategyName) {
  FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
  fogTopologyManager.createExampleTopology();
  const FogTopologyPlanPtr &topologyPlan = fogTopologyManager.getTopologyPlan();

  FogOptimizer queryOptimizer;

  return queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);
}