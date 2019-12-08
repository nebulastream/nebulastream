#include "Services/OptimizerService.hpp"
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>

using namespace iotdb;
using namespace web;
using namespace std;

json::value OptimizerService::getExecutionPlanAsJson(InputQueryPtr inputQuery, string optimizationStrategyName) {
  return getExecutionPlan(inputQuery, optimizationStrategyName).getExecutionGraphAsJson();
}

FogExecutionPlan OptimizerService::getExecutionPlan(InputQueryPtr inputQuery, string optimizationStrategyName) {
  FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
  const FogTopologyPlanPtr &topologyPlan = fogTopologyManager.getTopologyPlan();

  FogOptimizer queryOptimizer;

  return queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);
}