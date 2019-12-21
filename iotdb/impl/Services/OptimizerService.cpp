#include "Services/OptimizerService.hpp"
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include <CodeGen/QueryPlanBuilder.hpp>


using namespace iotdb;
using namespace web;
using namespace std;

json::value OptimizerService::getExecutionPlanAsJson(InputQueryPtr inputQuery, string optimizationStrategyName) {
  return getExecutionPlan(inputQuery, optimizationStrategyName).getExecutionGraphAsJson();
}

FogExecutionPlan OptimizerService::getExecutionPlan(InputQueryPtr inputQuery, string optimizationStrategyName) {
  FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
  const FogTopologyPlanPtr &topologyPlan = fogTopologyManager.getTopologyPlan();
  cout << "topo=" << topologyPlan->getTopologyPlanString() << endl;
  FogOptimizer queryOptimizer;

  QueryPlanBuilder queryPlanBuilder;
  const json::value &basePlan = queryPlanBuilder.getBasePlan(inputQuery);

  cout << "query plan=" << basePlan << endl;


  return queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);
}
