#include "Services/OptimizerService.hpp"
#include <CodeGen/QueryPlanBuilder.hpp>
#include "../../include/Optimizer/NESOptimizer.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"


using namespace iotdb;
using namespace web;
using namespace std;

json::value OptimizerService::getExecutionPlanAsJson(InputQueryPtr inputQuery, string optimizationStrategyName) {
  return getExecutionPlan(inputQuery, optimizationStrategyName).getExecutionGraphAsJson();
}

NESExecutionPlan OptimizerService::getExecutionPlan(InputQueryPtr inputQuery, string optimizationStrategyName) {
  NESTopologyManager &nesTopologyManager = NESTopologyManager::getInstance();
  const NESTopologyPlanPtr &topologyPlan = nesTopologyManager.getNESTopologyPlan();
  IOTDB_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString())

  NESOptimizer queryOptimizer;

  QueryPlanBuilder queryPlanBuilder;
  const json::value &basePlan = queryPlanBuilder.getBasePlan(inputQuery);

  IOTDB_DEBUG("OptimizerService: query plan=" << basePlan)

  return queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);
}
