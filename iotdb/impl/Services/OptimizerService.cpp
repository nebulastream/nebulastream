#include <Services/OptimizerService.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Util/Logger.hpp>

using namespace NES;
using namespace web;
using namespace std;

json::value OptimizerService::getExecutionPlanAsJson(InputQueryPtr inputQuery, string optimizationStrategyName) {
  return getExecutionPlan(inputQuery, optimizationStrategyName).getExecutionGraphAsJson();
}

NESExecutionPlan OptimizerService::getExecutionPlan(InputQueryPtr inputQuery, string optimizationStrategyName) {
  NESTopologyManager &nesTopologyManager = NESTopologyManager::getInstance();
  const NESTopologyPlanPtr &topologyPlan = nesTopologyManager.getNESTopologyPlan();
  NES_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString())

  NESOptimizer queryOptimizer;

  OperatorJsonUtil operatorJsonUtil;
  const json::value &basePlan = operatorJsonUtil.getBasePlan(inputQuery);

  NES_DEBUG("OptimizerService: query plan=" << basePlan)

  return queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);
}
