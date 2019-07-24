
#include <cpprest/json.h>
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include "Services/FogTopologyService.hpp"

using namespace iotdb;

json::value FogTopologyService::getFogTopologyAsJson() {

    FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
    fogTopologyManager.createExampleTopology();
    auto fogTopology = fogTopologyManager.getFogTopologyGraphAsTreeJson();
    return fogTopology;
}

json::value FogTopologyService::getExecutionPlanAsJson(string userQuery, string optimizationStrategyName) {
    //build query from string
    InputQuery inputQuery(iotdb::createQueryFromCodeString(userQuery));

    FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
    fogTopologyManager.createExampleTopology();
    const FogTopologyPlanPtr &topologyPlan = fogTopologyManager.getTopologyPlan();
    
    FogOptimizer queryOptimizer;

    const FogExecutionPlan &fogExecutionPlan = queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan);

    return fogExecutionPlan.getExecutionGraphAsJson();
}