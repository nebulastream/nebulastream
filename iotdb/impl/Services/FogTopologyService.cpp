
#include <cpprest/json.h>
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/QueryOptimizer.hpp>
#include "Services/FogTopologyService.hpp"

using namespace iotdb;

json::value FogTopologyService::getFogTopologyAsJson() {

    FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
    fogTopologyManager.createExampleTopology();
    auto fogTopology = fogTopologyManager.getFogTopologyGraphAsTreeJson();
    return fogTopology;
}

json::value FogTopologyService::getExecutionPlanAsJson(std::string userQuery) {
    //build query from string
    InputQuery inputQuery(iotdb::createQueryFromCodeString(userQuery));

    FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
    fogTopologyManager.createExampleTopology();
    const FogTopologyPlanPtr &topologyPlan = fogTopologyManager.getTopologyPlan();
    
    QueryOptimizer queryOptimizer;

    const ExecutionGraph &graph = queryOptimizer.prepareExecutionGraph("HLF", inputQuery, topologyPlan);

    json::value val;
    return val;
}