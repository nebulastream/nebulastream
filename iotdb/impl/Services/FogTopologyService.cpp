
#include <cpprest/json.h>
#include <Topology/FogTopologyManager.hpp>
#include "Services/FogTopologyService.hpp"

using namespace iotdb;

json::value FogTopologyService::getFogTopologyAsJson() {

    FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
    fogTopologyManager.createExampleTopology();
    auto fogTopology = fogTopologyManager.getFogTopologyGraphAsTreeJson();
    return fogTopology;
}