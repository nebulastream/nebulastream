
#include <Topology/FogTopologyManager.hpp>
#include "Services/FogTopologyService.hpp"

#include <cpprest/json.h>

using namespace iotdb;

json::value FogTopologyService::getFogTopologyAsJson() {

  FogTopologyManager &fogTopologyManager = FogTopologyManager::getInstance();
  fogTopologyManager.createExampleTopology();
  return fogTopologyManager.getFogTopologyGraphAsJson();
}