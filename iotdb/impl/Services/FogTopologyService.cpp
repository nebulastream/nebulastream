
#include "Services/FogTopologyService.hpp"

#include <cpprest/json.h>
#include "../../include/Topology/NESTopologyManager.hpp"

using namespace iotdb;

json::value FogTopologyService::getFogTopologyAsJson() {

  NESTopologyManager &fogTopologyManager = NESTopologyManager::getInstance();
  fogTopologyManager.createExampleTopology();
  return fogTopologyManager.getNESTopologyGraphAsJson();
}