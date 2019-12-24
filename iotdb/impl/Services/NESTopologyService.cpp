
#include "../../include/Services/NESTopologyService.hpp"

#include <cpprest/json.h>
#include "../../include/Topology/NESTopologyManager.hpp"

using namespace iotdb;

json::value NESTopologyService::getNESTopologyAsJson() {

  NESTopologyManager &fogTopologyManager = NESTopologyManager::getInstance();
  fogTopologyManager.createExampleTopology();
  return fogTopologyManager.getNESTopologyGraphAsJson();
}