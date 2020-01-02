
#include "../../include/Services/NESTopologyService.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
using namespace iotdb;

json::value NESTopologyService::getNESTopologyAsJson() {

  NESTopologyManager &fogTopologyManager = NESTopologyManager::getInstance();
  //TODO:fix the cyclic dependency
//  createExampleTopology();
  return fogTopologyManager.getNESTopologyGraphAsJson();
}
