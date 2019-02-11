#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include "Topology/FogTopologyPlan.hpp"

/**
 * TODO: add return of create
 */
typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

class FogTopologyManager {
public:
  FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

  FogTopologyWorkerNodePtr createFogWorkerNode() { return currentPlan->createFogWorkerNode(); }

  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(ptr); }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(ptr); }

  FogTopologySensorNodePtr createFogSensorNode() { return currentPlan->createFogSensorNode(); }

  FogTopologyLinkPtr createFogNodeLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {
    return currentPlan->createFogNodeLink(pSourceNode, pDestNode);
  }

  bool removeFogNodeLink(FogTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

  void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }
  std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

private:
  FogTopologyPlanPtr currentPlan;
};

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
