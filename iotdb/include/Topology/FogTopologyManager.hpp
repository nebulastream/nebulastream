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

  FogTopologyWorkerNodePtr createFogWorkerNode()
  {
	  return currentPlan->createFogWorkerNode();
  }

  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr)
  {
	  return currentPlan->removeFogWorkerNode(ptr);
  }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr)
  {
	  return currentPlan->removeFogSensorNode(ptr);
  }

  FogTopologySensorNodePtr createFogSensorNode()
  {
	  return currentPlan->createFogSensorNode();
  }

  FogTopologyLinkPtr createFogNodeLink(size_t pSourceNodeId, size_t pDestNodeId)
  {
	  return currentPlan->createFogNodeLink(pSourceNodeId, pDestNodeId);
  }

  bool removeFogNodeLink(FogTopologyLinkPtr linkPtr)
  {
	  return currentPlan->removeFogTopologyLink(linkPtr);
  }


  FogTopologyPlanPtr getPlan() { return currentPlan; }

private:
  FogTopologyPlanPtr currentPlan;
};

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
