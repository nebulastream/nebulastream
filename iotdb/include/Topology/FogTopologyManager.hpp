#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include "Topology/FogTopologyPlan.hpp"

typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

class FogTopologyManager {
public:
  FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

  FogTopologyNodePtr createFogNode()
  {
	  return currentPlan->createFogWorkerNode();
  }

  FogTopologySensorPtr createSensorNode()
  {
	  return currentPlan->createFogSensorNode();
  }

  void createFogNodeLink(size_t pSourceNodeId, size_t pDestNodeId)
  {
	  return currentPlan->createFogNodeLink(pSourceNodeId, pDestNodeId);
  }

  FogTopologyPlanPtr getPlan() { return currentPlan; }

private:
  FogTopologyPlanPtr currentPlan;
};

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
