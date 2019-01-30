#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include "Topology/FogTopologyPlan.hpp"

typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;



class FogTopologyManager{
public:
	FogTopologyManager()
	{
		currentPlan = std::make_shared<FogTopologyPlan>();
	}


	void addFogNode(FogTopologyNodePtr ptr)
	{
		currentPlan->addFogNode(ptr);
	}

	void addSensorNode(FogTopologySensorPtr ptr)
	{
		currentPlan->addFogSensor(ptr);
	}

	void addLink(size_t pSourceNodeId, size_t pDestNodeId, LinkType type)
	{
		currentPlan->addFogTopologyLink(pSourceNodeId,pDestNodeId,type);
	}

	FogTopologyPlanPtr getPlan()
	{
		return currentPlan;
	}

private:
	FogTopologyPlanPtr currentPlan;

};



#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
