#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_

#include <memory>

#include "Topology/FogTopologyPlan.hpp"

typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;



class FogTopologyManager{
public:
	FogTopologyManager()
	{
		currentPlan = std::make_shared<FogTopologyPlan>();
	}


	void addFogNode(FogToplogyNodePtr ptr)
	{
		currentPlan->addFogNode(ptr);
	}

	void addSensorNode(FogToplogySensorPtr ptr)
	{
		currentPlan->addFogSensor(ptr);
	}

	void addLink(size_t pSourceNodeID, size_t pDestNodeID, LinkType type)
	{
		currentPlan->addFogTopologyLink(pSourceNodeID,pDestNodeID,type);
	}

	FogTopologyPlanPtr getPlan()
	{
		return currentPlan;
	}

private:
	FogTopologyPlanPtr currentPlan;

};



#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_ */
