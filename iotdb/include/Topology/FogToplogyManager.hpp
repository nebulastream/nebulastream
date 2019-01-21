#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_

#include "../Topology/FogTopologyPlan.hpp"

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

	static FogTopologyPlanPtr getPlan()
	{
		return FogTopologyPlanPtr();
	}

private:
	FogTopologyPlanPtr currentPlan;

};



#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYMANAGER_HPP_ */
