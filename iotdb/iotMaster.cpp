#include <Topology/FogTopologyManager.hpp>

/**
 *
 * Open Questions:
 * 		- support half-duplex links?
 *		- how to handle the ids among nodes?
 *		- how does a node knows to whom it is connected?
 *		- make a parrent class for fogentries?
 *
 *	Todo:
 *		- add remove columns/row in matrix
 *		- add node ids
 *		- add unit tests
 *		- make TopologyManager Singleton
 *		- add createFogNode/FogSensor
 *		- make it thread safe
 */

void createTestTopo(FogTopologyManager* fMgnr)
{
	FogTopologyNodePtr f1 = std::make_shared<FogTopologyNode>();
	fMgnr->addFogNode(f1);

	FogTopologySensorPtr s1 = std::make_shared<FogTopologySensor>();
	fMgnr->addSensorNode(s1);

	FogTopologySensorPtr s2 = std::make_shared<FogTopologySensor>();
	fMgnr->addSensorNode(s2);

	fMgnr->addLink(f1->getNodeId(), s1->getSensorId(), NodeToSensor);
	fMgnr->addLink(f1->getNodeId(), s2->getSensorId(), NodeToSensor);

	FogTopologyPlanPtr fPlan = fMgnr->getPlan();

	fPlan->printPlan();
}

int main(int argc, const char *argv[]) {
	FogTopologyManager* fMgnr = new FogTopologyManager();
	createTestTopo(fMgnr);
}
