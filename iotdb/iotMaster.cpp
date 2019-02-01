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
	FogTopologyNodePtr f1 = fMgnr->createFogNode();

	FogTopologySensorPtr s1 = fMgnr->createSensorNode();

//	FogTopologySensorPtr s2 = fMgnr->createSensorNode();

	fMgnr->createFogNodeLink(s1->getID(), f1->getID());

	FogTopologyPlanPtr fPlan = fMgnr->getPlan();

	fPlan->printPlan();
}

int main(int argc, const char *argv[]) {
	FogTopologyManager* fMgnr = new FogTopologyManager();
	createTestTopo(fMgnr);
}
