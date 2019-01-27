#include <Topology/FogToplogyManager.hpp>

/**
 *
 * Open Questions:
 * 		- support half-duplex links?
 *		- how to handle the ids among nodes?
 *
 *	Todo:
 *		- add remove columns/row in matrix
 *		- add unit tests
 */

void createTestTopo(FogTopologyManager* fMgnr)
{
	FogToplogyNodePtr f1 = std::make_shared<FogToplogyNode>();
	fMgnr->addFogNode(f1);

	FogToplogySensorPtr s1 = std::make_shared<FogToplogySensor>();
	fMgnr->addSensorNode(s1);

	FogToplogySensorPtr s2 = std::make_shared<FogToplogySensor>();
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
