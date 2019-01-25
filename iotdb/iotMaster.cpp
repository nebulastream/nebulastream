#include <Topology/FogToplogyManager.hpp>


void createTestTopo(FogTopologyManager* fMgnr)
{
	FogToplogyNodePtr f1 = std::make_shared<FogToplogyNode>();
	fMgnr->addFogNode(f1);

	FogToplogySensorPtr s1 = std::make_shared<FogToplogySensor>();
	fMgnr->addSensorNode(s1);

	fMgnr->addLink(f1->getNodeId(), s1->getSensorId(), NodeToSensor);

	FogTopologyPlanPtr fPlan = fMgnr->getPlan();

	fPlan->printPlan();
}

int main(int argc, const char *argv[]) {
	FogTopologyManager* fMgnr = new FogTopologyManager();
	createTestTopo(fMgnr);

}
