#include <Topology/FogToplogyManager.hpp>


void createTestTopo()
{
	FogToplogyNodePtr f1 = std::make_shared<FogToplogyNode>(1);

	FogToplogySensorPtr s1 = std::make_shared<FogToplogySensor>(1);

	s1->addParentNode(f1);







}

int main(int argc, const char *argv[]) {
	FogTopologyManager* fMgnr = new FogTopologyManager();




	FogTopologyPlanPtr fPlan = fMgnr->getPlan();

}
