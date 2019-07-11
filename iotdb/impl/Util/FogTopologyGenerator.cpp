
#include <Util/FogTopologyGenerator.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

void FogTopologyGenerator::generateExampleTopology() {

    FogTopologyManager& fMgnr = FogTopologyManager::getInstance();

    FogTopologySensorNodePtr sensorNode1 = fMgnr.createFogSensorNode(CPUCapacity::HIGH);
    FogTopologySensorNodePtr sensorNode2 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
    FogTopologySensorNodePtr sensorNode3 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
    FogTopologySensorNodePtr sensorNode4 = fMgnr.createFogSensorNode(CPUCapacity::MEDIUM);

}