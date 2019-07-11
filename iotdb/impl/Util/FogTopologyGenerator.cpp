
#include <Util/FogTopologyGenerator.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

void FogTopologyGenerator::generateExampleTopology() {

    FogTopologyManager& fMgnr = FogTopologyManager::getInstance();

    const FogTopologySensorNodePtr& sensorNode1 = fMgnr.createFogSensorNode(CPUCapacity::HIGH);
    const FogTopologySensorNodePtr& sensorNode2 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
    const FogTopologySensorNodePtr& sensorNode3 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
    const FogTopologySensorNodePtr& sensorNode4 = fMgnr.createFogSensorNode(CPUCapacity::MEDIUM);

    const FogTopologyWorkerNodePtr &workerNode1 = fMgnr.createFogWorkerNode(CPUCapacity::MEDIUM);
    const FogTopologyWorkerNodePtr &workerNode2 = fMgnr.createFogWorkerNode(CPUCapacity::MEDIUM);

    const FogTopologyWorkerNodePtr &sinkNode = fMgnr.createFogWorkerNode(CPUCapacity::HIGH);

    fMgnr.createFogTopologyLink(sensorNode1, workerNode1);
    fMgnr.createFogTopologyLink(sensorNode2, workerNode1);

    fMgnr.createFogTopologyLink(sensorNode3, workerNode2);
    fMgnr.createFogTopologyLink(sensorNode4, workerNode2);

    fMgnr.createFogTopologyLink(workerNode1, sinkNode);
    fMgnr.createFogTopologyLink(workerNode2, sinkNode);

    const FogTopologyPlanPtr &topologyPlan = fMgnr.getTopologyPlan();
}