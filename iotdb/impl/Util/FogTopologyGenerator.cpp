
#include <Util/FogTopologyGenerator.hpp>
#include <Topology/FogTopologyManager.hpp>
#include <cpprest/json.h>

namespace iotdb {

    std::shared_ptr<FogTopologyPlan> FogTopologyGenerator::generateExampleTopology() {

        FogTopologyManager &fMgnr = FogTopologyManager::getInstance();

        const FogTopologyWorkerNodePtr &sinkNode = fMgnr.createFogWorkerNode(CPUCapacity::HIGH);

        const FogTopologyWorkerNodePtr &workerNode1 = fMgnr.createFogWorkerNode(CPUCapacity::MEDIUM);
        const FogTopologyWorkerNodePtr &workerNode2 = fMgnr.createFogWorkerNode(CPUCapacity::MEDIUM);

        const FogTopologySensorNodePtr &sensorNode1 = fMgnr.createFogSensorNode(CPUCapacity::HIGH);
        const FogTopologySensorNodePtr &sensorNode2 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
        const FogTopologySensorNodePtr &sensorNode3 = fMgnr.createFogSensorNode(CPUCapacity::LOW);
        const FogTopologySensorNodePtr &sensorNode4 = fMgnr.createFogSensorNode(CPUCapacity::MEDIUM);


        fMgnr.createFogTopologyLink(workerNode1, sinkNode);
        fMgnr.createFogTopologyLink(workerNode2, sinkNode);

        fMgnr.createFogTopologyLink(sensorNode1, workerNode1);
        fMgnr.createFogTopologyLink(sensorNode2, workerNode1);

        fMgnr.createFogTopologyLink(sensorNode3, workerNode2);
        fMgnr.createFogTopologyLink(sensorNode4, workerNode2);

        const FogTopologyPlanPtr &topologyPlan = fMgnr.getTopologyPlan();
        return topologyPlan;
    }

    web::json::value
    FogTopologyGenerator::getFogTopologyAsJson(const std::shared_ptr<FogTopologyPlan> *fogTopologyPtr) {

        std::cout << fogTopologyPtr->get()->getRootNode()->getId();

    }
}
