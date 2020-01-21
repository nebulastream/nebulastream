#ifndef INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_
#define INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_

namespace NES{
//#include "NESTopologyPlan.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Topology/NESTopologyManager.hpp>

/**\brief:
   *          This is a temporary method used for simulating an example topology.
   *
   */
  //TODO: this should not be here and should be moved to a test class
  void createExampleTopology() {

    NESTopologyManager::getInstance().resetNESTopologyPlan();

    Schema schema = Schema::create().addField("id", BasicType::UINT32).addField(
          "value", BasicType::UINT64);
    const NESTopologyCoordinatorNodePtr &sinkNode = NESTopologyManager::getInstance().createNESCoordinatorNode("localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr &workerNode1 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode2 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode3 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode4 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode5 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode6 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode7 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr &workerNode8 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode9 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::LOW);
    const NESTopologyWorkerNodePtr &workerNode10 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode11 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr &workerNode12 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode13 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode14 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode15 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::LOW);
    const NESTopologyWorkerNodePtr &workerNode16 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode17 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode18 = NESTopologyManager::getInstance().createNESWorkerNode("localhost", CPUCapacity::MEDIUM);


    const NESTopologySensorNodePtr &sensorNode1 = NESTopologyManager::getInstance().createNESSensorNode("localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    StreamCatalog::instance().addLogicalStream("temperature1", std::make_shared<Schema>(schema));
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>("", "", sensorNode1, "temperature1");
    assert(StreamCatalog::instance().addPhysicalStream("temperature1", e1));

    const NESTopologySensorNodePtr &sensorNode2 = NESTopologyManager::getInstance().createNESSensorNode("localhost", CPUCapacity::LOW);
    sensorNode2->setPhysicalStreamName("humidity1");
    StreamCatalog::instance().addLogicalStream("humidity1", std::make_shared<Schema>(schema));
    StreamCatalogEntryPtr e2 = std::make_shared<StreamCatalogEntry>("", "", sensorNode2, "humidity1");
    assert(StreamCatalog::instance().addPhysicalStream("humidity1", e2));

    const NESTopologySensorNodePtr &sensorNode3 = NESTopologyManager::getInstance().createNESSensorNode("localhost", CPUCapacity::LOW);
    sensorNode3->setPhysicalStreamName("temperature2");
    StreamCatalog::instance().addLogicalStream("temperature2", std::make_shared<Schema>(schema));
    StreamCatalogEntryPtr e3 = std::make_shared<StreamCatalogEntry>("", "", sensorNode3, "temperature2");
    assert(StreamCatalog::instance().addPhysicalStream("temperature2", e3));

    const NESTopologySensorNodePtr &sensorNode4 = NESTopologyManager::getInstance().createNESSensorNode("localhost", CPUCapacity::MEDIUM);
    sensorNode4->setPhysicalStreamName("humidity2");
    StreamCatalog::instance().addLogicalStream("humidity2", std::make_shared<Schema>(schema));
    StreamCatalogEntryPtr e4 = std::make_shared<StreamCatalogEntry>("", "", sensorNode4, "humidity2");
    assert(StreamCatalog::instance().addPhysicalStream("humidity2", e4));

    NESTopologyManager::getInstance().createNESTopologyLink(workerNode1, sinkNode);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode2, sinkNode);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode3, sinkNode);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode4, workerNode1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode5, workerNode2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode6, workerNode3);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode10, workerNode6);

    NESTopologyManager::getInstance().createNESTopologyLink(workerNode7, workerNode4);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode8, workerNode4);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode8, workerNode5);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode9, workerNode6);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode10, workerNode5);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode11, workerNode7);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode12, workerNode8);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode13, workerNode12);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode13, workerNode11);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode14, workerNode12);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode15, workerNode10);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode16, workerNode15);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode17, workerNode15);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode18, workerNode9);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode12, workerNode7);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode15, workerNode18);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode1, workerNode13);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode2, workerNode14);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode3, workerNode16);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode4, workerNode17);

  }
}
#endif /* INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_ */
