#ifndef INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_
#define INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_

namespace iotdb{
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
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode1, workerNode1);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode2, workerNode1);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode3, workerNode2);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode4, workerNode2);
  }
}
#endif /* INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_ */
