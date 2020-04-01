#ifndef INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_
#define INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_

namespace NES {
#include <Catalogs/StreamCatalog.hpp>
#include <Topology/NESTopologyManager.hpp>

/**\brief:
   *          This is a temporary method used for simulating an example topology.
   *
   */
void createExampleTopology() {

    NESTopologyManager::getInstance().resetNESTopologyPlan();

    SchemaPtr schema = SchemaTemp::create()->addField("id", BasicType::UINT32)->addField(
        "value", BasicType::UINT64);
    const NESTopologyCoordinatorNodePtr
        & sinkNode =
        NESTopologyManager::getInstance().createNESCoordinatorNode(/**Node Id**/ 0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr
        & workerNode1 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode2 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/2, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode3 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/3, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode4 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/4, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode5 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/5, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode6 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/6, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode7 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/7, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr
        & workerNode8 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/8, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode9 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/9, "localhost", CPUCapacity::LOW);
    const NESTopologyWorkerNodePtr
        & workerNode10 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/10, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode11 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/11, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr
        & workerNode12 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/12, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode13 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/13, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode14 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/14, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode15 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/15, "localhost", CPUCapacity::LOW);
    const NESTopologyWorkerNodePtr
        & workerNode16 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/16, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode17 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/17, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr
        & workerNode18 =
        NESTopologyManager::getInstance().createNESWorkerNode(/**Node Id**/18, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr
        & sensorNode1 =
        NESTopologyManager::getInstance().createNESSensorNode(/**Node Id**/19, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    StreamCatalog::instance().addLogicalStream("temperature", std::make_shared<SchemaTemp>(schema));
    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode1);
    assert(StreamCatalog::instance().addPhysicalStream("temperature", e1));

    const NESTopologySensorNodePtr
        & sensorNode2 =
        NESTopologyManager::getInstance().createNESSensorNode(/**Node Id**/20, "localhost", CPUCapacity::LOW);
    sensorNode2->setPhysicalStreamName("humidity1");
    StreamCatalog::instance().addLogicalStream("humidity", std::make_shared<SchemaTemp>(schema));
    streamConf.physicalStreamName = "humidity1";
    StreamCatalogEntryPtr e2 = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode2);
    assert(StreamCatalog::instance().addPhysicalStream("humidity", e2));

    const NESTopologySensorNodePtr
        & sensorNode3 =
        NESTopologyManager::getInstance().createNESSensorNode(/**Node Id**/21, "localhost", CPUCapacity::LOW);
    sensorNode3->setPhysicalStreamName("temperature2");
    streamConf.physicalStreamName = "temperature2";
    StreamCatalogEntryPtr e3 = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode3);
    assert(StreamCatalog::instance().addPhysicalStream("temperature", e3));

    const NESTopologySensorNodePtr
        & sensorNode4 =
        NESTopologyManager::getInstance().createNESSensorNode(/**Node Id**/22, "localhost", CPUCapacity::MEDIUM);
    sensorNode4->setPhysicalStreamName("humidity2");
    streamConf.physicalStreamName = "humidity2";
    StreamCatalogEntryPtr e4 = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode4);
    assert(StreamCatalog::instance().addPhysicalStream("humidity", e4));

    NESTopologyManager::getInstance().createNESTopologyLink(workerNode1,
                                                            sinkNode,
        /**Link Capacity**/3,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode2,
                                                            sinkNode,
        /**Link Capacity**/3,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode3,
                                                            sinkNode,
        /**Link Capacity**/3,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode4,
                                                            workerNode1,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode5,
                                                            workerNode2,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode6,
                                                            workerNode3,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode10,
                                                            workerNode6,
        /**Link Capacity**/2,
        /**Link Latency**/2);

    NESTopologyManager::getInstance().createNESTopologyLink(workerNode7,
                                                            workerNode4,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode8,
                                                            workerNode4,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode8,
                                                            workerNode5,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode9,
                                                            workerNode6,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode10,
                                                            workerNode5,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode11,
                                                            workerNode7,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode12,
                                                            workerNode8,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode13,
                                                            workerNode12,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode13,
                                                            workerNode11,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode14,
                                                            workerNode12,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode15,
                                                            workerNode10,
        /**Link Capacity**/2,
        /**Link Latency**/1);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode16,
                                                            workerNode15,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode17,
                                                            workerNode15,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode18,
                                                            workerNode9,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode12,
                                                            workerNode7,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(workerNode15,
                                                            workerNode18,
        /**Link Capacity**/2,
        /**Link Latency**/2);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode1,
                                                            workerNode13,
        /**Link Capacity**/1,
        /**Link Latency**/3);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode2,
                                                            workerNode14,
        /**Link Capacity**/1,
        /**Link Latency**/3);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode3,
                                                            workerNode16,
        /**Link Capacity**/1,
        /**Link Latency**/3);
    NESTopologyManager::getInstance().createNESTopologyLink(sensorNode4,
                                                            workerNode17,
        /**Link Capacity**/1,
        /**Link Latency**/3);

}
}
#endif /* INCLUDE_TOPOLOGY_TESTTOPOLOGY_HPP_ */
