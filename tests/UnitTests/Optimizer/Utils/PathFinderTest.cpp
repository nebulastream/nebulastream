#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace std;
using namespace NES;

class PathFinderTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup PathFinderTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("PathFinderTest.log", NES::LOG_DEBUG);
        std::cout << "Setup PathFinderTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup PathFinderTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down PathFinderTest test class." << std::endl;
    }
};

TEST_F(PathFinderTest, find_path_with_max_bandwidth) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode = topologyManager->createNESWorkerNode(
        0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(3, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf, sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode, 3, 1);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode, 3, 1);
    topologyManager->createNESTopologyLink(sensorNode1, workerNode1, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode1, workerNode2, 3, 1);

    auto path = pathFinder->findPathWithMaxBandwidth(sensorNode1, sinkNode);

    EXPECT_FALSE(path.empty());

    std::vector<NESTopologyEntryPtr> expectedList = {sensorNode1, workerNode2,
                                                     sinkNode};

    for (NESTopologyEntryPtr expectedNode : expectedList) {
        auto result = std::find_if(
            path.begin(), path.end(),
            [expectedNode](NESTopologyEntryPtr actualNode) {
                return actualNode->getId() == expectedNode->getId();
            });

        EXPECT_TRUE(result != path.end());
    }
}

TEST_F(PathFinderTest, find_path_with_max_of_min_bandwidth) {

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode =
        topologyManager->createNESWorkerNode(
            0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode3 = topologyManager->createNESWorkerNode(3, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode4 = topologyManager->createNESWorkerNode(4, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(5, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature",
                                    schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode,
                                           3, 1);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode,
                                           3, 1);
    topologyManager->createNESTopologyLink(workerNode3,
                                           workerNode1, 2, 1);
    topologyManager->createNESTopologyLink(workerNode4,
                                           workerNode2, 1, 1);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode3, 2, 3);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode4, 3, 1);

    auto path = pathFinder->findPathWithMaxBandwidth(sensorNode1, sinkNode);

    EXPECT_FALSE(path.empty());

    std::vector<NESTopologyEntryPtr> expectedList = {sensorNode1, workerNode3,
                                                     workerNode1, sinkNode};

    for (NESTopologyEntryPtr expectedNode : expectedList) {
        auto result = std::find_if(
            path.begin(), path.end(),
            [expectedNode](NESTopologyEntryPtr actualNode) {
                return actualNode->getId() == expectedNode->getId();
            });

        EXPECT_TRUE(result != path.end());
    }
}

TEST_F(PathFinderTest, find_path_with_min_latency) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode =
        topologyManager->createNESWorkerNode(
            0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(3, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature",
                                    schema);

    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode,
                                           3, 3);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode,
                                           3, 1);

    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode1, 1, 1);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode2, 1, 1);

    auto path = pathFinder->findPathWithMinLinkLatency(sensorNode1, sinkNode);

    EXPECT_FALSE(path.empty());

    std::vector<NESTopologyEntryPtr> expectedList = {sensorNode1, workerNode2,
                                                     sinkNode};

    for (NESTopologyEntryPtr expectedNode : expectedList) {
        auto result = std::find_if(
            path.begin(), path.end(),
            [expectedNode](NESTopologyEntryPtr actualNode) {
                return actualNode->getId() == expectedNode->getId();
            });

        EXPECT_TRUE(result != path.end());
    }
}

TEST_F(PathFinderTest, find_path_with_min_of_max_latency) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode =
        topologyManager->createNESWorkerNode(
            0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode3 = topologyManager->createNESWorkerNode(3, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode4 = topologyManager->createNESWorkerNode(4, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(5, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode,
                                           3, 1);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode,
                                           3, 1);
    topologyManager->createNESTopologyLink(workerNode3,
                                           workerNode1, 3, 1);
    topologyManager->createNESTopologyLink(workerNode4,
                                           workerNode2, 3, 2);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode3, 1, 1);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode4, 1, 1);

    auto path = pathFinder->findPathWithMinLinkLatency(sensorNode1, sinkNode);

    EXPECT_FALSE(path.empty());

    std::vector<NESTopologyEntryPtr> expectedList = {sensorNode1, workerNode3,
                                                     workerNode1, sinkNode};

    for (NESTopologyEntryPtr expectedNode : expectedList) {
        auto result = std::find_if(
            path.begin(), path.end(),
            [expectedNode](NESTopologyEntryPtr actualNode) {
                return actualNode->getId() == expectedNode->getId();
            });

        EXPECT_TRUE(result != path.end());
    }
}

TEST_F(PathFinderTest, find_all_paths_between_source_destination) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    //prepare
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode =
        topologyManager->createNESWorkerNode(
            0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(3, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode,
                                           3, 1);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode,
                                           3, 1);

    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode1, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode1,
                                           workerNode2, 1, 3);

    //execute
    auto paths = pathFinder->findAllPathsBetween(sensorNode1, sinkNode);

    //assert
    EXPECT_TRUE(paths.size() == 2);
}

TEST_F(PathFinderTest, find_common_path_between_source_destination) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    //prepare
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode =
        topologyManager->createNESWorkerNode(
            0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode3 = topologyManager->createNESWorkerNode(3, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode4 = topologyManager->createNESWorkerNode(4, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode5 = topologyManager->createNESWorkerNode(5, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode6 = topologyManager->createNESWorkerNode(6, "localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr workerNode7 = topologyManager->createNESWorkerNode(7, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode8 = topologyManager->createNESWorkerNode(8, "localhost", CPUCapacity::MEDIUM);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(9, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    const NESTopologySensorNodePtr sensorNode2 = topologyManager->createNESSensorNode(10, "localhost", CPUCapacity::LOW);
    sensorNode2->setPhysicalStreamName("humidity1");
    streamCatalog->addLogicalStream("humidity1", schema);
    conf.physicalStreamName = "humidity1";
    StreamCatalogEntryPtr e2 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode2);
    assert(streamCatalog->addPhysicalStream("humidity1", e2));

    const NESTopologySensorNodePtr sensorNode3 = topologyManager->createNESSensorNode(11, "localhost", CPUCapacity::LOW);
    sensorNode3->setPhysicalStreamName("temperature2");

    conf.physicalStreamName = "temperature2";
    StreamCatalogEntryPtr e3 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode3);
    assert(streamCatalog->addPhysicalStream("temperature", e3));

    const NESTopologySensorNodePtr sensorNode4 = topologyManager->createNESSensorNode(12, "localhost", CPUCapacity::MEDIUM);
    sensorNode4->setPhysicalStreamName("humidity2");
    streamCatalog->addLogicalStream("humidity2", schema);

    conf.physicalStreamName = "humidity2";
    StreamCatalogEntryPtr e4 = std::make_shared<StreamCatalogEntry>(conf,
                                                                    sensorNode4);
    assert(streamCatalog->addPhysicalStream("humidity2", e4));

    topologyManager->createNESTopologyLink(workerNode1, sinkNode, 3, 1);
    topologyManager->createNESTopologyLink(workerNode2, sinkNode, 3, 1);
    topologyManager->createNESTopologyLink(workerNode3, sinkNode, 3, 1);
    topologyManager->createNESTopologyLink(workerNode4, workerNode1, 2, 2);
    topologyManager->createNESTopologyLink(workerNode4, workerNode2, 2, 2);
    topologyManager->createNESTopologyLink(workerNode5, workerNode2, 2, 2);
    topologyManager->createNESTopologyLink(workerNode5, workerNode3, 2, 2);
    topologyManager->createNESTopologyLink(workerNode6, workerNode4, 2, 2);
    topologyManager->createNESTopologyLink(workerNode7, workerNode5, 2, 2);

    topologyManager->createNESTopologyLink(sensorNode1, workerNode6, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode2, workerNode6, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode3, workerNode7, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode4, workerNode7, 1, 3);

    const vector<NESTopologyEntryPtr>& pathFromSensor1 = std::vector<NESTopologyEntryPtr>{sensorNode1, workerNode6, workerNode4, workerNode2, sinkNode};
    const vector<NESTopologyEntryPtr>& pathFromSensor2 = std::vector<NESTopologyEntryPtr>{sensorNode2, workerNode6, workerNode4, workerNode2, sinkNode};
    const vector<NESTopologyEntryPtr>& pathFromSensor3 = std::vector<NESTopologyEntryPtr>{sensorNode3, workerNode7, workerNode5, workerNode2, sinkNode};
    const vector<NESTopologyEntryPtr>& PathFromSensor4 = std::vector<NESTopologyEntryPtr>{sensorNode4, workerNode7, workerNode5, workerNode2, sinkNode};
    std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> expectedMap =
        {{sensorNode1, pathFromSensor1}, {sensorNode2, pathFromSensor2}, {sensorNode3, pathFromSensor3}, {sensorNode4, PathFromSensor4}};

    // execute
    const vector<NESTopologyEntryPtr>& sources = std::vector<NESTopologyEntryPtr>{sensorNode1, sensorNode2, sensorNode3, sensorNode4};
    auto actualMap = pathFinder->findUniquePathBetween(sources, sinkNode);

    // assert
    EXPECT_TRUE(actualMap.size() == 4);
    for (auto pair : actualMap) {
        vector<NESTopologyEntryPtr>& expectedPath = expectedMap[pair.first];
        for (NESTopologyEntryPtr expectedNode : expectedPath) {
            auto result = std::find_if(pair.second.begin(), pair.second.end(), [expectedNode](NESTopologyEntryPtr actualNode) {
                return actualNode->getId() == expectedNode->getId();
            });

            EXPECT_TRUE(result != pair.second.end());
        }
    }
}

TEST_F(PathFinderTest, find_path_from_non_linked_source) {

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());
    try {
        SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const NESTopologyWorkerNodePtr sinkNode = topologyManager->createNESWorkerNode(0, "localhost", CPUCapacity::HIGH);

        const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);
        sensorNode1->setPhysicalStreamName("temperature1");
        streamCatalog->addLogicalStream("temperature", schema);

        PhysicalStreamConfig conf;
        conf.physicalStreamName = "temperature1";
        StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf, sensorNode1);
        assert(streamCatalog->addPhysicalStream("temperature", e1));

        const auto& pathList = pathFinder->findPathBetween(sensorNode1, sinkNode);

        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(PathFinderTest, find_path_between_non_linked_source_and_destination) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode = topologyManager->createNESWorkerNode(0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::HIGH);
    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(2, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);

    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf, sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(sensorNode1, workerNode1, 1, 3);

    const auto& pathList = pathFinder->findPathBetween(sensorNode1, sinkNode);
    EXPECT_TRUE(pathList.empty());
}

TEST_F(PathFinderTest, find_path_between_linked_source_and_destination) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    PathFinderPtr pathFinder = PathFinder::create(topologyManager->getNESTopologyPlan());

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const NESTopologyWorkerNodePtr sinkNode = topologyManager->createNESWorkerNode(0, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode1 = topologyManager->createNESWorkerNode(1, "localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr workerNode2 = topologyManager->createNESWorkerNode(2, "localhost", CPUCapacity::HIGH);

    const NESTopologySensorNodePtr sensorNode1 = topologyManager->createNESSensorNode(3, "localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    streamCatalog->addLogicalStream("temperature", schema);
    PhysicalStreamConfig conf;
    conf.physicalStreamName = "temperature1";
    StreamCatalogEntryPtr e1 = std::make_shared<StreamCatalogEntry>(conf, sensorNode1);
    assert(streamCatalog->addPhysicalStream("temperature", e1));

    topologyManager->createNESTopologyLink(workerNode2, sinkNode, 1, 3);
    topologyManager->createNESTopologyLink(workerNode1, workerNode2, 1, 3);
    topologyManager->createNESTopologyLink(sensorNode1, workerNode1, 1, 3);

    const auto& actualPath = pathFinder->findPathBetween(sensorNode1, sinkNode);
    EXPECT_TRUE(!actualPath.empty());

    std::vector<NESTopologyEntryPtr> expectedPath = {sensorNode1, workerNode1, workerNode2, sinkNode};

    for (NESTopologyEntryPtr expectedNode : expectedPath) {
        auto result = std::find_if(actualPath.begin(), actualPath.end(), [expectedNode](NESTopologyEntryPtr actualNode) {
            return actualNode->getId() == expectedNode->getId();
        });

        EXPECT_TRUE(result != actualPath.end());
    }
}
