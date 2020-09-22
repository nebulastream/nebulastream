#include <gtest/gtest.h>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>

#include <string>
using namespace std;
using namespace NES;

class CoordinatorEngineTest : public testing::Test {
  public:
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup NES Coordinator test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup NES Coordinator test case." << std::endl;
        NES::setupLogging("CoordinatorEngineTest.log", NES::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Nodes to topology");
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup NES Coordinator test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down NES Coordinator test class." << std::endl;
    }

    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;
    //std::string sensor_type = "default";
};

TEST_F(CoordinatorEngineTest, testRegisterUnregisterNode) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topology);

    auto nodeStats = NodeStats();
    size_t nodeId = coordinatorEngine->registerNode(ip, publish_port, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId, 0);

    size_t nodeId1 = coordinatorEngine->registerNode(ip, publish_port + 2, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId1, 0);

    //test register existing node
    auto nodeStats2 = NodeStats();
    size_t nodeId2 = coordinatorEngine->registerNode(ip, publish_port, 5000, 6, nodeStats2, NodeType::Sensor);
    EXPECT_EQ(nodeId2, 0);

    //test unregister not existing node
    bool successUnregisterNotExistingNode = coordinatorEngine->unregisterNode(552);
    EXPECT_FALSE(successUnregisterNotExistingNode);

    //test unregister existing node
    bool successUnregisterExistingNode = coordinatorEngine->unregisterNode(nodeId1);
    EXPECT_TRUE(successUnregisterExistingNode);
}

TEST_F(CoordinatorEngineTest, testRegisterUnregisterLogicalStream) {
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topology);
    std::string logicalStreamName = "testStream";
    std::string testSchema =
        "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    bool successRegisterLogicalStream = coordinatorEngine->registerLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(successRegisterLogicalStream);

    //test register existing stream
    bool successRegisterExistingLogicalStream = coordinatorEngine->registerLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(!successRegisterExistingLogicalStream);

    //test unregister not existing node
    bool successUnregisterNotExistingLogicalStream = coordinatorEngine->unregisterLogicalStream("asdasd");
    EXPECT_TRUE(!successUnregisterNotExistingLogicalStream);

    //test unregister existing node
    bool successUnregisterExistingLogicalStream = coordinatorEngine->unregisterLogicalStream(logicalStreamName);
    EXPECT_TRUE(successUnregisterExistingLogicalStream);
}

TEST_F(CoordinatorEngineTest, testRegisterUnregisterPhysicalStream) {
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topology);
    std::string physicalStreamName = "testStream";
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "testCSV.csv",
                                                                /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream");

    auto nodeStats = NodeStats();
    size_t nodeId = coordinatorEngine->registerNode(address, 4000, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId, 0);

    //setup test
    std::string testSchema =
        "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    bool successRegisterLogicalStream = coordinatorEngine->registerLogicalStream(conf->getLogicalStreamName(), testSchema);
    EXPECT_TRUE(successRegisterLogicalStream);

    // common case
    bool successRegisterPhysicalStream = coordinatorEngine->registerPhysicalStream(nodeId, conf->getSourceType(), conf->getSourceConfig(),
                                                                                   conf->getSourceFrequency(), conf->getNumberOfTuplesToProducePerBuffer(),
                                                                                   conf->getNumberOfBuffersToProduce(), conf->getPhysicalStreamName(),
                                                                                   conf->getLogicalStreamName());
    EXPECT_TRUE(successRegisterPhysicalStream);

    //test register existing stream
    bool successRegisterExistingPhysicalStream = coordinatorEngine->registerPhysicalStream(nodeId, conf->getSourceType(), conf->getSourceConfig(),
                                                                                           conf->getSourceFrequency(), conf->getNumberOfTuplesToProducePerBuffer(),
                                                                                           conf->getNumberOfBuffersToProduce(), conf->getPhysicalStreamName(),
                                                                                           conf->getLogicalStreamName());
    EXPECT_TRUE(!successRegisterExistingPhysicalStream);

    //test unregister not existing physical stream
    bool successUnregisterNotExistingPhysicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, "asd", conf->getLogicalStreamName());
    EXPECT_TRUE(!successUnregisterNotExistingPhysicalStream);

    //test unregister not existing local stream
    bool successUnregisterNotExistingLocicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, conf->getPhysicalStreamName(), "asd");
    EXPECT_TRUE(!successUnregisterNotExistingLocicalStream);

    //test unregister existing node
    bool successUnregisterExistingPhysicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, conf->getPhysicalStreamName(),
                                                    conf->getLogicalStreamName());
    EXPECT_TRUE(successUnregisterExistingPhysicalStream);
}