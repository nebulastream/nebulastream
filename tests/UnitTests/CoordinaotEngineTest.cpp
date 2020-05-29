#include <gtest/gtest.h>
#include <SourceSink/PrintSink.hpp>
#include <SourceSink/ZmqSource.hpp>

#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include "../../include/Topology/TopologyManager.hpp"

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
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);

    size_t nodeId = coordinatorEngine->registerNode(address, 6, "",
                                                    NESNodeType::Sensor);
    EXPECT_NE(nodeId, 0);

    //test register existing node
    size_t nodeId2 = coordinatorEngine->registerNode(address, 6, "",
                                                     NESNodeType::Sensor);
    EXPECT_EQ(nodeId2, 0);

    //test unregister not existing node
    bool successUnregisterNotExistingNode = coordinatorEngine->unregisterNode(552);
    EXPECT_NE(successUnregisterNotExistingNode, true);

    //test unregister existing node
    bool successUnregisterExistingNode = coordinatorEngine->unregisterNode(nodeId);
    EXPECT_TRUE(successUnregisterExistingNode);
}

TEST_F(CoordinatorEngineTest, testRegisterUnregisterLogicalStream) {
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
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
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    CoordinatorEnginePtr coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
    std::string physicalStreamName = "testStream";
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "testStream";
    conf.physicalStreamName = "physical_test";
    conf.sourceType = "CSVSource";
    conf.sourceConfig = "testCSV.csv";
    conf.numberOfBuffersToProduce = 3;
    conf.sourceFrequency = 1;
    size_t nodeId = coordinatorEngine->registerNode(address, 6, "",
                                                    NESNodeType::Sensor);
    EXPECT_NE(nodeId, 0);

    //setup test
    std::string testSchema =
        "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    bool successRegisterLogicalStream = coordinatorEngine->registerLogicalStream(conf.logicalStreamName, testSchema);
    EXPECT_TRUE(successRegisterLogicalStream);

    // common case
    bool successRegisterPhysicalStream = coordinatorEngine->registerPhysicalStream(nodeId,
                                                                                   conf.sourceType,
                                                                                   conf.sourceConfig,
                                                                                   conf.sourceFrequency,
                                                                                   conf.numberOfBuffersToProduce,
                                                                                   conf.physicalStreamName,
                                                                                   conf.logicalStreamName);
    EXPECT_TRUE(successRegisterPhysicalStream);

    //test register existing stream
    bool successRegisterExistingPhysicalStream = coordinatorEngine->registerPhysicalStream(nodeId,
                                                                                           conf.sourceType,
                                                                                           conf.sourceConfig,
                                                                                           conf.sourceFrequency,
                                                                                           conf.numberOfBuffersToProduce,
                                                                                           conf.physicalStreamName,
                                                                                           conf.logicalStreamName);
    EXPECT_TRUE(!successRegisterExistingPhysicalStream);

    //test unregister not existing physical stream
    bool successUnregisterNotExistingPhysicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, "asd", conf.logicalStreamName);
    EXPECT_TRUE(!successUnregisterNotExistingPhysicalStream);

    //test unregister not existing local stream
    bool successUnregisterNotExistingLocicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, conf.physicalStreamName, "asd");
    EXPECT_TRUE(!successUnregisterNotExistingLocicalStream);

    //test unregister existing node
    bool successUnregisterExistingPhysicalStream =
        coordinatorEngine->unregisterPhysicalStream(nodeId, conf.physicalStreamName,
                                                    conf.logicalStreamName);
    EXPECT_TRUE(successUnregisterExistingPhysicalStream);
}