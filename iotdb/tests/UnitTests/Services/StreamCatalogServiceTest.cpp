#include <gtest/gtest.h>
#include <Services/StreamCatalogService.hpp>
#include <Util/Logger.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Topology/NESTopologySensorNode.hpp>

using namespace NES;

class StreamCatalogServiceTest : public testing::Test {
  public:

    std::string testSchema = "Schema::create().addField(\"id\", BasicType::UINT32)"
                             ".addField(\"value\", BasicType::UINT64);";
    const std::string defaultLogicalStreamName = "default_logical";

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup StreamCatalogService test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup StreamCatalogService test case." << std::endl;
        setupLogging();
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup StreamCatalogService case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down StreamCatalogService class." << std::endl;
    }
  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "StreamCatalogServiceTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NESLogger->setLevel(log4cxx::Level::getDebug());
        //    logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NESLogger->addAppender(file);
        NESLogger->addAppender(console);
    }

};

TEST_F(StreamCatalogServiceTest, get_all_logical_stream) {
    StreamCatalogServicePtr streamCatalogServicePtr = StreamCatalogService::getInstance();
    const map<std::string, std::string>& allLogicalStream = streamCatalogServicePtr->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 2);
    for (auto const[key, value] : allLogicalStream) {
      bool cmp = key != defaultLogicalStreamName && key != "exdra";
      EXPECT_EQ(cmp, false);
    }
}

TEST_F(StreamCatalogServiceTest, add_logical_stream) {
    StreamCatalogServicePtr streamCatalogServicePtr = StreamCatalogService::getInstance();
    streamCatalogServicePtr->addNewLogicalStream("test", testSchema);
    const map<std::string, std::string>& allLogicalStream = streamCatalogServicePtr->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 3);
}

TEST_F(StreamCatalogServiceTest, get_physicalStream_for_logical_stream) {

    std::string newLogicalStreamName = "test_stream";

    StreamCatalogServicePtr streamCatalogServicePtr = StreamCatalogService::getInstance();
    streamCatalogServicePtr->addNewLogicalStream(newLogicalStreamName, testSchema);

    NESTopologyManager &topologyManager = NESTopologyManager::getInstance();
    NESTopologySensorNodePtr sensorNode = topologyManager.createNESSensorNode(1,
        "127.0.0.1", CPUCapacity::LOW);

    PhysicalStreamConfig conf;
    conf.sourceType = "sensor";
    StreamCatalogEntryPtr catalogEntryPtr = std::make_shared<StreamCatalogEntry>(
        conf, sensorNode);
    StreamCatalog::instance().addPhysicalStream(newLogicalStreamName, catalogEntryPtr);
    const vector<StreamCatalogEntryPtr>
        & allPhysicalStream = streamCatalogServicePtr->getAllPhysicalStream(newLogicalStreamName);
    EXPECT_EQ(allPhysicalStream.size(), 1);
}

TEST_F(StreamCatalogServiceTest, delete_logical_stream) {
    StreamCatalogServicePtr streamCatalogServicePtr = StreamCatalogService::getInstance();
    bool success = streamCatalogServicePtr->removeLogicalStream(defaultLogicalStreamName);
    EXPECT_TRUE(success);
}
