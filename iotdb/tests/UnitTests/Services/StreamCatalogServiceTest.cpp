#include <gtest/gtest.h>
#include <Services/StreamCatalogService.hpp>
#include <Util/Logger.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Topology/NESTopologySensorNode.hpp>

using namespace NES;

class StreamCatalogServiceTest : public testing::Test {
 public:

  std::string testSchema =
      "SchemaTemp::create()->addField(\"id\", BasicType::UINT32)"
          "->addField(\"value\", BasicType::UINT64);";
  const std::string defaultLogicalStreamName = "default_logical";

  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup StreamCatalogService test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup StreamCatalogService test case." << std::endl;
    NES::setupLogging("StreamCatalogServiceTest.log", NES::LOG_DEBUG);
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Setup StreamCatalogService case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down StreamCatalogService class." << std::endl;
  }
};

TEST_F(StreamCatalogServiceTest, get_all_logical_stream) {
  StreamCatalogServicePtr streamCatalogServicePtr =
      StreamCatalogService::getInstance();
  const map<std::string, std::string> &allLogicalStream =
      streamCatalogServicePtr->getAllLogicalStreamAsString();
  EXPECT_EQ(allLogicalStream.size(), 2);
  for (auto const [key, value] : allLogicalStream) {
    bool cmp = key != defaultLogicalStreamName && key != "exdra";
    EXPECT_EQ(cmp, false);
  }
}

TEST_F(StreamCatalogServiceTest, add_logical_stream) {
  StreamCatalogServicePtr streamCatalogServicePtr =
      StreamCatalogService::getInstance();
  streamCatalogServicePtr->addNewLogicalStream("test", testSchema);
  const map<std::string, std::string> &allLogicalStream =
      streamCatalogServicePtr->getAllLogicalStreamAsString();
  EXPECT_EQ(allLogicalStream.size(), 3);
}

TEST_F(StreamCatalogServiceTest, get_physicalStream_for_logical_stream) {

  std::string newLogicalStreamName = "test_stream";

  StreamCatalogServicePtr streamCatalogServicePtr =
      StreamCatalogService::getInstance();
  streamCatalogServicePtr->addNewLogicalStream(newLogicalStreamName,
                                               testSchema);

  NESTopologyManager &topologyManager = NESTopologyManager::getInstance();
  NESTopologySensorNodePtr sensorNode = topologyManager.createNESSensorNode(
      1, "127.0.0.1", CPUCapacity::LOW);

  PhysicalStreamConfig conf;
  conf.sourceType = "sensor";
  StreamCatalogEntryPtr catalogEntryPtr = std::make_shared<StreamCatalogEntry>(
      conf, sensorNode);
  StreamCatalog::instance().addPhysicalStream(newLogicalStreamName,
                                              catalogEntryPtr);
  const vector<StreamCatalogEntryPtr> &allPhysicalStream =
      streamCatalogServicePtr->getAllPhysicalStream(newLogicalStreamName);
  EXPECT_EQ(allPhysicalStream.size(), 1);
}

TEST_F(StreamCatalogServiceTest, delete_logical_stream) {
  StreamCatalogServicePtr streamCatalogServicePtr =
      StreamCatalogService::getInstance();
  bool success = streamCatalogServicePtr->removeLogicalStream(
      defaultLogicalStreamName);
  EXPECT_TRUE(success);
}
