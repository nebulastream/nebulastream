#include "gtest/gtest.h"

#include <iostream>

#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

#include <Topology/NESTopologyManager.hpp>
#include <API/Schema.hpp>

//#include <Actors/CoordinatorActor.hpp>
#include <Util/Logger.hpp>
//#include <Actors/WorkerActor.hpp>


using namespace NES;

/* - nesTopologyManager ---------------------------------------------------- */
class StreamCatalogTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup StreamCatalogTest test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    setupLogging();
    StreamCatalog::instance().reset();
    std::cout << "Setup StreamCatalogTest test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Tear down StreamCatalogTest test case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down StreamCatalogTest test class." << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "StreamCatalogTest.log");
    log4cxx::FileAppenderPtr file(
        new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(
        new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    NES::NESLogger->setLevel(log4cxx::Level::getDebug());
//    logger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    NES::NESLogger->addAppender(file);
    NES::NESLogger->addAppender(console);
  }
};

TEST_F(StreamCatalogTest, add_get_log_stream_test) {
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "test_stream");
  EXPECT_NE(sPtr, nullptr);

  map<std::string, SchemaPtr> allLogicalStream = StreamCatalog::instance().getAllLogicalStream();
  string exp =
      "id:UINT32value:UINT64\n";
  EXPECT_EQ(allLogicalStream.size(), 3);

  SchemaPtr testSchema = allLogicalStream["test_stream"];
  EXPECT_EQ("\n", testSchema->toString());

  SchemaPtr defaultSchema = allLogicalStream["default_logical"];
  EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(StreamCatalogTest, add_remove_log_stream_test) {
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));

  EXPECT_TRUE(StreamCatalog::instance().removeLogicalStream("test_stream"));

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "test_stream");
  EXPECT_EQ(sPtr, nullptr);

  string exp =
      "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream name=test_stream schema:\n\n";

  map<std::string, SchemaPtr> allLogicalStream = StreamCatalog::instance().getAllLogicalStream();

  EXPECT_NE(1, allLogicalStream.size());

  EXPECT_FALSE(StreamCatalog::instance().removeLogicalStream("test_stream22"));

//  cout << "log streams=" << StreamCatalog::instance().getLogicalStreamAndSchemaAsString() << endl;

}

TEST_F(StreamCatalogTest, get_not_existing_key_test) {
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "test_stream22");
  EXPECT_EQ(sPtr, nullptr);
}

TEST_F(StreamCatalogTest, add_get_physical_stream_test) {
//  NESTopologyManager::getInstance().resetNESTopologyPlan();
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));

  NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
      .createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "test2";
  streamConf.logicalStreamName = "test_stream";

  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf, sensorNode);

  EXPECT_TRUE(
      StreamCatalog::instance().addPhysicalStream(streamConf.logicalStreamName,
                                                  sce));

  std::string expected =
      "stream name=test_stream with 1 elements:physicalName=test2 logicalStreamName=test_stream sourceType=DefaultSource sourceConfig=1 sourceFrequency=0 numberOfBuffersToProduce=1 on node=1\n";
  cout << " string="
       << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;

  EXPECT_EQ(expected,
            StreamCatalog::instance().getPhysicalStreamAndSchemaAsString());
}

//TODO: add test for a second physical stream add

TEST_F(StreamCatalogTest, add_remove_physical_stream_test) {
  NESTopologyManager::getInstance().resetNESTopologyPlan();
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));

  NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
      .createNESSensorNode(1,"localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "test2";
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf, sensorNode);

  EXPECT_TRUE(
      StreamCatalog::instance().addPhysicalStream(streamConf.logicalStreamName,
                                                  sce));

  EXPECT_TRUE(
      StreamCatalog::instance().removePhysicalStream(
          streamConf.logicalStreamName, sce));

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
}

TEST_F(StreamCatalogTest, add_physical_for_not_existing_logical_stream_test) {
  NESTopologyManager::getInstance().resetNESTopologyPlan();
  NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
      .createNESSensorNode(1,"localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf, sensorNode);

  EXPECT_TRUE(
      StreamCatalog::instance().addPhysicalStream(streamConf.logicalStreamName,
                                                  sce));


  std::string expected =
      "stream name=default_logical with 1 elements:physicalName=default_physical logicalStreamName=default_logical sourceType=DefaultSource sourceConfig=1 sourceFrequency=0 numberOfBuffersToProduce=1 on node=1\n";
  EXPECT_EQ(expected,
            StreamCatalog::instance().getPhysicalStreamAndSchemaAsString());
}

