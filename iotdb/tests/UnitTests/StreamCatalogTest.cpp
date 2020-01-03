#include "gtest/gtest.h"
//#include <Actors/WorkerActor.hpp>

#include <iostream>

#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

#include <Topology/NESTopologyManager.hpp>
#include <API/Schema.hpp>
//#include <Actors/CoordinatorActor.hpp>
#include <Util/Logger.hpp>

using namespace iotdb;

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
    logger->setLevel(log4cxx::Level::getDebug());
//    logger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }
};

TEST_F(StreamCatalogTest, add_get_log_stream_test) {
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "test_stream");
  EXPECT_NE(sPtr, nullptr);

  string exp =
      "logical stream name=default_logical schema:id:UINT32value:UINT64\n\nlogical stream name=test_stream schema:\n\n";
  EXPECT_EQ(exp, StreamCatalog::instance().getLogicalStreamAndSchemaAsString());
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
  EXPECT_NE(exp, StreamCatalog::instance().getLogicalStreamAndSchemaAsString());

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
      .createNESSensorNode("localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "test2";
  streamConf.logicalStreamName = "test_stream";
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

  EXPECT_TRUE(
      StreamCatalog::instance().addPhysicalStream(streamConf.logicalStreamName,
                                                  sce));

  std::string expected =
      "stream name=test_stream with 1 elements:physicalName=test2 on node=0\n";
  cout << " string="
       << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;

  EXPECT_EQ(expected,
            StreamCatalog::instance().getPhysicalStreamAndSchemaAsString());
}

TEST_F(StreamCatalogTest, add_remove_physical_stream_test) {
  NESTopologyManager::getInstance().resetNESTopologyPlan();
  StreamCatalog::instance().addLogicalStream(
      "test_stream", std::make_shared<Schema>(Schema()));

  NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
      .createNESSensorNode("localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "test2";
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

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
      .createNESSensorNode("localhost", CPUCapacity::HIGH);

  PhysicalStreamConfig streamConf;
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

  EXPECT_TRUE(
      StreamCatalog::instance().addPhysicalStream(streamConf.logicalStreamName,
                                                  sce));

  std::string expected =
      "stream name=default_logical with 1 elements:physicalName=default_physical on node=0\n";
  EXPECT_EQ(expected,
            StreamCatalog::instance().getPhysicalStreamAndSchemaAsString());
}

TEST_F(StreamCatalogTest, register_log_stream_from_node_test) {

  // Create Coordinator

  // Create Worker
//  WorkerActorConfig cfg;
//  cfg.load<io::middleman>();
//  actor_system system { cfg };
//  infer_handle_from_class_t<iotdb::WorkerActor> client;
//
//  PhysicalStreamConfig defaultConf;
//  client = system.spawn<iotdb::WorkerActor>(cfg.ip, cfg.publish_port,
//                                            cfg.receive_port, defaultConf);
//  if (!cfg.host.empty() && cfg.publish_port > 0) {
//    //send connect message to worker to try to connect
//    anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
//  } else
//    cout << "*** no server received via config, "
//         << R"(please use "connect <host> <port>" before using the calculator)"
//         << endl;

}

