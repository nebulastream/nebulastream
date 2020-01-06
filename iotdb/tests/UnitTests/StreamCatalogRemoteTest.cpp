#include <gtest/gtest.h>
#include <Actors/CoordinatorActor.hpp>
#include <Actors/WorkerActor.hpp>

#include <Util/Logger.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include "caf/io/all.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>

namespace iotdb {

class StreamCatalogRemoteTest : public testing::Test {
 public:
  std::string host = "localhost";
  std::string queryString =
      "InputQuery inputQuery = InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); "
          "return inputQuery;";

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup StreamCatalogRemoteTest test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down StreamCatalogRemoteTest test class." << std::endl;
  }
 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
    log4cxx::FileAppenderPtr file(
        new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(
        new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    iotdbLogger->setLevel(log4cxx::Level::getDebug());
//    iotdbLogger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    iotdbLogger->addAppender(file);
    iotdbLogger->addAppender(console);
  }
};

TEST_F(StreamCatalogRemoteTest, test_add_log_stream_remote_test) {
  cout << "*** Running test test_add_log_stream_remote_test" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  //create test schema
  std::string testSchema =
      "Schema schema = Schema::create().addField(\"id\", BasicType::UINT32).addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(worker, register_log_stream_atom::value, "testStream",
            testSchemaFileName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << "streams="
       << StreamCatalog::instance().getLogicalStreamAndSchemaAsString() << endl;
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream");
  EXPECT_NE(sPtr, nullptr);

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_existing_log_stream_remote_test) {
  cout << "*** Running test test_add_existing_log_stream_remote_test" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  //create test schema
  std::string testSchema =
      "Schema schema = Schema::create().addField(\"no\", BasicType::UINT32).addField("
          "\"val\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(worker, register_log_stream_atom::value, "default_logical",
            testSchemaFileName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  //check if schma was not overwritten
  SchemaPtr sch = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sch, nullptr);

  string exp =
      "logical stream name=default_logical schema:id:UINT32value:UINT64\n\n";
  EXPECT_EQ(exp, StreamCatalog::instance().getLogicalStreamAndSchemaAsString());

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_empty_log_stream_remote_test) {
  cout << "*** Running test test_add_remove_empty_log_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  //create test schema
  std::string testSchema =
      "Schema schema = Schema::create().addField(\"id\", BasicType::UINT32).addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  anon_send(worker, register_log_stream_atom::value, "testStream",
            testSchemaFileName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getLogicalStreamAndSchemaAsString() << endl;
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream");
  EXPECT_NE(sPtr, nullptr);

  anon_send(worker, remove_log_stream_atom::value, "testStream");
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getLogicalStreamAndSchemaAsString() << endl;
  SchemaPtr sPtr2 = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream");
  EXPECT_EQ(sPtr2, nullptr);

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_not_empty_log_stream_remote_test) {
  cout << "*** Running test test_add_remove_not_empty_log_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  anon_send(worker, remove_log_stream_atom::value, "default_logical");
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getLogicalStreamAndSchemaAsString() << endl;
  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_existing_logical_stream_remote_test) {
  cout << "*** Running test add_physical_to_existing_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  PhysicalStreamConfig conf;
  conf.logicalStreamName = "default_logical";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "OneGeneratorSource";
  conf.sourceConfig = "2";

  anon_send(worker, register_phy_stream_atom::value, conf.sourceType,
            conf.sourceConfig, conf.physicalStreamName, conf.logicalStreamName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 2);
  EXPECT_EQ(phys[0]->getPhysicalName(), "default_physical");
  EXPECT_EQ(phys[1]->getPhysicalName(), "physical_test");

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_new_logical_stream_remote_test) {
  cout << "*** Running test add_physical_to_new_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  //create test schema
  std::string testSchema =
      "Schema schema = Schema::create().addField(\"id\", BasicType::UINT32).addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  anon_send(worker, register_log_stream_atom::value, "testStream",
            testSchemaFileName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  PhysicalStreamConfig conf;
  conf.logicalStreamName = "testStream";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "OneGeneratorSource";
  conf.sourceConfig = "2";

  anon_send(worker, register_phy_stream_atom::value, conf.sourceType,
            conf.sourceConfig, conf.physicalStreamName, conf.logicalStreamName);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("testStream");

  EXPECT_EQ(phys.size(), 1);
  EXPECT_EQ(phys[0]->getPhysicalName(), "physical_test");

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, remove_physical_from_new_logical_stream_remote_test) {
  cout << "*** Running test remove_physical_from_new_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(worker, remove_phy_stream_atom::value, "default_logical",
            "default_physical");
  std::this_thread::sleep_for(std::chrono::seconds(2));

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 0);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, remove_not_existing_stream_remote_test) {
  cout << "*** Running test remove_not_existing_physical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;  //streamConf.physicalStreamName
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(worker, remove_phy_stream_atom::value, "default_logical2",
            "default_physical");
  std::this_thread::sleep_for(std::chrono::seconds(2));

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 1);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

}
