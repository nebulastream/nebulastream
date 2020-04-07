#include <gtest/gtest.h>
#include <Actors/CoordinatorActor.hpp>
#include <Actors/WorkerActor.hpp>

#include <Util/Logger.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include "caf/io/all.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>

namespace NES {

class StreamCatalogRemoteTest : public testing::Test {
 public:
  std::string host = "localhost";
  std::string queryString =
      "InputQuery inputQueryPtr = InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); "
          "return inputQueryPtr;";

  static void SetUpTestCase() {
    NES::setupLogging("StreamCatalogRemoteTest.log", NES::LOG_DEBUG);
    NES_INFO("Setup StreamCatalogRemoteTest test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down StreamCatalogRemoteTest test class." << std::endl;
  }
};

TEST_F(StreamCatalogRemoteTest, test_add_log_stream_remote_test) {

  NES_INFO(
      "StreamCatalogRemoteTest: Running test test_add_log_stream_remote_test");
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordHandler = system_coord.spawn<NES::CoordinatorActor>();

  // try to publish actor at given port
  NES_INFO(
      "StreamCatalogRemoteTest (test_add_log_stream_remote_test):  try publish at port" << c_cfg.publish_port);
  auto expected_port = io::publish(coordHandler, c_cfg.publish_port);
  if (!expected_port) {
    NES_ERROR(
        "ACTORSCLITEST (test_add_log_stream_remote_test): publish failed: " << system_coord.render(expected_port.error()));
    return;
  }
  NES_INFO(
      "StreamCatalogRemoteTest (test_add_log_stream_remote_test): coordinator successfully published at port " << *expected_port);

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;
  auto workerHandler = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                                  w_cfg.receive_port);

  bool connected = false;
  scoped_actor self { sw };
  self->request(workerHandler, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&connected](const bool &c) mutable {
        connected = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error during test_add_log_stream_remote_test " << "\n" << error_msg);
      });
  EXPECT_TRUE(connected);

  //create test schema
  std::string testSchema =
      "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  bool registered = false;
  self->request(workerHandler, task_timeout, register_log_stream_atom::value,
                "testStream1", testSchemaFileName).receive(
      [&registered](const bool &c) mutable {
        registered = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error during testSequentialMultiQueries " << "\n" << error_msg);
      });
  EXPECT_TRUE(registered);

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream1");
  EXPECT_NE(sPtr, nullptr);

  self->request(workerHandler, task_timeout, exit_reason::user_shutdown);
  self->request(coordHandler, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_existing_log_stream_remote_test) {
  cout << "*** Running test test_add_existing_log_stream_remote_test" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  //create test schema
  std::string testSchema =
      "Schema::create()->addField(\"no\", BasicType::UINT32)->addField("
          "\"val\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  success = false;
  self->request(worker, task_timeout, register_log_stream_atom::value,
                "default_logical", testSchemaFileName).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(!success);

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  //check if schma was not overwritten
  SchemaPtr sch = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sch, nullptr);

  map<std::string, SchemaPtr> allLogicalStream = StreamCatalog::instance()
      .getAllLogicalStream();
  string exp = "id:UINT32value:UINT64\n";
  EXPECT_EQ(allLogicalStream.size(), 2);

  SchemaPtr defaultSchema = allLogicalStream["default_logical"];
  EXPECT_EQ(exp, defaultSchema->toString());

  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_empty_log_stream_remote_test) {
  cout << "*** Running test test_add_remove_empty_log_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  //create test schema
  std::string testSchema =
      "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  success = false;
  self->request(worker, task_timeout, register_log_stream_atom::value,
                "testStream", testSchemaFileName).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream");
  EXPECT_NE(sPtr, nullptr);

  success = false;
  self->request(worker, task_timeout, remove_log_stream_atom::value,
                "testStream").receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  SchemaPtr sPtr2 = StreamCatalog::instance().getSchemaForLogicalStream(
      "testStream");
  EXPECT_EQ(sPtr2, nullptr);
  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_not_empty_log_stream_remote_test) {
  cout << "*** Running test test_add_remove_not_empty_log_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  success = false;
  self->request(worker, task_timeout, remove_log_stream_atom::value,
                "default_logical").receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(!success);

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_existing_logical_stream_remote_test) {
  cout << "*** Running test add_physical_to_existing_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);
  PhysicalStreamConfig conf;
  conf.logicalStreamName = "default_logical";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "DefaultSource";
  conf.numberOfBuffersToProduce = 2;

  success = false;
  self->request(worker, task_timeout, register_phy_stream_atom::value,
                conf.sourceType, conf.sourceConfig, conf.sourceFrequency,
                conf.numberOfBuffersToProduce, conf.physicalStreamName,
                conf.logicalStreamName).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 2);
  EXPECT_EQ(phys[0]->getPhysicalName(), "default_physical");
  EXPECT_EQ(phys[1]->getPhysicalName(), "physical_test");
  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_new_logical_stream_remote_test) {
  cout << "*** Running test add_physical_to_new_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);
  //create test schema
  std::string testSchema =
      "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
          "\"value\", BasicType::UINT64);";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();

  bool registered = false;
  self->request(worker, task_timeout, register_log_stream_atom::value,
                "testStream", testSchemaFileName).receive(
      [&registered](const bool &c) mutable {
        registered = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error during add_physical_to_new_logical_stream_remote_test" << "\n" << error_msg);
      });
  EXPECT_TRUE(registered);

  PhysicalStreamConfig conf;
  conf.logicalStreamName = "testStream";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "DefaultSource";
  conf.numberOfBuffersToProduce = 2;

  success = false;
  self->request(worker, task_timeout, register_phy_stream_atom::value,
                conf.sourceType, conf.sourceConfig, conf.sourceFrequency,
                conf.numberOfBuffersToProduce, conf.physicalStreamName,
                conf.logicalStreamName).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("testStream");

  EXPECT_EQ(phys.size(), 1);
  EXPECT_EQ(phys[0]->getPhysicalName(), "physical_test");

  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, remove_physical_from_new_logical_stream_remote_test) {
  cout << "*** Running test remove_physical_from_new_logical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  success = false;
  self->request(worker, task_timeout, remove_phy_stream_atom::value,
                "default_logical", "default_physical").receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(!success);

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 0);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(StreamCatalogRemoteTest, remove_not_existing_stream_remote_test) {
  cout << "*** Running test remove_not_existing_physical_stream_remote_test"
       << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord { c_cfg };
  auto coordinator = system_coord.spawn<NES::CoordinatorActor>();

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
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  scoped_actor self { sw };
  bool success = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(success);

  success = false;
  self->request(worker, task_timeout, remove_phy_stream_atom::value,
                "default_logical2", "default_physical").receive(
      [&success](const bool &c) mutable {
        success = c;
      }
      , [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "StreamCatalogRemoteTest: Error return value " << "\n" << error_msg);
      });
  EXPECT_TRUE(!success);

  SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
      "default_logical");
  EXPECT_NE(sPtr, nullptr);

  cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
       << endl;
  std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
      .getPhysicalStreams("default_logical");

  EXPECT_EQ(phys.size(), 1);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  self->request(worker, task_timeout, exit_reason::user_shutdown);
  self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

}
