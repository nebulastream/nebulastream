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

class ActorsCliTest : public testing::Test {
 public:
  std::string host = "localhost";
  std::string queryString =
      "InputQuery inputQuery = InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); "
          "return inputQuery;";

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup ActorCoordinatorWorkerTest test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down ActorsCli test class." << std::endl;
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

TEST_F(ActorsCliTest, testRegisterUnregisterSensor) {
  cout << "*** Running test testSpawnDespawnCoordinatorWorkers" << endl;
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
  std::this_thread::sleep_for(std::chrono::seconds(2));

  anon_send(worker, disconnect_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  //TODO: this should also test the result

  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
   std::this_thread::sleep_for(std::chrono::seconds(2));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);

}
TEST_F(ActorsCliTest, testSpawnDespawnCoordinatorWorkers) {
  cout << "*** Running test testSpawnDespawnCoordinatorWorkers" << endl;
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

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowTopology) {
  cout << "*** Running test testShowTopology" << endl;
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
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(coordinator, topology_json_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowRegistered) {
  cout << "*** Running test testShowRegistered" << endl;
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
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  //Prepare Actor System
  scoped_actor self { system_coord };
  bool connected = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&connected](const bool &c) mutable {
        connected = c;
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  EXPECT_TRUE(connected);

  // check registration
  string uuid;
  self->request(coordinator, task_timeout, register_query_atom::value,
                queryString, "BottomUp").receive(
      [&uuid](const string &_uuid) mutable {
        uuid = _uuid;
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  IOTDB_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
  EXPECT_TRUE(!uuid.empty());

  // check length of registered queries
  size_t query_size = 0;
  self->request(coordinator, task_timeout, show_registered_queries_atom::value)
      .receive(
      [&query_size](const size_t length) mutable {
        query_size = length;
        IOTDB_INFO("ACTORSCLITEST: Query length " << length);
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  EXPECT_EQ(query_size, 1);

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testDeleteQuery) {
  cout << "*** Running test testDeleteQuery" << endl;
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
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

  //Prepare Actor System
  scoped_actor self { system_coord };
  bool connected = false;
  self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                c_cfg.publish_port).receive(
      [&connected](const bool &c) mutable {
        connected = c;
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  EXPECT_TRUE(connected);

  // check registration
  string uuid;
  self->request(coordinator, task_timeout, register_query_atom::value,
                queryString, "BottomUp").receive(
      [&uuid](const string &_uuid) mutable {
        uuid = _uuid;
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  IOTDB_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
  EXPECT_TRUE(!uuid.empty());

  // check length of registered queries
  size_t query_size = 0;
  self->request(coordinator, task_timeout, show_registered_queries_atom::value)
      .receive(
      [&query_size](const size_t length) mutable {
        query_size = length;
        IOTDB_INFO("ACTORSCLITEST: Query length " << length);
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        IOTDB_ERROR(
            "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
      });
  EXPECT_EQ(query_size, 1);

  anon_send(coordinator, deploy_query_atom::value, uuid);

  // let query run for some arbitrary seconds
  std::this_thread::sleep_for(std::chrono::seconds(3));
  anon_send(coordinator, deregister_query_atom::value, uuid);

  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_queries_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testShowRunning) {
  cout << "*** Running test testShowRunning" << endl;
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
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(coordinator, register_query_atom::value, queryString, "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deploy_query_atom::value, "FIXME_USE_UUID");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_running_queries_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deregister_query_atom::value, "FIXME_USE_UUID");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_queries_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testShowOperators) {
  cout << "*** Running test testShowOperators" << endl;
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
  std::this_thread::sleep_for(std::chrono::seconds(2));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(coordinator, register_query_atom::value, queryString, "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deploy_query_atom::value, "FIXME_USE_UUID");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_running_operators_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, deregister_query_atom::value, "FIXME_USE_UUID");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_queries_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, testSequentialMultiQueries) {
  cout << "*** Running test testShowOperators" << endl;
  CoordinatorActorConfig ccfg;
  ccfg.load<io::middleman>();
  actor_system system_coord { ccfg };
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << ccfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, ccfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);
  anon_send(worker, connect_atom::value, w_cfg.host, ccfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  for (int i = 0; i < 1; i++) {
    cout << "Sequence " << i << endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, register_query_atom::value, queryString, "BottomUp");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, deploy_query_atom::value, "FIXME_USE_UUID");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, show_running_operators_atom::value);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, deregister_query_atom::value, "FIXME_USE_UUID");
    std::this_thread::sleep_for(std::chrono::seconds(3));
    anon_send(coordinator, show_running_operators_atom::value);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}
}
