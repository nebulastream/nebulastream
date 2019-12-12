#include <gtest/gtest.h>
#include <Actors/CoordinatorActor.hpp>
#include <Actors/WorkerActor.hpp>

#include <Util/Logger.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include "caf/io/all.hpp"


namespace iotdb {

class ActorsCliTest : public testing::Test {
 public:
  std::string host = "localhost";

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup ActorCoordinatorWorkerTest test class.");
  }

  static void TearDownTestCase() { std::cout << "Tear down ActorCoordinatorWorkerTest test class." << std::endl; }
 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    // logger->setLevel(log4cxx::Level::getDebug());
    logger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }
};

TEST_F(ActorsCliTest, testSpawnDespawnCoordinatorWorkers) {
  cout << "*** Running test testSpawnDespawnCoordinatorWorkers" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord{c_cfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowTopology) {
  cout << "*** Running test testShowTopology" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord{c_cfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
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
  actor_system system_coord{c_cfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send(coordinator, register_query_atom::value, "example", "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_registered_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testDeleteQuery) {
  cout << "*** Running test testDeleteQuery" << endl;
  CoordinatorActorConfig ccfg;
  ccfg.load<io::middleman>();
  actor_system system_coord{ccfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << ccfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, ccfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, ccfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  string description = "example";
  anon_send(coordinator, register_query_atom::value, description, "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_registered_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deploy_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  anon_send(coordinator, deregister_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testShowRunning) {
  cout << "*** Running test testShowRunning" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord{c_cfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  string description = "example";
  anon_send(coordinator, register_query_atom::value, description, "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deploy_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_running_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deregister_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, DISABLED_testShowOperators) {
  cout << "*** Running test testShowOperators" << endl;
  CoordinatorActorConfig c_cfg;
  c_cfg.load<io::middleman>();
  actor_system system_coord{c_cfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << c_cfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, c_cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, c_cfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  string description = "example";
  anon_send(coordinator, register_query_atom::value, description, "BottomUp");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, deploy_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  anon_send(coordinator, show_running_operators_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, deregister_query_atom::value, description);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  anon_send(coordinator, show_running_atom::value);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(ActorsCliTest, testSequentialMultiQueries) {
  cout << "*** Running test testShowOperators" << endl;
  CoordinatorActorConfig ccfg;
  ccfg.load<io::middleman>();
  actor_system system_coord{ccfg};
  auto coordinator = system_coord.spawn<iotdb::CoordinatorActor>();

  // try to publish actor at given port
  cout << "*** try publish at port " << ccfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, ccfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system_coord.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  actor_system sw{w_cfg};
  auto worker = sw.spawn<iotdb::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, ccfg.publish_port);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  string description = "example";
  for (int i = 0; i < 1; i++) {
    cout << "Sequence " << i << endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, register_query_atom::value, description, "BottomUp");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, deploy_query_atom::value, description);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, show_running_operators_atom::value);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    anon_send(coordinator, deregister_query_atom::value, description);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    anon_send(coordinator, show_running_operators_atom::value);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));

  anon_send_exit(worker, exit_reason::user_shutdown);
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

}
