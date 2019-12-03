#include <gtest/gtest.h>
#include <Actors/ActorCoordinator.hpp>
#include <Actors/ActorWorker.hpp>

#include <Util/Logger.hpp>

#include "caf/io/all.hpp"

namespace iotdb {
class ActorCoordinatorWorkerTest : public testing::Test {
 public:
  std::string host = "localhost";
  std::string sensor_type = "cars";

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

/* Test serialization for Schema  */
TEST_F(ActorCoordinatorWorkerTest, testSpawnDespawnCoordinatorWorkers) {
  coordinator_config ccfg;
  ccfg.load<io::middleman>();
  actor_system system{ccfg};

  auto coordinator = system.spawn<iotdb::actor_coordinator>(ccfg.ip, ccfg.publish_port, ccfg.receive_port);

  // try to publish actor at given port
  cout << "*** try publish at port " << ccfg.publish_port << endl;
  auto expected_port = io::publish(coordinator, ccfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  worker_config w_cfg;
  auto worker = system.spawn<iotdb::actor_worker>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port, w_cfg.sensor_type);
  anon_send(worker, connect_atom::value, w_cfg.host, w_cfg.publish_port);

  coordinator->unregister_from_system();
  worker->unregister_from_system();
}
}
