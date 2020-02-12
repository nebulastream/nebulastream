#include <cassert>
#include <iostream>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <sstream>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

class WorkerCoordinatorStarterTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
#endif
    NES_INFO("Setup WorkerCoordinatorStarterTest test class.");
  }
  static void TearDownTestCase() {
    std::cout << "Tear down WorkerCoordinatorStarterTest class." << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WorkerCoordinatorStarterTest.log");
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

TEST_F(WorkerCoordinatorStarterTest, start_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  sleep(2);
  cout << "wakeup" << endl;

  cout << "stopping worker" << endl;
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  cout << "stopping coordinator" << endl;
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, start_stop_coordinator_worker) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  sleep(2);
  cout << "wakeup" << endl;

  cout << "stopping coordinator" << endl;
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);

  cout << "stopping worker" << endl;
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);
}

TEST_F(WorkerCoordinatorStarterTest, start_connect_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  sleep(1);
  bool retConWrk = wrk->connect();
  EXPECT_TRUE(retConWrk);
  cout << "worker started connected " << endl;

  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, start_connect_stop_without_disconnect_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  sleep(1);
  bool retConWrk = wrk->connect();
  EXPECT_TRUE(retConWrk);
  cout << "worker started connected " << endl;

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);

  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);
}


TEST_F(WorkerCoordinatorStarterTest, start_connect_disconnect_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  sleep(1);
  bool retConWrk = wrk->connect();
  EXPECT_TRUE(retConWrk);
  cout << "worker started connected " << endl;

  sleep(1);
  bool retDisWrk = wrk->disconnect();
  EXPECT_TRUE(retDisWrk);
  cout << "worker started connected " << endl;

  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(2);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}


}
