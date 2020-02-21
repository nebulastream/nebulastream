#include <cassert>
#include <iostream>

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

class MultiWorkerTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
#endif
    NES_INFO("Setup MultiWorkerTest test class.");
  }
  static void TearDownTestCase() {
    std::cout << "Tear down MultiWorkerTest class." << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "MultiWorkerTest.log");
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
TEST_F(MultiWorkerTest, start_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker 1" << endl;
  NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
  bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart1);
  cout << "worker1 started successfully" << endl;

  cout << "start worker 2" << endl;
  NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
  bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart2);
  cout << "worker2 started successfully" << endl;

  sleep(1);
  cout << "wakeup" << endl;

  cout << "stopping worker" << endl;
  bool retStopWrk1 = wrk1->stop();
  EXPECT_TRUE(retStopWrk1);

  cout << "stopping worker" << endl;
  bool retStopWrk2 = wrk2->stop();
  EXPECT_TRUE(retStopWrk2);

  cout << "stopping coordinator" << endl;
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, start_stop_coordinator_worker) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker 1" << endl;
  NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
  bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart1);
  cout << "worker1 started successfully" << endl;

  cout << "start worker 2" << endl;
  NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
  bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart2);
  cout << "worker2 started successfully" << endl;

  sleep(2);
  cout << "wakeup" << endl;

  cout << "stopping coordinator" << endl;
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);

  cout << "stopping worker 1" << endl;
  bool retStopWrk1 = wrk1->stop();
  EXPECT_TRUE(retStopWrk1);

  cout << "stopping worker 2" << endl;
  bool retStopWrk2 = wrk2->stop();
  EXPECT_TRUE(retStopWrk2);
}

TEST_F(MultiWorkerTest, start_connect_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker 1" << endl;
  NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
  bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart1);
  cout << "worker1 started successfully" << endl;

  cout << "start worker 2" << endl;
  NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
  bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart2);
  cout << "worker2 started successfully" << endl;

  sleep(1);
  bool retConWrk1 = wrk1->connect();
  EXPECT_TRUE(retConWrk1);
  cout << "worker 1 connected " << endl;

  sleep(1);
  bool retConWrk2 = wrk2->connect();
  EXPECT_TRUE(retConWrk2);
  cout << "worker 2 connected " << endl;

  sleep(1);
  bool retStopWrk1 = wrk1->stop();
  EXPECT_TRUE(retStopWrk1);

  bool retStopWrk2 = wrk2->stop();
  EXPECT_TRUE(retStopWrk2);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, start_with_connect_stop_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker 1" << endl;
  NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
  bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                               port);
  EXPECT_TRUE(retStart1);
  cout << "worker1 started successfully" << endl;

  cout << "start worker 2" << endl;
  NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
  bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true,
                               port);
  EXPECT_TRUE(retStart2);
  cout << "worker2 started successfully" << endl;

  sleep(1);
  bool retStopWrk1 = wrk1->stop();
  EXPECT_TRUE(retStopWrk1);

  bool retStopWrk2 = wrk2->stop();
  EXPECT_TRUE(retStopWrk2);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, start_connect_stop_without_disconnect_worker_coordinator) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker 1" << endl;
  NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
  bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart1);
  cout << "worker1 started successfully" << endl;

  cout << "start worker 2" << endl;
  NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
  bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false,
                               port);
  EXPECT_TRUE(retStart2);
  cout << "worker2 started successfully" << endl;

  sleep(1);
  bool retConWrk1 = wrk1->connect();
  EXPECT_TRUE(retConWrk1);
  cout << "worker 1 started connected " << endl;

  bool retConWrk2 = wrk2->connect();
  EXPECT_TRUE(retConWrk2);
  cout << "worker 2 started connected " << endl;

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);

  bool retStopWrk1 = wrk1->stop();
  EXPECT_TRUE(retStopWrk1);

  bool retStopWrk2 = wrk2->stop();
  EXPECT_TRUE(retStopWrk2);
}
TEST_F(MultiWorkerTest, test_ten_worker) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);
  //start 10 worker
  std::vector<NesWorkerPtr> wPtrs;
  for (size_t i = 0; i < 10; i++) {
    cout << "start worker" << i << endl;
    wPtrs.push_back(std::make_shared<NesWorker>());
    bool retStart = wPtrs[i]->start(/**blocking**/false, /**withConnect**/false,
                                    port);
    EXPECT_TRUE(retStart);

  }

  //connect 10 worker
  for (size_t i = 0; i < 10; i++) {
    cout << "connect worker" << i << endl;
    bool retConWrk = wPtrs[i]->connect();
    EXPECT_TRUE(retConWrk);
  }

  //disconnect 10 worker
  for (size_t i = 0; i < 10; i++) {
    cout << "disconnect worker" << i << endl;
    bool retConWrk = wPtrs[i]->disconnect();
    EXPECT_TRUE(retConWrk);
  }

  //stop 10 worker
  for (size_t i = 0; i < 10; i++) {
    cout << "stop worker" << i << endl;
    bool retConWrk = wPtrs[i]->stop();
    EXPECT_TRUE(retConWrk);
  }

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

}
