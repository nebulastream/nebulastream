#include <iostream>

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

class WorkerCoordinatorStarterTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("WorkerCoordinatorStarterTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WorkerCoordinatorStarterTest test class.");
    }
    void TearDown() {
        std::cout << "Tear down WorkerCoordinatorStarterTest class." << std::endl;
    }
};

TEST_F(WorkerCoordinatorStarterTest, start_stop_worker_coordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    cout << "wakeup" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, start_stop_coordinator_worker) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    cout << "wakeup" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
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
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker started connected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
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
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker started connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk = wrk->stop(false);
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
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker started connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker started connected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, start_reconnect_stop_worker_coordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker started connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker started connected " << endl;

    bool retConWrk2 = wrk->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker started connected " << endl;

    bool retDisWrk2 = wrk->disconnect();
    EXPECT_TRUE(retDisWrk2);
    cout << "worker started connected " << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}
