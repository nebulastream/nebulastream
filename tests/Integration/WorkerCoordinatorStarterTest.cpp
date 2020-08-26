#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using std::cout;
using std::endl;
#define DEBUG_OUTPUT
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
// TODO use grpc async queue to fix this issue - I am currently increasing the rpc port by 30 on every test! this is very bad!
uint64_t rpcPort = 4000;

class WorkerCoordinatorStarterTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("WorkerCoordinatorStarterTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WorkerCoordinatorStarterTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
    }

    void TearDown() {
        std::cout << "Tear down WorkerCoordinatorStarterTest class." << std::endl;
    }

    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;
};

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);

    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;
    cout << "wakeup" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    cout << crd.use_count() << " use cnt coord" << endl;
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator10times) {
    cout << "start coordinator" << endl;
    for(size_t i = 0; i < 10; i++)
    {
        cout << "iteration = " << i << endl;
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
        size_t port = crd->startCoordinator(/**blocking**/ false);
        EXPECT_NE(port, 0);
        cout << "coordinator started successfully" << endl;

        cout << "start worker" << endl;
        NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        cout << "worker started connected successfully" << endl;

        cout << "wakeup" << endl;

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop(false);
        EXPECT_TRUE(retStopWrk);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator(false);
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        crd.reset();
        wrk.reset();
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        EXPECT_TRUE(retStopCord);
    }
}
TEST_F(WorkerCoordinatorStarterTest, startStopCoordinatorWorker) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;

    cout << "wakeup" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWithoutDisconnectWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectDisconnectStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startReconnectStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retConWrk2 = wrk->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker got connected " << endl;

    bool retDisWrk2 = wrk->disconnect();
    EXPECT_TRUE(retDisWrk2);
    cout << "worker got disconnected " << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES
