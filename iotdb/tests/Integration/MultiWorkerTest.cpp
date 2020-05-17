#include <iostream>

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

using namespace std;

namespace NES {

class MultiWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultiWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultiWorkerTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down MultiWorkerTest class." << std::endl;
    }
};

TEST_F(MultiWorkerTest, startStopWorkerCoordinatorSingle) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startStopCoordinatorWorker) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker 1" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping worker 2" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(MultiWorkerTest, startConnectStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWithConnectStopWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startConnectStopWithoutDisconnectWorkerCoordinator) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 started connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 started connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(MultiWorkerTest, testMultipleWorker) {
    size_t numWorkers = 3;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;
    //start 10 worker
    std::vector<NesWorkerPtr> wPtrs;
    for (size_t i = 0; i < numWorkers; i++) {
        cout << "start worker" << i << endl;
        wPtrs.push_back(std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
            std::to_string(port+(i+5)*10), NESNodeType::Sensor));
        bool retStart = wPtrs[i]->start(/**blocking**/false, /**withConnect**/false);
        EXPECT_TRUE(retStart);

    }

    //connect 10 worker
    for (size_t i = 0; i < numWorkers; i++) {
        cout << "connect worker" << i << endl;
        bool retConWrk = wPtrs[i]->connect();
        EXPECT_TRUE(retConWrk);
    }

    //disconnect 10 worker
    for (size_t i = 0; i < numWorkers; i++) {
        cout << "disconnect worker" << i << endl;
        bool retConWrk = wPtrs[i]->disconnect();
        EXPECT_TRUE(retConWrk);
    }

    //stop 10 worker
    for (size_t i = 0; i < numWorkers; i++) {
        cout << "stop worker" << i << endl;
        bool retConWrk = wPtrs[i]->stop(false);
        EXPECT_TRUE(retConWrk);
    }

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}
