#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <ctime>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class UpdateTopologyRemoteTest : public testing::Test {
  public:
    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;

    static void SetUpTestCase() {
        NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpdateTopologyRemoteTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
    }

    static void TearDownTestCase() {
        std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl;
    }
};

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnId) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, port + 10, port + 11, NESNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, port + 20, port + 21, NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    cout << "worker started successfully" << endl;

    TopologyManagerPtr topologyManger = crd->getTopology();

    //NOTE: we cannot check full output as ids change each run
    std::string retString = topologyManger->getNESTopologyPlanString();
    cout << "returned=" << retString;
    ASSERT_TRUE(retString.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString.find("1--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString.find("2--0 [label=") != std::string::npos);

    size_t pos1Beg = retString.find("1[label");
    std::string firstIdPart1 = retString.substr(pos1Beg + 9);
    size_t pos1End = firstIdPart1.find(" ");
    std::string firstId = firstIdPart1.substr(0, pos1End);
    istringstream f1(firstId);
    size_t firstIdInt;
    f1 >> firstIdInt;
    cout << "firstId=" << firstId << " firstint" << firstIdInt << endl;

    size_t pos2Beg = retString.find("2[label");
    std::string secondPart1 = retString.substr(pos2Beg + 9);
    size_t secondEnd = secondPart1.find(" ");
    std::string secondId = secondPart1.substr(0, secondEnd);
    istringstream f(secondId);
    size_t secIdInt;
    f >> secIdInt;
    cout << "secondId=" << secondId << " secint" << secIdInt << endl;

    cout << "ADD NEW PARENT" << endl;
    bool successAddPar = wrk->addParent(secIdInt);
    EXPECT_TRUE(successAddPar);

    //NOTE: we cannot check full output as ids change each run
    std::string retString2 = topologyManger->getNESTopologyPlanString();
    cout << "retString2=" << retString2;
    ASSERT_TRUE(retString2.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString2.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString2.find("1--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString2.find("2--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString2.find("1--2 [label=") != std::string::npos);

    cout << "REMOVE NEW PARENT" << endl;
    bool successRemoveParent = wrk->removeParent(secIdInt);
    EXPECT_TRUE(successRemoveParent);
    EXPECT_TRUE(successAddPar);

    std::string retString3 = topologyManger->getNESTopologyPlanString();
    //NOTE: we cannot check full output as ids change each run
    cout << "retString3=" << retString3;
    ASSERT_TRUE(retString3.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--2 [label=") == std::string::npos);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping worker 2" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnIdAndSelf) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, port + 10, port + 11, NESNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, port + 20, port + 21, NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    cout << "worker started successfully" << endl;

    TopologyManagerPtr topologyManger = crd->getTopology();

    //NOTE: we cannot check full output as ids change each run
    std::string retString = topologyManger->getNESTopologyPlanString();
    cout << "returned=" << retString;
    ASSERT_TRUE(retString.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString.find("1--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString.find("2--0 [label=") != std::string::npos);

    size_t pos1Beg = retString.find("1[label");
    std::string firstIdPart1 = retString.substr(pos1Beg + 9);
    size_t pos1End = firstIdPart1.find(" ");
    std::string firstId = firstIdPart1.substr(0, pos1End);
    istringstream f1(firstId);
    size_t firstIdInt;
    f1 >> firstIdInt;
    cout << "firstId=" << firstId << " firstint" << firstIdInt << endl;

    size_t pos2Beg = retString.find("2[label");
    std::string secondPart1 = retString.substr(pos2Beg + 9);
    size_t secondEnd = secondPart1.find(" ");
    std::string secondId = secondPart1.substr(0, secondEnd);
    istringstream f(secondId);
    size_t secIdInt;
    f >> secIdInt;
    cout << "secondId=" << secondId << " secint" << secIdInt << endl;

    cout << "REMOVE NEW PARENT" << endl;
    bool successRemoveParent = wrk->removeParent(firstIdInt);
    EXPECT_TRUE(!successRemoveParent);

    //NOTE: we cannot check full output as ids change each run
    std::string retString3 = topologyManger->getNESTopologyPlanString();
    cout << "retString3=" << retString3;
    ASSERT_TRUE(retString3.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--0 [label=") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--1 [label=") == std::string::npos);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping worker 2" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
