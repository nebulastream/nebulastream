#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <ctime>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
namespace NES {

class UpdateTopologyRemoteTest : public testing::Test {
  public:
    std::string host = "localhost";
    std::string queryString =
        "InputQuery inputQueryPtr = InputQuery::from(default_logical).filter(default_logical[\"id\"] < 42).print(std::cout); "
        "return inputQueryPtr;";

    static void SetUpTestCase() {
        NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpdateTopologyRemoteTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl;
    }
};

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnId) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart2);
    cout << "worker started successfully" << endl;

    NESTopologyManager::getInstance().printNESTopologyPlan();

    //NOTE: we cannot check full output as ids change each run
    std::string retString = NESTopologyManager::getInstance().getNESTopologyPlanString();
    cout << "returned=" << retString;
    ASSERT_TRUE(retString.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString.find("1--0 [label=\"0\"]") != std::string::npos);
    ASSERT_TRUE(retString.find("2--0 [label=\"1\"]") != std::string::npos);

    size_t pos1Beg = retString.find("1[label");
    std::string firstIdPart1 = retString.substr(pos1Beg + 9);
    size_t pos1End = firstIdPart1.find(" ");
    std::string firstId = firstIdPart1.substr(0, pos1End);
    cout << "firstId=" << firstId << endl;

    size_t pos2Beg = retString.find("2[label");
    std::string secondPart1 = retString.substr(pos2Beg + 9);
    size_t secondEnd = secondPart1.find(" ");
    std::string secondId = secondPart1.substr(0, secondEnd);
    cout << "secondId=" << secondId << endl;

    cout << "ADD NEW PARENT" << endl;
    bool successAddPar = wrk->addParent(secondId);
    EXPECT_TRUE(successAddPar);

    //NOTE: we cannot check full output as ids change each run
    std::string retString2 = NESTopologyManager::getInstance().getNESTopologyPlanString();
    cout << "retString2=" << retString2;
    ASSERT_TRUE(retString2.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString2.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString2.find("1--0 [label=\"0\"]") != std::string::npos);
    ASSERT_TRUE(retString2.find("2--0 [label=\"1\"]") != std::string::npos);
    ASSERT_TRUE(retString2.find("1--2 [label=\"2\"]") != std::string::npos);

    cout << "REMOVE NEW PARENT" << endl;
    bool successRemoveParent = wrk->removeParent(secondId);
    EXPECT_TRUE(successAddPar);

    //NOTE: we cannot check full output as ids change each run
    std::string retString3 = NESTopologyManager::getInstance().getNESTopologyPlanString();
    cout << "retString3=" << retString3;
    ASSERT_TRUE(retString3.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--0 [label=\"1\"]") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--2 [label=\"2\"]") == std::string::npos);

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
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart2);
    cout << "worker started successfully" << endl;

    NESTopologyManager::getInstance().printNESTopologyPlan();

    //NOTE: we cannot check full output as ids change each run
    std::string retString = NESTopologyManager::getInstance().getNESTopologyPlanString();
    cout << "returned=" << retString;
    ASSERT_TRUE(retString.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString.find("1--0 [label=\"0\"]") != std::string::npos);
    ASSERT_TRUE(retString.find("2--0 [label=\"1\"]") != std::string::npos);

    size_t pos1Beg = retString.find("1[label");
    std::string firstIdPart1 = retString.substr(pos1Beg + 9);
    size_t pos1End = firstIdPart1.find(" ");
    std::string firstId = firstIdPart1.substr(0, pos1End);
    cout << "firstId=" << firstId << endl;

    size_t pos2Beg = retString.find("2[label");
    std::string secondPart1 = retString.substr(pos2Beg + 9);
    size_t secondEnd = secondPart1.find(" ");
    std::string secondId = secondPart1.substr(0, secondEnd);
    cout << "secondId=" << secondId << endl;

    cout << "REMOVE NEW PARENT" << endl;
    bool successRemoveParent = wrk->removeParent(firstId);
    EXPECT_TRUE(!successRemoveParent);

    //NOTE: we cannot check full output as ids change each run
    std::string retString3 = NESTopologyManager::getInstance().getNESTopologyPlanString();
    cout << "retString3=" << retString3;
    ASSERT_TRUE(retString3.find("type=Worker\"];") != std::string::npos);
    ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
    ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--0 [label=\"1\"]") != std::string::npos);
    ASSERT_TRUE(retString3.find("2--1 [label=\"2\"]") == std::string::npos);

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

}

