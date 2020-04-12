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
                "InputQuery inputQueryPtr = InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); "
                "return inputQueryPtr;";

        static void SetUpTestCase() {
            NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
            NES_INFO("Setup UpdateTopologyRemoteTest test class.");
        }

        static void TearDownTestCase() {
            std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl;
        }
    };

    TEST_F(UpdateTopologyRemoteTest, remove_not_existing_parts) {
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
        size_t port = crd->startCoordinator(/**blocking**/false);
        EXPECT_NE(port, 0);
        cout << "coordinator started successfully" << endl;
        sleep(1);

        cout << "start worker" << endl;
        NesWorkerPtr wrk = std::make_shared<NesWorker>();
        bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
        EXPECT_TRUE(retStart);
        cout << "worker started successfully" << endl;

        // Remove link between root and not existing node
        std::string retString = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "returned=" << retString;

        size_t pos1Beg = retString.find("1[label");
        std::string firstIdPart1 = retString.substr(pos1Beg + 9);
        size_t pos1End = firstIdPart1.find(" ");
        std::string firstId = firstIdPart1.substr(0, pos1End);
        firstId += "333";
        cout << "firstId=" << firstId << endl;
        std::string par = "0";
        cout << "REMOVE PARENT" << endl;
        bool succesRemoveNotExistingPath = wrk->removeLink(firstId, par);
        EXPECT_TRUE(!succesRemoveNotExistingPath);

        //NOTE: we cannot check full output as ids change each run
        std::string retString3 = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retString3=" << retString3;
        ASSERT_TRUE(retString3.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);

        // Remove link between two not existing node
        cout << "REMOVE PARENT2" << endl;
        bool succesRemoveNotExistingPathTNE = wrk->removeLink("123333", "12334");
        EXPECT_TRUE(!succesRemoveNotExistingPathTNE);

        std::string retStringTNE = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retStringTNE=" << retStringTNE;
        ASSERT_TRUE(retStringTNE.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retStringTNE.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retStringTNE.find("1--0 [label=\"0\"]") != std::string::npos);

        // Remove link between same node
        cout << "REMOVE PARENT3" << endl;
        bool successRemoveNotExistingPathSN = wrk->removeLink("123333", "12333");
        EXPECT_TRUE(!successRemoveNotExistingPathSN);

        //NOTE: we cannot check full output as ids change each run
        std::string retStringSN = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retStringSN=" << retStringSN;
        ASSERT_TRUE(retStringSN.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retStringSN.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retStringSN.find("1--0 [label=\"0\"]") != std::string::npos);

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop();
        EXPECT_TRUE(retStopWrk);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator();
        EXPECT_TRUE(retStopCord);
    }

    TEST_F(UpdateTopologyRemoteTest, add_and_remove_path) {
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
        ASSERT_TRUE(retString.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
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
        bool successAddPar = wrk->addNewLink(firstId, secondId);
        EXPECT_TRUE(successAddPar);

        //NOTE: we cannot check full output as ids change each run
        std::string retString2 = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retString2=" << retString2;
        ASSERT_TRUE(retString2.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retString2.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retString2.find("1--0 [label=\"0\"]") != std::string::npos);
        ASSERT_TRUE(retString2.find("2--0 [label=\"1\"]") != std::string::npos);
        ASSERT_TRUE(retString2.find("1--2 [label=\"2\"]") != std::string::npos);

        cout << "REMOVE NEW PARENT" << endl;
        bool successRemoveParent = wrk->removeLink(firstId, secondId);
        EXPECT_TRUE(successAddPar);

        //NOTE: we cannot check full output as ids change each run
        std::string retString3 = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retString3=" << retString3;
        ASSERT_TRUE(retString3.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("2--0 [label=\"1\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--2 [label=\"2\"]") == std::string::npos);

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop();
        EXPECT_TRUE(retStopWrk);

        cout << "stopping worker 2" << endl;
        bool retStopWrk2 = wrk2->stop();
        EXPECT_TRUE(retStopWrk2);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator();
        EXPECT_TRUE(retStopCord);
    }

    TEST_F(UpdateTopologyRemoteTest, add_and_remove_path_with_own_id) {
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
        ASSERT_TRUE(retString.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
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
        ASSERT_TRUE(retString2.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
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
        ASSERT_TRUE(retString3.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("2--0 [label=\"1\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--2 [label=\"2\"]") == std::string::npos);

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop();
        EXPECT_TRUE(retStopWrk);

        cout << "stopping worker 2" << endl;
        bool retStopWrk2 = wrk2->stop();
        EXPECT_TRUE(retStopWrk2);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator();
        EXPECT_TRUE(retStopCord);
    }

    TEST_F(UpdateTopologyRemoteTest, add_and_remove_path_with_own_id_and_self) {
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
        ASSERT_TRUE(retString.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
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

        cout << "ADD NEW PARENT for id=" << wrk->getIdFromServer() << endl;
        bool successAddPar = wrk->addParent(firstId);
        EXPECT_TRUE(!successAddPar);

        cout << "REMOVE NEW PARENT" << endl;
        bool successRemoveParent = wrk->removeParent(firstId);
        EXPECT_TRUE(!successAddPar);

        //NOTE: we cannot check full output as ids change each run
        std::string retString3 = NESTopologyManager::getInstance().getNESTopologyPlanString();
        cout << "retString3=" << retString3;
        ASSERT_TRUE(retString3.find("0[label=\"0 type=Coordinator\"];") != std::string::npos);
        ASSERT_TRUE(retString3.find("type=Sensor(default_physical)") != std::string::npos);
        ASSERT_TRUE(retString3.find("1--0 [label=\"0\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("2--0 [label=\"1\"]") != std::string::npos);
        ASSERT_TRUE(retString3.find("2--1 [label=\"2\"]") == std::string::npos);

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop();
        EXPECT_TRUE(retStopWrk);

        cout << "stopping worker 2" << endl;
        bool retStopWrk2 = wrk2->stop();
        EXPECT_TRUE(retStopWrk2);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator();
        EXPECT_TRUE(retStopCord);
    }

}

