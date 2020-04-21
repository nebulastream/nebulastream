#include <iostream>

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

using namespace std;

namespace NES {

class QueryDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down QueryDeploymentTest class." << std::endl;
    }
};

TEST_F(QueryDeploymentTest, test_deploy_one_worker_print) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "InputQuery::from(default_logical).print(std::cout);";

    crd->deployQuery(query, "BottomUp");
    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);
}

TEST_F(QueryDeploymentTest, test_deploy_two_worker_print) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    string query = "InputQuery::from(default_logical).print(std::cout);";

    crd->deployQuery(query, "BottomUp");
    sleep(2);

    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop();
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);
}

TEST_F(QueryDeploymentTest, test_deploy_one_worker_file_output) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "InputQuery::from(default_logical).writeToFile(\"test.out\");";

    crd->deployQuery(query, "BottomUp");
    sleep(2);
    ifstream my_file("test.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("test.out");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|id:UINT32|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}


TEST_F(QueryDeploymentTest, test_deploy_undeploy_one_worker_file_output) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "InputQuery::from(default_logical).writeToFile(\"test.out\");";

    string id = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(id, "");

    sleep(2);
    bool successUndeploy = crd->undeployQuery(id);
    ASSERT_TRUE(successUndeploy);

    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(QueryDeploymentTest, DISABLED_test_deploy_two_worker_file_output) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    string query = "InputQuery::from(default_logical).writeToFile(\"test.out\");";
    string id = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(id, "");
    sleep(2);
    cout << "Test read content now" << endl;
    ifstream my_file("test.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("test.out");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|id:UINT32|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|id:UINT32|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    cout << "Test stop now" << endl;
    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop();
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}


TEST_F(QueryDeploymentTest, DISABLED_test_deploy_and_undeploy_two_worker_file_output) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>();
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>();
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true,
                                               port, "localhost");
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    string query = "InputQuery::from(default_logical).writeToFile(\"test.out\");";

    string id = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(id, "");
    sleep(2);
    bool successUndeploy = crd->undeployQuery(id);
    ASSERT_TRUE(successUndeploy);

    bool retStopWrk1 = wrk1->stop();
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop();
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

}
