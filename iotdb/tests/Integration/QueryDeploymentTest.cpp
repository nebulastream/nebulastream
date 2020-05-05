#include <iostream>

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
#include <Util/TestUtils.hpp>

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

    string queryId = crd->deployQuery(query, "BottomUp");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    crd->undeployQuery(queryId);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
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

    std::string queryId = crd->deployQuery(query, "BottomUp");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 2));

    crd->undeployQuery(queryId);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
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

    std::string queryId = crd->deployQuery(query, "BottomUp");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

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

    crd->undeployQuery(queryId);
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
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

    string queryId = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(queryId, "");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));
    bool successUndeploy = crd->undeployQuery(queryId);
    ASSERT_TRUE(successUndeploy);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(QueryDeploymentTest, test_deploy_two_worker_file_output_with_exceptions) {
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
    string queryId = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(queryId, "");


    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 2));

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

    //could not be stopped because query is still running
    try {
        cout << "Test stop wrk1" << endl;
        bool retStopWrk1 = wrk1->stop(false);
        EXPECT_TRUE(!retStopWrk1);
    }
    catch (...) {
        SUCCEED();
    }

    try {
        cout << "Test stop wrk2" << endl;
        bool retStopWrk2 = wrk2->stop(false);
        EXPECT_TRUE(!retStopWrk2);
    }
    catch (...) {
        SUCCEED();
    }

    try {
        cout << "Test stop crd" << endl;
        bool retStopCord = crd->stopCoordinator(false);
        EXPECT_TRUE(!retStopCord);
    }
    catch (...) {
        SUCCEED();
    }

    cout << "Test stop wrk1 forcefully" << endl;
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    cout << "Test stop wrk2 forcefully" << endl;
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    cout << "shut down coordinator forcefully" << endl;
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);

    cout << " remove file" << endl;
    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}


TEST_F(QueryDeploymentTest, test_deploy_and_undeploy_two_worker_file_output) {
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

    string queryId = crd->deployQuery(query, "BottomUp");
    ASSERT_NE(queryId, "");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 2));

    bool successUndeploy = crd->undeployQuery(queryId);
    ASSERT_TRUE(successUndeploy);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

}
