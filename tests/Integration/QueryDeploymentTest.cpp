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
    void SetUp() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down QueryDeploymentTest class." << std::endl;
    }
};

TEST_F(QueryDeploymentTest, testDeployOneWorkerPrint) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";

    string queryId = crd->addQuery(query, "BottomUp");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    cout << "remove query" << endl;
    crd->removeQuery(queryId);

    cout << "stop worker 1" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stop coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    cout << "test finished" << endl;
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerPrint) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";

    cout << "start query" << endl;
    std::string queryId = crd->addQuery(query, "BottomUp");

    cout << "check and wait result" << endl;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 2));

    cout << "remove query" << endl;
    crd->removeQuery(queryId);

    cout << "stop worker 1" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stop worker 2" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    cout << "stop coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutput) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
        std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\", FILE_OVERWRITE, BINARY_TYPE));";

    cout << "add query" << endl;
    std::string queryId = crd->addQuery(query, "BottomUp");

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

    cout << "remove query" << endl;
    crd->removeQuery(queryId);

    cout << "stop worker" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stop coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}


TEST_F(QueryDeploymentTest, testDeployUndeployOneWorkerFileOutput) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\", FILE_OVERWRITE, BINARY_TYPE));";

    string queryId = crd->addQuery(query, "BottomUp");
    ASSERT_NE(queryId, "");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));
    bool successUndeploy = crd->removeQuery(queryId);
    ASSERT_TRUE(successUndeploy);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(QueryDeploymentTest, testDeployAndUndeployTwoWorkerFileOutput) {
    remove("test.out");

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\", FILE_OVERWRITE, BINARY_TYPE));";

    string queryId = crd->addQuery(query, "BottomUp");
    ASSERT_NE(queryId, "");

    cout << "check crd" << endl;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 2));
    cout << "check wrk1" << endl;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    cout << "check wrk2" << endl;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, 1));

    bool successUndeploy = crd->removeQuery(queryId);
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
