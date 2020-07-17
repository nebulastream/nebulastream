#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class QueryDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 2;
    }

    void TearDown() {
        std::cout << "Tear down QueryDeploymentTest class." << std::endl;
    }

    std::string ipAddress = "localhost";
    uint64_t restPort = 8081;
};

/**
 * TODO: this test requires issue 750 to be addressed. Currently, make it disabled.
 */
TEST_F(QueryDeploymentTest, DISABLED_testDeployOneWorkerMergePrint) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "Query::from(\"default_logical\").merge(Query::from(\"default_logical\")).sink(PrintSinkDescriptor::create());";

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

TEST_F(QueryDeploymentTest, testDeployOneWorkerPrint) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    string queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerPrint) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost",
                                                    std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    string queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 2));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutput) {
    remove("test.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    std::string queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

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
    NES_INFO("QueryDeploymentTest: content=" << content);
    NES_INFO("QueryDeploymentTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(QueryDeploymentTest, testDeployUndeployOneWorkerFileOutput) {
    remove("test.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    std::string queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(QueryDeploymentTest, testDeployUndeployTwoWorkerFileOutput) {
    remove("test.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 20), NESNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    string queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 2));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

}// namespace NES
