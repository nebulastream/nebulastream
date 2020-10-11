#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class QueryDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() {
        std::cout << "Tear down QueryDeploymentTest class." << std::endl;
    }

    std::string ipAddress = "127.0.0.1";
};

/**
 * Test deploying merge query with source on two different worker node using bottom up strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    std::string outputFilePath =
        "testDeployTwoWorkerMergeUsingBottomUp.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "",
                                                                   1, 0, 3,
                                                                   "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "",
                                                                     1, 0, 3,
                                                                     "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"car\").merge(Query::from(\"truck\")).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    std::string outputFilePath =
        "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "",
                                                                   1, 0, 3,
                                                                   "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "",
                                                                     1, 0, 3,
                                                                     "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"car\").merge(Query::from(\"truck\")).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

TEST_F(QueryDeploymentTest, testDeployOneWorker) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployOneWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployOneWorker): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployOneWorker): expContent=" << expectedContent);
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
}

/**
 * @brief Test deploy query with print sink with one worker using top down strategy
 */
TEST_F(QueryDeploymentTest, testDeployOneWorkerUsingTopDownStrategy) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployOneWorkerUsingTopDownStrategy.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployOneWorkerUsingTopDownStrategy): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerUsingTopDownStrategy): expContent=" << expectedContent);
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
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryEventTime) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/exdra.csv",
                                                                1, 0, 1,
                                                                "test_stream", "exdra");

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute(\"metadata_generated\")), "
                   "Seconds(10)), Sum::on(Attribute(\"features_properties_capacity\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"DONT_APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    sleep(17);
    //TODO: check result
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 2));
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 2));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

#if 0
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:INT64|features_properties_capacity:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1262343610000|1262343620000|1|736|\n"
        "|1262343620000|1262343630000|1|0|\n"
        "|1262343630000|1262343640000|1|0|\n"
        "|1262343640000|1262343650000|1|0|\n"
        "|1262343650000|1262343660000|1|0|\n"
        "|1262343660000|1262343670000|1|0|\n"
        "|1262343670000|1262343680000|1|0|\n"
        "|1262343680000|1262343690000|1|0|\n"
        "|1262343620000|1262343630000|2|1348|\n"
        "|1262343630000|1262343640000|2|0|\n"
        "|1262343640000|1262343650000|2|0|\n"
        "|1262343650000|1262343660000|2|0|\n"
        "|1262343660000|1262343670000|2|0|\n"
        "|1262343670000|1262343680000|2|0|\n"
        "|1262343680000|1262343690000|2|0|\n"
        "|1262343630000|1262343640000|3|4575|\n"
        "|1262343640000|1262343650000|3|0|\n"
        "|1262343650000|1262343660000|3|0|\n"
        "|1262343660000|1262343670000|3|0|\n"
        "|1262343670000|1262343680000|3|0|\n"
        "|1262343680000|1262343690000|3|0|\n"
        "|1262343640000|1262343650000|4|1358|\n"
        "|1262343650000|1262343660000|4|0|\n"
        "|1262343660000|1262343670000|4|0|\n"
        "|1262343670000|1262343680000|4|0|\n"
        "|1262343680000|1262343690000|4|0|\n"
        "|1262343650000|1262343660000|5|1288|\n"
        "|1262343660000|1262343670000|5|0|\n"
        "|1262343670000|1262343680000|5|0|\n"
        "|1262343680000|1262343690000|5|0|\n"
        "|1262343660000|1262343670000|6|3458|\n"
        "|1262343670000|1262343680000|6|0|\n"
        "|1262343680000|1262343690000|6|0|\n"
        "|1262343670000|1262343680000|7|1128|\n"
        "|1262343680000|1262343690000|7|0|\n"
        "|1262343680000|1262343690000|8|1079|\n"
        "|1262343600000|1262343610000|10|2632|\n"
        "|1262343610000|1262343620000|10|0|\n"
        "|1262343620000|1262343630000|10|0|\n"
        "|1262343630000|1262343640000|10|0|\n"
        "|1262343640000|1262343650000|10|0|\n"
        "|1262343650000|1262343660000|10|0|\n"
        "|1262343660000|1262343670000|10|0|\n"
        "|1262343670000|1262343680000|10|0|\n"
        "|1262343680000|1262343690000|10|0|\n"
        "|1262343600000|1262343610000|11|4653|\n"
        "|1262343610000|1262343620000|11|0|\n"
        "|1262343620000|1262343630000|11|0|\n"
        "|1262343630000|1262343640000|11|0|\n"
        "|1262343640000|1262343650000|11|0|\n"
        "|1262343650000|1262343660000|11|0|\n"
        "|1262343660000|1262343670000|11|0|\n"
        "|1262343670000|1262343680000|11|0|\n"
        "|1262343680000|1262343690000|11|0|\n"
        "+----------------------------------------------------+";
#else
    string expectedContent = "start:INTEGER,end:INTEGER,key:INTEGER,features_properties_capacity:INTEGER\n"
                             "1262343610000,1262343620000,1,736\n"
                             "1262343620000,1262343630000,1,0\n"
                             "1262343630000,1262343640000,1,0\n"
                             "1262343640000,1262343650000,1,0\n"
                             "1262343650000,1262343660000,1,0\n"
                             "1262343660000,1262343670000,1,0\n"
                             "1262343670000,1262343680000,1,0\n"
                             "1262343680000,1262343690000,1,0\n"
                             "1262343620000,1262343630000,2,1348\n"
                             "1262343630000,1262343640000,2,0\n"
                             "1262343640000,1262343650000,2,0\n"
                             "1262343650000,1262343660000,2,0\n"
                             "1262343660000,1262343670000,2,0\n"
                             "1262343670000,1262343680000,2,0\n"
                             "1262343680000,1262343690000,2,0\n"
                             "1262343630000,1262343640000,3,4575\n"
                             "1262343640000,1262343650000,3,0\n"
                             "1262343650000,1262343660000,3,0\n"
                             "1262343660000,1262343670000,3,0\n"
                             "1262343670000,1262343680000,3,0\n"
                             "1262343680000,1262343690000,3,0\n"
                             "1262343640000,1262343650000,4,1358\n"
                             "1262343650000,1262343660000,4,0\n"
                             "1262343660000,1262343670000,4,0\n"
                             "1262343670000,1262343680000,4,0\n"
                             "1262343680000,1262343690000,4,0\n"
                             "1262343650000,1262343660000,5,1288\n"
                             "1262343660000,1262343670000,5,0\n"
                             "1262343670000,1262343680000,5,0\n"
                             "1262343680000,1262343690000,5,0\n"
                             "1262343660000,1262343670000,6,3458\n"
                             "1262343670000,1262343680000,6,0\n"
                             "1262343680000,1262343690000,6,0\n"
                             "1262343670000,1262343680000,7,1128\n"
                             "1262343680000,1262343690000,7,0\n"
                             "1262343680000,1262343690000,8,1079\n"
                             "1262343600000,1262343610000,10,2632\n"
                             "1262343610000,1262343620000,10,0\n"
                             "1262343620000,1262343630000,10,0\n"
                             "1262343630000,1262343640000,10,0\n"
                             "1262343640000,1262343650000,10,0\n"
                             "1262343650000,1262343660000,10,0\n"
                             "1262343660000,1262343670000,10,0\n"
                             "1262343670000,1262343680000,10,0\n"
                             "1262343680000,1262343690000,10,0\n"
                             "1262343600000,1262343610000,11,4653\n"
                             "1262343610000,1262343620000,11,0\n"
                             "1262343620000,1262343630000,11,0\n"
                             "1262343630000,1262343640000,11,0\n"
                             "1262343640000,1262343650000,11,0\n"
                             "1262343650000,1262343660000,11,0\n"
                             "1262343660000,1262343670000,11,0\n"
                             "1262343670000,1262343680000,11,0\n"
                             "1262343680000,1262343690000,11,0\n";
#endif
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryEventTime): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryEventTime): expContent=" << expectedContent);
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
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerCentralSlidingWindowQueryEventTime) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker 1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv",
                                                                  1, 0, 1,
                                                                  "test_stream", "window");

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath =
        "testDeployOneWorkerCentralSlidingWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), SlidingWindow::of(TimeCharacteristic::createEventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), Sum::on(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    sleep(25);
    NES_DEBUG("wakeup");

    ifstream my_file("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:INT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|10000|20000|1|870|\n"
        "|5000|15000|1|570|\n"
        "|0|10000|1|307|\n"
        "|10000|20000|4|0|\n"
        "|5000|15000|4|0|\n"
        "|0|10000|4|6|\n"
        "|10000|20000|11|0|\n"
        "|5000|15000|11|0|\n"
        "|0|10000|11|30|\n"
        "|10000|20000|12|0|\n"
        "|5000|15000|12|0|\n"
        "|0|10000|12|7|\n"
        "|10000|20000|16|0|\n"
        "|5000|15000|16|0|\n"
        "|0|10000|16|12|\n"
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
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerCentralWindowQueryProcessingTime) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployOneWorkerCentralWindowQueryProcessingTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/exdra.csv",
                                                                1, 0, 2,
                                                                "test_stream", "exdra");
    wrk1->registerPhysicalStream(conf);

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(TimeCharacteristic::createProcessingTime(), "
                   "Seconds(1)), Sum::on(Attribute(\"metadata_generated\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    sleep(12);
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    std::ifstream ifs(outputFilePath);
    std::string line;
    int rowNumber = 0;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
        if (rowNumber == 4) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): content=" << content.at(3));
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): expContent=2524687220000");
            EXPECT_EQ(content.at(3), "2524687220000");
        }
        if (rowNumber == 5) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): content=" << content.at(3));
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): expContent=1262343620010");
            EXPECT_EQ(content.at(3), "1262343620010");
        }
        if (rowNumber > 17 && rowNumber < 28) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): content=" << content.at(3));
            NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): expContent=0");
            EXPECT_EQ(content.at(3), "0");
        }
    }
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): content=" << rowNumber);
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerCentralWindowQueryProcessingTime): expContent=57");
    EXPECT_EQ(rowNumber, 57);

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

TEST_F(QueryDeploymentTest, testDeployOneWorkerDistributedWindowQueryProcessingTime) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker 1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").windowByKey(Attribute(\"value\"), TumblingWindow::of(TimeCharacteristic::createProcessingTime(), "
                   "Seconds(1)), Sum::on(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\"query.out\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    cout << "wait start" << endl;
    sleep(10);
    cout << "wakeup" << endl;
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    std::ifstream ifs("query.out");
    std::string line;
    int rowNumber = 0;
    //TODO that query result is empty?
    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
    }
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerDistributedWindowQueryProcessingTime): content=" << rowNumber);
    NES_INFO("QueryDeploymentTest(testDeployOneWorkerDistributedWindowQueryProcessingTime): expContent=0");
    EXPECT_EQ(rowNumber, 0);

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    //    NES_INFO("QueryDeploymentTest: Stop worker 2");
    //    bool retStopWrk2 = wrk2->stop(true);
    //    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerDistributedWindowQueryEventTime) {
    ::testing::GTEST_FLAG(throw_on_failure) = true;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker 1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath =
        "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv",
                                                                1, 3, 3,
                                                                "test_stream", "window");
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);

    NES_INFO("QueryDeploymentTest: Submit query");
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum::on(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"DONT_APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    cout << "wait start" << endl;
    sleep(25);
    cout << "wakeup" << endl;

    ifstream my_file("testDeployOneWorkerDistributedWindowQueryEventTime.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

#if 0
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:INT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1000|2000|1|34|\n"
        "|2000|3000|1|0|\n"
        "|2000|3000|2|56|\n"
        "+----------------------------------------------------+";
#else
    string expectedContent =
        "start:INTEGER,end:INTEGER,key:INTEGER,value:INTEGER\n"
        "1000,2000,1,34\n"
        "2000,3000,1,0\n"
        "2000,3000,2,56\n";
#endif

    NES_INFO("QueryDeploymentTest: content=" << content);
    NES_INFO("QueryDeploymentTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

TEST_F(QueryDeploymentTest, testDeployTwoWorker) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployTwoWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployTwoWorker): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployTwoWorker): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

TEST_F(QueryDeploymentTest, testDeployTwoWorkerUsingTopDownStrategy) {
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployTwoWorkerUsingTopDownStrategy.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath);
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

    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerUsingTopDownStrategy): content=" << content);
    NES_INFO("QueryDeploymentTest(testDeployTwoWorkerUsingTopDownStrategy): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

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
    NES_INFO("QueryDeploymentTest (testDeployOneWorkerFileOutput): content=" << content);
    NES_INFO("QueryDeploymentTest (testDeployOneWorkerFileOutput): expContent=" << expectedContent);
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

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
    NES_INFO("QueryDeploymentTest (testDeployUndeployOneWorkerFileOutput): content=" << content);
    NES_INFO("QueryDeploymentTest (testDeployUndeployOneWorkerFileOutput): expContent=" << expectedContent);
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

TEST_F(QueryDeploymentTest, testDeployUndeployTwoWorkerFileOutput) {
    remove("test.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test.out\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

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

    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): content=" << content);
    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesTwoWorkerFileOutput) {
    remove("test1.out");
    remove("test2.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test1.out\"));";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "BottomUp");
    string query2 = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test2.out\"));";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId1, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId2, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 2));

    ifstream my_file1("test1.out");
    EXPECT_TRUE(my_file1.good());

    std::ifstream ifs1("test1.out");
    std::string actualContent1((std::istreambuf_iterator<char>(ifs1)),
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

    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): content=" << actualContent1);
    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(actualContent1, expectedContent);

    ifstream my_file2("test2.out");
    EXPECT_TRUE(my_file2.good());

    std::ifstream ifs2("test2.out");
    std::string actualContent2((std::istreambuf_iterator<char>(ifs2)),
                               (std::istreambuf_iterator<char>()));

    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): content=" << actualContent2);
    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): expContent=" << expectedContent);

    EXPECT_EQ(actualContent2, expectedContent);

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId1);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

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

    int response1 = remove("test1.out");
    EXPECT_EQ(response1, 0);

    int response2 = remove("test2.out");
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesOnTwoWorkerFileOutputWithQueryMerging) {
    remove("test1.out");
    remove("test2.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, std::thread::hardware_concurrency(), true);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test1.out\"));";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "BottomUp");
    string query2 = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"test2.out\"));";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId1, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId2, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 2));

    ifstream my_file1("test1.out");
    EXPECT_TRUE(my_file1.good());

    std::ifstream ifs1("test1.out");
    std::string actualContent1((std::istreambuf_iterator<char>(ifs1)),
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

    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): content=" << actualContent1);
    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(actualContent1, expectedContent);

    ifstream my_file2("test2.out");
    EXPECT_TRUE(my_file2.good());

    std::ifstream ifs2("test2.out");
    std::string actualContent2((std::istreambuf_iterator<char>(ifs2)),
                               (std::istreambuf_iterator<char>()));

    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): content=" << actualContent2);
    NES_INFO("QueryDeploymentTest (testDeployUndeployTwoWorkerFileOutput): expContent=" << expectedContent);

    EXPECT_EQ(actualContent2, expectedContent);

    NES_INFO("QueryDeploymentTest: Remove query " << queryId1);
    queryService->validateAndQueueStopRequest(queryId1);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryDeploymentTest: Remove query " << queryId2);
    queryService->validateAndQueueStopRequest(queryId2);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

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

    int response1 = remove("test1.out");
    EXPECT_EQ(response1, 0);

    int response2 = remove("test2.out");
    EXPECT_EQ(response2, 0);
}

}// namespace NES
