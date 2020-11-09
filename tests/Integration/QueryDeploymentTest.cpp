/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
    string query = R"(Query::from("car").merge(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
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

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
TEST_F(QueryDeploymentTest, DISABLED_testDeployTwoWorkerJoinUsingTopDownOnSameSchema) {
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
    NES_INFO("QueryDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath =
        "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "",
                                                                   1, 0, 1,
                                                                   "physical_car", "car");
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "",
                                                                     1, 0, 1,
                                                                     "physical_truck", "truck");
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("car").join(Query::from("truck"), Attribute("id"), TumblingWindow::of(EventTime(Attribute("id")),
        Seconds(1))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

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
    string query = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
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
    string query = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
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
