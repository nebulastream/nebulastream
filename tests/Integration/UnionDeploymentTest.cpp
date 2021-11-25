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
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class UnionDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("UnionDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UnionDeploymentTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { NES_INFO("Tear down UnionDeploymentTest class."); }
};

/**
 * Test deploying unionWith query with source on two different worker node using bottom up strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingBottomUp.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_car");
    sourceConfig->setLogicalStreamName("car");
    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(sourceConfig);
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_truck");
    sourceConfig->setLogicalStreamName("truck");
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create(sourceConfig);
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): content=" << content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_car");
    sourceConfig->setLogicalStreamName("car");
    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(sourceConfig);
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_truck");
    sourceConfig->setLogicalStreamName("truck");
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create(sourceConfig);
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content=" << content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDownWithDifferentSpeed) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_car");
    sourceConfig->setLogicalStreamName("car");
    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(sourceConfig);
    wrk1->registerPhysicalStream(confCar);

    wrk2->registerLogicalStream("truck", testSchemaFileName);

    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_truck");
    sourceConfig->setLogicalStreamName("truck");
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create(sourceConfig);
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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
        "|car$id:UINT32|car$value:UINT64|\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content=" << content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testMergeTwoDifferentStreams) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker2 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_car");
    sourceConfig->setLogicalStreamName("car");
    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(confCar);

    std::string testSchema2 = R"(Schema::create()->addField("id", BasicType::UINT16)->addField("value", BasicType::UINT64);)";
    std::string testSchemaFileName2 = "testSchema2.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << testSchema2;
    out2.close();
    wrk2->registerLogicalStream("truck", testSchemaFileName2);

    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_truck");
    sourceConfig->setLogicalStreamName("truck");
    //register physical stream
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create(sourceConfig);
    wrk2->registerPhysicalStream(confTruck);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    cout << "queryid=" << queryId << endl;
    EXPECT_TRUE(!TestUtils::waitForQueryToStart(queryId, queryCatalog));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are above a unionWith operator and will be pushed down towards both of the available sources.
 *       2 filter operators are already below unionWith operator and need to be pushed down normally towards its respective source.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig("CSVSource");

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    sourceConfig->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    sourceConfig->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->as<CSVSourceConfig>()->setPhysicalStreamName("physical_ruby");
    sourceConfig->as<CSVSourceConfig>()->setLogicalStreamName("ruby");
    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby = PhysicalStreamConfig::create(sourceConfig);

    sourceConfig->as<CSVSourceConfig>()->setPhysicalStreamName("physical_diamond");
    sourceConfig->as<CSVSourceConfig>()->setLogicalStreamName("diamond");

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".filter(Attribute(\"id\") < 12)"
                   ".unionWith(Query::from(\"diamond\")"
                   ".filter(Attribute(\"value\") < 15))"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"value\") < 17)"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") > 1)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "|9|1|2|\n"
                                        "|10|1|2|\n"
                                        "|11|1|2|\n"
                                        "|12|1|2|\n"
                                        "|13|1|2|\n"
                                        "|14|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|2|1|2|\n"
                                         "|2|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|1|2|\n"
                                         "|4|1|2|\n"
                                         "|5|1|2|\n"
                                         "|6|1|2|\n"
                                         "|7|1|2|\n"
                                         "|8|1|2|\n"
                                         "|9|1|2|\n"
                                         "|10|1|2|\n"
                                         "|11|1|2|\n"
                                         "|12|1|2|\n"
                                         "|13|1|2|\n"
                                         "|14|1|2|\n"
                                         "|15|1|2|\n"
                                         "|16|1|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): content="
             << content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 1 filter operator is above a unionWith operator and will be pushed down towards both of the available sources.
 *       1 filter operator is already below unionWith operator and needs to be pushed down normally towards its own source.
 */
TEST_F(UnionDeploymentTest, testOneFilterPushDownWithMergeOfTwoDifferentStreams) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig("CSVSource");

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testOneFilterPushDownWithMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    sourceConfig->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    sourceConfig->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->as<CSVSourceConfig>()->setPhysicalStreamName("physical_ruby");
    sourceConfig->as<CSVSourceConfig>()->setLogicalStreamName("ruby");
    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby = PhysicalStreamConfig::create(sourceConfig);

    sourceConfig->as<CSVSourceConfig>()->setPhysicalStreamName("physical_diamond");
    sourceConfig->as<CSVSourceConfig>()->setLogicalStreamName("diamond");

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".unionWith(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") > 3))"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"id\") > 4)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|12|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|11|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|12|2|\n"
                                         "|2|11|2|\n"
                                         "|2|16|2|\n"
                                         "|3|11|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("UnionDeploymentTest(testOneFilterPushDownWithMergeOfTwoDifferentStreams): content=" << content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are already below unionWith operator and needs to be pushed down normally towards their respective source.
 *       Here the filters don't need to be pushed down over an existing unionWith operator.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig("CSVSource");

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Coordinator started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams.out";
    remove(outputFilePath.c_str());

    //register logical stream
    std::string rareStonesSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT32))->addField(createField("id", BasicType::UINT32))->addField(createField("timestamp", BasicType::INT32));)";
    std::string rareStonesSchemaFileName = "rareStonesSchema.hpp";
    std::ofstream out(rareStonesSchemaFileName);
    out << rareStonesSchema;
    out.close();

    wrk1->registerLogicalStream("ruby", rareStonesSchemaFileName);
    wrk2->registerLogicalStream("diamond", rareStonesSchemaFileName);

    sourceConfig->as<CSVSourceConfig>()->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    sourceConfig->as<CSVSourceConfig>()->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->as<CSVSourceConfig>()->as<CSVSourceConfig>()->setPhysicalStreamName("physical_ruby");
    sourceConfig->as<CSVSourceConfig>()->as<CSVSourceConfig>()->setLogicalStreamName("ruby");
    //register physical stream
    PhysicalStreamConfigPtr confStreamRuby = PhysicalStreamConfig::create(sourceConfig);

    sourceConfig->as<CSVSourceConfig>()->setPhysicalStreamName("physical_diamond");
    sourceConfig->as<CSVSourceConfig>()->setLogicalStreamName("diamond");

    PhysicalStreamConfigPtr confStreamDiamond = PhysicalStreamConfig::create(sourceConfig);

    wrk1->registerPhysicalStream(confStreamRuby);
    wrk2->registerPhysicalStream(confStreamDiamond);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") < 9)"
                   ".unionWith(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") < 12)"
                   ".filter(Attribute(\"value\") < 6))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|1|2|\n"
                                        "|1|12|2|\n"
                                        "|1|4|2|\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|1|1|\n"
                                         "|1|4|1|\n"
                                         "|2|1|1|\n"
                                         "|2|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|1|1|\n"
                                         "|4|1|1|\n"
                                         "|5|1|1|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentStreams): content=" << content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentSubQry="
             << expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentStreams): "
             "expectedContentMainQry="
             << expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}
}// namespace NES
