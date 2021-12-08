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
#include <Common/DataTypes/DataTypeFactory.hpp>
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
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <MaterializedView/MaterializedView.hpp>

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class MaterializedViewTest : public testing::Test {
public:
    static void SetUpTestCase() {
        NES::setupLogging("MaterializedViewTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MaterializedViewTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }
};

/**
 * Test ...
 */
/*TEST_F(MaterializedViewTest, test) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    remove("test.out");

    NES_INFO("MaterializedViewTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTest: Coordinator started successfully");

    NES_INFO("MaterializedViewTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(false, true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MaterializedViewTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "test.out";
    NES_INFO("MaterializedViewTest: Submit query");

    Query query = Query::from("default_logical").sink(PrintSinkDescriptor::create());//.sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(1));
    auto queryPlan = query.getQueryPlan();

    auto queryId = queryService->addQueryRequest(queryPlan, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    if (!queryCatalogEntry) {
        FAIL();
    }
    NES_TRACE("TestUtils: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
    bool isQueryRunning = queryCatalogEntry->getQueryStatus() == QueryStatus::Running;
    if (isQueryRunning) {
        FAIL();
    }
    NES_INFO("MaterializedViewTest: Sleeping");
    sleep(60);
    NES_INFO("MaterializedViewTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MaterializedViewTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTest: Test finished");
}*/


TEST_F(MaterializedViewTest, test2) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    //register logical stream qnv
    std::string stream =
            R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    wrk1->registerLogicalStream("stream", testSchemaFileName);

    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("stream");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);
    //register physical stream
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(streamConf);

    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream2");
    PhysicalStreamConfigPtr streamConf2 = PhysicalStreamConfig::create(srcConf);
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(1);
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(0);
    wrk1->registerPhysicalStream(streamConf2);

    /*std::string outputFilePath = "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("id") < 5).sink(FileSinkDescriptor::create(")" + outputFilePath
            + R"(", "CSV_FORMAT", "APPEND"));)";
    */
    //Query query = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    Query query = Query::from("default_logical").sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(1));
    auto queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(1);
    auto queryId = queryService->addQueryRequest(queryPlan, "BottomUp");

    NES_INFO("QueryDeploymentTest: queryId" << queryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}