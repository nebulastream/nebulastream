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
#include <Configurations/Sources/MaterializedViewSourceConfig.hpp>
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
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Views/MaterializedView.hpp>

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
};

TEST_F(MaterializedViewTest, DISABLED_MaterializedViewTupleBufferSinkTest) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(false, true);
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
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(1);
    wrk1->registerPhysicalStream(streamConf2);

    // TODO: wrong stream name?
    Query query = Query::from("default_logical").sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(1));
    auto queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(1);
    auto queryId = queryService->addQueryRequest(queryPlan, "BottomUp");

    NES_INFO("QueryDeploymentTest: queryId" << queryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    // TODO: Needed?
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


// @Brief Ad hoc query on empty materialized view
TEST_F(MaterializedViewTest, DISABLED_MaterializedViewTupleBufferSourceTest) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("MaterializedViewSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("MaterializedViewTupleBufferSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Coordinator started successfully");

    ///////////// Init Ad Hoc Query ///////////////
    NES_INFO("MaterializedViewTupleBufferSourceTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(false, true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    //register logical stream
    std::string stream =
            R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    wrk1->registerLogicalStream("stream2", testSchemaFileName);

    // materialized view physical stream
    size_t viewId = 1;
    srcConf->as<MaterializedViewSourceConfig>()->setPhysicalStreamName("MV");
    srcConf->as<MaterializedViewSourceConfig>()->setLogicalStreamName("stream2");
    srcConf->as<MaterializedViewSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->as<MaterializedViewSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf->as<MaterializedViewSourceConfig>()->setSourceFrequency(1);
    srcConf->as<MaterializedViewSourceConfig>()->setId(viewId);
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(streamConf);

    Query adhoc_query = Query::from("stream2").sink(PrintSinkDescriptor::create());
    auto adhoc_queryPlan = adhoc_query.getQueryPlan();
    adhoc_queryPlan->setQueryId(2);
    auto adhocQueryId = queryService->addQueryRequest(adhoc_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSourceTest: queryId" << adhocQueryId);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Remove query");
    queryService->validateAndQueueStopRequest(adhocQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MaterializedViewTupleBufferSourceTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Test finished");
}

// TODO better test by writing result into to file....
TEST_F(MaterializedViewTest, MaterializedViewTupleBufferSinkAndSourceTest) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("MaterializedViewSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("MaterializedViewTupleBufferSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    ///////////// Init Maintenance Query ///////////////
    //register logical stream
    std::string stream =
            R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    crd->getNesWorker()->registerLogicalStream("stream", testSchemaFileName);

    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("stream");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    crd->getNesWorker()->registerPhysicalStream(streamConf);

    ///////////// Init Maintenance Query ///////////////
    size_t viewId = 1;
    Query maintenance_query = Query::from("stream")
            .sink(Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(viewId));
    auto maintenance_queryPlan = maintenance_query.getQueryPlan();
    maintenance_queryPlan->setQueryId(1);
    auto maintenanceQueryId = queryService->addQueryRequest(maintenance_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSourceTest: queryId" << maintenanceQueryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(maintenanceQueryId, queryCatalog));

    ///////////// Init Ad Hoc Query ///////////////
    crd->getNesWorker()->registerLogicalStream("stream2", testSchemaFileName);

    // materialized view physical stream
    srcConf2->as<MaterializedViewSourceConfig>()->setPhysicalStreamName("MV");
    srcConf2->as<MaterializedViewSourceConfig>()->setLogicalStreamName("stream2");
    srcConf2->as<MaterializedViewSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf2->as<MaterializedViewSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf2->as<MaterializedViewSourceConfig>()->setSourceFrequency(1);
    srcConf2->as<MaterializedViewSourceConfig>()->setId(viewId);
    PhysicalStreamConfigPtr streamConf2 = PhysicalStreamConfig::create(srcConf2);
    crd->getNesWorker()->registerPhysicalStream(streamConf2);

    Query adhoc_query = Query::from("stream2").sink(PrintSinkDescriptor::create());
    auto adhoc_queryPlan = adhoc_query.getQueryPlan();
    adhoc_queryPlan->setQueryId(2);
    auto adhocQueryId = queryService->addQueryRequest(adhoc_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSourceTest: queryId" << adhocQueryId);
    globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(adhocQueryId, queryCatalog));

    ///////////////// Stop Query  ////////////////
    NES_INFO("MaterializedViewTupleBufferSourceTest: Remove maintenance query");
    queryService->validateAndQueueStopRequest(maintenanceQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(maintenanceQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Remove ad hoc query");
    queryService->validateAndQueueStopRequest(adhocQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Test finished");
}