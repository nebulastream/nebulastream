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

#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MaterializedViewSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Views/MaterializedView.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class MaterializedViewTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MaterializedViewTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MaterializedViewTest test class.");
    }
};

/// @brief tests if a query with materialized view sink starts properly
TEST_F(MaterializedViewTest, MaterializedViewTupleViewSinkTest) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("MaterializedViewTupleViewSinkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    // register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("stream", stream);

    NES_INFO("MaterializedViewTupleViewSinkTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    Query query = Query::from("stream").sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(1));
    auto queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(1);
    auto queryId = queryService->addQueryRequest(queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleViewSinkTest: queryId" << queryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    NES_INFO("MaterializedViewTupleViewSinkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MaterializedViewTupleViewSinkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTupleViewSinkTest: Test finished");
}

/// @brief tests if a query with materialized view source starts properly
TEST_F(MaterializedViewTest, MaterializedViewTupleBufferSourceTest) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("stream", stream);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1->setNumberOfSlots(12);
    // materialized view physical stream
    size_t viewId = 1;
    auto materializeViewSourceType = Configurations::Experimental::MaterializedView::MaterializedViewSourceType::create();
    materializeViewSourceType->setId(viewId);
    auto physicalSource1 = PhysicalSource::create("stream", "MV", materializeViewSourceType);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");


    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    Query adhoc_query = Query::from("stream").sink(PrintSinkDescriptor::create());
    auto adhoc_queryPlan = adhoc_query.getQueryPlan();
    adhoc_queryPlan->setQueryId(1);
    auto adhocQueryId = queryService->addQueryRequest(adhoc_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSourceTest: queryId" << adhocQueryId);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Remove query");
    queryService->validateAndQueueStopRequest(adhocQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSourceTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Test finished");
}

// @brief tests with two concurrent queries if writing and reading of MVs works properly
TEST_F(MaterializedViewTest, MaterializedViewTupleBufferSinkAndSourceTest) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("stream", stream);
    crd->getStreamCatalogService()->registerLogicalSource("stream2", stream);
    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1->setNumberOfSlots(12);
    // materialized view physical stream
    size_t viewId = 1;
    auto materializeViewSourceType = Configurations::Experimental::MaterializedView::MaterializedViewSourceType::create();
    auto physicalSource1 = PhysicalSource::create("stream2", "MV", materializeViewSourceType);
    materializeViewSourceType->setId(viewId);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource2 = PhysicalSource::create("stream", "test_stream", materializeViewSourceType);
    workerConfig1->addPhysicalSource(physicalSource1);
    workerConfig1->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");


    Query maintenance_query =
        Query::from("stream").sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(viewId));
    auto maintenance_queryPlan = maintenance_query.getQueryPlan();
    maintenance_queryPlan->setQueryId(1);
    auto maintenanceQueryId = queryService->addQueryRequest(maintenance_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: queryId" << maintenanceQueryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(maintenanceQueryId, queryCatalog));


    Query adhoc_query = Query::from("stream2").sink(PrintSinkDescriptor::create());
    auto adhoc_queryPlan = adhoc_query.getQueryPlan();
    adhoc_queryPlan->setQueryId(2);
    auto adhocQueryId = queryService->addQueryRequest(adhoc_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: queryId" << adhocQueryId);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(adhocQueryId, queryCatalog));

    // Stop Queries
    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Remove maintenance query");
    queryService->validateAndQueueStopRequest(maintenanceQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(maintenanceQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Remove ad hoc query");
    queryService->validateAndQueueStopRequest(adhocQueryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(adhocQueryId, queryCatalog));

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Test finished");
}