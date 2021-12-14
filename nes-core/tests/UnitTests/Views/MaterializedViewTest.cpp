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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Configurations/Sources/MaterializedViewSourceConfig.hpp>
#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Views/MaterializedView.hpp>
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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);

    NES_INFO("MaterializedViewTupleViewSinkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTupleViewSinkTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    // register logical stream
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

    // register physical stream
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    crd->getNesWorker()->registerPhysicalStream(streamConf);

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("MaterializedViewSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);

    NES_INFO("MaterializedViewTupleBufferSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTupleBufferSourceTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream
    std::string stream =
            R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    crd->getNesWorker()->registerLogicalStream("stream", testSchemaFileName);

    // materialized view physical stream
    size_t viewId = 1;
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setPhysicalStreamName("MV");
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setLogicalStreamName("stream");
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setSourceFrequency(1);
    srcConf->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setId(viewId);
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    crd->getNesWorker()->registerPhysicalStream(streamConf);

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("MaterializedViewSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string stream =
            R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();

    // Init Maintenance Query
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

    size_t viewId = 1;
    Query maintenance_query = Query::from("stream")
            .sink(NES::Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(viewId));
    auto maintenance_queryPlan = maintenance_query.getQueryPlan();
    maintenance_queryPlan->setQueryId(1);
    auto maintenanceQueryId = queryService->addQueryRequest(maintenance_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: queryId" << maintenanceQueryId);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(maintenanceQueryId, queryCatalog));

    // Init Ad Hoc Query
    crd->getNesWorker()->registerLogicalStream("stream2", testSchemaFileName);

    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setPhysicalStreamName("MV");
    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setLogicalStreamName("stream2");
    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setNumberOfTuplesToProducePerBuffer(1);
    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setNumberOfBuffersToProduce(10000);
    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setSourceFrequency(1);
    srcConf2->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->setId(viewId);
    PhysicalStreamConfigPtr streamConf2 = PhysicalStreamConfig::create(srcConf2);
    crd->getNesWorker()->registerPhysicalStream(streamConf2);

    Query adhoc_query = Query::from("stream2").sink(PrintSinkDescriptor::create());
    auto adhoc_queryPlan = adhoc_query.getQueryPlan();
    adhoc_queryPlan->setQueryId(2);
    auto adhocQueryId = queryService->addQueryRequest(adhoc_queryPlan, "BottomUp");

    NES_INFO("MaterializedViewTupleBufferSinkAndSourceTest: queryId" << adhocQueryId);
    globalQueryPlan = crd->getGlobalQueryPlan();
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