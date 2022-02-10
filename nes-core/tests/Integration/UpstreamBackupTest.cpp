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

#include <API/Schema.hpp>
#include <Components/NesCoordinator.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Components/NesWorker.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Services/QueryService.hpp>
#include <Sources/DataSource.hpp>
#include <Util/GatheringMode.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <gmock/gmock.h>
#include <Sinks/Mediums/SinkMedium.hpp>
using namespace std;

namespace NES {

using namespace Configurations;

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class UpstreamBackupTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("UpstreamBackupTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpstreamBackupTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down UpstreamBackupTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

// testing w/ original running routine
class MockNesCoordinator : public NesCoordinator {
  public:
    MockNesCoordinator(CoordinatorConfigurationPtr coordinatorConfig)
        : NesCoordinator(coordinatorConfig){
            // nop
        };
    MOCK_METHOD(bool, propagatePunctuation, (uint64_t timestamp, uint64_t queryId));

    uint64_t startCoordinator(bool blocking) { return NesCoordinator::startCoordinator(blocking); }
    QueryServicePtr getQueryService() { return NesCoordinator::getQueryService(); }
    QueryCatalogPtr getQueryCatalog() { return NesCoordinator::getQueryCatalog(); }
    SourceCatalogPtr getStreamCatalog() { return NesCoordinator::getStreamCatalog(); }
    GlobalQueryPlanPtr getGlobalQueryPlan() { return NesCoordinator::getGlobalQueryPlan(); }
    StreamCatalogServicePtr getStreamCatalogService() const { return NesCoordinator::getStreamCatalogService(); }
};

/*
 * @brief test message passing between sink-coordinator-sources
 */
TEST_F(UpstreamBackupTest, testMessagePassingSinkCoordinatorSources) {
    //Setup Coordinator
    std::string window = R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))
                                            ->addField(createField("timestamp", UINT64));)";
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("UpstreamBackupTest: Start coordinator");
    MockNesCoordinator crd(crdConf);
    crd.getStreamCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd.startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("UpstreamBackupTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(rpcPort);
    CSVSourceTypePtr srcConf = CSVSourceType::create();
    srcConf->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(10);
    srcConf->setSourceFrequency(10);
    srcConf->setNumberOfBuffersToProduce(10);
    auto windowStream = PhysicalSource::create("window", "test_stream", srcConf);
    workerConfig1->addPhysicalSource(windowStream);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");


    QueryServicePtr queryService = crd.getQueryService();
    QueryCatalogPtr queryCatalog = crd.getQueryCatalog();

    std::string outputFilePath = "testUpstreamBackupTest.out";
    remove(outputFilePath.c_str());


    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");


    //create sink
//    GlobalQueryPlanPtr globalQueryPlan = crd.getGlobalQueryPlan();
//    auto vector = queryCatalog->getQueryCatalogEntry(queryId);
//    auto logicalSinkOperators = queryCatalog->getQueryCatalogEntry(queryId)->getExecutedQueryPlan()->getSinkOperators();
    auto plans = crd.getNodeEngine()->getDeployedQEPs();

    auto sinks = plans[0]->getSinks();
    for (auto& sink : sinks) {
            sink->propagateEpoch(1644426604);
        //should end calasdl propagate
        //DataSinkPtr fileOutputSink =
            //ConvertLogicalToPhysicalSink::createDataSink(sink->getId(), sink->getSinkDescriptor() , crd.getStreamCatalog()->getSchemaForLogicalStream("window"), wrk1->getNodeEngine(), globalQueryPlan->getSharedQueryPlan(queryId), 1);
        //fileOutputSink->propagateEpoch(1644426604);
    }
    EXPECT_CALL(crd, propagatePunctuation(1644426604, queryId))
                                    .Times(1)
        .WillOnce(testing::Return(true));
}
}