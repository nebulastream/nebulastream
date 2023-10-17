/*
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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class AssignWatermarkTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AssignWatermarkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AssignWatermarkTest test class.");
    }

    std::string ipAddress = "127.0.0.1";
};

/*
 * @brief test event time watermark for central tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralTumblingWindow) {
    //Setup Coordinator
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("window", "x1");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(4);
    // register physical source with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    workerConfig->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testWatermarkAssignmentCentralTumblingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "1000,2000,1,12\n"
        "2000,3000,1,24\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, DISABLED_testWatermarkAssignmentDistributedTumblingWindow) {
    //Setup Coordinator
    auto window = Schema::create()
                             ->addField(createField("value", BasicType::UINT64))
                             ->addField(createField("id", BasicType::UINT64))
                             ->addField(createField("timestamp", BasicType::UINT64));
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold.setValue(0);
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold.setValue(0);
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    //Add Source To Worker
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("window", "test_stream");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(4);
    workerConfig1->physicalSourceTypes.add(csvSourceType);
    //Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");
    //Setup Worker 2
    NES_INFO("AssignWatermarkTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");
    //Setup Worker 3
    NES_INFO("AssignWatermarkTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testWatermarkAssignmentDistributedTumblingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("AssignWatermarkTest: Submit query");

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "1000,2000,1,36\n"
        "2000,3000,1,72\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for central sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralSlidingWindow) {
    //Setup Coordinator
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold.setValue(1000);
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold.setValue(1000);

    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    //Add Source to Worker
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("window", "test_stream");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(4);
    workerConfig->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testWatermarkAssignmentCentralSlidingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "2500,3500,1,10\n"
        "2000,3000,1,24\n"
        "1500,2500,1,30\n"
        "1000,2000,1,12\n"
        "500,1500,1,6\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, DISABLED_testWatermarkAssignmentDistributedSlidingWindow) {
    //Setup Coordinator
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold.setValue(0);
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold.setValue(0);

    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    //Add Source to Worker
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("window", "test_stream");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(4);
    workerConfig1->physicalSourceTypes.add(csvSourceType);
    //Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");

    //Setup Worker 2
    NES_INFO("AssignWatermarkTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testWatermarkAssignmentDistributedSlidingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("AssignWatermarkTest: Submit query");

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50),Milliseconds()))"
        ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500)))"
        ".byKey(Attribute(\"id\"))"
        ".apply(Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "500,1500,1,18\n"
        "1000,2000,1,36\n"
        "1500,2500,1,90\n"
        "2000,3000,1,72\n"
        "2500,3500,1,30\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("AssignWatermarkTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

}// namespace NES
