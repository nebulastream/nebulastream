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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <BaseIntegrationTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

static uint64_t workerThreads = 4;

/**
 * @brief NOTE ALL TESTS IN THIS FILE ARE DISABLED and must migrated to the testHarness
 */
class DistributedWindowDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributedWindowDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedWindowDeploymentTest test class.");
    }
};

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest,
       DISABLED_testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnDifferentNodes) {
    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder()).enableNautilus();

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerToCoordinator()
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 5UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 68}, {2000, 3000, 2, 112}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedTumblingWindowQueryEventTimeWithMergeAndComputeOnSameNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    //register logical source qnv
    auto window = Schema::create()
                      ->addField("id", BasicType::UINT64)
                      ->addField("value", BasicType::UINT64)
                      ->addField("ts", BasicType::UINT64);
    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig2->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig3->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("DistributedWindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType3->setGatheringInterval(0);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(3);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig4->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 5");
    WorkerConfigurationPtr workerConfig5 = WorkerConfiguration::create();
    workerConfig5->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType4->setGatheringInterval(0);
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(3);
    auto physicalSource4 = PhysicalSource::create("window", "test_stream", csvSourceType4);
    workerConfig5->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig5));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("DistributedWindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    "BottomUp",
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,68\n"
                             "2000,3000,2,112\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    remove(outputFilePath.c_str());
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testMultipleAggregationFunctionsOnMultipleWorkers) {
    // This test creates a window operation with two aggregation functions (sum and count).
    // The old window strategy produces the wrong result (0.0000) for any aggregation other than the first.
    // Using the THREAD_LOCAL window strategy produces the correct result.
    // However, at the moment, distributed windowing is broken.
    // If we disable distributed windowing, then all the data is sent to the internal worker of the coordinator.
    // To compute the result of both aggregation function, we have to use THREAD_LOCAL windowing on the internal worker of the coordinator.
    // The test therefore checks if we can set THREAD_LOCAL windowing for the internal worker.

    // given: Set up both workers to use ThreadLocal aggregation
    auto workerConfiguration1 = WorkerConfiguration::create();
    workerConfiguration1->queryCompiler.windowingStrategy =
        QueryCompilation::WindowingStrategy::THREAD_LOCAL;
    auto workerConfiguration2 = WorkerConfiguration::create();
    workerConfiguration2->queryCompiler.windowingStrategy =
        QueryCompilation::WindowingStrategy::THREAD_LOCAL;
    // given: Set up the input schema: a timestamp, a key, a value
    struct InputData {
        uint64_t timestamp;
        uint32_t key;
        uint32_t value;
    };
    auto inputSchema = Schema::create()
                           ->addField("timestamp", DataTypeFactory::createUInt64())
                           ->addField("key", DataTypeFactory::createUInt32())
                           ->addField("value", DataTypeFactory::createUInt32());
    ASSERT_EQ(sizeof(InputData), inputSchema->getSchemaSizeInBytes());
    // given: A window query with two aggregation functions
    auto query = Query::from("input")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .byKey(Attribute("key"))
                     .apply(Sum(Attribute("value")), Count());
    // given: Set up the test harness with two workers
    auto testHarness = TestHarness{query, *restPort, *rpcCoordinatorPort, getTestResourceFolder()}
                           .addLogicalSource("input", inputSchema)
                           .attachWorkerWithMemorySourceToWorkerWithId("input", 1, workerConfiguration1)
                           .attachWorkerWithMemorySourceToWorkerWithId("input", 1, workerConfiguration2);
    // given: Set up the input data on first worker.
    // Groups 1, 2 with 2 values each, value sums are 4, 6.
    testHarness.pushElement<InputData>({1000U, 1U, 1U}, 2);
    testHarness.pushElement<InputData>({1100U, 2U, 2U}, 2);
    testHarness.pushElement<InputData>({1200U, 1U, 3U}, 2);
    testHarness.pushElement<InputData>({1300U, 2U, 4U}, 2);
    // given: Set up the input data on second worker.
    // Groups 1, 3 with 2 values each, value sums are 12, 14..
    testHarness.pushElement<InputData>({1000U, 1U, 5U}, 3);
    testHarness.pushElement<InputData>({1100U, 3U, 6U}, 3);
    testHarness.pushElement<InputData>({1200U, 1U, 7U}, 3);
    testHarness.pushElement<InputData>({1300U, 3U, 8U}, 3);
    // given: The internal coordinator also uses THREAD_LOCAL aggregation and distributed window is disabled.
    testHarness.validate().setupTopology([](CoordinatorConfigurationPtr coordinatorConfiguration) {
        coordinatorConfiguration->worker.queryCompiler.windowingStrategy =
            QueryCompilation::WindowingStrategy::THREAD_LOCAL;
        coordinatorConfiguration->optimizer.distributedWindowChildThreshold = 1000;
        coordinatorConfiguration->optimizer.distributedWindowCombinerThreshold = 1000;
    });
    // then: Output should contain 3 groups with sums 16, 6, 14 and counts 4, 2, 2.
    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t sum;
        uint64_t count;
        bool operator==(const Output& rhs) const {
            return start == rhs.start && end == rhs.end && key == rhs.key && sum == rhs.sum && count == rhs.count;
        }
    };
    auto expected = std::vector<Output>{{1000, 2000, 1, 16, 4}, {1000, 2000, 2, 6, 2}, {1000, 2000, 3, 14, 2}};
    auto actual = testHarness.getOutput<Output>(expected.size(), "BottomUp", "NONE", "IN_MEMORY");
    for (auto i : {0, 1, 2}) {
        NES_DEBUG("{} = ({},{},{},{},{})", i, actual[i].start, actual[i].end, actual[i].key, actual[i].sum, actual[i].count);
    }
    EXPECT_EQ(expected.size(), actual.size());
    EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected));
};

/**
 * @brief test central tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    auto workerConfig1 = WorkerConfiguration::create();
    auto workerConfig2 = WorkerConfiguration::create();
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto windowSource = PhysicalSource::create("windowSource", "test_stream", sourceConfig);
    workerConfig1->physicalSources.add(windowSource);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;

    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("windowSource", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    workerConfig2->coordinatorPort = port;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    auto query = Query::from("windowSource")
                     .window(TumblingWindow::of(IngestionTime(),

                                                Seconds(1)))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    "BottomUp",
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief tests window with ingestion time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;

    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(5);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(5);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    auto query = Query::from("window")
                     .window(TumblingWindow::of(IngestionTime(), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    "BottomUp",
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

// TODO this test needs rethinking as we just check if the file sink creates a file, there is no ingestion time windowing currently
TEST_F(DistributedWindowDeploymentTest, DISABLED_testCentralWindowIngestionTimeIngestionTime) {

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowSource = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->physicalSources.add(windowSource);
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    //creating schema
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    workerConfig->coordinatorPort = port;
    workerConfig->queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    auto query = Query::from("window")
                     .window(TumblingWindow::of(IngestionTime(), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    "BottomUp",
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central sliding window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedNonKeySlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .apply(Sum(Attribute("value")));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNautilus()
                                  .addLogicalSource("window", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
                                  .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 120}, {5000, 15000, 190}, {10000, 20000, 290}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedNonKeyTumblingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder()).enableNautilus();

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 6}, {2000, 3000, 12}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed sliding window and event time, for now disabled see issue #3324
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNautilus()
                                  .addLogicalSource("window", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
                                  .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 1, 102},
                                          {5000, 15000, 1, 190},
                                          {10000, 20000, 1, 290},
                                          {0, 10000, 4, 2},
                                          {0, 10000, 11, 10},
                                          {0, 10000, 12, 2},
                                          {0, 10000, 16, 4}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testYSBWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string input =
        R"(Schema::create()->addField("ysb$user_id", BasicType::UINT64)->addField("ysb$page_id", BasicType::UINT64)->addField("ysb$campaign_id", BasicType::UINT64)->addField("ysb$ad_type", BasicType::UINT64)->addField("ysb$event_type", BasicType::UINT64)->addField("ysb$current_ms", BasicType::UINT64)->addField("ysb$ip", BasicType::UINT64)->addField("ysb$d1", BasicType::UINT64)->addField("ysb$d2", BasicType::UINT64)->addField("ysb$d3", BasicType::UINT32)->addField("ysb$d4", BasicType::UINT16);)";
    ASSERT_TRUE(crd->getSourceCatalogService()->registerLogicalSource("ysb", input));
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->numWorkerThreads = workerThreads;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    auto ysbSchema = Schema::create()
                         ->addField("ysb$user_id", BasicType::UINT64)
                         ->addField("ysb$page_id", BasicType::UINT64)
                         ->addField("ysb$campaign_id", BasicType::UINT64)
                         ->addField("ysb$ad_type", BasicType::UINT64)
                         ->addField("ysb$event_type", BasicType::UINT64)
                         ->addField("ysb$current_ms", BasicType::UINT64)
                         ->addField("ysb$ip", BasicType::UINT64)
                         ->addField("ysb$d1", BasicType::UINT64)
                         ->addField("ysb$d2", BasicType::UINT64)
                         ->addField("ysb$d3", BasicType::UINT32)
                         ->addField("ysb$d4", BasicType::UINT16);

    auto func = [](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct __attribute__((packed)) YsbRecord {
            YsbRecord() = default;
            YsbRecord(uint64_t userId,
                      uint64_t pageId,
                      uint64_t campaignId,
                      uint64_t adType,
                      uint64_t eventType,
                      uint64_t currentMs,
                      uint64_t ip)
                : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType),
                  currentMs(currentMs), ip(ip) {}

            uint64_t userId{};
            uint64_t pageId{};
            uint64_t campaignId{};
            uint64_t adType{};
            uint64_t eventType{};
            uint64_t currentMs{};
            uint64_t ip{};

            // placeholder to reach 78 bytes
            uint64_t dummy1{0};
            uint64_t dummy2{0};
            uint32_t dummy3{0};
            uint16_t dummy4{0};

            YsbRecord(const YsbRecord& rhs) {
                userId = rhs.userId;
                pageId = rhs.pageId;
                campaignId = rhs.campaignId;
                adType = rhs.adType;
                eventType = rhs.eventType;
                currentMs = rhs.currentMs;
                ip = rhs.ip;
            }
            [[nodiscard]] std::string toString() const {
                return "YsbRecord(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId)
                    + ", campaignId=" + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType="
                    + std::to_string(eventType) + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
            }
        };

        auto* records = buffer.getBuffer<YsbRecord>();
        auto ts =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();

        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            //                    memset(&records, 0, sizeof(YsbRecord));
            records[u].userId = 1;
            records[u].pageId = 0;
            records[u].adType = 0;
            records[u].campaignId = rand() % 10000;
            records[u].eventType = u % 3;
            records[u].currentMs = ts;
            records[u].ip = 0x01020304;
        }
        NES_WARNING("Lambda last entry is={}", records[numberOfTuplesToProduce - 1].toString());
    };

    auto lambdaSourceType = LambdaSourceType::create(func, 10, 100, GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("ysb", "YSB_phy", lambdaSourceType);
    workerConfig->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "ysb.out";
    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"ysb\").window(TumblingWindow::of(EventTime(Attribute(\"current_ms\")), "
                   "Milliseconds(10))).byKey(Attribute(\"campaign_id\")).apply(Sum(Attribute(\"user_id\"))).sink("
                   "FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(1, outputFilePath));

    // queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    //here we can only check if the file exists and has some content

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

//TODO test needs to be fixed, since it fails randomly. Covered in issue #2258
TEST_F(DistributedWindowDeploymentTest, DISABLED_testCentralWindowEventTime) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(3);
    auto physicalSource = PhysicalSource::create("window", "test_stream", csvSourceType);
    workerConfig1->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,1\n"
                             "2000,3000,1,2\n"
                             "1000,2000,4,1\n"
                             "2000,3000,11,2\n"
                             "1000,2000,12,1\n"
                             "2000,3000,16,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");
    // queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

//TODO test needs to be fixed, since it fails randomly. Covered in issue #2234
TEST_F(DistributedWindowDeploymentTest, DISABLED_testCentralWindowEventTimeWithTimeUnit) {

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->numWorkerThreads = workerThreads;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(3);
    auto physicalSource = PhysicalSource::create("window", "test_stream", csvSourceType);
    workerConfig->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\"), Seconds()), "
                   "Minutes(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "960000,1020000,1,1\n"
                             "1980000,2040000,1,2\n"
                             "960000,1020000,4,1\n"
                             "1980000,2040000,11,2\n"
                             "960000,1020000,12,1\n"
                             "1980000,2040000,16,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("DistributedWindowDeploymentTest: Submit query");

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "1000,2000,1,34\n"
        "2000,3000,2,56\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");
    // queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTimeTimeUnit) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream1", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream2", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("DistributedWindowDeploymentTest: Submit query");

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\"), Seconds()), "
                   "Minutes(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "960000,1020000,1,34\n"
        "1980000,2040000,2,56\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed sliding window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    workerConfig2->numWorkerThreads = workerThreads;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    ifstream my_file(getTestResourceFolder() / "outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(getTestResourceFolder() / "outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,10000,1,102\n"
        "5000,15000,1,190\n"
        "10000,20000,1,290\n"
        "0,10000,4,2\n"
        "0,10000,11,10\n"
        "0,10000,12,2\n"
        "0,10000,16,4\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testCentralNonKeyTumblingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("windowStream", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("windowStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "windowStream$start:INTEGER(64 bits),windowStream$end:INTEGER(64 bits),windowStream$value:INTEGER(64 bits)\n"
        "1000,2000,3\n"
        "2000,3000,6\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)))"
                   ".apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    ifstream my_file(getTestResourceFolder() / "outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(getTestResourceFolder() / "outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
                             "0,10000,60\n"
                             "5000,15000,95\n"
                             "10000,20000,145\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");
    // queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testDistributedNonKeyTumblingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("windowStream", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("windowStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("windowStream", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "windowStream$start:INTEGER(64 bits),windowStream$end:INTEGER(64 bits),windowStream$value:INTEGER(64 bits)\n"
        "1000,2000,6\n"
        "2000,3000,12\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");
    //  queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testDistributedNonKeySlidingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    //size slide
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)))"
                   ".apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
                             "0,10000,120\n"
                             "5000,15000,190\n"
                             "10000,20000,290\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

TEST_F(DistributedWindowDeploymentTest, testCentralWindowIngestionTimeIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numWorkerThreads = workerThreads;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(5);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

TEST_F(DistributedWindowDeploymentTest, testDistributedWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numWorkerThreads = workerThreads;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(5);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("windowStream", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(6);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("windowStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, testDistributedNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("windowStream", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("windowStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("windowStream", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest,
       testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnDifferentNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->numWorkerThreads = workerThreads;
    workerConfig3->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType3->setGatheringInterval(0);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(3);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);
    EXPECT_TRUE(retStart3);
    NES_INFO("DistributedWindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    workerConfig4->numWorkerThreads = workerThreads;
    workerConfig4->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType4->setGatheringInterval(0);
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(3);
    auto physicalSource4 = PhysicalSource::create("window", "test_stream", csvSourceType4);
    workerConfig4->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 5");
    WorkerConfigurationPtr workerConfig5 = WorkerConfiguration::create();
    workerConfig5->coordinatorPort = port;
    workerConfig5->numWorkerThreads = workerThreads;
    workerConfig5->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType5 = CSVSourceType::create();
    csvSourceType5->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType5->setGatheringInterval(0);
    csvSourceType5->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType5->setNumberOfBuffersToProduce(3);
    auto physicalSource5 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig5->physicalSources.add(physicalSource5);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig5));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("DistributedWindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "1000,2000,1,68\n"
        "2000,3000,2,112\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest,
       testDeployDistributedWithMergingTumblingWindowQueryEventTimeWithMergeAndComputeOnSameNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numWorkerThreads = workerThreads;
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numWorkerThreads = workerThreads;
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->numWorkerThreads = workerThreads;
    workerConfig3->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType3->setGatheringInterval(0);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(3);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);
    EXPECT_TRUE(retStart3);
    NES_INFO("DistributedWindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    workerConfig4->numWorkerThreads = workerThreads;
    workerConfig4->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType4->setGatheringInterval(0);
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(3);
    auto physicalSource4 = PhysicalSource::create("window", "test_stream", csvSourceType4);
    workerConfig4->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("DistributedWindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("DistributedWindowDeploymentTest: Start worker 5");
    WorkerConfigurationPtr workerConfig5 = WorkerConfiguration::create();
    workerConfig5->coordinatorPort = port;
    workerConfig5->numWorkerThreads = workerThreads;
    workerConfig5->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType5 = CSVSourceType::create();
    csvSourceType5->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType5->setGatheringInterval(0);
    csvSourceType5->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType5->setNumberOfBuffersToProduce(3);
    auto physicalSource5 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig5->physicalSources.add(physicalSource5);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig5));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("DistributedWindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("DistributedWindowDeploymentTest: Submit query");

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "1000,2000,1,68\n"
        "2000,3000,2,112\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("DistributedWindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}

/*
 * @brief Test if the avg aggregation can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithAvgAggregation) {
    struct Car {
        uint64_t key;
        double value1;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value1", DataTypeFactory::createDouble())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Avg(Attribute("value1"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1, 2, 2, 1000}, 2)
                                  .pushElement<Car>({1, 4, 4, 1500}, 2)
                                  .pushElement<Car>({1, 5, 5, 2000}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        double value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 3}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithMaxAggregation) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Max(Attribute("value"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1, 15, 1000}, 2)
                                  .pushElement<Car>({1, 99, 1500}, 2)
                                  .pushElement<Car>({1, 20, 2000}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 99}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation of negative values can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithNegativeValues) {
    struct Car {
        int32_t key;
        int32_t value;
        int64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createInt32())
                         ->addField("value", DataTypeFactory::createInt32())
                         ->addField("timestamp", DataTypeFactory::createInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1, -15, 1000}, 2)
                                  .pushElement<Car>({1, -99, 1500}, 2)
                                  .pushElement<Car>({1, -20, 2000}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        int64_t start;
        int64_t end;
        int32_t key;
        int32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, -15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the max aggregation with uint64 data type can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithUint64AggregatedField) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("id", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(28);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(false);

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("id")).apply(Max(Attribute("value"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("car", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput =
        {{0, 10000, 1, 9}, {10000, 20000, 1, 19}, {0, 10000, 4, 1}, {0, 10000, 11, 3}, {0, 10000, 12, 1}, {0, 10000, 16, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the min aggregation can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithMinAggregation) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1, 15, 1000}, 2)
                                  .pushElement<Car>({1, 99, 1500}, 2)
                                  .pushElement<Car>({1, 20, 2000}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the min aggregation with float data type can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithFloatMinAggregation) {
    struct Car {
        uint32_t key;
        float value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createFloat())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Min(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1, 15.0, 1000}, 2)
                                  .pushElement<Car>({1, 99.0, 1500}, 2)
                                  .pushElement<Car>({1, 20.0, 2000}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        float value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the Count aggregation can be deployed
 */
TEST_F(DistributedWindowDeploymentTest, testDeploymentOfWindowWithCountAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Count()))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2)
                                  .pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2)
                                  .pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t count;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && count == rhs.count && start == rhs.start && end == rhs.end);
        }
    };
    auto outputsize = sizeof(Output);
    NES_DEBUG("{}", outputsize);
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(DistributedWindowDeploymentTest, DISABLED_testLongWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 1000;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 1000;

    NES_INFO("DistributedWindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string input =
        R"(Schema::create()->addField("key", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    ASSERT_TRUE(crd->getSourceCatalogService()->registerLogicalSource("schema", input));
    NES_DEBUG("DistributedWindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("DistributedWindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->numWorkerThreads = workerThreads;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    std::atomic<int> recordCounter = 0;
    auto func = [&recordCounter](Runtime::TupleBuffer& buffer, uint64_t) {
        struct __attribute__((packed)) Record {
            Record() = default;
            Record(uint64_t key, uint64_t value, uint64_t ts) : key(key), value(value), ts(ts) {}

            uint64_t key;
            uint64_t value;
            uint64_t ts;

            Record(const Record& rhs) {
                key = rhs.key;
                value = rhs.value;
                ts = rhs.ts;
            }
            std::string toString() const {
                return "Record(key=" + std::to_string(key) + ", value=" + std::to_string(value) + ", ts=" + std::to_string(ts);
            }
        };

        auto records = buffer.getBuffer<Record>();
        auto ts = (recordCounter) / 100U;
        for (auto u = 0u; u < 100U; ++u) {
            //                    memset(&records, 0, sizeof(YsbRecord));
            records[u].key = recordCounter % 10;
            records[u].value = 1;
            records[u].ts = ts;
            recordCounter++;
        }
        NES_WARNING("Lambda last entry is={}", records[100 - 1].toString());
        buffer.setNumberOfTuples(100U);
        return;
    };

    auto lambdaSourceType = LambdaSourceType::create(func, 100, 0, GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("schema", "Source_phy", lambdaSourceType);
    workerConfig->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DistributedWindowDeploymentTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "source.out";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    auto schema = Schema::create()
                      ->addField("key", BasicType::UINT64)
                      ->addField("value", BasicType::UINT64)
                      ->addField("ts", BasicType::UINT64);

    NES_INFO("DistributedWindowDeploymentTest: Submit query");
    string query = "Query::from(\"schema\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Milliseconds(10))).byKey(Attribute(\"key\")).apply(Sum(Attribute(\"value\"))).sink("
                   "FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(1U, outputFilePath));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    //here we can only check if the file exists and has some content

    NES_INFO("DistributedWindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("DistributedWindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("DistributedWindowDeploymentTest: Test finished");
}
}// namespace NES
