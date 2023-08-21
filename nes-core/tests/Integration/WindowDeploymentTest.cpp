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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
using namespace std;

namespace NES {

using namespace Configurations;

class WindowDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WindowDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup WindowDeploymentTest test class.");
    }
};

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testYSBWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string input =
        R"(Schema::create()->addField("ysb$user_id", BasicType::UINT64)->addField("ysb$page_id", BasicType::UINT64)->addField("ysb$campaign_id", BasicType::UINT64)->addField("ysb$ad_type", BasicType::UINT64)->addField("ysb$event_type", BasicType::UINT64)->addField("ysb$current_ms", BasicType::UINT64)->addField("ysb$ip", BasicType::UINT64)->addField("ysb$d1", BasicType::UINT64)->addField("ysb$d2", BasicType::UINT64)->addField("ysb$d3", BasicType::UINT32)->addField("ysb$d4", BasicType::UINT16);)";
    ASSERT_TRUE(crd->getSourceCatalogService()->registerLogicalSource("ysb", input));
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->coordinatorPort = port;

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

    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
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
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "ysb.out";

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"ysb\").window(TumblingWindow::of(EventTime(Attribute(\"current_ms\")), "
                   "Milliseconds(10))).byKey(Attribute(\"campaign_id\")).apply(Sum(Attribute(\"user_id\"))).sink("
                   "FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(1, outputFilePath));

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    //here we can only check if the file exists and has some content

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testCentralWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 1},
                                          {2000, 3000, 1, 2},
                                          {1000, 2000, 4, 1},
                                          {2000, 3000, 11, 2},
                                          {1000, 2000, 12, 1},
                                          {2000, 3000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testCentralWindowEventTimeWithTimeUnit) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp"), Seconds()), Minutes(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{960000, 1020000, 1, 1},
                                          {1980000, 2040000, 1, 2},
                                          {960000, 1020000, 4, 1},
                                          {1980000, 2040000, 11, 2},
                                          {960000, 1020000, 12, 1},
                                          {1980000, 2040000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 1, 51},
                                          {5000, 15000, 1, 95},
                                          {10000, 20000, 1, 145},
                                          {0, 10000, 4, 1},
                                          {0, 10000, 11, 5},
                                          {0, 10000, 12, 1},
                                          {0, 10000, 16, 2}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeployDistributedTumblingWindowQueryEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->optimizer.distributedWindowChildThreshold = 0;
    coordinatorConfig->optimizer.distributedWindowCombinerThreshold = 0;

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    auto windowSource = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig1->physicalSources.add(windowSource);

    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    workerConfig2->physicalSources.add(windowSource);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,34\n"
                             "2000,3000,2,56\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeployDistributedTumblingWindowQueryEventTimeTimeUnit) {
    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts"), Seconds()), Minutes(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
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
        uint64_t id;
        uint64_t value;

        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
    std::vector<Output> expectedOutput = {{960000, 1020000, 1, 34}, {1980000, 2040000, 2, 56}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed sliding window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    testHarness.addLogicalSource("window", testSchema)
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
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1))).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 3}, {2000, 3000, 6}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    testHarness.addLogicalSource("window", testSchema)
        .attachWorkerWithCSVSourceToCoordinator("window", sourceConfig)
        .validate()
        .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };
    std::vector<Output> expectedOutput = {{0, 10000, 60}, {5000, 15000, 95}, {10000, 20000, 145}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDistributedNonKeyTumblingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
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
 * @brief test central sliding window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDistributedNonKeySlidingWindowEventTime) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(10),Seconds(5))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
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
    std::vector<Output> expectedOutput = {{0, 10000, 120}, {5000, 15000, 190}, {10000, 20000, 290}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

// TODO this test needs rethinking as we just check if the file sink creates a file, there is no ingestion time windowing currently
TEST_F(WindowDeploymentTest, DISABLED_testCentralWindowIngestionTimeIngestionTime) {

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(5);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowSource = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->physicalSources.add(windowSource);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    //creating schema
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief tests window with ingestion time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDistributedWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(5);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setGatheringInterval(5);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(6);
    sourceConfig->setNumberOfBuffersToProduce(3);

    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    auto windowSource = PhysicalSource::create("windowSource", "test_stream", sourceConfig);
    workerConfig->physicalSources.add(windowSource);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("windowSource", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowSource\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDistributedNonKeyTumblingWindowIngestionTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    auto workerConfig1 = WorkerConfiguration::create();
    auto workerConfig2 = WorkerConfiguration::create();
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto windowSource = PhysicalSource::create("windowSource", "test_stream", sourceConfig);
    workerConfig1->physicalSources.add(windowSource);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;

    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", BasicType::UINT64))->addField(createField("id", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("windowSource", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    workerConfig2->coordinatorPort = port;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowSource\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1))).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest,
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

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
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
TEST_F(WindowDeploymentTest, DISABLED_testDistributedTumblingWindowQueryEventTimeWithMergeAndComputeOnSameNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig2->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig3->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("WindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType3->setGatheringInterval(0);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(3);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig4->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 5");
    WorkerConfigurationPtr workerConfig5 = WorkerConfiguration::create();
    workerConfig5->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType4->setGatheringInterval(0);
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(3);
    auto physicalSource4 = PhysicalSource::create("window", "test_stream", csvSourceType4);
    workerConfig5->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig5));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("WindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1))).byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,68\n"
                             "2000,3000,2,112\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("WindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("WindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    remove(outputFilePath.c_str());
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test tumbling window with multiple aggregations
 */
TEST_F(WindowDeploymentTest, testMultipleWindowAggregation) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    //register logical source qnv
    std::string ktmSchema =
        R"(Schema::create()->addField(createField("Time",BasicType::UINT64))->
            addField(createField("Dist",BasicType::UINT64))->
            addField(createField("ABS_Front_Wheel_Press",BasicType::FLOAT64))->
            addField(createField("ABS_Rear_Wheel_Press",BasicType::FLOAT64))->
            addField(createField("ABS_Front_Wheel_Speed",BasicType::FLOAT64))->
            addField(createField("ABS_Rear_Wheel_Speed",BasicType::FLOAT64))->
            addField(createField("V_GPS",BasicType::FLOAT64))->
            addField(createField("MMDD",BasicType::FLOAT64))->
            addField(createField("HHMM",BasicType::FLOAT64))->
            addField(createField("LAS_Ax1",BasicType::FLOAT64))->
            addField(createField("LAS_Ay1",BasicType::FLOAT64))->
            addField(createField("LAS_Az_Vertical_Acc",BasicType::FLOAT64))->
            addField(createField("ABS_Lean_Angle",BasicType::FLOAT64))->
            addField(createField("ABS_Pitch_Info",BasicType::FLOAT64))->
            addField(createField("ECU_Gear_Position",BasicType::FLOAT64))->
            addField(createField("ECU_Accel_Position",BasicType::FLOAT64))->
            addField(createField("ECU_Engine_Rpm",BasicType::FLOAT64))->
            addField(createField("ECU_Water_Temperature",BasicType::FLOAT64))->
            addField(createField("ECU_Oil_Temp_Sensor_Data",BasicType::UINT64))->
            addField(createField("ECU_Side_StanD",BasicType::UINT64))->
            addField(createField("Longitude",BasicType::FLOAT64))->
            addField(createField("Latitude",BasicType::FLOAT64))->
            addField(createField("Altitude",BasicType::FLOAT64)))";

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("ktm", ktmSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_EQ(port, *rpcCoordinatorPort);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    QueryCompilerConfiguration queryCompilerConfiguration;
    queryCompilerConfiguration.windowingStrategy = QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
    workerConfig1->queryCompiler = queryCompilerConfiguration;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    // create source
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "ktm.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("ktm", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart2 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    ASSERT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "ktm-results.csv";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        R"(Query::from("ktm").window(TumblingWindow::of(EventTime(Attribute("Time")), Seconds(1)))
        .apply(Avg(Attribute("ABS_Lean_Angle"))->as(Attribute("avg_value_1")), Avg(Attribute("ABS_Pitch_Info"))->as(Attribute("avg_value_2")), Avg(Attribute("ABS_Front_Wheel_Speed"))->as(Attribute("avg_value_3")), Count()->as(Attribute("count_value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "ktm$start:INTEGER(64 bits),ktm$end:INTEGER(64 bits),ktm$avg_value_1:Float(64 bits),ktm$avg_value_2:"
                             "Float(64 bits),ktm$avg_value_3:Float(64 bits),ktm$count_value:INTEGER(64 bits)\n"
                             "1543620000000,1543620001000,14.400000,0.800000,0.500000,2\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    remove(outputFilePath.c_str());
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithDoubleKey) {
    struct Car {
        double key;
        uint64_t value1;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createDouble())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value1"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1.2, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        double key;
        uint64_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatKey) {
    struct Car {
        float key;
        uint32_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFloat())
                         ->addField("value1", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value1"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1.2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        float key;
        uint32_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithBoolKey) {
    struct Car {
        bool key;
        std::array<char, 3> value1;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createBoolean())
                         ->addField("value1", DataTypeFactory::createFixedChar(3))
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value2"))).project(Attribute("value2")))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::array<char, 3> charArrayValue = {'A', 'B', 'C'};
    testHarness.pushElement<Car>({true, charArrayValue, 2, 1000}, 2);
    testHarness.pushElement<Car>({false, charArrayValue, 4, 1500}, 2);
    testHarness.pushElement<Car>({true, charArrayValue, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{2}, {4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWitCharKey) {
    struct Car {
        char key;
        std::array<char, 3> value1;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createChar())
                         ->addField("value1", DataTypeFactory::createFixedChar(3))
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value2"))).project(Attribute("value2")))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::array<char, 3> charArrayValue = {'A', 'B', 'C'};
    testHarness.pushElement<Car>({'A', charArrayValue, 2, 1000}, 2);
    testHarness.pushElement<Car>({'B', charArrayValue, 4, 1500}, 2);
    testHarness.pushElement<Car>({'C', charArrayValue, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{2}, {4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFixedChar) {
    struct Car {
        NES::ExecutableTypes::Array<char, 4> key;
        uint32_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFixedChar(4))
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    NES::ExecutableTypes::Array<char, 4> keyOne = "aaa";
    NES::ExecutableTypes::Array<char, 4> keyTwo = "bbb";
    NES::ExecutableTypes::Array<char, 4> keyThree = "ccc";

    testHarness.pushElement<Car>({keyOne, 2, 1000}, 2);
    testHarness.pushElement<Car>({keyTwo, 4, 1500}, 2);
    testHarness.pushElement<Car>({keyThree, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        std::array<char, 4> key;
        uint32_t value1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value1 == rhs.value1 && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, keyOne, 2}, {1000, 2000, keyTwo, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the avg aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithAvgAggregation) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1, 5, 5, 2000}, 2);

    testHarness.validate().setupTopology();

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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregation) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15, 1000}, 2);
    testHarness.pushElement<Car>({1, 99, 1500}, 2);
    testHarness.pushElement<Car>({1, 20, 2000}, 2);

    testHarness.validate().setupTopology();

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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithNegativeValues) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, -15, 1000}, 2);
    testHarness.pushElement<Car>({1, -99, 1500}, 2);
    testHarness.pushElement<Car>({1, -20, 2000}, 2);

    testHarness.validate().setupTopology();

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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithUint64AggregatedField) {
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

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("id")).apply(Max(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder());

    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setSkipHeader(false);

    testHarness.addLogicalSource("car", carSchema)
        .attachWorkerWithCSVSourceToCoordinator("car", sourceConfig)
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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMinAggregation) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15, 1000}, 2);
    testHarness.pushElement<Car>({1, 99, 1500}, 2);
    testHarness.pushElement<Car>({1, 20, 2000}, 2);

    testHarness.validate().setupTopology();

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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatMinAggregation) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1, 15.0, 1000}, 2);
    testHarness.pushElement<Car>({1, 99.0, 1500}, 2);
    testHarness.pushElement<Car>({1, 20.0, 2000}, 2);

    testHarness.validate().setupTopology();

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
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithCountAggregation) {
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
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

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

/*
 * @brief Test if the Median aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMedianAggregation) {
    struct Car {
        uint64_t key;
        double value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt64())
                         ->addField("value", DataTypeFactory::createDouble())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Median(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 30ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 90ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 1800ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 60ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        double median;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && median == rhs.median && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {{1000ULL, 2000ULL, 1ULL, 30}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test aggregation with field rename
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFieldRename) {
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
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Count()->as(Attribute("Frequency"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);

    testHarness.validate().setupTopology();

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
 * for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testMultipleAggregationFunctionsOnMultipleWorkers) {
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
        QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
    auto workerConfiguration2 = WorkerConfiguration::create();
    workerConfiguration2->queryCompiler.windowingStrategy =
        QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
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
    auto query =
        R"(Query::from("input").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value")), Count()))";
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
            QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
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
}// namespace NES
