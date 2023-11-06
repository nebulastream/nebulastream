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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
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

template<typename Key = uint64_t>
struct __attribute__((__packed__)) KeyedWindowOutput {
    uint64_t start;
    uint64_t end;
    Key id;
    uint64_t value;

    bool operator==(KeyedWindowOutput<Key> const& rhs) const {
        return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
    }

    friend ostream& operator<<(ostream& os, const KeyedWindowOutput<Key>& output) {
        os << "start: " << output.start << " end: " << output.end << " id: " << output.id << " value: " << output.value;
        return os;
    }
};

struct __attribute__((__packed__)) NonKeyedWindowOutput {
    uint64_t start;
    uint64_t end;
    uint64_t value;

    bool operator==(NonKeyedWindowOutput const& rhs) const {
        return (start == rhs.start && end == rhs.end && value == rhs.value);
    }

    friend ostream& operator<<(ostream& os, const NonKeyedWindowOutput& output) {
        os << "start: " << output.start << " end: " << output.end << " value: " << output.value;
        return os;
    }
};

TEST_F(WindowDeploymentTest, testTumblingWindowEventTimeWithTimeUnit) {

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp"), Seconds()), Minutes(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig = CSVSourceType::create("window", "window1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::vector<KeyedWindowOutput<>> expectedOutput = {{0, 60000, 1, 9},
                                                       {0, 60000, 12, 1},
                                                       {0, 60000, 4, 1},
                                                       {0, 60000, 11, 5},
                                                       {0, 60000, 16, 2}};

    std::vector<KeyedWindowOutput<>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<>>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig = CSVSourceType::create("window", "window1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::vector<KeyedWindowOutput<>> expectedOutput = {{0, 10000, 1, 51},
                                                       {0, 10000, 4, 1},
                                                       {0, 10000, 11, 5},
                                                       {0, 10000, 12, 1},
                                                       {0, 10000, 16, 2},
                                                       {5000, 15000, 1, 95},
                                                       {10000, 20000, 1, 145},
                                                       {15000, 25000, 1, 126},
                                                       {20000, 30000, 1, 41}};

    std::vector<KeyedWindowOutput<>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<>>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeployDistributedTumblingWindowQueryEventTimeTimeUnit) {

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts"), Seconds()), Minutes(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig1 = CSVSourceType::create("window", "window1");
    sourceConfig1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig1->setGatheringInterval(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig1->setNumberOfBuffersToProduce(3);

    auto sourceConfig2 = CSVSourceType::create("window", "window2");
    sourceConfig2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig2->setGatheringInterval(0);
    sourceConfig2->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig2->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig1)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig2)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    std::vector<KeyedWindowOutput<>> expectedOutput = {{960000, 1020000, 1, 34}, {1980000, 2040000, 2, 56}};

    std::vector<KeyedWindowOutput<>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<>>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig = CSVSourceType::create("window", "window2");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::vector<NonKeyedWindowOutput> expectedOutput = {{1000, 2000, 3}, {2000, 3000, 6}, {3000, 4000, 9}};

    std::vector<NonKeyedWindowOutput> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<NonKeyedWindowOutput>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig = CSVSourceType::create("window", "window2");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    std::vector<NonKeyedWindowOutput> expectedOutput = {{0, 10000, 60},
                                                        {5000, 15000, 95},
                                                        {10000, 20000, 145},
                                                        {15000, 25000, 126},
                                                        {20000, 30000, 41}};

    std::vector<NonKeyedWindowOutput> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<NonKeyedWindowOutput>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();

    auto sourceConfig = CSVSourceType::create("windowSource", "test_stream");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(6);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto workerConfig = WorkerConfiguration::create();
    workerConfig->physicalSourceTypes.add(sourceConfig);
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    workerConfig->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    NES_INFO("WindowDeploymentTest: Start coordinator");
    auto crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("windowSource", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    auto wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    auto queryService = crd->getQueryService();
    auto queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    auto query = Query::from("windowSource")
                     .window(TumblingWindow::of(IngestionTime(), Seconds(1)))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    auto globalQueryPlan = crd->getGlobalQueryPlan();
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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value1")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({1.2, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    std::vector<KeyedWindowOutput<double>> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}, {2000, 3000, 1.7, 5}};
    std::vector<KeyedWindowOutput<double>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<double>>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatKey) {
    struct __attribute__((__packed__)) Car2 {
        float key;
        uint64_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFloat())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car2), carSchema->getSchemaSizeInBytes());
    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value1")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car2>({1.2, 2, 1000}, 2);
    testHarness.pushElement<Car2>({1.5, 4, 1500}, 2);
    testHarness.pushElement<Car2>({1.7, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    std::vector<KeyedWindowOutput<float>> expectedOutput = {{1000, 2000, 1.2, 2}, {1000, 2000, 1.5, 4}, {2000, 3000, 1.7, 5}};
    std::vector<KeyedWindowOutput<float>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<float>>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief TODO support bool key for aggregations #4151
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithBoolKey) {
    struct __attribute__((__packed__)) Car {
        bool key;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createBoolean())
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value2")))
                                       .project(Attribute("value2"));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.pushElement<Car>({true, 2, 1000}, 2);
    testHarness.pushElement<Car>({false, 4, 1500}, 2);
    testHarness.pushElement<Car>({true, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{2}, {4}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief TODO inplace chars are not implemented in Nautilus #2739
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWitCharKey) {
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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value2")))
                                       .project(Attribute("value2"));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
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
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief TODO inplace chars are not implemented in Nautilus
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithFixedChar) {
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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
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
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Avg(Attribute("value1")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 3}, {2000, 3000, 1, 5}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Max(Attribute("value")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 99}, {2000, 3000, 1, 20}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Max(Attribute("value")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
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

    std::vector<Output> expectedOutput = {{1000, 2000, 1, -15}, {2000, 3000, 1, -20}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                                       .byKey(Attribute("id"))
                                       .apply(Max(Attribute("value")));

    auto sourceConfig = CSVSourceType::create("car", "car1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setSkipHeader(false);

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
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

    std::vector<KeyedWindowOutput<>> expectedOutput = {{0, 10000, 1, 9},
                                                       {10000, 20000, 1, 19},
                                                       {0, 10000, 4, 1},
                                                       {0, 10000, 11, 3},
                                                       {0, 10000, 12, 1},
                                                       {0, 10000, 16, 2},
                                                       {20000, 30000, 1, 21}};
    std::vector<KeyedWindowOutput<>> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<KeyedWindowOutput<>>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Min(Attribute("value")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 20}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Count());
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2}, {2000, 3000, 1, 1}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test if the Median aggregation can be deployed
 * TODO enable if median is implemented #4096
*/
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithMedianAggregation) {
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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Median(Attribute("value")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

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

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Count()->as(Attribute("Frequency")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2}, {2000, 3000, 1, 1}};
    std::vector<Output> actualOutput =
        testHarness.runQuery(expectedOutput.size(), "BottomUp").getOutput<Output>();

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
