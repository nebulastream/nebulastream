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
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <chrono>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {
class MultiThreadedTest : public Testing::BaseIntegrationTest,
                          public Runtime::BufferRecycler,
                          public ::testing::WithParamInterface<uint64_t> {
  public:
    const uint64_t numTuplesPerBuffer = 1;
    static constexpr auto queryCompiler = QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    static constexpr auto dumpNone = QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultiThreadedTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultiThreadedTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseIntegrationTest::SetUp();

        // Creating the execution engine
        const uint64_t numberOfWorkerThreads = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpNone, numberOfWorkerThreads);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down MultiThreadedTest test case.");

        // Stopping the execution engine
        EXPECT_TRUE(executionEngine->stop());
        NES::Testing::BaseIntegrationTest::TearDown();
    }

    void recyclePooledBuffer(Runtime::detail::MemorySegment*) override {}

    void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override {}

    template<typename ResultRecord>
    std::vector<ResultRecord>& runQuery(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                        const uint64_t expectedNumberOfTuples,
                                        const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                        const Query& query) {

        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (auto [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::fillBufferFromCsv(fileNameInputBuffers, inputSchema, bufferManager, numTuplesPerBuffer));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

        // Emitting the input buffers
        auto dataSourceCnt = 0_u64;
        for (const auto& inputBuffers : allInputBuffers) {
            auto source = executionEngine->getDataSource(queryPlan, dataSourceCnt++);
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(queryPlan, Runtime::QueryTerminationType::Graceful));
        testSink->waitTillCompleted(expectedNumberOfTuples);

        // Checking for correctness
        return testSink->getResult();
    }
};

TEST_P(MultiThreadedTest, testFilterQuery) {
    struct ResultRecord {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
        bool operator==(const ResultRecord& rhs) const {
            return value == rhs.value && id == rhs.id && timestamp == rhs.timestamp;
        }
    };
    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto& outputSchema = inputSchema;

    const std::string fileNameBuffers("window.csv");
    const std::vector<ResultRecord> expectedTuples = {{1, 1, 1000},
                                                      {1, 12, 1001},
                                                      {1, 4, 1002},
                                                      {2, 1, 2000},
                                                      {2, 11, 2001},
                                                      {2, 16, 2002},
                                                      {3, 1, 3000},
                                                      {3, 11, 3001},
                                                      {3, 1, 3003},
                                                      {3, 1, 3200},
                                                      {4, 1, 4000},
                                                      {5, 1, 5000},
                                                      {6, 1, 6000},
                                                      {7, 1, 7000},
                                                      {8, 1, 8000},
                                                      {9, 1, 9000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("value") < 10).sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQuery<ResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(MultiThreadedTest, testProjectQuery) {
    struct ResultRecord {
        uint64_t id;

        bool operator==(const ResultRecord& rhs) const { return id == rhs.id; }
    };
    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()->addField(createField("test1$id", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<ResultRecord> expectedTuples = {{1}, {12}, {4}, {1}, {11}, {16}, {1}, {11}, {1}, {1}, {1}, {1}, {1}, {1}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query =
        TestQuery::from(testSourceDescriptor).filter(Attribute("value") < 8).project(Attribute("id")).sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQuery<ResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

struct NonKeyedResultRecord {
    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t value;

    bool operator==(const NonKeyedResultRecord& rhs) const {
        return windowStart == rhs.windowStart && windowEnd == rhs.windowEnd && value == rhs.value;
    }
    friend std::ostream& operator<<(std::ostream& os, const NonKeyedResultRecord& record);
};

std::ostream& operator<<(std::ostream& os, const NonKeyedResultRecord& record) {
    os << record.windowStart << "-" << record.windowEnd << "-" << record.value;
    return os;
}

struct KeyedResultRecord {
    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t id;
    uint64_t value;

    bool operator==(const KeyedResultRecord& rhs) const {
        return windowStart == rhs.windowStart && windowEnd == rhs.windowEnd && id == rhs.id && value == rhs.value;
    }
    friend std::ostream& operator<<(std::ostream& os, const KeyedResultRecord& record);
};

std::ostream& operator<<(std::ostream& os, const KeyedResultRecord& record) {
    os << record.windowStart << "-" << record.windowEnd << "-" << record.id << "-" << record.value;
    return os;
}

TEST_P(MultiThreadedTest, testNonKeyedEventTimeTumblingWindowAggregation) {

    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()
                                  ->addField(createField("test1$start", BasicType::UINT64))
                                  ->addField(createField("test1$end", BasicType::UINT64))
                                  ->addField(createField("test1$timestamp", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<NonKeyedResultRecord> expectedTuples = {
        {1000, 2000, 3},    {2000, 3000, 6},    {3000, 4000, 12},   {4000, 5000, 4},    {5000, 6000, 5},    {6000, 7000, 6},
        {7000, 8000, 7},    {8000, 9000, 8},    {9000, 10000, 9},   {10000, 11000, 10}, {11000, 12000, 11}, {12000, 13000, 12},
        {13000, 14000, 13}, {14000, 15000, 14}, {15000, 16000, 15}, {16000, 17000, 16}, {17000, 18000, 17}, {18000, 19000, 18},
        {19000, 20000, 19}, {20000, 21000, 20}, {21000, 22000, 21}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<NonKeyedResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor)
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                           .apply(Sum(Attribute("value")))
                           .sink(testSinkDescriptor);
    // Running the query
    const auto resultRecords =
        runQuery<NonKeyedResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(MultiThreadedTest, testNonKeyedEventTimeSlidingWindowAggregation) {

    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()
                                  ->addField(createField("test1$start", BasicType::UINT64))
                                  ->addField(createField("test1$end", BasicType::UINT64))
                                  ->addField(createField("test1$timestamp", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<NonKeyedResultRecord> expectedTuples = {
        {1000, 2000, 3},    {1500, 2500, 6},    {2000, 3000, 6},    {2500, 3500, 12},   {3000, 4000, 12},   {3500, 4500, 4},
        {4000, 5000, 4},    {4500, 5500, 5},    {5000, 6000, 5},    {5500, 6500, 6},    {6000, 7000, 6},    {6500, 7500, 7},
        {7000, 8000, 7},    {7500, 8500, 8},    {8000, 9000, 8},    {8500, 9500, 9},    {9000, 10000, 9},   {9500, 10500, 10},
        {10000, 11000, 10}, {10500, 11500, 11}, {11000, 12000, 11}, {11500, 12500, 12}, {12000, 13000, 12}, {12500, 13500, 13},
        {13000, 14000, 13}, {13500, 14500, 14}, {14000, 15000, 14}, {14500, 15500, 15}, {15000, 16000, 15}, {15500, 16500, 16},
        {16000, 17000, 16}, {16500, 17500, 17}, {17000, 18000, 17}, {17500, 18500, 18}, {18000, 19000, 18}, {18500, 19500, 19},
        {19000, 20000, 19}, {19500, 20500, 20}, {20000, 21000, 20}, {20500, 21500, 21}, {21000, 22000, 21}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<NonKeyedResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor)
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000), Milliseconds(500)))
                           .apply(Sum(Attribute("value")))
                           .sink(testSinkDescriptor);
    // Running the query
    const auto resultRecords =
        runQuery<NonKeyedResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(MultiThreadedTest, testKeyedEventTimeTumblingWindowAggregation) {

    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()
                                  ->addField(createField("test1$start", BasicType::UINT64))
                                  ->addField(createField("test1$end", BasicType::UINT64))
                                  ->addField(createField("test1$timestamp", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<KeyedResultRecord> expectedTuples = {
        {1000, 2000, 1, 1},    {1000, 2000, 12, 1},   {1000, 2000, 4, 1},    {2000, 3000, 11, 2},   {2000, 3000, 1, 2},
        {2000, 3000, 16, 2},   {3000, 4000, 1, 9},    {3000, 4000, 11, 3},   {4000, 5000, 1, 4},    {5000, 6000, 1, 5},
        {6000, 7000, 1, 6},    {7000, 8000, 1, 7},    {8000, 9000, 1, 8},    {9000, 10000, 1, 9},   {10000, 11000, 1, 10},
        {11000, 12000, 1, 11}, {12000, 13000, 1, 12}, {13000, 14000, 1, 13}, {14000, 15000, 1, 14}, {15000, 16000, 1, 15},
        {16000, 17000, 1, 16}, {17000, 18000, 1, 17}, {18000, 19000, 1, 18}, {19000, 20000, 1, 19}, {20000, 21000, 1, 20},
        {21000, 22000, 1, 21}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<KeyedResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor)
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                           .byKey(Attribute("id"))
                           .apply(Sum(Attribute("value")))
                           .sink(testSinkDescriptor);
    // Running the query
    const auto resultRecords =
        runQuery<KeyedResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(MultiThreadedTest, testMultipleNonKeyedEventTimeTumblingWindows) {

    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()
                                  ->addField(createField("test1$start", BasicType::UINT64))
                                  ->addField(createField("test1$end", BasicType::UINT64))
                                  ->addField(createField("test1$timestamp", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<NonKeyedResultRecord> expectedTuples =
        {{0, 2000, 3}, {2000, 4000, 18}, {4000, 6000, 9}, {6000, 8000, 13}, {8000, 10000, 17}, {10000, 12000, 21}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<NonKeyedResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor)
                           .filter(Attribute("value") < 12)// this is merely to keep the number of output tuples under control
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                           .apply(Sum(Attribute("value")))
                           .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)))
                           .apply(Sum(Attribute("value")))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords =
        runQuery<NonKeyedResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(MultiThreadedTest, testMultipleKeyedEventTimeTumblingWindows) {

    const auto inputSchema = Schema::create()
                                 ->addField(createField("test1$value", BasicType::UINT64))
                                 ->addField(createField("test1$id", BasicType::UINT64))
                                 ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto outputSchema = Schema::create()
                                  ->addField(createField("test1$start", BasicType::UINT64))
                                  ->addField(createField("test1$end", BasicType::UINT64))
                                  ->addField(createField("test1$id", BasicType::UINT64))
                                  ->addField(createField("test1$timestamp", BasicType::UINT64));

    const std::string fileNameBuffers("window.csv");
    const std::vector<KeyedResultRecord> expectedTuples = {{0, 2000, 1, 1},
                                                           {0, 2000, 4, 1},
                                                           {0, 2000, 12, 1},
                                                           {2000, 4000, 11, 5},
                                                           {2000, 4000, 1, 11},
                                                           {2000, 4000, 16, 2},
                                                           {4000, 6000, 1, 9},
                                                           {6000, 8000, 1, 13},
                                                           {8000, 10000, 1, 17},
                                                           {10000, 12000, 1, 21}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<KeyedResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);
    const auto query = TestQuery::from(testSourceDescriptor)
                           .filter(Attribute("value") < 12)// this is merely to keep the number of output tuples under control
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                           .byKey(Attribute("id"))
                           .apply(Sum(Attribute("value")))
                           .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)))
                           .byKey(Attribute("id"))
                           .apply(Sum(Attribute("value")))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords =
        runQuery<KeyedResultRecord>({{inputSchema, fileNameBuffers}}, expectedTuples.size(), testSink, query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

INSTANTIATE_TEST_CASE_P(testQueriesMultiThreaded,
                        MultiThreadedTest,
                        ::testing::Values(1, 2, 3, 4, 8),
                        [](const testing::TestParamInfo<MultiThreadedTest::ParamType>& info) {
                            return std::to_string(info.param) + "Workerthreads";
                        });

}// namespace NES