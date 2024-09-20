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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <chrono>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {
class JoinMultiThreadedTest
    : public Testing::BaseIntegrationTest,
      public Runtime::BufferRecycler,
      public ::testing::WithParamInterface<
          std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy, uint64_t>> {
  public:
    const uint64_t numTuplesPerBuffer = 2;
    static constexpr auto dumpNone = QueryCompilation::DumpMode::NONE;
    static constexpr auto waitTillStoppingQuery = std::chrono::milliseconds(100);
    static constexpr DecomposedQueryId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinMultiThreadedTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JoinMultiThreadedTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseIntegrationTest::SetUp();

        // Creating the execution engine
        joinStrategy = std::get<0>(JoinMultiThreadedTest::GetParam());
        windowingStrategy = std::get<1>(JoinMultiThreadedTest::GetParam());
        const uint64_t numberOfWorkerThreads = std::get<2>(JoinMultiThreadedTest::GetParam());
        executionEngine =
            std::make_shared<Testing::TestExecutionEngine>(dumpNone, numberOfWorkerThreads, joinStrategy, windowingStrategy);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down JoinMultiThreadedTest test case.");

        // Stopping the execution engine
        EXPECT_TRUE(executionEngine->stop());
        NES::Testing::BaseIntegrationTest::TearDown();
    }

    void recyclePooledBuffer(Runtime::detail::MemorySegment*) override {}

    void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override {}

    template<typename ResultRecord>
    std::vector<ResultRecord>& runQueryAndPrintMissingRecords(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                                              const std::vector<ResultRecord>& expectedTuples,
                                                              const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                                              const Query& query) {
        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (auto [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::createExpectedBuffersFromCsv(fileNameInputBuffers, inputSchema, bufferManager, numTuplesPerBuffer));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                               defaultSharedQueryId,
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto queryPlan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers
        auto dataSourceCnt = 0_u64;
        for (const auto& inputBuffers : allInputBuffers) {
            auto source = executionEngine->getDataSource(queryPlan, dataSourceCnt++);
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(waitTillStoppingQuery);

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(queryPlan, Runtime::QueryTerminationType::Graceful));
        testSink->waitTillCompleted(expectedTuples.size());

        // Checking for correctness
        auto& result = testSink->getResult();
        EXPECT_EQ(result.size(), expectedTuples.size());
        EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expectedTuples));

        // Printing out missing records in result
        std::vector<ResultRecord> missingRecords(expectedTuples);
        missingRecords.erase(std::remove_if(missingRecords.begin(),
                                            missingRecords.end(),
                                            [&result](auto item) {
                                                return std::find(result.begin(), result.end(), item) != result.end();
                                            }),
                             missingRecords.end());
        auto missingRecordsStr =
            std::accumulate(missingRecords.begin(), missingRecords.end(), std::string(), [](const std::string& str, auto item) {
                return str.empty() ? item.toString() : str + "\n" + item.toString();
            });
        NES_INFO("Missing records: {}", missingRecordsStr);

        return result;
    }

    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
};

TEST_P(JoinMultiThreadedTest, testOneJoin) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << ","
                << "," << window1win1 << "," << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2
                << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };
    const auto inputSchemaLeft = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
    const auto inputSchemaRight = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("test2");
    const auto joinFieldNameLeft = "test1$id";
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight);

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {1000, 2000, 12, 1, 1001, 12, 5, 1011},     {1000, 2000, 4, 1, 1002, 4, 3, 1102},
        {1000, 2000, 4, 1, 1002, 4, 3, 1112},       {2000, 3000, 1, 2, 2000, 1, 2, 2010},
        {2000, 3000, 11, 2, 2001, 11, 2, 2301},     {3000, 4000, 1, 3, 3000, 1, 3, 3009},
        {3000, 4000, 1, 3, 3000, 1, 3, 3201},       {3000, 4000, 11, 3, 3001, 11, 3, 3001},
        {3000, 4000, 1, 3, 3003, 1, 3, 3009},       {3000, 4000, 1, 3, 3003, 1, 3, 3201},
        {3000, 4000, 1, 3, 3200, 1, 3, 3009},       {3000, 4000, 1, 3, 3200, 1, 3, 3201},
        {4000, 5000, 1, 4, 4000, 1, 4, 4001},       {5000, 6000, 1, 5, 5000, 1, 5, 5500},
        {6000, 7000, 1, 6, 6000, 1, 6, 6000},       {7000, 8000, 1, 7, 7000, 1, 7, 7000},
        {8000, 9000, 1, 8, 8000, 1, 8, 8000},       {9000, 10000, 1, 9, 9000, 1, 9, 9000},
        {10000, 11000, 1, 10, 10000, 1, 10, 10000}, {11000, 12000, 1, 11, 11000, 1, 11, 11000},
        {12000, 13000, 1, 12, 12000, 1, 12, 12000}, {13000, 14000, 1, 13, 13000, 1, 13, 13000},
        {14000, 15000, 1, 14, 14000, 1, 14, 14000}, {15000, 16000, 1, 15, 15000, 1, 15, 15000},
        {16000, 17000, 1, 16, 16000, 1, 16, 16000}, {17000, 18000, 1, 17, 17000, 1, 17, 17000},
        {18000, 19000, 1, 18, 18000, 1, 18, 18000}, {19000, 20000, 1, 19, 19000, 1, 19, 19000},
        {20000, 21000, 1, 20, 20000, 1, 20, 20000}, {21000, 22000, 1, 21, 21000, 1, 21, 21000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQueryAndPrintMissingRecords<ResultRecord>(
        {{inputSchemaLeft, fileNameBuffersLeft}, {inputSchemaRight, fileNameBuffersRight}},
        expectedTuples,
        testSink,
        query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(JoinMultiThreadedTest, testTwoJoins) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << ","
                << "," << window1win1 << "," << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2
                << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1
                && window1timestamp == rhs.window1timestamp && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2
                && window2timestamp == rhs.window2timestamp && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3
                && window3timestamp == rhs.window3timestamp;
        }
    };
    const auto inputSchemaLeft = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
    const auto inputSchemaRight = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("test2");
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("id3", BasicType::UINT64))
                                      ->addField(createField("value3", BasicType::UINT64))
                                      ->addField(createField("timestamp", BasicType::UINT64))
                                      ->updateSourceName("test3");
    const auto joinFieldNameLeft = "test1$id";
    const auto joinSchemaLeftRight = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight);
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird);
    ASSERT_EQ(sizeof(ResultRecord), outputSchema->getSchemaSizeInBytes());

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::vector<ResultRecord> expectedTuples = {{1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001},
                                                      {1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001},
                                                      {1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300},
                                                      {3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000},
                                                      {12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
                                                      {13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
                                                      {14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
                                                      {15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);

    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQueryAndPrintMissingRecords<ResultRecord>({{inputSchemaLeft, fileNameBuffersLeft},
                                                                             {inputSchemaRight, fileNameBuffersRight},
                                                                             {inputSchemaThird, fileNameBuffersThird}},
                                                                            expectedTuples,
                                                                            testSink,
                                                                            query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(JoinMultiThreadedTest, testThreeJoins) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << ","
                << "," << window1win1 << "," << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2
                << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1
                && window1timestamp == rhs.window1timestamp && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2
                && window2timestamp == rhs.window2timestamp && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3
                && window3timestamp == rhs.window3timestamp && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4
                && window4timestamp == rhs.window4timestamp;
        }
    };
    const auto inputSchemaLeft = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
    const auto inputSchemaRight = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("test2");
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("id3", BasicType::UINT64))
                                      ->addField(createField("value3", BasicType::UINT64))
                                      ->addField(createField("timestamp", BasicType::UINT64))
                                      ->updateSourceName("test3");
    const auto inputSchemaFourth = Schema::create()
                                       ->addField(createField("id4", BasicType::UINT64))
                                       ->addField(createField("value4", BasicType::UINT64))
                                       ->addField(createField("timestamp", BasicType::UINT64))
                                       ->updateSourceName("test4");
    const auto joinFieldNameLeft = "test1$id";
    const auto joinSchemaLeftRight = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight);
    const auto joinSchemaLeftRightThird = Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird);
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRightThird, inputSchemaFourth);
    ASSERT_EQ(sizeof(ResultRecord), outputSchema->getSchemaSizeInBytes());

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::string fileNameBuffersFourth("window4.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {12000, 13000, 12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {13000, 14000, 13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {14000, 15000, 14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {15000, 16000, 15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {3000, 4000, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {1000, 2000, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);
    const auto testSourceDescriptorFourth = executionEngine->createDataSource(inputSchemaFourth);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorFourth))
                           .where(Attribute("id") == Attribute("id4"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto& resultRecords = runQueryAndPrintMissingRecords<ResultRecord>({{inputSchemaLeft, fileNameBuffersLeft},
                                                                              {inputSchemaRight, fileNameBuffersRight},
                                                                              {inputSchemaThird, fileNameBuffersThird},
                                                                              {inputSchemaFourth, fileNameBuffersFourth}},
                                                                             expectedTuples,
                                                                             testSink,
                                                                             query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(JoinMultiThreadedTest, oneJoinSlidingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << ","
                << "," << window1id1 << "," << window1timestamp << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2value2 == rhs.window2value2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };
    const auto inputSchemaLeft = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
    const auto inputSchemaRight = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("test2");

    const auto joinFieldNameLeft = "test1$id";
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight);

    const std::string fileNameBuffersLeft("window7.csv");
    const std::string fileNameBuffersRight("window7.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {250, 1250, 1, 1, 1240, 1, 1, 1240},      {250, 1250, 4, 2, 1120, 4, 2, 1120},
        {250, 1250, 12, 3, 1180, 12, 3, 1180},    {500, 1500, 1, 1, 1240, 1, 1, 1240},
        {500, 1500, 4, 2, 1120, 4, 2, 1120},      {500, 1500, 12, 3, 1180, 12, 3, 1180},
        {500, 1500, 4, 2, 1120, 4, 6, 1480},      {500, 1500, 12, 3, 1180, 12, 5, 1475},
        {500, 1500, 12, 5, 1475, 12, 3, 1180},    {500, 1500, 4, 6, 1480, 4, 2, 1120},
        {500, 1500, 12, 5, 1475, 12, 5, 1475},    {500, 1500, 4, 6, 1480, 4, 6, 1480},
        {500, 1500, 5, 7, 1350, 5, 7, 1350},      {750, 1750, 1, 1, 1240, 1, 1, 1240},
        {750, 1750, 4, 2, 1120, 4, 2, 1120},      {750, 1750, 12, 3, 1180, 12, 3, 1180},
        {750, 1750, 4, 2, 1120, 4, 6, 1480},      {750, 1750, 12, 3, 1180, 12, 5, 1475},
        {750, 1750, 12, 3, 1180, 12, 4, 1501},    {750, 1750, 12, 5, 1475, 12, 3, 1180},
        {750, 1750, 4, 6, 1480, 4, 2, 1120},      {750, 1750, 12, 5, 1475, 12, 5, 1475},
        {750, 1750, 4, 6, 1480, 4, 6, 1480},      {750, 1750, 5, 7, 1350, 5, 7, 1350},
        {750, 1750, 12, 5, 1475, 12, 4, 1501},    {750, 1750, 12, 4, 1501, 12, 3, 1180},
        {750, 1750, 12, 4, 1501, 12, 5, 1475},    {750, 1750, 12, 4, 1501, 12, 4, 1501},
        {750, 1750, 3, 10, 1650, 3, 10, 1650},    {1000, 2000, 1, 1, 1240, 1, 1, 1240},
        {1000, 2000, 4, 2, 1120, 4, 2, 1120},     {1000, 2000, 12, 3, 1180, 12, 3, 1180},
        {1000, 2000, 4, 2, 1120, 4, 6, 1480},     {1000, 2000, 12, 3, 1180, 12, 5, 1475},
        {1000, 2000, 12, 3, 1180, 12, 4, 1501},   {1000, 2000, 1, 1, 1240, 1, 9, 1999},
        {1000, 2000, 12, 5, 1475, 12, 3, 1180},   {1000, 2000, 4, 6, 1480, 4, 2, 1120},
        {1000, 2000, 12, 5, 1475, 12, 5, 1475},   {1000, 2000, 4, 6, 1480, 4, 6, 1480},
        {1000, 2000, 5, 7, 1350, 5, 7, 1350},     {1000, 2000, 12, 5, 1475, 12, 4, 1501},
        {1000, 2000, 5, 7, 1350, 5, 8, 1750},     {1000, 2000, 12, 4, 1501, 12, 3, 1180},
        {1000, 2000, 12, 4, 1501, 12, 5, 1475},   {1000, 2000, 12, 4, 1501, 12, 4, 1501},
        {1000, 2000, 3, 10, 1650, 3, 10, 1650},   {1000, 2000, 1, 9, 1999, 1, 1, 1240},
        {1000, 2000, 5, 8, 1750, 5, 7, 1350},     {1000, 2000, 5, 8, 1750, 5, 8, 1750},
        {1000, 2000, 1, 9, 1999, 1, 9, 1999},     {1000, 2000, 20, 12, 1987, 20, 12, 1987},
        {1250, 2250, 12, 5, 1475, 12, 5, 1475},   {1250, 2250, 4, 6, 1480, 4, 6, 1480},
        {1250, 2250, 5, 7, 1350, 5, 7, 1350},     {1250, 2250, 12, 5, 1475, 12, 4, 1501},
        {1250, 2250, 5, 7, 1350, 5, 8, 1750},     {1250, 2250, 12, 4, 1501, 12, 5, 1475},
        {1250, 2250, 12, 4, 1501, 12, 4, 1501},   {1250, 2250, 3, 10, 1650, 3, 10, 1650},
        {1250, 2250, 3, 10, 1650, 3, 11, 2240},   {1250, 2250, 5, 8, 1750, 5, 7, 1350},
        {1250, 2250, 5, 8, 1750, 5, 8, 1750},     {1250, 2250, 1, 9, 1999, 1, 9, 1999},
        {1250, 2250, 20, 12, 1987, 20, 12, 1987}, {1250, 2250, 20, 12, 1987, 20, 13, 2010},
        {1250, 2250, 3, 11, 2240, 3, 10, 1650},   {1250, 2250, 20, 13, 2010, 20, 12, 1987},
        {1250, 2250, 3, 11, 2240, 3, 11, 2240},   {1250, 2250, 20, 13, 2010, 20, 13, 2010},
        {1250, 2250, 17, 14, 2200, 17, 14, 2200}, {1500, 2500, 12, 4, 1501, 12, 4, 1501},
        {1500, 2500, 3, 10, 1650, 3, 10, 1650},   {1500, 2500, 3, 10, 1650, 3, 11, 2240},
        {1500, 2500, 5, 8, 1750, 5, 8, 1750},     {1500, 2500, 1, 9, 1999, 1, 9, 1999},
        {1500, 2500, 20, 12, 1987, 20, 12, 1987}, {1500, 2500, 20, 12, 1987, 20, 13, 2010},
        {1500, 2500, 3, 11, 2240, 3, 10, 1650},   {1500, 2500, 20, 13, 2010, 20, 12, 1987},
        {1500, 2500, 3, 11, 2240, 3, 11, 2240},   {1500, 2500, 20, 13, 2010, 20, 13, 2010},
        {1500, 2500, 17, 14, 2200, 17, 14, 2200}, {1500, 2500, 42, 17, 2400, 42, 17, 2400},
        {1750, 2750, 5, 8, 1750, 5, 8, 1750},     {1750, 2750, 1, 9, 1999, 1, 9, 1999},
        {1750, 2750, 20, 12, 1987, 20, 12, 1987}, {1750, 2750, 20, 12, 1987, 20, 13, 2010},
        {1750, 2750, 20, 13, 2010, 20, 12, 1987}, {1750, 2750, 3, 11, 2240, 3, 11, 2240},
        {1750, 2750, 20, 13, 2010, 20, 13, 2010}, {1750, 2750, 17, 14, 2200, 17, 14, 2200},
        {1750, 2750, 17, 14, 2200, 17, 15, 2600}, {1750, 2750, 42, 17, 2400, 42, 17, 2400},
        {1750, 2750, 17, 15, 2600, 17, 14, 2200}, {1750, 2750, 17, 15, 2600, 17, 15, 2600},
        {2000, 3000, 3, 11, 2240, 3, 11, 2240},   {2000, 3000, 20, 13, 2010, 20, 13, 2010},
        {2000, 3000, 17, 14, 2200, 17, 14, 2200}, {2000, 3000, 17, 14, 2200, 17, 15, 2600},
        {2000, 3000, 17, 14, 2200, 17, 16, 2800}, {2000, 3000, 42, 17, 2400, 42, 17, 2400},
        {2000, 3000, 42, 17, 2400, 42, 18, 2990}, {2000, 3000, 17, 15, 2600, 17, 14, 2200},
        {2000, 3000, 17, 15, 2600, 17, 15, 2600}, {2000, 3000, 17, 15, 2600, 17, 16, 2800},
        {2000, 3000, 17, 16, 2800, 17, 14, 2200}, {2000, 3000, 42, 18, 2990, 42, 17, 2400},
        {2000, 3000, 17, 16, 2800, 17, 15, 2600}, {2000, 3000, 17, 16, 2800, 17, 16, 2800},
        {2000, 3000, 42, 18, 2990, 42, 18, 2990}, {2250, 3250, 42, 17, 2400, 42, 17, 2400},
        {2250, 3250, 42, 17, 2400, 42, 18, 2990}, {2250, 3250, 17, 15, 2600, 17, 15, 2600},
        {2250, 3250, 17, 15, 2600, 17, 16, 2800}, {2250, 3250, 42, 18, 2990, 42, 17, 2400},
        {2250, 3250, 17, 16, 2800, 17, 15, 2600}, {2250, 3250, 17, 16, 2800, 17, 16, 2800},
        {2250, 3250, 42, 18, 2990, 42, 18, 2990}, {2500, 3500, 17, 15, 2600, 17, 15, 2600},
        {2500, 3500, 17, 15, 2600, 17, 16, 2800}, {2500, 3500, 17, 16, 2800, 17, 15, 2600},
        {2500, 3500, 17, 16, 2800, 17, 16, 2800}, {2500, 3500, 42, 18, 2990, 42, 18, 2990},
        {2750, 3750, 17, 16, 2800, 17, 16, 2800}, {2750, 3750, 42, 18, 2990, 42, 18, 2990}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(250)))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQueryAndPrintMissingRecords<ResultRecord>(
        {{inputSchemaLeft, fileNameBuffersLeft}, {inputSchemaRight, fileNameBuffersRight}},
        expectedTuples,
        testSink,
        query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

TEST_P(JoinMultiThreadedTest, threeJoinsSlidingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << ","
                << "," << window1win1 << "," << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2
                << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1
                && window1timestamp == rhs.window1timestamp && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2
                && window2timestamp == rhs.window2timestamp && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3
                && window3timestamp == rhs.window3timestamp && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4
                && window4timestamp == rhs.window4timestamp;
        }
    };
    const auto inputSchemaLeft = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
    const auto inputSchemaRight = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("test2");
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("id3", BasicType::UINT64))
                                      ->addField(createField("value3", BasicType::UINT64))
                                      ->addField(createField("timestamp", BasicType::UINT64))
                                      ->updateSourceName("test3");
    const auto inputSchemaFourth = Schema::create()
                                       ->addField(createField("id4", BasicType::UINT64))
                                       ->addField(createField("value4", BasicType::UINT64))
                                       ->addField(createField("timestamp", BasicType::UINT64))
                                       ->updateSourceName("test4");

    const auto joinSchemaLeftRight = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight);
    const auto joinSchemaLeftRightThird = Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird);
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRightThird, inputSchemaFourth);

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::string fileNameBuffersFourth("window4.csv");

    const std::vector<ResultRecord> expectedTuples = {
        {500, 1500, 500, 1500, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {500, 1500, 500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 500, 1500, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {500, 1500, 500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 1000, 2000, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {500, 1500, 1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {500, 1500, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 500, 1500, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {1000, 2000, 500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 500, 1500, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {1000, 2000, 500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {1000, 2000, 1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300},
        {1000, 2000, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001},
        {2500, 3500, 2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {2500, 3500, 2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {2500, 3500, 3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {2500, 3500, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {3000, 4000, 2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {3000, 4000, 2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {3000, 4000, 3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {3000, 4000, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000},
        {11500, 12500, 11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {11500, 12500, 11500, 12500, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {11500, 12500, 12000, 13000, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {11500, 12500, 12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {12000, 13000, 11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {12000, 13000, 11500, 12500, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {12000, 13000, 12000, 13000, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {12000, 13000, 12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000},
        {12500, 13500, 12500, 13500, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {12500, 13500, 12500, 13500, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {12500, 13500, 13000, 14000, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {12500, 13500, 13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {13000, 14000, 12500, 13500, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {13000, 14000, 12500, 13500, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {13000, 14000, 13000, 14000, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {13000, 14000, 13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000},
        {13500, 14500, 13500, 14500, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {13500, 14500, 13500, 14500, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {13500, 14500, 14000, 15000, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {13500, 14500, 14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {14000, 15000, 13500, 14500, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {14000, 15000, 13500, 14500, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {14000, 15000, 14000, 15000, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {14000, 15000, 14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000},
        {14500, 15500, 14500, 15500, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {14500, 15500, 14500, 15500, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {14500, 15500, 15000, 16000, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {14500, 15500, 15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {15000, 16000, 14500, 15500, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {15000, 16000, 14500, 15500, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {15000, 16000, 15000, 16000, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000},
        {15000, 16000, 15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);
    const auto testSourceDescriptorFourth = executionEngine->createDataSource(inputSchemaFourth);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(TestQuery::from(testSourceDescriptorFourth))
                           .where(Attribute("id") == Attribute("id4"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .sink(testSinkDescriptor);

    // Running the query
    const auto resultRecords = runQueryAndPrintMissingRecords<ResultRecord>({{inputSchemaLeft, fileNameBuffersLeft},
                                                                             {inputSchemaRight, fileNameBuffersRight},
                                                                             {inputSchemaThird, fileNameBuffersThird},
                                                                             {inputSchemaFourth, fileNameBuffersFourth}},
                                                                            expectedTuples,
                                                                            testSink,
                                                                            query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedTuples.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedTuples));
}

INSTANTIATE_TEST_CASE_P(testJoinQueriesMultiThreaded,
                        JoinMultiThreadedTest,
                        ::testing::Combine(ALL_JOIN_STRATEGIES, ALL_WINDOW_STRATEGIES, ::testing::Values(1, 2, 3, 4, 8)),
                        [](const testing::TestParamInfo<JoinMultiThreadedTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param))) + "_"
                                + std::to_string(std::get<2>(info.param)) + "Workerthreads";
                        });

}// namespace NES
