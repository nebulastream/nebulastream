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
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
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
class JoinMultiThreadedTest
    : public Testing::BaseIntegrationTest,
      public Runtime::BufferRecycler,
      public ::testing::WithParamInterface<
          std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy, uint64_t>> {
  public:
    const uint64_t numTuplesPerBuffer = 2;
    static constexpr auto dumpNone = QueryCompilation::DumpMode::NONE;
    static constexpr auto waitTillStoppingQuery = std::chrono::milliseconds(100);
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinMultiThreadedTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JoinMultiThreadedTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseIntegrationTest::SetUp();

        // Creating the execution engine
        const auto joinStrategy = std::get<0>(JoinMultiThreadedTest::GetParam());
        const auto windowingStrategy = std::get<1>(JoinMultiThreadedTest::GetParam());
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
auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId, query.getQueryPlan()->getRootOperators());
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
};

TEST_P(JoinMultiThreadedTest, testOneJoin) {
    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1window2Key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << "," << window1window2Key << "," << window1win1 << ","
                << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1window2Key == rhs.window1window2Key && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1
                && window1timestamp == rhs.window1timestamp && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2
                && window2timestamp == rhs.window2timestamp;
        }
    };
    const auto inputSchemaLeft = Schema::create()
                                     ->addField(createField("test1$win1", BasicType::UINT64))
                                     ->addField(createField("test1$id1", BasicType::UINT64))
                                     ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto inputSchemaRight = Schema::create()
                                      ->addField(createField("test2$win2", BasicType::UINT64))
                                      ->addField(createField("test2$id2", BasicType::UINT64))
                                      ->addField(createField("test2$timestamp", BasicType::UINT64));
    const auto joinFieldNameLeft = "test1$id1";
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight, joinFieldNameLeft);

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
        {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},       {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
        {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},    {3000, 4000, 1, 3, 1, 3000, 3, 1, 3009},
        {3000, 4000, 1, 3, 1, 3000, 3, 1, 3201},       {3000, 4000, 11, 3, 11, 3001, 3, 11, 3001},
        {3000, 4000, 1, 3, 1, 3003, 3, 1, 3009},       {3000, 4000, 1, 3, 1, 3003, 3, 1, 3201},
        {3000, 4000, 1, 3, 1, 3200, 3, 1, 3009},       {3000, 4000, 1, 3, 1, 3200, 3, 1, 3201},
        {4000, 5000, 1, 4, 1, 4000, 4, 1, 4001},       {5000, 6000, 1, 5, 1, 5000, 5, 1, 5500},
        {6000, 7000, 1, 6, 1, 6000, 6, 1, 6000},       {7000, 8000, 1, 7, 1, 7000, 7, 1, 7000},
        {8000, 9000, 1, 8, 1, 8000, 8, 1, 8000},       {9000, 10000, 1, 9, 1, 9000, 9, 1, 9000},
        {10000, 11000, 1, 10, 1, 10000, 10, 1, 10000}, {11000, 12000, 1, 11, 1, 11000, 11, 1, 11000},
        {12000, 13000, 1, 12, 1, 12000, 12, 1, 12000}, {13000, 14000, 1, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14, 1, 14000, 14, 1, 14000}, {15000, 16000, 1, 15, 1, 15000, 15, 1, 15000},
        {16000, 17000, 1, 16, 1, 16000, 16, 1, 16000}, {17000, 18000, 1, 17, 1, 17000, 17, 1, 17000},
        {18000, 19000, 1, 18, 1, 18000, 18, 1, 18000}, {19000, 20000, 1, 19, 1, 19000, 19, 1, 19000},
        {20000, 21000, 1, 20, 1, 20000, 20, 1, 20000}, {21000, 22000, 1, 21, 1, 21000, 21, 1, 21000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
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
    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1window2Key;
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
            oss << window1window2Start << "," << window1window2End << "," << window1window2Key << "," << window1win1 << ","
                << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1window2Key == rhs.window1window2Key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp;
        }
    };
    const auto inputSchemaLeft = Schema::create()
                                     ->addField(createField("test1$win1", BasicType::UINT64))
                                     ->addField(createField("test1$id1", BasicType::UINT64))
                                     ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto inputSchemaRight = Schema::create()
                                      ->addField(createField("test2$win2", BasicType::UINT64))
                                      ->addField(createField("test2$id2", BasicType::UINT64))
                                      ->addField(createField("test2$timestamp", BasicType::UINT64));
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("test3$win3", BasicType::UINT64))
                                      ->addField(createField("test3$id3", BasicType::UINT64))
                                      ->addField(createField("test3$timestamp", BasicType::UINT64));
    const auto joinFieldNameLeft = "test1$id1";
    const auto joinSchemaLeftRight =
        Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight, joinFieldNameLeft);
    const auto outputSchema =
        Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird, joinFieldNameLeft);
    ASSERT_EQ(sizeof(ResultRecord), outputSchema->getSchemaSizeInBytes());

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300},
        {3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000},
        {12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);

    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id3"))
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
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1window2Key;
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
            oss << window1window2Start << "," << window1window2End << "," << window1window2Key << "," << window1win1 << ","
                << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1window2Key == rhs.window1window2Key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };
    const auto inputSchemaLeft = Schema::create()
                                     ->addField(createField("test1$win1", BasicType::UINT64))
                                     ->addField(createField("test1$id1", BasicType::UINT64))
                                     ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto inputSchemaRight = Schema::create()
                                      ->addField(createField("test2$win2", BasicType::UINT64))
                                      ->addField(createField("test2$id2", BasicType::UINT64))
                                      ->addField(createField("test2$timestamp", BasicType::UINT64));
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("test3$win3", BasicType::UINT64))
                                      ->addField(createField("test3$id3", BasicType::UINT64))
                                      ->addField(createField("test3$timestamp", BasicType::UINT64));
    const auto inputSchemaFourth = Schema::create()
                                       ->addField(createField("test4$win4", BasicType::UINT64))
                                       ->addField(createField("test4$id4", BasicType::UINT64))
                                       ->addField(createField("test4$timestamp", BasicType::UINT64));
    const auto joinFieldNameLeft = "test1$id1";
    const auto joinSchemaLeftRight =
        Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight, joinFieldNameLeft);
    const auto joinSchemaLeftRightThird =
        Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird, joinFieldNameLeft);
    const auto outputSchema =
        Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRightThird, inputSchemaFourth, joinFieldNameLeft);
    ASSERT_EQ(sizeof(ResultRecord), outputSchema->getSchemaSizeInBytes());

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::string fileNameBuffersFourth("window4.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {12000, 13000, 1, 12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {13000, 14000, 1, 13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {15000, 16000, 1, 15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {3000, 4000, 11, 3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);
    const auto testSourceDescriptorFourth = executionEngine->createDataSource(inputSchemaFourth);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(TestQuery::from(testSourceDescriptorFourth))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id4"))
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
    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1window2Key;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        std::string toString() {
            std::ostringstream oss;
            oss << window1window2Start << "," << window1window2End << "," << window1window2Key << "," << window1id1 << ","
                << window1timestamp << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1window2Key == rhs.window1window2Key && window1value1 == rhs.window1value1
                && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp && window2value2 == rhs.window2value2
                && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };
    const auto inputSchemaLeft = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                     ->addField("test1$value1", BasicType::UINT64)
                                     ->addField("test1$id1", BasicType::UINT64)
                                     ->addField("test1$timestamp", BasicType::UINT64);

    const auto inputSchemaRight = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                      ->addField("test2$value2", BasicType::UINT64)
                                      ->addField("test2$id2", BasicType::UINT64)
                                      ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto outputSchema = Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight, joinFieldNameLeft);

    const std::string fileNameBuffersLeft("window7.csv");
    const std::string fileNameBuffersRight("window7.csv");
    const std::vector<ResultRecord> expectedTuples = {
        {250, 1250, 1, 1, 1, 1240, 1, 1, 1240},       {250, 1250, 4, 2, 4, 1120, 2, 4, 1120},
        {250, 1250, 12, 3, 12, 1180, 3, 12, 1180},    {500, 1500, 1, 1, 1, 1240, 1, 1, 1240},
        {500, 1500, 4, 2, 4, 1120, 2, 4, 1120},       {500, 1500, 12, 3, 12, 1180, 3, 12, 1180},
        {500, 1500, 4, 2, 4, 1120, 6, 4, 1480},       {500, 1500, 12, 3, 12, 1180, 5, 12, 1475},
        {500, 1500, 12, 5, 12, 1475, 3, 12, 1180},    {500, 1500, 4, 6, 4, 1480, 2, 4, 1120},
        {500, 1500, 12, 5, 12, 1475, 5, 12, 1475},    {500, 1500, 4, 6, 4, 1480, 6, 4, 1480},
        {500, 1500, 5, 7, 5, 1350, 7, 5, 1350},       {750, 1750, 1, 1, 1, 1240, 1, 1, 1240},
        {750, 1750, 4, 2, 4, 1120, 2, 4, 1120},       {750, 1750, 12, 3, 12, 1180, 3, 12, 1180},
        {750, 1750, 4, 2, 4, 1120, 6, 4, 1480},       {750, 1750, 12, 3, 12, 1180, 5, 12, 1475},
        {750, 1750, 12, 3, 12, 1180, 4, 12, 1501},    {750, 1750, 12, 5, 12, 1475, 3, 12, 1180},
        {750, 1750, 4, 6, 4, 1480, 2, 4, 1120},       {750, 1750, 12, 5, 12, 1475, 5, 12, 1475},
        {750, 1750, 4, 6, 4, 1480, 6, 4, 1480},       {750, 1750, 5, 7, 5, 1350, 7, 5, 1350},
        {750, 1750, 12, 5, 12, 1475, 4, 12, 1501},    {750, 1750, 12, 4, 12, 1501, 3, 12, 1180},
        {750, 1750, 12, 4, 12, 1501, 5, 12, 1475},    {750, 1750, 12, 4, 12, 1501, 4, 12, 1501},
        {750, 1750, 3, 10, 3, 1650, 10, 3, 1650},     {1000, 2000, 1, 1, 1, 1240, 1, 1, 1240},
        {1000, 2000, 4, 2, 4, 1120, 2, 4, 1120},      {1000, 2000, 12, 3, 12, 1180, 3, 12, 1180},
        {1000, 2000, 4, 2, 4, 1120, 6, 4, 1480},      {1000, 2000, 12, 3, 12, 1180, 5, 12, 1475},
        {1000, 2000, 12, 3, 12, 1180, 4, 12, 1501},   {1000, 2000, 1, 1, 1, 1240, 9, 1, 1999},
        {1000, 2000, 12, 5, 12, 1475, 3, 12, 1180},   {1000, 2000, 4, 6, 4, 1480, 2, 4, 1120},
        {1000, 2000, 12, 5, 12, 1475, 5, 12, 1475},   {1000, 2000, 4, 6, 4, 1480, 6, 4, 1480},
        {1000, 2000, 5, 7, 5, 1350, 7, 5, 1350},      {1000, 2000, 12, 5, 12, 1475, 4, 12, 1501},
        {1000, 2000, 5, 7, 5, 1350, 8, 5, 1750},      {1000, 2000, 12, 4, 12, 1501, 3, 12, 1180},
        {1000, 2000, 12, 4, 12, 1501, 5, 12, 1475},   {1000, 2000, 12, 4, 12, 1501, 4, 12, 1501},
        {1000, 2000, 3, 10, 3, 1650, 10, 3, 1650},    {1000, 2000, 1, 9, 1, 1999, 1, 1, 1240},
        {1000, 2000, 5, 8, 5, 1750, 7, 5, 1350},      {1000, 2000, 5, 8, 5, 1750, 8, 5, 1750},
        {1000, 2000, 1, 9, 1, 1999, 9, 1, 1999},      {1000, 2000, 20, 12, 20, 1987, 12, 20, 1987},
        {1250, 2250, 12, 5, 12, 1475, 5, 12, 1475},   {1250, 2250, 4, 6, 4, 1480, 6, 4, 1480},
        {1250, 2250, 5, 7, 5, 1350, 7, 5, 1350},      {1250, 2250, 12, 5, 12, 1475, 4, 12, 1501},
        {1250, 2250, 5, 7, 5, 1350, 8, 5, 1750},      {1250, 2250, 12, 4, 12, 1501, 5, 12, 1475},
        {1250, 2250, 12, 4, 12, 1501, 4, 12, 1501},   {1250, 2250, 3, 10, 3, 1650, 10, 3, 1650},
        {1250, 2250, 3, 10, 3, 1650, 11, 3, 2240},    {1250, 2250, 5, 8, 5, 1750, 7, 5, 1350},
        {1250, 2250, 5, 8, 5, 1750, 8, 5, 1750},      {1250, 2250, 1, 9, 1, 1999, 9, 1, 1999},
        {1250, 2250, 20, 12, 20, 1987, 12, 20, 1987}, {1250, 2250, 20, 12, 20, 1987, 13, 20, 2010},
        {1250, 2250, 3, 11, 3, 2240, 10, 3, 1650},    {1250, 2250, 20, 13, 20, 2010, 12, 20, 1987},
        {1250, 2250, 3, 11, 3, 2240, 11, 3, 2240},    {1250, 2250, 20, 13, 20, 2010, 13, 20, 2010},
        {1250, 2250, 17, 14, 17, 2200, 14, 17, 2200}, {1500, 2500, 12, 4, 12, 1501, 4, 12, 1501},
        {1500, 2500, 3, 10, 3, 1650, 10, 3, 1650},    {1500, 2500, 3, 10, 3, 1650, 11, 3, 2240},
        {1500, 2500, 5, 8, 5, 1750, 8, 5, 1750},      {1500, 2500, 1, 9, 1, 1999, 9, 1, 1999},
        {1500, 2500, 20, 12, 20, 1987, 12, 20, 1987}, {1500, 2500, 20, 12, 20, 1987, 13, 20, 2010},
        {1500, 2500, 3, 11, 3, 2240, 10, 3, 1650},    {1500, 2500, 20, 13, 20, 2010, 12, 20, 1987},
        {1500, 2500, 3, 11, 3, 2240, 11, 3, 2240},    {1500, 2500, 20, 13, 20, 2010, 13, 20, 2010},
        {1500, 2500, 17, 14, 17, 2200, 14, 17, 2200}, {1500, 2500, 42, 17, 42, 2400, 17, 42, 2400},
        {1750, 2750, 5, 8, 5, 1750, 8, 5, 1750},      {1750, 2750, 1, 9, 1, 1999, 9, 1, 1999},
        {1750, 2750, 20, 12, 20, 1987, 12, 20, 1987}, {1750, 2750, 20, 12, 20, 1987, 13, 20, 2010},
        {1750, 2750, 20, 13, 20, 2010, 12, 20, 1987}, {1750, 2750, 3, 11, 3, 2240, 11, 3, 2240},
        {1750, 2750, 20, 13, 20, 2010, 13, 20, 2010}, {1750, 2750, 17, 14, 17, 2200, 14, 17, 2200},
        {1750, 2750, 17, 14, 17, 2200, 15, 17, 2600}, {1750, 2750, 42, 17, 42, 2400, 17, 42, 2400},
        {1750, 2750, 17, 15, 17, 2600, 14, 17, 2200}, {1750, 2750, 17, 15, 17, 2600, 15, 17, 2600},
        {2000, 3000, 3, 11, 3, 2240, 11, 3, 2240},    {2000, 3000, 20, 13, 20, 2010, 13, 20, 2010},
        {2000, 3000, 17, 14, 17, 2200, 14, 17, 2200}, {2000, 3000, 17, 14, 17, 2200, 15, 17, 2600},
        {2000, 3000, 17, 14, 17, 2200, 16, 17, 2800}, {2000, 3000, 42, 17, 42, 2400, 17, 42, 2400},
        {2000, 3000, 42, 17, 42, 2400, 18, 42, 2990}, {2000, 3000, 17, 15, 17, 2600, 14, 17, 2200},
        {2000, 3000, 17, 15, 17, 2600, 15, 17, 2600}, {2000, 3000, 17, 15, 17, 2600, 16, 17, 2800},
        {2000, 3000, 17, 16, 17, 2800, 14, 17, 2200}, {2000, 3000, 42, 18, 42, 2990, 17, 42, 2400},
        {2000, 3000, 17, 16, 17, 2800, 15, 17, 2600}, {2000, 3000, 17, 16, 17, 2800, 16, 17, 2800},
        {2000, 3000, 42, 18, 42, 2990, 18, 42, 2990}, {2250, 3250, 42, 17, 42, 2400, 17, 42, 2400},
        {2250, 3250, 42, 17, 42, 2400, 18, 42, 2990}, {2250, 3250, 17, 15, 17, 2600, 15, 17, 2600},
        {2250, 3250, 17, 15, 17, 2600, 16, 17, 2800}, {2250, 3250, 42, 18, 42, 2990, 17, 42, 2400},
        {2250, 3250, 17, 16, 17, 2800, 15, 17, 2600}, {2250, 3250, 17, 16, 17, 2800, 16, 17, 2800},
        {2250, 3250, 42, 18, 42, 2990, 18, 42, 2990}, {2500, 3500, 17, 15, 17, 2600, 15, 17, 2600},
        {2500, 3500, 17, 15, 17, 2600, 16, 17, 2800}, {2500, 3500, 17, 16, 17, 2800, 15, 17, 2600},
        {2500, 3500, 17, 16, 17, 2800, 16, 17, 2800}, {2500, 3500, 42, 18, 42, 2990, 18, 42, 2990},
        {2750, 3750, 17, 16, 17, 2800, 16, 17, 2800}, {2750, 3750, 42, 18, 42, 2990, 18, 42, 2990}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
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
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1window2Key;
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
            oss << window1window2Start << "," << window1window2End << "," << window1window2Key << "," << window1win1 << ","
                << window1id1 << "," << window1timestamp << "," << window2win2 << "," << window2id2 << "," << window2timestamp;
            return oss.str();
        }

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2Start == rhs.window1window2Start
                && window1window2End == rhs.window1window2End && window1window2Key == rhs.window1window2Key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };
    const auto inputSchemaLeft = Schema::create()
                                     ->addField(createField("test1$win1", BasicType::UINT64))
                                     ->addField(createField("test1$id1", BasicType::UINT64))
                                     ->addField(createField("test1$timestamp", BasicType::UINT64));
    const auto inputSchemaRight = Schema::create()
                                      ->addField(createField("test2$win2", BasicType::UINT64))
                                      ->addField(createField("test2$id2", BasicType::UINT64))
                                      ->addField(createField("test2$timestamp", BasicType::UINT64));
    const auto inputSchemaThird = Schema::create()
                                      ->addField(createField("test3$win3", BasicType::UINT64))
                                      ->addField(createField("test3$id3", BasicType::UINT64))
                                      ->addField(createField("test3$timestamp", BasicType::UINT64));
    const auto inputSchemaFourth = Schema::create()
                                       ->addField(createField("test4$win4", BasicType::UINT64))
                                       ->addField(createField("test4$id4", BasicType::UINT64))
                                       ->addField(createField("test4$timestamp", BasicType::UINT64));
    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameThird = "test3$id3";
    const auto joinFieldNameFourth = "test4$id4";
    const auto joinSchemaLeftRight =
        Runtime::Execution::Util::createJoinSchema(inputSchemaLeft, inputSchemaRight, joinFieldNameLeft);
    const auto joinSchemaLeftRightThird =
        Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRight, inputSchemaThird, joinFieldNameThird);
    const auto outputSchema =
        Runtime::Execution::Util::createJoinSchema(joinSchemaLeftRightThird, inputSchemaFourth, joinFieldNameFourth);

    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersThird("window4.csv");
    const std::string fileNameBuffersFourth("window4.csv");

    const std::vector<ResultRecord> expectedTuples = {
        {500, 1500, 12, 500, 1500, 12, 500, 1500, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {500, 1500, 4, 500, 1500, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 4, 500, 1500, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 12, 500, 1500, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {500, 1500, 4, 500, 1500, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 4, 500, 1500, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 12, 1000, 2000, 12, 500, 1500, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {500, 1500, 4, 1000, 2000, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 4, 1000, 2000, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 12, 1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {500, 1500, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {500, 1500, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 500, 1500, 12, 500, 1500, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {1000, 2000, 4, 500, 1500, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 500, 1500, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 500, 1500, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {1000, 2000, 4, 500, 1500, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 500, 1500, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 500, 1500, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {1000, 2000, 4, 1000, 2000, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 500, 1500, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {2500, 3500, 11, 2500, 3500, 11, 2500, 3500, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {2500, 3500, 11, 2500, 3500, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {2500, 3500, 11, 3000, 4000, 11, 2500, 3500, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {2500, 3500, 11, 3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {3000, 4000, 11, 2500, 3500, 11, 2500, 3500, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {3000, 4000, 11, 2500, 3500, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {3000, 4000, 11, 3000, 4000, 11, 2500, 3500, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {3000, 4000, 11, 3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {11500, 12500, 1, 11500, 12500, 1, 11500, 12500, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {11500, 12500, 1, 11500, 12500, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {11500, 12500, 1, 12000, 13000, 1, 11500, 12500, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {11500, 12500, 1, 12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {12000, 13000, 1, 11500, 12500, 1, 11500, 12500, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {12000, 13000, 1, 11500, 12500, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {12000, 13000, 1, 12000, 13000, 1, 11500, 12500, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {12000, 13000, 1, 12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {12500, 13500, 1, 12500, 13500, 1, 12500, 13500, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {12500, 13500, 1, 12500, 13500, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {12500, 13500, 1, 13000, 14000, 1, 12500, 13500, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {12500, 13500, 1, 13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {13000, 14000, 1, 12500, 13500, 1, 12500, 13500, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {13000, 14000, 1, 12500, 13500, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {13000, 14000, 1, 13000, 14000, 1, 12500, 13500, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {13000, 14000, 1, 13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {13500, 14500, 1, 13500, 14500, 1, 13500, 14500, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {13500, 14500, 1, 13500, 14500, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {13500, 14500, 1, 14000, 15000, 1, 13500, 14500, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {13500, 14500, 1, 14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {14000, 15000, 1, 13500, 14500, 1, 13500, 14500, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {14000, 15000, 1, 13500, 14500, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {14000, 15000, 1, 14000, 15000, 1, 13500, 14500, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {14000, 15000, 1, 14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {14500, 15500, 1, 14500, 15500, 1, 14500, 15500, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {14500, 15500, 1, 14500, 15500, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {14500, 15500, 1, 15000, 16000, 1, 14500, 15500, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {14500, 15500, 1, 15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {15000, 16000, 1, 14500, 15500, 1, 14500, 15500, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {15000, 16000, 1, 14500, 15500, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {15000, 16000, 1, 15000, 16000, 1, 14500, 15500, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {15000, 16000, 1, 15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000}};

    // Creating sink, source, and the query
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(outputSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(inputSchemaLeft);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(inputSchemaRight);
    const auto testSourceDescriptorThird = executionEngine->createDataSource(inputSchemaThird);
    const auto testSourceDescriptorFourth = executionEngine->createDataSource(inputSchemaFourth);
    const auto query = TestQuery::from(testSourceDescriptorLeft)
                           .joinWith(TestQuery::from(testSourceDescriptorRight))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(TestQuery::from(testSourceDescriptorThird))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(TestQuery::from(testSourceDescriptorFourth))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id4"))
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