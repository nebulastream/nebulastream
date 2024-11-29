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
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <gmock/gmock-matchers.h>
#include <iostream>

namespace NES::Runtime::Execution {

using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class StreamIntervalJoinQueryExecutionTest
    : public Testing::BaseUnitTest,
      public ::testing::WithParamInterface<
          std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr DecomposedQueryId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamIntervalJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup StreamIntervalJoinQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup StreamIntervalJoinQueryExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto numWorkerThreads = 1;
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down StreamIntervalJoinQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs an interval join query with the inputs (Schema, CSVFile).
     * Note: Currently, we only support csv files as the input
     * @return Vector of ResultRecords
     */
    template<typename ResultRecord>
    std::vector<ResultRecord> runQueryWithCsvFiles(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                                   const uint64_t expectedNumberOfTuples,
                                                   const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                                   const Query& query,
                                                   const uint64_t bufferSize = 8196) {

        // Creating the input buffers
        auto bufferManager = std::make_shared<BufferManager>(bufferSize);// executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (auto [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::createExpectedBuffersFromCsv(fileNameInputBuffers, inputSchema, bufferManager));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                               defaultSharedQueryId,
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers
        auto dataSourceCount = 0_u64;
        for (const auto& inputBuffers : allInputBuffers) {
            auto source = executionEngine->getDataSource(plan, dataSourceCount);
            dataSourceCount++;
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query");
        EXPECT_TRUE(executionEngine->stopQuery(plan, Runtime::QueryTerminationType::Graceful));
        testSink->waitTillCompleted(expectedNumberOfTuples);

        // Checking for correctness
        return testSink->getResult();
    }

    template<typename ResultRecord>
    std::vector<ResultRecord> runSingleJoinQuery(const TestUtils::CsvFileParams& csvFileParams,
                                                 const TestUtils::JoinParams& joinParams,
                                                 const std::string keyFieldNames[2],
                                                 const int64_t lowerBoundMilliSeconds,
                                                 const int64_t upperBoundMilliSeconds,
                                                 const string timestampFieldName,
                                                 const uint64_t bufferSize = 8196) {
        // Getting the expected output tuples
        auto bufferManager = executionEngine->getBufferManager();
        std::vector<ResultRecord> expectedSinkVector;
        const auto expectedSinkBuffers =
            TestUtils::createExpectedBuffersFromCsv(csvFileParams.expectedFile, joinParams.outputSchema, bufferManager);

        for (const auto& buf : expectedSinkBuffers) {
            const auto tmpVec = TestUtils::createVecFromTupleBuffer<ResultRecord>(buf);
            expectedSinkVector.insert(expectedSinkVector.end(), tmpVec.begin(), tmpVec.end());
        }

        const auto testSink = executionEngine->createCollectSink<ResultRecord>(joinParams.outputSchema);
        const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
        const auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
        const auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

        auto query = TestQuery::from(testSourceDescriptorLeft)
                         .intervalJoinWith(TestQuery::from(testSourceDescriptorRight))
                         .where(Attribute(keyFieldNames[0]) == Attribute(keyFieldNames[1]))
                         .lowerBound(EventTime(Attribute(timestampFieldName)), lowerBoundMilliSeconds, Milliseconds())
                         .upperBound(upperBoundMilliSeconds, Milliseconds())
                         .sink(testSinkDescriptor);

        // Running the query
        auto resultRecords = runQueryWithCsvFiles<ResultRecord>({{joinParams.inputSchemas[0], csvFileParams.inputCsvFiles[0]},
                                                                 {joinParams.inputSchemas[1], csvFileParams.inputCsvFiles[1]}},
                                                                expectedSinkVector.size(),
                                                                testSink,
                                                                query,
                                                                bufferSize);

        // Checking for correctness
        EXPECT_EQ(resultRecords.size(), expectedSinkVector.size());
        EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedSinkVector));
        return resultRecords;
    }
};

/**
 * This test checks a simple IntervalJoin version, where intervals are only created in the future (after occurring of a left tuple)
 * (lowerB and upperB >= 0) and the left tuple arrives before the right tuples belonging to the interval
 */
TEST_F(StreamIntervalJoinQueryExecutionTest, streamIntervalJoinExecutionTestOne) {

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)//Join key left
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)// Join key right
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const int64_t lowerBoundMilliseconds = 0;
    const int64_t upperBoundMilliseconds = 3;
    const auto timestampFieldName = "timestamp";

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_interval_join_left_test_1.csv",
                                           "stream_interval_join_right_test_1.csv",
                                           "stream_interval_join_sink_test_1.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::string keyFieldNames[2] = {"f2_left", "f2_right"};
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams,
                                                                joinParams,
                                                                keyFieldNames,
                                                                lowerBoundMilliseconds,
                                                                upperBoundMilliseconds,
                                                                timestampFieldName);
}

/**
 * This test manipulates the input buffer size to tests if the IVJoin handles right tuples occurring before the left tuples, i.e.,
 * storing and adding them to the interval when the left tuple arrives after the right. (lowerB and upperB >= 0)
 */
TEST_F(StreamIntervalJoinQueryExecutionTest, streamIntervalJoinExecutionTestTwo) {

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)//Join key left
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)// Join key right
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const int64_t lowerBoundMilliseconds = 0;
    const int64_t upperBoundMilliseconds = 3;
    const uint64_t bufferSize = 64;
    const auto timestampFieldName = "timestamp";

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_interval_join_left_test_2.csv",
                                           "stream_interval_join_right_test_2.csv",
                                           "stream_interval_join_sink_test_2.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::string keyFieldNames[2] = {"f2_left", "f2_right"};
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams,
                                                                joinParams,
                                                                keyFieldNames,
                                                                lowerBoundMilliseconds,
                                                                upperBoundMilliseconds,
                                                                timestampFieldName,
                                                                bufferSize);
}

/**
 * This test creates an IVJoin that only creates intervals for the past (lowerB and upperB < 0)
 */
TEST_F(StreamIntervalJoinQueryExecutionTest, streamIntervalJoinExecutionTestThree) {

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)//Join key left
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)// Join key right
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const int64_t lowerBoundMilliseconds = -4;
    const int64_t upperBoundMilliseconds = -1;
    const auto timestampFieldName = "timestamp";

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_interval_join_left_test_3.csv",
                                           "stream_interval_join_right_test_3.csv",
                                           "stream_interval_join_sink_test_3.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::string keyFieldNames[2] = {"f2_left", "f2_right"};
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams,
                                                                joinParams,
                                                                keyFieldNames,
                                                                lowerBoundMilliseconds,
                                                                upperBoundMilliseconds,
                                                                timestampFieldName);
}

/**
 * This test creates an IVJoin with bounds into the past and future and test different schemas
 */
TEST_F(StreamIntervalJoinQueryExecutionTest, streamIntervalJoinExecutionTestFour) {

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2f3_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right && test2f3_right == rhs.test2f3_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)//Join key left
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)// Join key right
                                 ->addField("f3_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const int64_t lowerBoundMilliseconds = -6;
    const int64_t upperBoundMilliseconds = 2;
    const auto timestampFieldName = "timestamp";

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_interval_join_left_test_4.csv",
                                           "stream_interval_join_right_test_4.csv",
                                           "stream_interval_join_sink_test_4.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::string keyFieldNames[2] = {"f2_left", "f2_right"};
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams,
                                                                joinParams,
                                                                keyFieldNames,
                                                                lowerBoundMilliseconds,
                                                                upperBoundMilliseconds,
                                                                timestampFieldName);
}

/**
 * This test creates an IVJoin with an interval including exactly 1 timestamp (lB = uB = 0 = leftstream.timestamp)
 */
TEST_F(StreamIntervalJoinQueryExecutionTest, streamIntervalJoinExecutionTestFive) {

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)//Join key left
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)// Join key right
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const int64_t lowerBoundMilliseconds = 0;
    const int64_t upperBoundMilliseconds = 0;
    const auto timestampFieldName = "timestamp";

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_interval_join_left_test_1.csv",
                                           "stream_interval_join_right_test_1.csv",
                                           "stream_interval_join_sink_test_5.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::string keyFieldNames[2] = {"f2_left", "f2_right"};
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams,
                                                                joinParams,
                                                                keyFieldNames,
                                                                lowerBoundMilliseconds,
                                                                upperBoundMilliseconds,
                                                                timestampFieldName);
}

}// namespace NES::Runtime::Execution
