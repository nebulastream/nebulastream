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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <gmock/gmock-matchers.h>

namespace NES::Runtime::Execution {

/**
 * @brief Creates a schema that consists of (value, id, timestamp) with all fields being a UINT64
 * @return SchemaPtr
 */
static SchemaPtr createValueIdTimeStamp() {
    return Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
        ->addField("value", BasicType::UINT64)
        ->addField("id", BasicType::UINT64)
        ->addField("timestamp", BasicType::UINT64);
}

class JoinDeploymentTest : public Testing::BaseIntegrationTest,
                           public ::testing::WithParamInterface<std::tuple<QueryCompilation::StreamJoinStrategy,
                                                                           QueryCompilation::WindowingStrategy>> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
        BaseIntegrationTest::SetUp();

        joinStrategy = std::get<0>(NES::Runtime::Execution::JoinDeploymentTest::GetParam());
        windowingStrategy = std::get<1>(NES::Runtime::Execution::JoinDeploymentTest::GetParam());
        bufferManager = std::make_shared<BufferManager>();
    }

    template<typename ResultRecord>
    std::vector<ResultRecord> runJoinQueryTwoLogicalStreams(const Query& query,
                                                            const TestUtils::CsvFileParams& csvFileParams,
                                                            const TestUtils::JoinParams& joinParams) {
        auto sourceConfig1 = TestUtils::createSourceConfig(csvFileParams.inputCsvFiles[0]);
        auto sourceConfig2 = TestUtils::createSourceConfig(csvFileParams.inputCsvFiles[1]);
        auto expectedSinkBuffer =
            TestUtils::fillBufferFromCsv(csvFileParams.expectedFile, joinParams.outputSchema, bufferManager)[0];
        auto expectedSinkVector = TestUtils::createVecFromTupleBuffer<ResultRecord>(expectedSinkBuffer);
        EXPECT_EQ(sizeof(ResultRecord), joinParams.outputSchema->getSchemaSizeInBytes());

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .enableNautilus()
                                      .setJoinStrategy(joinStrategy)
                                      .setWindowingStrategy(windowingStrategy)
                                      .addLogicalSource("test1", joinParams.inputSchemas[0])
                                      .addLogicalSource("test2", joinParams.inputSchemas[1])
                                      .attachWorkerWithCSVSourceToCoordinator("test1", sourceConfig1)
                                      .attachWorkerWithCSVSourceToCoordinator("test2", sourceConfig2)
                                      .validate()
                                      .setupTopology();

        auto actualResult = testHarness.getOutput<ResultRecord>(expectedSinkBuffer.getNumberOfTuples());


        EXPECT_EQ(actualResult.size(), expectedSinkBuffer.getNumberOfTuples());
        EXPECT_THAT(actualResult, ::testing::UnorderedElementsAreArray(expectedSinkVector));

        return actualResult;
    }

    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
};

/**
* Test deploying join with same data and same schema
 * */
TEST_P(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    TestUtils::JoinParams joinParams(createValueIdTimeStamp(), createValueIdTimeStamp(), "id", "id");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink2.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Id == rhs.test2Id && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink3.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceSlidingWindow) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Value;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Value == rhs.test2Value && test2Id == rhs.test2Id
                && test2Timestamp == rhs.test2Timestamp;
        }
    };

    TestUtils::JoinParams joinParams(createValueIdTimeStamp(), createValueIdTimeStamp(), "id", "id");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink5.csv");
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    auto query =
        Query::from("test1")
            .joinWith(Query::from("test2"))
            .where(Attribute("id"))
            .equalsTo(Attribute("id"))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(windowSize), Milliseconds(windowSlide)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testSlidingWindowDifferentAttributes) {
    struct ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        uint64_t test1Value;
        uint64_t test1Id;
        uint64_t test1Timestamp;
        uint64_t test2Id;
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Value == rhs.test1Value && test1Id == rhs.test1Id
                && test1Timestamp == rhs.test1Timestamp && test2Id == rhs.test2Id && test2Timestamp == rhs.test2Timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink6.csv");
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    auto query =
        Query::from("test1")
            .joinWith(Query::from("test2"))
            .where(Attribute("id1"))
            .equalsTo(Attribute("id2"))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(windowSize), Milliseconds(windowSlide)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
// TODO this test can be enabled once #3638 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithFixedCharKey) {
    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;
        uint64_t test1test2Key;
        char test1Id[7];
        uint64_t test1Timestamp;
        char test2Id[7];
        uint64_t test2Timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1test2Key == rhs.test1test2Key && test1Timestamp == rhs.test1Timestamp
                && (std::strcmp(test1Id, rhs.test1Id) == 0) && (std::strcmp(test2Id, rhs.test2Id) == 0)
                && test2Timestamp == rhs.test2Timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$id1", BasicType::TEXT)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::TEXT)
                                 ->addField("test2$timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window5.csv", "window6.csv", "window_sink4.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams<ResultRecord>(query, csvFileParams, joinParams);
}

INSTANTIATE_TEST_CASE_P(
    testJoinQueries,
    JoinDeploymentTest,
    JOIN_STRATEGIES_WINDOW_STRATEGIES,
    [](const testing::TestParamInfo<JoinDeploymentTest::ParamType>& info) {
        return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_" +
               std::string(magic_enum::enum_name(std::get<1>(info.param)));
    });
}// namespace NES::Runtime::Execution