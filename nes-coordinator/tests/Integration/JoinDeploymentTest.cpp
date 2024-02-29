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
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
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
        ->addField("test1$value", BasicType::UINT64)
        ->addField("test1$id", BasicType::UINT64)
        ->addField("test1$timestamp", BasicType::UINT64);
}

class JoinDeploymentTest : public Testing::BaseIntegrationTest,
                           public ::testing::WithParamInterface<
                               std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy>> {
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

    void runJoinQueryTwoLogicalStreams(const Query& query,
                                       const TestUtils::CsvFileParams& csvFileParams,
                                       const TestUtils::JoinParams& joinParams) {

        const auto logicalSourceNameOne = "test1";
        const auto logicalSourceNameTwo = "test2";
        const auto physicalSourceNameOne = "test1_physical";
        const auto physicalSourceNameTwo = "test2_physical";
        auto sourceConfig1 =
            TestUtils::createSourceTypeCSV({logicalSourceNameOne, physicalSourceNameOne, csvFileParams.inputCsvFiles[0]});
        auto sourceConfig2 =
            TestUtils::createSourceTypeCSV({logicalSourceNameTwo, physicalSourceNameTwo, csvFileParams.inputCsvFiles[1]});

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .setJoinStrategy(joinStrategy)
                                      .setWindowingStrategy(windowingStrategy)
                                      .addLogicalSource(logicalSourceNameOne, joinParams.inputSchemas[0])
                                      .addLogicalSource(logicalSourceNameTwo, joinParams.inputSchemas[1])
                                      .attachWorkerWithCSVSourceToCoordinator(sourceConfig1)
                                      .attachWorkerWithCSVSourceToCoordinator(sourceConfig2)
                                      .validate()
                                      .setupTopology();

        // Run the query and get the actual dynamic buffers
        std::ifstream expectedFileStream(std::filesystem::path(TEST_DATA_DIRECTORY) / csvFileParams.expectedFile);
        auto actualBuffers =
            testHarness.validate().setupTopology().runQuery(NES::Util::countLines(expectedFileStream)).getOutput();

        // Comparing equality
        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromStream(expectedFileStream, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
        for (auto& buf : actualBuffers) {
            NES_INFO("Buf:\n{}", NES::Util::printTupleBufferAsCSV(buf.getBuffer(), joinParams.outputSchema));
        }
        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
    }

    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
};

/**
* Test deploying join with same data and same schema
 * */
TEST_P(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    TestUtils::JoinParams joinParams(createValueIdTimeStamp(), createValueIdTimeStamp(), "id", "id");
    TestUtils::CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceTumblingWindow) {
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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceSlidingWindow) {
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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testSlidingWindowDifferentAttributes) {
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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
TEST_P(JoinDeploymentTest, testJoinWithVarSizedData) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING ||
        joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE ||
        joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL) {
        GTEST_SKIP();
    }

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

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

TEST_P(JoinDeploymentTest, joinResultLargerThanSingleTupleBuffer) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value", BasicType::UINT64)
                                ->addField("id1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    TestUtils::JoinParams joinParams(leftSchema, rightSchema, "id1", "id2");
    TestUtils::CsvFileParams csvFileParams("window8.csv", "window9.csv", "window_sink8.csv");
    auto query = Query::from("test1")
                     .joinWith(Query::from("test2"))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000000)));

    runJoinQueryTwoLogicalStreams(query, csvFileParams, joinParams);
}

INSTANTIATE_TEST_CASE_P(testJoinQueries,
                        JoinDeploymentTest,
                        JOIN_STRATEGIES_WINDOW_STRATEGIES,
                        [](const testing::TestParamInfo<JoinDeploymentTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param)));
                        });
}// namespace NES::Runtime::Execution