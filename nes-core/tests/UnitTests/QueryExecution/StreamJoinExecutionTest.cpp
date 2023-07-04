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

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
namespace NES::Runtime::Execution {

constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class StreamJoinQueryExecutionTest : public Testing::TestWithErrorHandling,
                                     public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO2("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO2("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test class."); }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

std::istream& operator>>(std::istream& is, std::string& l) {
    std::getline(is, l);
    return is;
}

/**
 * @brief checks if the buffers contain the same tuples
 * @param buffer1
 * @param buffer2
 * @param schema
 * @return boolean if the buffers contain the same tuples
 */
bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, const uint64_t schemaSizeInByte) {
    NES_DEBUG2("Checking if the buffers are equal, so if they contain the same tuples");
    if (buffer1.getNumberOfTuples() != buffer2.getNumberOfTuples()) {
        NES_DEBUG2("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<size_t> sameTupleIndices;
    for (auto idxBuffer1 = 0UL; idxBuffer1 < buffer1.getNumberOfTuples(); ++idxBuffer1) {
        bool idxFoundInBuffer2 = false;
        for (auto idxBuffer2 = 0UL; buffer2.getNumberOfTuples(); ++idxBuffer2) {
            if (sameTupleIndices.contains(idxBuffer2)) {
                continue;
            }
            auto startPosBuffer1 = buffer1.getBuffer() + schemaSizeInByte * idxBuffer1;
            auto startPosBuffer2 = buffer2.getBuffer() + schemaSizeInByte * idxBuffer2;
            auto equalTuple = (memcmp(startPosBuffer1, startPosBuffer2, schemaSizeInByte) == 0);
            if (equalTuple) {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2) {
            NES_DEBUG2("Buffers do not contain the same tuples, as tuple could not be found in both buffers!");
            return false;
        }
    }

    return (sameTupleIndices.size() == buffer1.getNumberOfTuples());
}

TEST_P(StreamJoinQueryExecutionTest, streamJoinExecutiontTestCsvFiles) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$f1_left", BasicType::UINT64)
                                ->addField("test1$f2_left", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$f1_right", BasicType::UINT64)
                                 ->addField("test2$f2_right", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$f2_left";
    const auto joinFieldNameRight = "test2$f2_right";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 20UL;
    const std::string fileNameBuffersLeft("stream_join_left.csv");
    const std::string fileNameBuffersRight("stream_join_right.csv");
    const std::string fileNameBuffersSink("stream_join_sink.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBufferFromCsv(fileNameBuffersLeft, leftSchema, bufferManager)[0];
    auto rightBuffer = fillBufferFromCsv(fileNameBuffersRight, rightSchema, bufferManager)[0];
    auto expectedSinkBuffer = fillBufferFromCsv(fileNameBuffersSink, joinSchema, bufferManager)[0];

    auto testSink = executionEngine->createDataSink(joinSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.setWatermark(1000);
    leftBuffer.setOriginId(2);
    leftBuffer.setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.setWatermark(1000);
    rightBuffer.setOriginId(3);
    rightBuffer.setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);

    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto resultBuffer = testSink->getResultBuffer(0);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer.getBuffer(), joinSchema));
    NES_DEBUG2("expectedSinkBuffer: {}", NES::Util::printTupleBufferAsCSV(expectedSinkBuffer, joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(
        checkIfBuffersAreEqual(resultBuffer.getBuffer(), expectedSinkBuffer, joinSchema->getSchemaSizeInBytes()));
}

TEST_P(StreamJoinQueryExecutionTest, DISABLED_streamJoinExecutiontTestWithWindows) {
    const auto leftInputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                     ->addField("test1$f1_left", BasicType::INT64)
                                     ->addField("test1$f2_left", BasicType::INT64)
                                     ->addField("test1$timestamp", BasicType::INT64)
                                     ->addField("test1$fieldForSum1", BasicType::INT64);

    const auto rightInputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                      ->addField("test2$f1_right", BasicType::INT64)
                                      ->addField("test2$f2_right", BasicType::INT64)
                                      ->addField("test2$timestamp", BasicType::INT64)
                                      ->addField("test2$fieldForSum2", BasicType::INT64);

    const auto sinkSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1test2$start", BasicType::INT64)
                                ->addField("test1test2$end", BasicType::INT64)
                                ->addField("test1test2$key", BasicType::INT64)

                                ->addField("test1$start", BasicType::INT64)
                                ->addField("test1$end", BasicType::INT64)
                                ->addField("test1$cnt", BasicType::INT64)
                                ->addField("test1$f1_left", BasicType::INT64)
                                ->addField("test1$f2_left", BasicType::INT64)
                                ->addField("test1$timestamp", BasicType::INT64)
                                ->addField("test1$fieldForSum1", BasicType::INT64)

                                ->addField("test2$start", BasicType::INT64)
                                ->addField("test2$end", BasicType::INT64)
                                ->addField("test2$cnt", BasicType::INT64)
                                ->addField("test2$f1_right", BasicType::INT64)
                                ->addField("test2$f2_right", BasicType::INT64)
                                ->addField("test2$timestamp", BasicType::INT64)
                                ->addField("test2$fieldForSum2", BasicType::INT64);

    const auto joinFieldNameLeft = "test1$f2_left";
    const auto joinFieldNameRight = "test2$f2_right";
    const auto timeStampField = "timestamp";

    //    const auto sinkSchema = Schema::create()->addField("test$sum", BasicType::INT64);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 10UL;
    const std::string fileNameBuffersLeft("stream_join_left_withSum.csv");
    const std::string fileNameBuffersRight("stream_join_right_withSum.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBufferFromCsv(fileNameBuffersLeft, leftInputSchema, bufferManager)[0];
    auto rightBuffer = fillBufferFromCsv(fileNameBuffersRight, rightInputSchema, bufferManager)[0];

    auto testSink = executionEngine->createDataSink(sinkSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftInputSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightInputSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .byKey(Attribute(joinFieldNameLeft))
                     .apply(Sum(Attribute("test1$fieldForSum1")))
                     .joinWith(TestQuery::from(testSourceDescriptorRight)
                                   .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                                   .byKey(Attribute(joinFieldNameRight))
                                   .apply(Sum(Attribute("test2$fieldForSum2"))))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute("test1$start")), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO2("Submitting query: {}", query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    leftBuffer.setWatermark(1000);
    leftBuffer.setOriginId(2);
    leftBuffer.setSequenceNumber(1);
    sourceLeft->emitBuffer(leftBuffer);

    rightBuffer.setWatermark(1000);
    rightBuffer.setOriginId(3);
    rightBuffer.setSequenceNumber(1);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    //    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto resultBuffer = testSink->getResultBuffer(0);

    NES_DEBUG2("resultBuffer: {}", NES::Util::printTupleBufferAsCSV(resultBuffer.getBuffer(), sinkSchema));
    if (testSink->getNumberOfResultBuffers() == 2) {
        NES_DEBUG2("resultBuffer1: {}", NES::Util::printTupleBufferAsCSV(testSink->getResultBuffer(1).getBuffer(), sinkSchema));
    }
}

INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQueryExecutionTest,
                        ::testing::Values(//QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                            QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<StreamJoinQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });

}// namespace NES::Runtime::Execution