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
// clang-format: off
#include "gtest/gtest.h"
// clang-format: on
#include <NesBaseTest.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>

namespace NES::Runtime::Execution {

class StreamJoinQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test>,
                                     public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};


std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

std::istream &operator>>(std::istream &is, std::string &l)
{
    std::getline(is, l);
    return is;
}

Runtime::MemoryLayouts::DynamicTupleBuffer fillBuffer(const std::string& csvFileName,
                     Runtime::MemoryLayouts::DynamicTupleBuffer buffer,
                     const SchemaPtr schema) {

    auto fullPath = std::string(TEST_DATA_DIRECTORY) + csvFileName;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(fullPath)), "File " << fullPath << " does not exist!!!");
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);

    std::ifstream inputFile(fullPath);
    std::istream_iterator<std::string> beginIt(inputFile);
    std::istream_iterator<std::string> endIt;
    for (auto it = beginIt; it != endIt; ++it) {
        std::string line = *it;
        parser->writeInputTupleToTupleBuffer(line, buffer.getNumberOfTuples(), buffer, schema);
    }

    NES_DEBUG("Created buffer \n" << Util::printTupleBufferAsCSV(buffer.getBuffer(), schema) << " from csvFile = " << fullPath);

    return buffer;
}

TEST_P(StreamJoinQueryExecutionTest, streamJoinExecutiontTestCsvFiles) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = leftSchema->get(1)->getName();
    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto timeStampField = leftSchema->get(2)->getName();
    ASSERT_EQ(leftSchema->get(2)->getName(), rightSchema->get(2)->getName());

    ASSERT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Schema::create(leftSchema->getLayoutType());
    joinSchema->addField("key", leftSchema->get(joinFieldNameLeft)->getDataType());
    joinSchema->copyFields(leftSchema);
    joinSchema->copyFields(rightSchema);


    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 20UL;
    const std::string fileNameBuffersLeft("stream_join_left.csv");
    const std::string fileNameBuffersRight("stream_join_right.csv");
    const std::string fileNameBuffersSink("stream_join_sink.csv");

    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema);

    auto testSink = executionEngine->createDateSink(joinSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft).joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    EXPECT_TRUE(memcmp(resultBuffer.getBuffer().getBuffer(), expectedSinkBuffer.getBuffer().getBuffer(),
                       expectedSinkBuffer.getNumberOfTuples() * joinSchema->getSchemaSizeInBytes()) == 0);

}


INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<StreamJoinQueryExecutionTest::ParamType>& info) {
                            return magic_enum::enum_flags_name(info.param);
                        });


} // namespace NES::Runtime::Execution