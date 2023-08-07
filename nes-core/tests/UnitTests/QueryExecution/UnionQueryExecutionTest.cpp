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
#include "API/Expressions/Expressions.hpp"
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>

using namespace NES;

constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class UnionQueryExecutionTest : public Testing::TestWithErrorHandling,
                                public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FilterQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("FilterQueryExecutionTest: Setup FilterQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test class."); }

    void createSchemaAndSink(const uint64_t numResultBuffers) {
        schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
        expectedResultBuffers = numResultBuffers;
        testSink = executionEngine->createDataSink(schema, expectedResultBuffers);
        testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, const uint64_t numberOfTuples = 10) {
        for (uint64_t recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
        }
        buf.setNumberOfTuples(numberOfTuples);
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer
    runQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> queryPlan, const uint64_t numInputTuples = 10, 
             uint64_t timeoutInMilliseconds = 500) {
        // Setup first source and emit buffer
        auto source1 = executionEngine->getDataSource(queryPlan, 0);
        auto inputBuffer1 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer1, numInputTuples);
        source1->emitBuffer(inputBuffer1);

        // Setup second source and emit buffer
        auto source2 = executionEngine->getDataSource(queryPlan, 1);
        auto inputBuffer2 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer2, numInputTuples);
        source2->emitBuffer(inputBuffer2);

        // Wait until the sink processed all tuples
        testSink->waitTillCompletedOrTimeout(timeoutInMilliseconds);
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), expectedResultBuffers);

        // Merge the result buffers, create a dynamic result buffer and check whether it contains the unified tuples.
        auto mergedBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, schema, executionEngine->getBufferManager());
        return Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(mergedBuffer, schema);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    SchemaPtr schema;
    std::shared_ptr<TestSink> testSink;
    uint64_t expectedResultBuffers;
    std::shared_ptr<TestUtils::TestSinkDescriptor> testSinkDescriptor;
};

TEST_P(UnionQueryExecutionTest, unionOperatorWithFilterOnUnionResult) {
    createSchemaAndSink(2);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                      .filter(Attribute("test$id") > 5)
                      .sink(testSinkDescriptor);

    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 8);
    for (uint32_t recordIndex = 0u; recordIndex < 8u; ++recordIndex) {
        NES_DEBUG("Result: {} at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), (recordIndex % 4) + 6);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

TEST_P(UnionQueryExecutionTest, unionOperatorWithFilterOnSources) {
    createSchemaAndSink(2);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query subQuery = TestQuery::from(testSourceDescriptor2).filter(Attribute("test$id") > 3);
    Query query =
        TestQuery::from(testSourceDescriptor1).filter(Attribute("test$id") > 7).unionWith(subQuery).sink(testSinkDescriptor);
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 8);
    for (uint32_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex + 8);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }
    for (uint32_t recordIndex = 2u; recordIndex < 8u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex + 2);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

TEST_P(UnionQueryExecutionTest, unionOperatorWithoutExecution) {
    createSchemaAndSink(2);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query query =
        TestQuery::from(testSourceDescriptor1).unionWith(TestQuery::from(testSourceDescriptor2)).sink(testSinkDescriptor);
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan);

    for (uint32_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
        NES_DEBUG("Result: {} at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex % 10);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

TEST_P(UnionQueryExecutionTest, unionOperatorWithoutDifferentSchemasAndManualProject) {
    createSchemaAndSink(2);
    auto schema2 = Schema::create()->addField("test2$id", BasicType::INT64)->addField("test2$one", BasicType::INT64);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema2);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2)
                                     .project(Attribute("test2$id").as("test$id"), Attribute("test2$one").as("test$one")))
                      .sink(testSinkDescriptor);
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan);

    for (uint32_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
        NES_DEBUG("Result: {} at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex % 10);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

TEST_P(UnionQueryExecutionTest, unionOperatorWithoutResults) {
    createSchemaAndSink(0);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                      .filter(Attribute("test$id") > 9)
                      .sink(testSinkDescriptor);
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan, 10, 500);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 0u);

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

INSTANTIATE_TEST_CASE_P(testFilterQueries,
                        UnionQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<UnionQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
