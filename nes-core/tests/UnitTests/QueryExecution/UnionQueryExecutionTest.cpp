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
// clang-format: on
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

//Todo: rename test class
// Dump IR
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

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
        }
        buf.setNumberOfTuples(10);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

TEST_P(UnionQueryExecutionTest, unionOperatorFailing) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);
    auto testSink = executionEngine->createDataSink(schema, 2);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                    //   .filter(Attribute("test$id") >= 0)
                      .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    // Setup first source and emit buffer
    auto source1 = executionEngine->getDataSource(plan, 0);
    auto inputBuffer1 = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer1);
    source1->emitBuffer(inputBuffer1);

    // Setup second source and emit buffer
    auto source2 = executionEngine->getDataSource(plan, 1);
    auto inputBuffer2 = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer2);
    source2->emitBuffer(inputBuffer2);

    // Wait until the sink processed all tuples
    //Todo: #4043 this blocks indefinitely if no tuples pass the filter
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2u);

    // Todo: check if merging actually works correctly
    // auto resultBuffer = 
    //     TestUtils::mergeBuffers(testSink->resultBuffers, schema, executionEngine->getBufferManager());
    // auto layout = NES::Runtime::MemoryLayouts::ColumnLayout::create(schema, executionEngine->getBufferManager()->getBufferSize());
    // auto dynamicResultBuffer = *std::make_unique<Runtime::MemoryLayouts::DynamicTupleBuffer>(layout, resultBuffer);

    auto resultBuffer1 = testSink->getResultBuffer(0);
    auto resultBuffer2 = testSink->getResultBuffer(1);
    EXPECT_EQ(resultBuffer1.getNumberOfTuples(), 10u);
    EXPECT_EQ(resultBuffer2.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer1[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer2[recordIndex][0].read<int64_t>(), recordIndex);
    }

    ASSERT_TRUE(executionEngine->stopQuery(plan));
}

//Todo: #4043: Currently, we cannot use the TestSink to check whether a query does not produce any results
// -> could call writeData with a nullptr to signalize that no TupleBuffer was produced
TEST_P(UnionQueryExecutionTest, DISABLED_unionOperatorWithoutResults) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);
    auto testSink = executionEngine->createDataSink(schema, 0);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                      .filter(Attribute("test$id") > 9)
                      .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    // Setup first source and emit buffer
    auto source1 = executionEngine->getDataSource(plan, 0);
    auto inputBuffer1 = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer1);
    source1->emitBuffer(inputBuffer1);

    // Setup second source and emit buffer
    auto source2 = executionEngine->getDataSource(plan, 1);
    auto inputBuffer2 = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer2);
    source2->emitBuffer(inputBuffer2);

    // Wait until the sink processed all tuples
    //Todo: #4040 this probably blocks if no tuples pass the filter
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0u);

    auto resultBuffer = 
        TestUtils::mergeBuffers(testSink->resultBuffers, schema, executionEngine->getBufferManager());
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 0u);
    // Todo: check that results are correct
    ASSERT_TRUE(executionEngine->stopQuery(plan));
}

INSTANTIATE_TEST_CASE_P(testFilterQueries,
                        UnionQueryExecutionTest,
                        ::testing::Values(
                                        //   QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<UnionQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
