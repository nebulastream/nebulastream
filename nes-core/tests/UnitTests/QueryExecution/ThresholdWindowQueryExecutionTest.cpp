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
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

class ThresholdWindowQueryExecutionTest
    : public Testing::TestWithErrorHandling<testing::Test>,
      public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup ThresholdWindowQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down ThresholdWindowQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down ThresholdWindowQueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 9; recordIndex++) {
        buf[recordIndex][0].write<int64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(recordIndex * 10);
    }
    // close the window
    buf[9][0].write<int64_t>(0);
    buf[9][1].write<int64_t>(0);
    buf.setNumberOfTuples(10);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTest) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$sum", BasicType::INT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Sum(Attribute("test$f2", INT64))->as(Attribute("test$sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 210LL);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}