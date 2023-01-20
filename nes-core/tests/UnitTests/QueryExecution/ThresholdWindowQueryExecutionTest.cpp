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

template<typename T>
void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 9; recordIndex++) {
        buf[recordIndex][0].write<int64_t>(recordIndex);
        buf[recordIndex][1].write<T>(recordIndex * 10);
        NES_DEBUG("Input tuples: f1=" << recordIndex << " f2=" << recordIndex * 10);
    }
    // close the window
    buf[9][0].write<int64_t>(0);
    buf[9][1].write<T>(0);
    buf.setNumberOfTuples(10);
    NES_DEBUG("Input tuples: f1=" << 0 << " f2=" << 0);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestSum) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$sum", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Sum(Attribute("test$f2", INT64))->as(Attribute("test$Sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int64_t>(inputBuffer);

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

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestWithMax) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Max", BasicType::INT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Max(Attribute("test$f2", INT64))->as(Attribute("test$Max")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int64_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 80LL);// Max

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestWithMin) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Min", BasicType::INT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Min(Attribute("test$f2", INT64))->as(Attribute("test$Min")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int64_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 60LL);// Min

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestWithAvg) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Avg", BasicType::INT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 6))
                     .apply(Avg(Attribute("test$f2", INT64))->as(Attribute("test$Avg")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int64_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 75LL);// Avg

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestWithCount) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Count", BasicType::INT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Count()->as(Attribute("test$Count")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int64_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 3LL);// Count

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// Test with int32 data types //
TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestSumFloat) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::FLOAT32);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Sum", BasicType::FLOAT32);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Sum(Attribute("test$f2", FLOAT32))->as(Attribute("test$Sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<float_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<float_t>(), 210.0);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestSumInt32) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::INT32);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Sum", BasicType::INT32);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Sum(Attribute("test$f2", INT32))->as(Attribute("test$Sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<int32_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int32_t>(), 210);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// Test with double data types //
TEST_F(ThresholdWindowQueryExecutionTest, simpleThresholdWindowTestSumDouble) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::INT64)->addField("test$f2", BasicType::FLOAT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$Sum", BasicType::FLOAT64);
    auto testSink = executionEngine->createDateSink(sinkSchema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(ThresholdWindow::of(Attribute("test$f1") > 5))
                     .apply(Sum(Attribute("test$f2", FLOAT64))->as(Attribute("test$Sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer<double_t>(inputBuffer);

    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<double_t>(), 210.0);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// TODO 3280: parameterize test for all agg function and all data types