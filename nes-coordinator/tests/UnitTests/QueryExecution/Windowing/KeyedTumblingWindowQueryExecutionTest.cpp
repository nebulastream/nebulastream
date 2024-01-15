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

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::DumpMode::NONE;

class KeyedTumblingWindowQueryExecutionTest : public Testing::BaseUnitTest,
                                              public ::testing::WithParamInterface<QueryCompilation::QueryCompilerType> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedTumblingWindowQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup KeyedTumblingWindowQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down KeyedTumblingWindowQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("QueryExecutionTest: Tear down KeyedTumblingWindowQueryExecutionTest test class.");
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 9; recordIndex++) {
        buf[recordIndex][0].write<uint64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(recordIndex % 2);
        buf[recordIndex][2].write<int64_t>(recordIndex * 10);
    }
    // close the window
    buf[9][0].write<uint64_t>(0);
    buf[9][1].write<int64_t>(0);
    buf.setNumberOfTuples(10);
}

TEST_F(KeyedTumblingWindowQueryExecutionTest, singleKeyTumblingWindow) {
    const auto expectedNumberOfTuples = 2;
    auto sourceSchema = Schema::create()
                            ->addField("test$ts", BasicType::UINT64)
                            ->addField("test$key", BasicType::INT64)
                            ->addField("test$value", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$sum", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(sinkSchema, expectedNumberOfTuples);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(5)))
                     .byKey(Attribute("test$key", BasicType::INT64))
                     .apply(Sum(Attribute("test$value", BasicType::INT64))->as(Attribute("test$sum")))
                     .project(Attribute("test$sum"))
                     .sink(testSinkDescriptor);

    auto decomposedQueryPlan = DecomposedQueryPlan::create(1, 1);
    for (const auto& rootOperator : query.getQueryPlan()->getRootOperators()) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedNumberOfTuples);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 60LL);// sum
    EXPECT_EQ(resultBuffer[1][0].read<int64_t>(), 40LL);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(KeyedTumblingWindowQueryExecutionTest, singleKeyTumblingWindowNoProjection) {
    const auto expectedNumberOfTuples = 2;
    auto sourceSchema = Schema::create()
                            ->addField("test$ts", BasicType::UINT64)
                            ->addField("test$key", BasicType::INT64)
                            ->addField("test$value", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()
                          ->addField("test$start", BasicType::INT64)
                          ->addField("test$end", BasicType::INT64)
                          ->addField("test$key", BasicType::INT64)
                          ->addField("test$sum", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(sinkSchema, expectedNumberOfTuples);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(5)))
                     .byKey(Attribute("test$key", BasicType::INT64))
                     .apply(Sum(Attribute("test$value", BasicType::INT64))->as(Attribute("test$sum")))
                     .sink(testSinkDescriptor);

    auto decomposedQueryPlan = DecomposedQueryPlan::create(1, 1);
    for (const auto& rootOperator : query.getQueryPlan()->getRootOperators()) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedNumberOfTuples);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 0LL); // start
    EXPECT_EQ(resultBuffer[0][1].read<int64_t>(), 5LL); // end
    EXPECT_EQ(resultBuffer[0][2].read<int64_t>(), 0LL); // key
    EXPECT_EQ(resultBuffer[0][3].read<int64_t>(), 60LL);// sum

    EXPECT_EQ(resultBuffer[1][0].read<int64_t>(), 0LL); // start
    EXPECT_EQ(resultBuffer[1][1].read<int64_t>(), 5LL); // end
    EXPECT_EQ(resultBuffer[1][2].read<int64_t>(), 1LL); // key
    EXPECT_EQ(resultBuffer[1][3].read<int64_t>(), 40LL);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(KeyedTumblingWindowQueryExecutionTest, multiKeyTumblingWindow) {
    const auto expectedNumberOfTuples = 2;
    auto sourceSchema = Schema::create()
                            ->addField("test$ts", BasicType::UINT64)
                            ->addField("test$key", BasicType::INT64)
                            ->addField("test$value", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()
                          ->addField("test$start", BasicType::INT64)
                          ->addField("test$end", BasicType::INT64)
                          ->addField("test$key", BasicType::INT64)
                          ->addField("test$key", BasicType::INT64)
                          ->addField("test$sum", BasicType::INT64);

    auto testSink = executionEngine->createDataSink(sinkSchema, expectedNumberOfTuples);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(5)))
                     .byKey(Attribute("test$key", BasicType::INT64), Attribute("test$key", BasicType::INT64))
                     .apply(Sum(Attribute("test$value", BasicType::INT64))->as(Attribute("test$sum")))
                     .sink(testSinkDescriptor);

    auto decomposedQueryPlan = DecomposedQueryPlan::create(1, 1);
    for (const auto& rootOperator : query.getQueryPlan()->getRootOperators()) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedNumberOfTuples);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 0LL); // start
    EXPECT_EQ(resultBuffer[0][1].read<int64_t>(), 5LL); // end
    EXPECT_EQ(resultBuffer[0][2].read<int64_t>(), 0LL); // key
    EXPECT_EQ(resultBuffer[0][3].read<int64_t>(), 0LL); // key
    EXPECT_EQ(resultBuffer[0][4].read<int64_t>(), 60LL);// sum

    EXPECT_EQ(resultBuffer[1][0].read<int64_t>(), 0LL); // start
    EXPECT_EQ(resultBuffer[1][1].read<int64_t>(), 5LL); // end
    EXPECT_EQ(resultBuffer[1][2].read<int64_t>(), 1LL); // key
    EXPECT_EQ(resultBuffer[1][3].read<int64_t>(), 1LL); // key
    EXPECT_EQ(resultBuffer[1][4].read<int64_t>(), 40LL);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}
