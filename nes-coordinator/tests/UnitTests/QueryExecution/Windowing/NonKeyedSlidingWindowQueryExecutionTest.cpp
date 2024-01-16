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

class NonKeyedSlidingWindowQueryExecutionTest : public Testing::BaseUnitTest,
                                                public ::testing::WithParamInterface<QueryCompilation::WindowingStrategy> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonKeyedTumblingWindowQueryExecutionTest.cpp.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup NonKeyedTumblingWindowQueryExecutionTest.cpp test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto windowStrategy = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(dumpMode,
                                                                         1,
                                                                         QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                                         windowStrategy);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down NonKeyedTumblingWindowQueryExecutionTest.cpp test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("QueryExecutionTest: Tear down NonKeyedTumblingWindowQueryExecutionTest.cpp test class.");
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 30; recordIndex++) {
        buf[recordIndex][0].write<uint64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(1);
    }
    buf.setNumberOfTuples(30);
}

TEST_P(NonKeyedSlidingWindowQueryExecutionTest, testSimpleSlidingWindow) {
    auto sourceSchema = Schema::create()->addField("test$f1", BasicType::UINT64)->addField("test$f2", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto sinkSchema = Schema::create()->addField("test$sum", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(sinkSchema, 4);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(SlidingWindow::of(EventTime(Attribute("test$f1")), Milliseconds(10), Milliseconds(5)))
                     .apply(Sum(Attribute("test$f2", BasicType::INT64))->as(Attribute("test$sum")))
                     .project(Attribute("test$sum"))
                     .sink(testSinkDescriptor);

auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId, query.getQueryPlan()->getRootOperators());
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 30);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 4u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1u);
    EXPECT_EQ(resultBuffer[0][0].read<int64_t>(), 10);// sum

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testNonKeyedSlidingWindow,
                        NonKeyedSlidingWindowQueryExecutionTest,
                        ::testing::Values(QueryCompilation::WindowingStrategy::SLICING,
                                          QueryCompilation::WindowingStrategy::BUCKETING),
                        [](const testing::TestParamInfo<NonKeyedSlidingWindowQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });