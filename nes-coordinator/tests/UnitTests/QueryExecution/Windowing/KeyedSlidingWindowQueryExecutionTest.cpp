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
#include <TestUtils/UtilityFunctions.hpp>
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

class KeyedSlidingWindowQueryExecutionTest : public Testing::BaseUnitTest,
                                             public ::testing::WithParamInterface<QueryCompilation::WindowingStrategy> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedSlidingWindowQueryExecutionTest.cpp.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup KeyedSlidingWindowQueryExecutionTest.cpp test class.");
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
        NES_DEBUG("QueryExecutionTest: Tear down KeyedSlidingWindowQueryExecutionTest.cpp test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("QueryExecutionTest: Tear down KeyedSlidingWindowQueryExecutionTest.cpp test class.");
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 30; recordIndex++) {
        buf[recordIndex][0].write<uint64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(recordIndex % 3);
        buf[recordIndex][2].write<int64_t>(1);
    }
    buf.setNumberOfTuples(30);
}

void createExpectedBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(0, 10, 0, 4));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(0, 10, 1, 3));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(0, 10, 2, 3));

    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(5, 15, 0, 3));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(5, 15, 1, 3));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(5, 15, 2, 4));

    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(10, 20, 0, 3));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(10, 20, 1, 4));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(10, 20, 2, 3));

    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(15, 25, 0, 4));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(15, 25, 1, 3));
    buf.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, int64_t, int64_t>(15, 25, 2, 3));
}

TEST_P(KeyedSlidingWindowQueryExecutionTest, testKeyedSlidingWindow) {
    const auto expectedNumberOfTuples = 12;
    auto sourceSchema = Schema::create()
                            ->addField("test$ts", BasicType::UINT64)
                            ->addField("test$key", BasicType::INT64)
                            ->addField("test$value", BasicType::INT64);
    auto sinkSchema = Schema::create()
                          ->addField("test$startTs", BasicType::UINT64)
                          ->addField("test$endTs", BasicType::UINT64)
                          ->addField("test$key", BasicType::INT64)
                          ->addField("test$sum", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);

    auto testSink = executionEngine->createDataSink(sinkSchema, expectedNumberOfTuples);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(SlidingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(10), Milliseconds(5)))
                     .byKey(Attribute("test$key"))
                     .apply(Sum(Attribute("test$value", BasicType::INT64))->as(Attribute("test$sum")))
                     .sink(testSinkDescriptor);

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId);
    for (const auto& rootOperator : query.getQueryPlan()->getRootOperators()) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 30);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 4u);

    // Create expected buffer
    auto expectedBuffer = executionEngine->getBufferManager()->getBufferBlocking();
    auto expectedDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(expectedBuffer, sinkSchema);
    createExpectedBuffer(expectedDynamicBuffer);
    std::vector<Runtime::MemoryLayouts::DynamicTupleBuffer> expectedBuffers = {expectedDynamicBuffer};
    auto resultBuffers = testSink->getResultBuffers();

    auto startTs = 0;
    auto endTs = 10;
    for (auto i = 0_u64; i < testSink->getNumberOfResultBuffers(); ++i) {
        auto resultBuffer = testSink->getResultBuffer(i);
        NES_INFO("Buffer: {}", NES::Runtime::Execution::Util::printTupleBufferAsCSV(resultBuffer.getBuffer(), sinkSchema));
    }

    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, resultBuffers));

    ASSERT_TRUE(executionEngine->stopQuery(plan));
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testNonKeyedSlidingWindow,
                        KeyedSlidingWindowQueryExecutionTest,
                        ::testing::Values(QueryCompilation::WindowingStrategy::SLICING,
                                          QueryCompilation::WindowingStrategy::BUCKETING),
                        [](const testing::TestParamInfo<KeyedSlidingWindowQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });