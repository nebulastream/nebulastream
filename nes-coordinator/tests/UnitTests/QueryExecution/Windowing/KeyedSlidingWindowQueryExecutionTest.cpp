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
        executionEngine =
            std::make_shared<Testing::TestExecutionEngine>(dumpMode,
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
};

void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 30; recordIndex++) {
        buf[recordIndex][0].write<uint64_t>(recordIndex);
        buf[recordIndex][1].write<int64_t>(recordIndex % 3);
        buf[recordIndex][2].write<int64_t>(1);
    }
    buf.setNumberOfTuples(30);
}

TEST_P(KeyedSlidingWindowQueryExecutionTest, testKeyedSlidingWindow) {
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

    auto testSink = executionEngine->createDataSink(sinkSchema, 4);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .window(SlidingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(10), Milliseconds(5)))
                     .byKey(Attribute("test$key"))
                     .apply(Sum(Attribute("test$value", BasicType::INT64))->as(Attribute("test$sum")))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(sourceSchema);
    fillBuffer(inputBuffer);
    ASSERT_EQ(inputBuffer.getBuffer().getNumberOfTuples(), 30);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 4u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 3u);
    // compare result for key 0
    EXPECT_EQ(resultBuffer[0]["test$startTs"].read<uint64_t>(), 0);
    EXPECT_EQ(resultBuffer[0]["test$endTs"].read<uint64_t>(), 10);
    EXPECT_EQ(resultBuffer[0]["test$key"].read<int64_t>(), 0);
    EXPECT_EQ(resultBuffer[0]["test$sum"].read<int64_t>(), 4);

    // compare result for key 1
    EXPECT_EQ(resultBuffer[1]["test$startTs"].read<uint64_t>(), 0);
    EXPECT_EQ(resultBuffer[1]["test$endTs"].read<uint64_t>(), 10);
    EXPECT_EQ(resultBuffer[1]["test$key"].read<int64_t>(), 1);
    EXPECT_EQ(resultBuffer[1]["test$sum"].read<int64_t>(), 3);

    // compare result for key 2
    EXPECT_EQ(resultBuffer[2]["test$startTs"].read<uint64_t>(), 0);
    EXPECT_EQ(resultBuffer[2]["test$endTs"].read<uint64_t>(), 10);
    EXPECT_EQ(resultBuffer[2]["test$key"].read<int64_t>(), 2);
    EXPECT_EQ(resultBuffer[2]["test$sum"].read<int64_t>(), 3);

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