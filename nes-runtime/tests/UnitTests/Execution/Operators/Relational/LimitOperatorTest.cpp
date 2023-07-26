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

#include <API/Schema.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Limit.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class LimitOperatorTest : public testing::Test {
  public:
    /* Will be called before any test in this class is executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LimitOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LimitOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LimitOperatorTest test class."); }
};

/**
 * @brief Tests if we limit the number of tuples
 */
TEST_F(LimitOperatorTest, TestLimit) {
    constexpr uint64_t LIMIT = 10;
    constexpr uint64_t TUPLES = 20;

    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);

    auto handler = std::make_shared<LimitOperatorHandler>(LIMIT);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));

    auto limitOperator = Limit(0);
    auto collector = std::make_shared<CollectOperator>();
    limitOperator.setChild(collector);
    for (uint64_t i = 0; i < TUPLES; ++i) {
        auto record = Record({{"f1", Value<>(0)}});
        limitOperator.execute(ctx, record);
    }

    ASSERT_EQ(collector->records.size(), LIMIT);
}

/**
 * @brief Tests with a limit of 0
 */
TEST_F(LimitOperatorTest, TestLimitZero) {
    constexpr uint64_t LIMIT = 0;
    constexpr uint64_t TUPLES = 20;

    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);

    auto handler = std::make_shared<LimitOperatorHandler>(LIMIT);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));

    auto limitOperator = Limit(0);
    auto collector = std::make_shared<CollectOperator>();
    limitOperator.setChild(collector);
    for (uint64_t i = 0; i < TUPLES; ++i) {
        auto record = Record({{"f1", Value<>(0)}});
        limitOperator.execute(ctx, record);
    }

    ASSERT_EQ(collector->records.size(), LIMIT);
}

}// namespace NES::Runtime::Execution::Operators