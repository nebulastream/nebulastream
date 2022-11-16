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
#include "Runtime/Execution/PipelineExecutionContext.hpp"
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

namespace NES::Runtime::Execution::Operators {

class ThresholdWindowOperatorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ThresholdWindowOperatorTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ThresholdWindowOperatorTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ThresholdWindowOperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ThresholdWindowOperatorTest test class." << std::endl; }
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    explicit MockedPipelineExecutionContext(OperatorHandlerPtr handler)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            {std::move(handler)}){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

/**
 * @brief Tests the threshold window operator with a sum aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithSumAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    auto thresholdWindowOperator = std::make_shared<ThresholdWindow>(greaterThanExpression, readF2);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto handler = std::make_shared<ThresholdWindowOperatorHandler>();
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read("sum"), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>(2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>(3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 2);
    EXPECT_EQ(collector->records[1].numberOfFields(), 1);
    EXPECT_EQ(collector->records[1].read("sum"), 5);

    thresholdWindowOperator->terminate(ctx);
}
}// namespace NES::Runtime::Execution::Operators