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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <utility>

namespace NES::Runtime::Execution::Operators {

class ThresholdWindowOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ThresholdWindowOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ThresholdWindowOperatorTest test class."); }
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
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    std::unique_ptr<Aggregation::SumAggregationValue> sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>(2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>(3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 5);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a sum aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithSumAggTestMinCountTrue) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    std::unique_ptr<Aggregation::SumAggregationValue> sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>(2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>(3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 5);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a sum aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithSumAggTestMinCountFalse) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 3, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    std::unique_ptr<Aggregation::SumAggregationValue> sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>(2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>(3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 0);

    thresholdWindowOperator->terminate(ctx);
}

}// namespace NES::Runtime::Execution::Operators
