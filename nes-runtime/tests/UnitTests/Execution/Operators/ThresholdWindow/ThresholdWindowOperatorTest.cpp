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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
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
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <utility>

namespace NES::Runtime::Execution::Operators {

// TODO #3468: parameterize the aggregation function instead of repeating the similar test
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

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
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
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
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

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 3, readF2, aggregationResultFieldName, sumAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(sumAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 0);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a min aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithMinAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Min";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto minAgg = std::make_shared<Aggregation::MinAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, minAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto minAggregationValue = std::make_unique<Aggregation::MinAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(minAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 2);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a Max aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithMaxAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Max";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, maxAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto maxAggregationValue = std::make_unique<Aggregation::MaxAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(maxAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>((int64_t) 1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 3);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a Max aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithAvgAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Avg";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerType, integerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, maxAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(avgAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>((int64_t) 1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 4)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 6)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 3);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a Count aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithCountAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Count";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = std::make_shared<Aggregation::CountAggregationFunction>(integerType, unsignedIntegerType);
    auto thresholdWindowOperator =
        std::make_shared<ThresholdWindow>(greaterThanExpression, 0, readF2, aggregationResultFieldName, countAgg, 0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto countAggregationValue = std::make_unique<Aggregation::CountAggregationValue<uint64_t>>();
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(countAggregationValue));
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>((int64_t) 1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 4)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 6)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 1);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), (uint64_t) 2);

    thresholdWindowOperator->terminate(ctx);
}
}// namespace NES::Runtime::Execution::Operators
