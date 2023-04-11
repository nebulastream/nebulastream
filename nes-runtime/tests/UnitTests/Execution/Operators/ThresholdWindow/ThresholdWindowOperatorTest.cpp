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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/GlobalThresholdWindow/GLobalThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/GlobalThresholdWindow/ThresholdWindowOperatorHandler.hpp>
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
    std::vector<Expressions::ExpressionPtr> aggFieldAccessExpressionsVector;
    std::vector<Nautilus::Record::RecordFieldIdentifier> resultFieldVector;
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggValues;
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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     3,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Min";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto minAgg = std::make_shared<Aggregation::MinAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(minAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto minAggregationValue = std::make_unique<Aggregation::MinAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(minAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Max";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(maxAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto maxAggregationValue = std::make_unique<Aggregation::MaxAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(maxAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
 * @brief Tests the threshold window operator with a Avg aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithAvgAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Avg";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto avgAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(avgAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(avgAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto aggregationResultFieldName = "Count";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = std::make_shared<Aggregation::CountAggregationFunction>(integerType, unsignedIntegerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggVector.push_back(countAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto countAggregationValue = std::make_unique<Aggregation::CountAggregationValue<uint64_t>>();
    aggValues.emplace_back(std::move(countAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

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

/**
 * @brief Tests the threshold window operator with multiple aggregations.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithMultipleAggregations) {
    //set up
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readF3 = std::make_shared<Expressions::ReadFieldExpression>("f3");
    auto readF4 = std::make_shared<Expressions::ReadFieldExpression>("f4");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldNameSum = "Sum";
    auto aggregationResultFieldNameMax = "Max";
    auto aggregationResultFieldNameMin = "Min";
    auto aggregationResultFieldNameMean = "Mean";
    auto aggregationResultFieldNameCount = "Count";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    auto unsignedIntegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto maxAgg = std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType);
    auto minAgg = std::make_shared<Aggregation::MinAggregationFunction>(integerType, integerType);
    auto avgAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerType, doubleType);
    auto countAgg = std::make_shared<Aggregation::CountAggregationFunction>(integerType, unsignedIntegerType);
    for (auto resultField : {aggregationResultFieldNameSum,
                             aggregationResultFieldNameMax,
                             aggregationResultFieldNameMin,
                             aggregationResultFieldNameMean,
                             aggregationResultFieldNameCount}) {
        resultFieldVector.emplace_back(resultField);
    }
    for (auto accessField : {readF2, readF1, readF3, readF4}) {
        aggFieldAccessExpressionsVector.push_back(accessField);
    }
    aggVector.push_back(sumAgg);
    aggVector.push_back(maxAgg);
    aggVector.push_back(minAgg);
    aggVector.push_back(avgAgg);
    aggVector.push_back(countAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    auto maxAggregationValue = std::make_unique<Aggregation::MaxAggregationValue<int64_t>>();
    auto minAggregationValue = std::make_unique<Aggregation::MinAggregationValue<int64_t>>();
    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int64_t>>();
    auto countAggregationValue = std::make_unique<Aggregation::CountAggregationValue<uint64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    aggValues.emplace_back(std::move(maxAggregationValue));
    aggValues.emplace_back(std::move(minAggregationValue));
    aggValues.emplace_back(std::move(avgAggregationValue));
    aggValues.emplace_back(std::move(countAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordFifty = Record({{"f1", Value<>((int64_t) 50)},
                               {"f2", Value<>((int64_t) 2)},
                               {"f3", Value<>((int64_t) 50)},
                               {"f4", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>((int64_t) 90)},
                                {"f2", Value<>((int64_t) 3)},
                                {"f3", Value<>((int64_t) 90)},
                                {"f4", Value<>((int64_t) 4)}});
    auto recordTwenty = Record({{"f1", Value<>((int64_t) 20)},
                                {"f2", Value<>((int64_t) 4)},
                                {"f3", Value<>((int64_t) 50)},
                                {"f4", Value<>((int64_t) 2)}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 5);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldNameSum), 5);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldNameMax), 90);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldNameMin), 50);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldNameMean), 3);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldNameCount), (uint64_t) 2);

    thresholdWindowOperator->terminate(ctx);
}

/**
 * @brief Tests the threshold window operator with a sum aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithFloatPredicateTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42.5);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto aggregationResultFieldName = "sum";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<GLobalThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10.75)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50.25)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90.35)}, {"f2", Value<>((int64_t) 3)}});
    auto recordTwenty = Record({{"f1", Value<>(20.85)}, {"f2", Value<>((int64_t) 4)}});// closes the window
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
TEST_F(ThresholdWindowOperatorTest, thresholdWindowWithMultAggOnGenratingMultipleWindowsTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    // Attribute(f1) > 42, sum(f2)

    auto sumAggregationResultFieldName = "sum";
    auto avgAggregationResultFieldName = "avg";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createDouble());

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);
    auto avgAgg = std::make_shared<Aggregation::AvgAggregationFunction>(integerType, doubleType);

    aggVector.push_back(sumAgg);
    aggVector.push_back(avgAgg);

    resultFieldVector.emplace_back(sumAggregationResultFieldName);
    resultFieldVector.emplace_back(avgAggregationResultFieldName);

    aggFieldAccessExpressionsVector.push_back(readF2);// for sum
    aggFieldAccessExpressionsVector.push_back(readF2);// for avg

    auto thresholdWindowOperator = std::make_shared<ThresholdWindow>(greaterThanExpression,
                                                                     0,
                                                                     aggFieldAccessExpressionsVector,
                                                                     resultFieldVector,
                                                                     aggVector,
                                                                     0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    auto avgAggregationValue = std::make_unique<Aggregation::AvgAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    aggValues.emplace_back(std::move(avgAggregationValue));

    auto handler = std::make_shared<ThresholdWindowOperatorHandler>(std::move(aggValues));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>((int64_t) 2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>((int64_t) 4)}});
    auto recordTwenty = Record({{"f1", Value<>(20)}, {"f2", Value<>((int64_t) 4)}});// closes the window
    auto recordSixty = Record({{"f1", Value<>(60)}, {"f2", Value<>((int64_t) 1)}});
    auto recordSeventy = Record({{"f1", Value<>(70)}, {"f2", Value<>((int64_t) 7)}});
    auto recordTwelve = Record({{"f1", Value<>(12)}, {"f2", Value<>((int64_t) 8)}});// closes the window

    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    thresholdWindowOperator->execute(ctx, recordSixty);
    thresholdWindowOperator->execute(ctx, recordSeventy);
    thresholdWindowOperator->execute(ctx, recordTwelve);

    EXPECT_EQ(collector->records.size(), 2);
    EXPECT_EQ(collector->records[0].numberOfFields(), 2);
    EXPECT_EQ(collector->records[0].read(sumAggregationResultFieldName), 6);
    EXPECT_EQ(collector->records[0].read(avgAggregationResultFieldName), 3);
    EXPECT_EQ(collector->records[1].numberOfFields(), 2);
    EXPECT_EQ(collector->records[1].read(sumAggregationResultFieldName), 8);
    EXPECT_EQ(collector->records[1].read(avgAggregationResultFieldName), 4);
    thresholdWindowOperator->terminate(ctx);
}

}// namespace NES::Runtime::Execution::Operators
