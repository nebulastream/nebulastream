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
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
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
class KeyedThresholdWindowOperatorTest : public Testing::TestWithErrorHandling {
  public:
    std::vector<Expressions::ExpressionPtr> aggFieldAccessExpressionsVector;
    std::vector<Nautilus::Record::RecordFieldIdentifier> resultFieldVector;
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::map<uint32_t, std::unique_ptr<Aggregation::AggregationValue>>> aggValues;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ThresholdWindowOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ThresholdWindowOperatorTest test class."); }
};

/**
* @brief Tests the keyed threshold window operator with a sum aggregation.
*/
TEST_F(KeyedThresholdWindowOperatorTest, thresholdWindowWithSumAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    // Attribute(f1) > 42, sum(f2)
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto keyFieldName = "key";
    auto aggregationResultFieldName = "sum";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg =
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<KeyedThresholdWindow>(greaterThanExpression,
                                                                          0,
                                                                          aggFieldAccessExpressionsVector,
                                                                          readKey,
                                                                          keyFieldName,
                                                                          resultFieldVector,
                                                                          aggVector,
                                                                          0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto handler = std::make_shared<KeyedThresholdWindowOperatorHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", +10_s64}, {"k1", +0_s64}, {"f2", +1_s64}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", +50_s64}, {"k1", +0_s64}, {"f2", +2_s64}});
    auto recordNinety = Record({{"f1", +90_s64}, {"k1", +0_s64}, {"f2", +3_s64}});
    auto recordTwenty = Record({{"f1", +20_s64}, {"k1", +0_s64}, {"f2", +4_s64}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 2);
    EXPECT_EQ(collector->records[0].read(keyFieldName), 0);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 5);

    thresholdWindowOperator->terminate(ctx);
}

/**
* @brief Tests the keyed threshold window operator with a max aggregation.
*/
TEST_F(KeyedThresholdWindowOperatorTest, thresholdWindowWithMaxAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    // Attribute(f1) > 42, sum(f2)
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto keyFieldName = "key";
    auto aggregationResultFieldName = "max";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg =
        std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<KeyedThresholdWindow>(greaterThanExpression,
                                                                          0,
                                                                          aggFieldAccessExpressionsVector,
                                                                          readKey,
                                                                          keyFieldName,
                                                                          resultFieldVector,
                                                                          aggVector,
                                                                          0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto handler = std::make_shared<KeyedThresholdWindowOperatorHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTen = Record({{"f1", +10_s64}, {"k1", +0_s64}, {"f2", +1_s64}});
    thresholdWindowOperator->execute(ctx, recordTen);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFifty = Record({{"f1", +50_s64}, {"k1", +0_s64}, {"f2", +2_s64}});
    auto recordNinety = Record({{"f1", +90_s64}, {"k1", +0_s64}, {"f2", +3_s64}});
    auto recordTwenty = Record({{"f1", +20_s64}, {"k1", +0_s64}, {"f2", +4_s64}});// closes the window
    thresholdWindowOperator->execute(ctx, recordFifty);
    thresholdWindowOperator->execute(ctx, recordNinety);
    thresholdWindowOperator->execute(ctx, recordTwenty);
    EXPECT_EQ(collector->records.size(), 1);
    EXPECT_EQ(collector->records[0].numberOfFields(), 2);
    EXPECT_EQ(collector->records[0].read(keyFieldName), 0);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 3);

    thresholdWindowOperator->terminate(ctx);
}

/**
* @brief Tests the keyed threshold window operator with a sum aggregation on rows having different keys.
*/
TEST_F(KeyedThresholdWindowOperatorTest, thresholdWindowWithSumTestDifferentKey) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    // Attribute(f1) > 42, sum(f2)
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto keyFieldName = "key";
    auto aggregationResultFieldName = "sum";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAgg =
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName);
    resultFieldVector.emplace_back(aggregationResultFieldName);
    aggFieldAccessExpressionsVector.push_back(readF2);
    aggVector.push_back(sumAgg);
    auto thresholdWindowOperator = std::make_shared<KeyedThresholdWindow>(greaterThanExpression,
                                                                          0,
                                                                          aggFieldAccessExpressionsVector,
                                                                          readKey,
                                                                          keyFieldName,
                                                                          resultFieldVector,
                                                                          aggVector,
                                                                          0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto handler = std::make_shared<KeyedThresholdWindowOperatorHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTenKey0 = Record({{"f1", +10_s64}, {"k1", +0_s64}, {"f2", +1_s64}});
    auto recordTenKey1 = Record({{"f1", +10_s64}, {"k1", +1_s64}, {"f2", +1_s64}});
    thresholdWindowOperator->execute(ctx, recordTenKey0);
    thresholdWindowOperator->execute(ctx, recordTenKey1);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFiftyKey0 = Record({{"f1", +50_s64}, {"k1", +0_s64}, {"f2", +2_s64}});
    auto recordNinetyKey0 = Record({{"f1", +90_s64}, {"k1", +0_s64}, {"f2", +3_s64}});
    auto recordTwentyKey0 = Record({{"f1", +20_s64}, {"k1", +0_s64}, {"f2", +4_s64}});// closes the window
    auto recordFiftyKey1 = Record({{"f1", +50_s64}, {"k1", +1_s64}, {"f2", +7_s64}});
    auto recordNinetyKey1 = Record({{"f1", +90_s64}, {"k1", +1_s64}, {"f2", +5_s64}});
    auto recordTwentyKey1 = Record({{"f1", +20_s64}, {"k1", +1_s64}, {"f2", +4_s64}});// closes the window

    thresholdWindowOperator->execute(ctx, recordFiftyKey0);
    thresholdWindowOperator->execute(ctx, recordNinetyKey0);
    thresholdWindowOperator->execute(ctx, recordTwentyKey0);
    thresholdWindowOperator->execute(ctx, recordFiftyKey1);
    thresholdWindowOperator->execute(ctx, recordNinetyKey1);
    thresholdWindowOperator->execute(ctx, recordTwentyKey1);

    EXPECT_EQ(collector->records.size(), 2);
    EXPECT_EQ(collector->records[0].numberOfFields(), 2);
    EXPECT_EQ(collector->records[0].read(keyFieldName), 0);
    EXPECT_EQ(collector->records[0].read(aggregationResultFieldName), 5);
    EXPECT_EQ(collector->records[1].numberOfFields(), 2);
    EXPECT_EQ(collector->records[1].read(keyFieldName), 1);
    EXPECT_EQ(collector->records[1].read(aggregationResultFieldName), 12);

    thresholdWindowOperator->terminate(ctx);
}

/**
* @brief Tests the keyed threshold window operator with a sum and a max aggregation on rows having different keys.
*/
TEST_F(KeyedThresholdWindowOperatorTest, thresholdWindowWithMultAggTestDifferentKey) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto fortyTwo = std::make_shared<Expressions::ConstantInt32ValueExpression>(42);
    // Attribute(f1) > 42, sum(f2)
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);

    auto keyFieldName = "key";

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto sumAggregationResultFieldName = "sum";
    auto sumAgg =
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, sumAggregationResultFieldName);

    auto maxAggregationResultFieldName = "max";
    auto maxAgg =
        std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType, readF2, maxAggregationResultFieldName);

    aggVector.push_back(sumAgg);
    aggVector.push_back(maxAgg);

    resultFieldVector.emplace_back(sumAggregationResultFieldName);
    resultFieldVector.emplace_back(maxAggregationResultFieldName);

    aggFieldAccessExpressionsVector.push_back(readF2);// sum on F2
    aggFieldAccessExpressionsVector.push_back(readF2);// max on F2

    auto thresholdWindowOperator = std::make_shared<KeyedThresholdWindow>(greaterThanExpression,
                                                                          0,
                                                                          aggFieldAccessExpressionsVector,
                                                                          readKey,
                                                                          keyFieldName,
                                                                          resultFieldVector,
                                                                          aggVector,
                                                                          0);

    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator->setChild(collector);

    auto handler = std::make_shared<KeyedThresholdWindowOperatorHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    thresholdWindowOperator->setup(ctx);

    auto recordTenKey0 = Record({{"f1", +10_s64}, {"k1", +0_s64}, {"f2", +1_s64}});
    auto recordTenKey1 = Record({{"f1", +10_s64}, {"k1", +1_s64}, {"f2", +1_s64}});
    thresholdWindowOperator->execute(ctx, recordTenKey0);
    thresholdWindowOperator->execute(ctx, recordTenKey1);
    EXPECT_EQ(collector->records.size(), 0);

    auto recordFiftyKey0 = Record({{"f1", +50_s64}, {"k1", +0_s64}, {"f2", +2_s64}});
    auto recordNinetyKey0 = Record({{"f1", +90_s64}, {"k1", +0_s64}, {"f2", +3_s64}});
    auto recordTwentyKey0 = Record({{"f1", +20_s64}, {"k1", +0_s64}, {"f2", +4_s64}});// closes the window
    auto recordFiftyKey1 = Record({{"f1", +50_s64}, {"k1", +1_s64}, {"f2", +7_s64}});
    auto recordNinetyKey1 = Record({{"f1", +90_s64}, {"k1", +1_s64}, {"f2", +5_s64}});
    auto recordTwentyKey1 = Record({{"f1", +20_s64}, {"k1", +1_s64}, {"f2", +4_s64}});// closes the window

    thresholdWindowOperator->execute(ctx, recordFiftyKey0);
    thresholdWindowOperator->execute(ctx, recordNinetyKey0);
    thresholdWindowOperator->execute(ctx, recordTwentyKey0);
    thresholdWindowOperator->execute(ctx, recordFiftyKey1);
    thresholdWindowOperator->execute(ctx, recordNinetyKey1);
    thresholdWindowOperator->execute(ctx, recordTwentyKey1);

    EXPECT_EQ(collector->records.size(), 2);
    ASSERT_EQ(collector->records[0].numberOfFields(), 3);
    EXPECT_EQ(collector->records[0].read(keyFieldName), 0);
    EXPECT_EQ(collector->records[0].read(sumAggregationResultFieldName), 5);
    EXPECT_EQ(collector->records[0].read(maxAggregationResultFieldName), 3);
    ASSERT_EQ(collector->records[1].numberOfFields(), 3);
    EXPECT_EQ(collector->records[1].read(keyFieldName), 1);
    EXPECT_EQ(collector->records[1].read(maxAggregationResultFieldName), 7);
    EXPECT_EQ(collector->records[1].read(sumAggregationResultFieldName), 12);

    thresholdWindowOperator->terminate(ctx);
}

}// namespace NES::Runtime::Execution::Operators
