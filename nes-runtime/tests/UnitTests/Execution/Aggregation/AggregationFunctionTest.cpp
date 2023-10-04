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

#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/HyperLogLogDistinctCountApproximation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/QuantileEstimationAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {
class AggregationFunctionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }
};

/**
 * Tests the lift, combine, lower and reset functions of the Sum Aggregation
 */
TEST_F(AggregationFunctionTest, sumAggregation) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto sumAgg = Aggregation::SumAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto sumValue = Aggregation::SumAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &sumValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto inputRecord = Record({{"value", incomingValue}});
    // test lift
    sumAgg.lift(memref, inputRecord);
    ASSERT_EQ(sumValue.sum, 1);

    // test combine
    sumAgg.combine(memref, memref);
    ASSERT_EQ(sumValue.sum, 2);

    // test lower
    auto result = Record();
    sumAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 2);

    // test reset
    sumAgg.reset(memref);
    EXPECT_EQ(sumValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Count Aggregation
 */
TEST_F(AggregationFunctionTest, countAggregation) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = Aggregation::CountAggregationFunction(integerType, unsignedIntegerType, readFieldExpression, "result");

    auto countValue = Aggregation::CountAggregationValue<uint64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &countValue);

    // test lift
    Record inputRecord;
    countAgg.lift(memref, inputRecord);
    ASSERT_EQ(countValue.count, 1_u64);

    // test combine
    countAgg.combine(memref, memref);
    ASSERT_EQ(countValue.count, 2_u64);

    // test lower
    auto result = Record();
    countAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 2_u64);

    // test reset
    countAgg.reset(memref);
    EXPECT_EQ(countValue.count, 0_u64);
}

/**
 * Tests the lift, combine, lower and reset functions of the Average Aggregation
 */
TEST_F(AggregationFunctionTest, AvgAggregationWithEventTs) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto readTsFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("evt_timestamp");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    auto avgAgg = Aggregation::AvgAggregationFunction(integerType, doubleType, readFieldExpression, readTsFieldExpression, "result");
    auto avgValue = Aggregation::AvgAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &avgValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>(2_s64);
    auto incomingTs = Nautilus::Value<Nautilus::Int64>(123456_s64);

    // test lift
    auto inputRecord = Record({{"value", incomingValue}, {"evt_timestamp", incomingTs}});
    avgAgg.lift(memref, inputRecord);
    EXPECT_EQ(avgValue.count, 1);
    EXPECT_EQ(avgValue.sum, 2);

    // test combine
    avgAgg.combine(memref, memref);
    EXPECT_EQ(avgValue.count, 2);
    EXPECT_EQ(avgValue.sum, 4);

    // test lower
    auto result = Record();
    avgAgg.lower(memref, result);

    EXPECT_EQ(result.read("result"), 2);

    // test reset
    avgAgg.reset(memref);
    EXPECT_EQ(avgValue.count, 0);
    EXPECT_EQ(avgValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Average Aggregation
 */
TEST_F(AggregationFunctionTest, AvgAggregation) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    auto avgAgg = Aggregation::AvgAggregationFunction(integerType, doubleType, readFieldExpression, "result");
    auto avgValue = Aggregation::AvgAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &avgValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>(2_s64);
    // test lift
    auto inputRecord = Record({{"value", incomingValue}});
    avgAgg.lift(memref, inputRecord);
    EXPECT_EQ(avgValue.count, 1);
    EXPECT_EQ(avgValue.sum, 2);

    // test combine
    avgAgg.combine(memref, memref);
    EXPECT_EQ(avgValue.count, 2);
    EXPECT_EQ(avgValue.sum, 4);

    // test lower
    auto result = Record();
    avgAgg.lower(memref, result);

    EXPECT_EQ(result.read("result"), 2);

    // test reset
    avgAgg.reset(memref);
    EXPECT_EQ(avgValue.count, 0);
    EXPECT_EQ(avgValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Min Aggregation
 */
TEST_F(AggregationFunctionTest, MinAggregation) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto minAgg = Aggregation::MinAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto minValue = Aggregation::MinAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &minValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>(5_s64);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>(10_s64);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValueTwo = Nautilus::Value<Nautilus::Int64>(2_s64);

    // lift value in minAgg with an initial value of 5, thus the current min should be 5
    auto inputRecord = Record({{"value", incomingValueFive}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueFive);

    // lift value in minAgg with a higher value of 10, thus the current min should still be 5
    inputRecord = Record({{"value", incomingValueTen}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueFive);

    // lift value in minAgg with a lower value of 1, thus the current min should change to 1
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // lift value in minAgg with a higher value of 2, thus the current min should still be 1
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // combine memrefs in minAgg
    auto anotherMinValue = Aggregation::MinAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &anotherMinValue);
    inputRecord = Record({{"value", incomingValueTen}});
    minAgg.lift(anotherMemref, inputRecord);

    // test if memref1 < memref2
    minAgg.combine(memref, anotherMemref);
    ASSERT_EQ(minValue.min, incomingValueOne);

    // test if memref1 > memref2
    minAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    // test if memref1 = memref2
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(anotherMemref, inputRecord);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    // lower value in minAgg
    auto result = Record();
    minAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 1);

    // test reset
    minAgg.reset(memref);
    EXPECT_EQ(minValue.min, std::numeric_limits<int64_t>::max());
}

/**
 * Tests the lift, combine, lower and reset functions of the Max Aggregation
 */
TEST_F(AggregationFunctionTest, MaxAggregation) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = Aggregation::MaxAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto maxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &maxValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>(5_s64);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>(10_s64);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValueFifteen = Nautilus::Value<Nautilus::Int64>(15_s64);

    // lift value in maxAgg with an initial value of 5, thus the current min should be 5

    auto inputRecord = Record({{"value", incomingValueFive}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueFive);

    // lift value in maxAgg with a higher value of 10, thus the current min should change to 10

    inputRecord = Record({{"value", incomingValueTen}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    // lift value in maxAgg with a lower value of 1, thus the current min should still be 10

    inputRecord = Record({{"value", incomingValueOne}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    // lift value in maxAgg with a higher value of 15, thus the current min should change to 15

    inputRecord = Record({{"value", incomingValueFifteen}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    // combine memrefs in maxAgg
    auto anotherMaxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &anotherMaxValue);

    inputRecord = Record({{"value", incomingValueOne}});
    maxAgg.lift(anotherMemref, inputRecord);

    // test if memref1 > memref2
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    // test if memref1 < memref2
    maxAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    // test if memref1 = memref2
    inputRecord = Record({{"value", incomingValueFifteen}});
    maxAgg.lift(anotherMemref, inputRecord);
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    // lower value in minAgg
    auto result = Record();
    maxAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), incomingValueFifteen);

    // test reset
    maxAgg.reset(memref);
    EXPECT_EQ(maxValue.max, std::numeric_limits<int64_t>::min());
}

/**
 * Tests the lift, combine, lower and reset functions of the HyperLogLog Aggregation, i.e., a event sequence
*  with identical value (1) that results in a distinct count of 1 (ideal case).
 */
TEST_F(AggregationFunctionTest, HyperLogLogAggregationSimpleTest) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto distinctCountEstimation =
        Aggregation::HyperLogLogDistinctCountApproximation(integerType, integerType, readFieldExpression, "result");
    auto distinctCountValue = Aggregation::HyperLogLogDistinctCountApproximationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &distinctCountValue);
    auto incomingValue = Nautilus::Value<Nautilus::Int64>(1_s64);

    // lift several records in HyperLogLogAgg with an initial value of 1
    auto inputRecord = Record({{"value", incomingValue}});

    // lift value in distinctCountEstimation
    distinctCountEstimation.lift(memref, inputRecord);
    distinctCountEstimation.lift(memref, inputRecord);
    distinctCountEstimation.lift(memref, inputRecord);
    distinctCountEstimation.lift(memref, inputRecord);
    distinctCountEstimation.lift(memref, inputRecord);

    // combine memrefs in HyperLogLogAgg
    distinctCountEstimation.combine(memref, memref);

    // lower value in HyperLogLogAgg
    auto result = Record();
    distinctCountEstimation.lower(memref, result);
    EXPECT_TRUE((bool) (result.read("result") > (1.0 * 0.9) && result.read("result") < (1.0 * 1.1)));
    // HyperLogLog estimates the distinct count thus we use 10% variance of the result
}

/**
 * Tests the lift, combine, lower and reset functions of the HyperLogLog Aggregation, i.e., the following event sequence
 *  {4,3,6,2,2,6,1,7} that results in a distinct count of 6 (ideal case).
 */
TEST_F(AggregationFunctionTest, HyperLogLogComplexTest) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto distinctCountEstimation =
        Aggregation::HyperLogLogDistinctCountApproximation(integerType, integerType, readFieldExpression, "result");
    auto distinctCountValue = Aggregation::HyperLogLogDistinctCountApproximationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &distinctCountValue);
    auto incomingValue4 = Nautilus::Value<Nautilus::Int64>(4_s64);
    auto incomingValue3 = Nautilus::Value<Nautilus::Int64>(3_s64);
    auto incomingValue6 = Nautilus::Value<Nautilus::Int64>(6_s64);
    auto incomingValue2 = Nautilus::Value<Nautilus::Int64>(2_s64);
    auto incomingValue1 = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValue7 = Nautilus::Value<Nautilus::Int64>(7_s64);

    // lift value in distinctCountEstimation
    auto inputRecord4 = Record({{"value", incomingValue4}});
    distinctCountEstimation.lift(memref, inputRecord4);
    auto inputRecord3 = Record({{"value", incomingValue3}});
    distinctCountEstimation.lift(memref, inputRecord3);
    auto inputRecord6 = Record({{"value", incomingValue6}});
    distinctCountEstimation.lift(memref, inputRecord6);
    auto inputRecord2 = Record({{"value", incomingValue2}});
    distinctCountEstimation.lift(memref, inputRecord2);
    distinctCountEstimation.lift(memref, inputRecord2);
    distinctCountEstimation.lift(memref, inputRecord6);
    auto inputRecord1 = Record({{"value", incomingValue1}});
    distinctCountEstimation.lift(memref, inputRecord1);
    auto inputRecord7 = Record({{"value", incomingValue7}});
    distinctCountEstimation.lift(memref, inputRecord7);

    // lower value in minAgg
    auto result = Record();
    distinctCountEstimation.lower(memref, result);
    EXPECT_TRUE((bool) (result.read("result") > (6.0 * 0.9) && result.read("result") < (6.0 * 1.1)));
    // HyperLogLog estimates the distinct count thus we use 10% variance of the result
}

/**
 * Tests the lift, lower and reset functions of the QuantileEstimationAggregation, i.e., the following event sequence
 *  {4,3,6,2,2,6,1,7,7,3} that results in a -> 1 2 2 3 3 | 4 6 6 7 7 = 3.5
 *  currently hardcoded: size of t-digest = 10 and quantile is median (0.5)
*/
TEST_F(AggregationFunctionTest, scanEmitPipelineQuantile) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto outputType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

    auto quantileEstimationAgg =
        Aggregation::QuantileEstimationAggregation(integerType, outputType, readFieldExpression, "result");
    auto quantileEstimationValue = Aggregation::QuantileEstimationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &quantileEstimationValue);
    auto incomingValue4 = Nautilus::Value<Nautilus::Int64>(4_s64);
    auto incomingValue3 = Nautilus::Value<Nautilus::Int64>(3_s64);
    auto incomingValue6 = Nautilus::Value<Nautilus::Int64>(6_s64);
    auto incomingValue2 = Nautilus::Value<Nautilus::Int64>(2_s64);
    auto incomingValue1 = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValue7 = Nautilus::Value<Nautilus::Int64>(7_s64);

    // lift value in distinctCountEstimation
    auto inputRecord4 = Record({{"value", incomingValue4}});
    quantileEstimationAgg.lift(memref, inputRecord4);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.max(), 4.0f);
    auto inputRecord3 = Record({{"value", incomingValue3}});
    quantileEstimationAgg.lift(memref, inputRecord3);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.min(), 3.0f);
    auto inputRecord6 = Record({{"value", incomingValue6}});
    quantileEstimationAgg.lift(memref, inputRecord6);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.size(), 3);
    auto inputRecord2 = Record({{"value", incomingValue2}});
    quantileEstimationAgg.lift(memref, inputRecord2);
    quantileEstimationAgg.lift(memref, inputRecord2);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.min(), 2.0f);
    quantileEstimationAgg.lift(memref, inputRecord6);
    auto inputRecord1 = Record({{"value", incomingValue1}});
    quantileEstimationAgg.lift(memref, inputRecord1);
    auto inputRecord7 = Record({{"value", incomingValue7}});
    quantileEstimationAgg.lift(memref, inputRecord7);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.max(), 7.0f);
    quantileEstimationAgg.lift(memref, inputRecord7);
    quantileEstimationAgg.lift(memref, inputRecord3);
    quantileEstimationValue.digest.merge();
    ASSERT_EQ(quantileEstimationValue.digest.size(), 10);

    // lower value in minAgg
    auto result = Record();
    quantileEstimationAgg.lower(memref, result);
    double expResult = quantileEstimationValue.digest.min() + (50 * quantileEstimationValue.digest.max() / 100);
    // the estimate quantile result = minVal + (quantile (0-100%) * maxVal/100) = 4.5 for this example
    EXPECT_TRUE((bool) (result.read("result") > (expResult * 0.9) && result.read("result") < (expResult * 1.1)));
    // Tdigest is an estimation the median thus we use 10% variance of the result
}

/**
 * Tests the combine, lower and reset functions of the QuantileEstimationAggregation, i.e., the following event sequence
 *  {4,3,6,2,2,6,1,7} and {7,10} that results in a -> 1 2 2 3 4 6 6 7 7 10
 *  currently hardcoded: size of t-digest = 10 and quantile is median (0.5)
*/
TEST_F(AggregationFunctionTest, scanEmitPipelineQuantileCombine) {
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto outputType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

    auto quantileEstimationAgg =
        Aggregation::QuantileEstimationAggregation(integerType, outputType, readFieldExpression, "result");
    auto quantileEstimationValue = Aggregation::QuantileEstimationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &quantileEstimationValue);
    auto memref2 = Nautilus::Value<Nautilus::MemRef>((int8_t*) &quantileEstimationValue);
    auto incomingValue4 = Nautilus::Value<Nautilus::Int64>(4_s64);
    auto incomingValue3 = Nautilus::Value<Nautilus::Int64>(3_s64);
    auto incomingValue6 = Nautilus::Value<Nautilus::Int64>(6_s64);
    auto incomingValue2 = Nautilus::Value<Nautilus::Int64>(2_s64);
    auto incomingValue1 = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValue7 = Nautilus::Value<Nautilus::Int64>(7_s64);
    auto incomingValue10 = Nautilus::Value<Nautilus::Int64>(10_s64);

    // lift value in distinctCountEstimation
    auto inputRecord4 = Record({{"value", incomingValue4}});
    quantileEstimationAgg.lift(memref, inputRecord4);
    auto inputRecord3 = Record({{"value", incomingValue3}});
    quantileEstimationAgg.lift(memref, inputRecord3);
    auto inputRecord6 = Record({{"value", incomingValue6}});
    quantileEstimationAgg.lift(memref, inputRecord6);
    auto inputRecord2 = Record({{"value", incomingValue2}});
    quantileEstimationAgg.lift(memref, inputRecord2);
    quantileEstimationAgg.lift(memref, inputRecord2);
    quantileEstimationAgg.lift(memref, inputRecord6);
    auto inputRecord1 = Record({{"value", incomingValue1}});
    quantileEstimationAgg.lift(memref, inputRecord1);
    auto inputRecord7 = Record({{"value", incomingValue7}});
    quantileEstimationAgg.lift(memref, inputRecord7);

    // create a second memref
    quantileEstimationAgg.lift(memref2, inputRecord7);
    auto inputRecord10 = Record({{"value", incomingValue10}});
    quantileEstimationAgg.lift(memref2, inputRecord10);

    // combine memrefs in HyperLogLogAgg
    quantileEstimationAgg.combine(memref, memref2);
    //combine calls merge of the tdigest thus we can skip it here
    ASSERT_EQ(quantileEstimationValue.digest.min(), 1.0f);
    ASSERT_EQ(quantileEstimationValue.digest.max(), 10.0f);
    ASSERT_EQ(quantileEstimationValue.digest.size(), 10);

    // lower value in minAgg
    auto result = Record();
    quantileEstimationAgg.lower(memref, result);
    double expResult = quantileEstimationValue.digest.min() + (50 * quantileEstimationValue.digest.max() / 100);
    // the estimate quantile result = minVal + (quantile (0-100%) * maxVal/100) =  1+(50*10/100) = 6 for this example;
    // note that min is in one memref and max is in the other memref to test combine
    EXPECT_TRUE((bool) (result.read("result") > (expResult * 0.9) && result.read("result") < (expResult * 1.1)));
    // Tdigest is an estimation the median thus we use 10% variance of the result
}

}// namespace NES::Runtime::Execution::Expressions
