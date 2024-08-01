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

#include <memory>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Runtime::Execution::Expressions
{
class AggregationFunctionTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }
};

/**
 * Tests the lift, combine, lower and reset functions of the Sum Aggregation
 */
TEST_F(AggregationFunctionTest, sumAggregation)
{
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto sumAgg = Aggregation::SumAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto sumValue = Aggregation::SumAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&sumValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto inputRecord = Record({{"value", incomingValue}});
    /// test lift
    sumAgg.lift(memref, inputRecord);
    ASSERT_EQ(sumValue.sum, 1);

    /// test combine
    sumAgg.combine(memref, memref);
    ASSERT_EQ(sumValue.sum, 2);

    /// test lower
    auto result = Record();
    sumAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 2);

    /// test reset
    sumAgg.reset(memref);
    EXPECT_EQ(sumValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Count Aggregation
 */
TEST_F(AggregationFunctionTest, countAggregation)
{
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto countAgg = Aggregation::CountAggregationFunction(integerType, unsignedIntegerType, readFieldExpression, "result");

    auto countValue = Aggregation::CountAggregationValue<uint64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&countValue);

    /// test lift
    Record inputRecord;
    countAgg.lift(memref, inputRecord);
    ASSERT_EQ(countValue.count, 1_u64);

    /// test combine
    countAgg.combine(memref, memref);
    ASSERT_EQ(countValue.count, 2_u64);

    /// test lower
    auto result = Record();
    countAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 2_u64);

    /// test reset
    countAgg.reset(memref);
    EXPECT_EQ(countValue.count, 0_u64);
}

/**
 * Tests the lift, combine, lower and reset functions of the Average Aggregation
 */
TEST_F(AggregationFunctionTest, AvgAggregation)
{
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr doubleType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    auto avgAgg = Aggregation::AvgAggregationFunction(integerType, doubleType, readFieldExpression, "result");
    auto avgValue = Aggregation::AvgAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&avgValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>(2_s64);
    /// test lift
    auto inputRecord = Record({{"value", incomingValue}});
    avgAgg.lift(memref, inputRecord);
    EXPECT_EQ(avgValue.count, 1);
    EXPECT_EQ(avgValue.sum, 2);

    /// test combine
    avgAgg.combine(memref, memref);
    EXPECT_EQ(avgValue.count, 2);
    EXPECT_EQ(avgValue.sum, 4);

    /// test lower
    auto result = Record();
    avgAgg.lower(memref, result);

    EXPECT_EQ(result.read("result"), 2);

    /// test reset
    avgAgg.reset(memref);
    EXPECT_EQ(avgValue.count, 0);
    EXPECT_EQ(avgValue.sum, 0);
}

/**
 * Tests the lift, combine, lower and reset functions of the Min Aggregation
 */
TEST_F(AggregationFunctionTest, MinAggregation)
{
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto minAgg = Aggregation::MinAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto minValue = Aggregation::MinAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&minValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>(5_s64);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>(10_s64);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValueTwo = Nautilus::Value<Nautilus::Int64>(2_s64);

    /// lift value in minAgg with an initial value of 5, thus the current min should be 5
    auto inputRecord = Record({{"value", incomingValueFive}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueFive);

    /// lift value in minAgg with a higher value of 10, thus the current min should still be 5
    inputRecord = Record({{"value", incomingValueTen}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueFive);

    /// lift value in minAgg with a lower value of 1, thus the current min should change to 1
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueOne);

    /// lift value in minAgg with a higher value of 2, thus the current min should still be 1
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(memref, inputRecord);
    ASSERT_EQ(minValue.min, incomingValueOne);

    /// combine memrefs in minAgg
    auto anotherMinValue = Aggregation::MinAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&anotherMinValue);
    inputRecord = Record({{"value", incomingValueTen}});
    minAgg.lift(anotherMemref, inputRecord);

    /// test if memref1 < memref2
    minAgg.combine(memref, anotherMemref);
    ASSERT_EQ(minValue.min, incomingValueOne);

    /// test if memref1 > memref2
    minAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    /// test if memref1 = memref2
    inputRecord = Record({{"value", incomingValueOne}});
    minAgg.lift(anotherMemref, inputRecord);
    ASSERT_EQ(anotherMinValue.min, incomingValueOne);

    /// lower value in minAgg
    auto result = Record();
    minAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), 1);

    /// test reset
    minAgg.reset(memref);
    EXPECT_EQ(minValue.min, std::numeric_limits<int64_t>::max());
}

/**
 * Tests the lift, combine, lower and reset functions of the Max Aggregation
 */
TEST_F(AggregationFunctionTest, MaxAggregation)
{
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto maxAgg = Aggregation::MaxAggregationFunction(integerType, integerType, readFieldExpression, "result");
    auto maxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&maxValue);
    auto incomingValueFive = Nautilus::Value<Nautilus::Int64>(5_s64);
    auto incomingValueTen = Nautilus::Value<Nautilus::Int64>(10_s64);
    auto incomingValueOne = Nautilus::Value<Nautilus::Int64>(1_s64);
    auto incomingValueFifteen = Nautilus::Value<Nautilus::Int64>(15_s64);

    /// lift value in maxAgg with an initial value of 5, thus the current min should be 5

    auto inputRecord = Record({{"value", incomingValueFive}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueFive);

    /// lift value in maxAgg with a higher value of 10, thus the current min should change to 10

    inputRecord = Record({{"value", incomingValueTen}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    /// lift value in maxAgg with a lower value of 1, thus the current min should still be 10

    inputRecord = Record({{"value", incomingValueOne}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueTen);

    /// lift value in maxAgg with a higher value of 15, thus the current min should change to 15

    inputRecord = Record({{"value", incomingValueFifteen}});
    maxAgg.lift(memref, inputRecord);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    /// combine memrefs in maxAgg
    auto anotherMaxValue = Aggregation::MaxAggregationValue<int64_t>();
    auto anotherMemref = Nautilus::Value<Nautilus::MemRef>((int8_t*)&anotherMaxValue);

    inputRecord = Record({{"value", incomingValueOne}});
    maxAgg.lift(anotherMemref, inputRecord);

    /// test if memref1 > memref2
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(maxValue.max, incomingValueFifteen);

    /// test if memref1 < memref2
    maxAgg.combine(anotherMemref, memref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    /// test if memref1 = memref2
    inputRecord = Record({{"value", incomingValueFifteen}});
    maxAgg.lift(anotherMemref, inputRecord);
    maxAgg.combine(memref, anotherMemref);
    ASSERT_EQ(anotherMaxValue.max, incomingValueFifteen);

    /// lower value in minAgg
    auto result = Record();
    maxAgg.lower(memref, result);
    ASSERT_EQ(result.read("result"), incomingValueFifteen);

    /// test reset
    maxAgg.reset(memref);
    EXPECT_EQ(maxValue.max, std::numeric_limits<int64_t>::min());
}
} /// namespace NES::Runtime::Execution::Expressions
