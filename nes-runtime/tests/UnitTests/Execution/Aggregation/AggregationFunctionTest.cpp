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
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {
class AggregationFunctionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup AddExpressionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

// TODO 3250: doc
TEST_F(AggregationFunctionTest, scanEmitPipelineSum) {
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto sumAgg = Aggregation::SumAggregationFunction(integerType, integerType);
    auto sumValue = Aggregation::SumAggregationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &sumValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    // test lift
    sumAgg.lift(memref, incomingValue);
    ASSERT_EQ(sumValue.sum, 1);

    // test combine
    sumAgg.combine(memref, memref);
    ASSERT_EQ(sumValue.sum, 2);

    // test lower
    auto aggregationResult = sumAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 2);
}

TEST_F(AggregationFunctionTest, scanEmitPipelineCount) {
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto countAgg = Aggregation::CountAggregationFunction(integerType, integerType);
    auto countValue = Aggregation::CountAggregationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &countValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 1);
    // test lift
    countAgg.lift(memref, incomingValue);
    ASSERT_EQ(countValue.count, 1);

    // test combine
    countAgg.combine(memref, memref);
    ASSERT_EQ(countValue.count, 2);

    // test lower
    auto aggregationResult = countAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 2);
}

TEST_F(AggregationFunctionTest, scanEmitPipelineAvg) {
    DataTypePtr integerType = DataTypeFactory::createInt64();
    DataTypePtr doubleType = DataTypeFactory::createDouble();
    auto avgAgg = Aggregation::AvgAggregationFunction(integerType, doubleType);
    auto avgValue = Aggregation::AvgAggregationValue();
//    auto memrefCount = Nautilus::Value<Nautilus::MemRef>((int8_t*) &avgValue.count);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &avgValue);

    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 2);
    // test lift
    avgAgg.lift(memref, incomingValue);
    EXPECT_EQ(avgValue.count, 1);
    EXPECT_EQ(avgValue.sum, 2);

    // test combine
  avgAgg.combine(memref, memref);
    EXPECT_EQ(avgValue.count, 2);
    EXPECT_EQ(avgValue.sum, 4);

    // test lower
    auto aggregationResult = avgAgg.lower(memref);
    EXPECT_EQ(aggregationResult, 2);

    avgAgg.reset(memref);
    EXPECT_EQ(avgValue.count, 0);
    EXPECT_EQ(avgValue.sum, 0);


}

TEST_F(AggregationFunctionTest, scanEmitPipelineMin) {
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto minAgg = Aggregation::MinAggregationFunction(integerType, integerType);
    auto minValue = Aggregation::MinAggregationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &minValue);
    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 1);

    // lift value in minAgg
    minAgg.lift(memref, incomingValue);
    ASSERT_EQ(minValue.min, incomingValue);
    // combine memrefs in minAgg
    minAgg.combine(memref, memref);
    ASSERT_EQ(minValue.min, 1);
    // lower value in minAgg
    auto aggregationResult = minAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 1);
}

TEST_F(AggregationFunctionTest, scanEmitPipelineMax) {
    DataTypePtr integerType = DataTypeFactory::createInt64();
    auto maxAgg = Aggregation::MaxAggregationFunction(integerType, integerType);
    auto maxValue = Aggregation::MaxAggregationValue();
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) &maxValue);
    auto incomingValue = Nautilus::Value<Nautilus::Int64>((int64_t) 2);

    // lift value in minAgg
    maxAgg.lift(memref, incomingValue);
    ASSERT_EQ(maxValue.max, incomingValue);
    // combine memrefs in minAgg
    maxAgg.combine(memref, memref);
    ASSERT_EQ(maxValue.max, 2);
    // lower value in minAgg
    auto aggregationResult = maxAgg.lower(memref);
    ASSERT_EQ(aggregationResult, 2);
}


}// namespace NES::Runtime::Execution::Expressions
