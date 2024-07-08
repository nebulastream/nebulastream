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
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {
class AggregationFunctionDataTypeTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<std::string> {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }

    static PhysicalTypePtr getDataType(std::string dataTypeString, DefaultPhysicalTypeFactory factory) {
        if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
            return factory.getPhysicalType(DataTypeFactory::createInt8());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
            return factory.getPhysicalType(DataTypeFactory::createInt16());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
            return factory.getPhysicalType(DataTypeFactory::createInt32());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
            return factory.getPhysicalType(DataTypeFactory::createInt64());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
            return factory.getPhysicalType(DataTypeFactory::createUInt8());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
            return factory.getPhysicalType(DataTypeFactory::createUInt16());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
            return factory.getPhysicalType(DataTypeFactory::createUInt32());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
            return factory.getPhysicalType(DataTypeFactory::createUInt64());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
            return factory.getPhysicalType(DataTypeFactory::createFloat());
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
            return factory.getPhysicalType(DataTypeFactory::createDouble());
        } else {
            NES_ERROR("Unknown data type: {}", dataTypeString);
            NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
        }
    }

    static Nautilus::Value<> getIncomingValue(std::string dataTypeString) {
        if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
            return Nautilus::Value<Nautilus::Int8>(1_s8);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
            return Nautilus::Value<Nautilus::Int16>(1_s16);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
            return Nautilus::Value<Nautilus::Int32>(1_s32);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
            return Nautilus::Value<Nautilus::Int64>(1_s64);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
            return Nautilus::Value<Nautilus::UInt8>(1_u8);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
            return Nautilus::Value<Nautilus::UInt16>(1_u16);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
            return Nautilus::Value<Nautilus::UInt32>(1_u32);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
            return Nautilus::Value<Nautilus::UInt64>(1_u64);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
            return Nautilus::Value<Nautilus::Float>((float) 1.0);
        } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
            return Nautilus::Value<Nautilus::Double>((double) 1.0);
        } else {
            NES_ERROR("Unknown data type: {}", dataTypeString);
            NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
        }
    }

    static std::shared_ptr<Aggregation::AggregationValue> getAggregationValue(std::string aggFnType, std::string dataTypeString) {
        if (std::equal(aggFnType.begin(), aggFnType.end(), "sum")) {
            if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
                return std::make_shared<Aggregation::SumAggregationValue<int8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
                return std::make_shared<Aggregation::SumAggregationValue<int16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
                return std::make_shared<Aggregation::SumAggregationValue<int32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
                return std::make_shared<Aggregation::SumAggregationValue<int64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
                return std::make_shared<Aggregation::SumAggregationValue<uint8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
                return std::make_shared<Aggregation::SumAggregationValue<uint16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
                return std::make_shared<Aggregation::SumAggregationValue<uint32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
                return std::make_shared<Aggregation::SumAggregationValue<uint64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
                return std::make_shared<Aggregation::SumAggregationValue<float>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
                return std::make_shared<Aggregation::SumAggregationValue<double>>();
            } else {
                NES_ERROR("Unknown data type: {}", dataTypeString);
                NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
            }
        } else if (std::equal(aggFnType.begin(), aggFnType.end(), "avg")) {
            if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
                return std::make_shared<Aggregation::AvgAggregationValue<int8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
                return std::make_shared<Aggregation::AvgAggregationValue<int16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
                return std::make_shared<Aggregation::AvgAggregationValue<int32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
                return std::make_shared<Aggregation::AvgAggregationValue<int64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
                return std::make_shared<Aggregation::AvgAggregationValue<uint8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
                return std::make_shared<Aggregation::AvgAggregationValue<uint16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
                return std::make_shared<Aggregation::AvgAggregationValue<uint32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
                return std::make_shared<Aggregation::AvgAggregationValue<uint64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
                return std::make_shared<Aggregation::AvgAggregationValue<float>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
                return std::make_shared<Aggregation::AvgAggregationValue<double>>();
            } else {
                NES_ERROR("Unknown data type: {}", dataTypeString);
                NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
            }
        } else if (std::equal(aggFnType.begin(), aggFnType.end(), "min")) {
            if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
                return std::make_shared<Aggregation::MinAggregationValue<int8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
                return std::make_shared<Aggregation::MinAggregationValue<int16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
                return std::make_shared<Aggregation::MinAggregationValue<int32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
                return std::make_shared<Aggregation::MinAggregationValue<int64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
                return std::make_shared<Aggregation::MinAggregationValue<uint8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
                return std::make_shared<Aggregation::MinAggregationValue<uint16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
                return std::make_shared<Aggregation::MinAggregationValue<uint32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
                return std::make_shared<Aggregation::MinAggregationValue<uint64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
                return std::make_shared<Aggregation::MinAggregationValue<float>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
                return std::make_shared<Aggregation::MinAggregationValue<double>>();
            } else {
                NES_ERROR("Unknown data type: {}", dataTypeString);
                NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
            }
        } else if (std::equal(aggFnType.begin(), aggFnType.end(), "max")) {
            if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
                return std::make_shared<Aggregation::MaxAggregationValue<int8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
                return std::make_shared<Aggregation::MaxAggregationValue<int16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
                return std::make_shared<Aggregation::MaxAggregationValue<int32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
                return std::make_shared<Aggregation::MaxAggregationValue<int64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
                return std::make_shared<Aggregation::MaxAggregationValue<uint8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
                return std::make_shared<Aggregation::MaxAggregationValue<uint16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
                return std::make_shared<Aggregation::MaxAggregationValue<uint32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
                return std::make_shared<Aggregation::MaxAggregationValue<uint64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
                return std::make_shared<Aggregation::MaxAggregationValue<float>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
                return std::make_shared<Aggregation::MaxAggregationValue<double>>();
            } else {
                NES_ERROR("Unknown data type: {}", dataTypeString);
                NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
            }
        } else if (std::equal(aggFnType.begin(), aggFnType.end(), "count")) {
            if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i8")) {
                return std::make_shared<Aggregation::CountAggregationValue<int8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i16")) {
                return std::make_shared<Aggregation::CountAggregationValue<int16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i32")) {
                return std::make_shared<Aggregation::CountAggregationValue<int32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "i64")) {
                return std::make_shared<Aggregation::CountAggregationValue<int64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui8")) {
                return std::make_shared<Aggregation::CountAggregationValue<uint8_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui16")) {
                return std::make_shared<Aggregation::CountAggregationValue<uint16_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui32")) {
                return std::make_shared<Aggregation::CountAggregationValue<uint32_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "ui64")) {
                return std::make_shared<Aggregation::CountAggregationValue<uint64_t>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f32")) {
                return std::make_shared<Aggregation::CountAggregationValue<float>>();
            } else if (std::equal(dataTypeString.begin(), dataTypeString.end(), "f64")) {
                return std::make_shared<Aggregation::CountAggregationValue<double>>();
            } else {
                NES_ERROR("Unknown data type: {}", dataTypeString);
                NES_THROW_RUNTIME_ERROR("Unknown data type: " << dataTypeString);
            }
        } else {
            NES_ERROR("Unknown aggregation function type type: {}", aggFnType);
            NES_THROW_RUNTIME_ERROR("Unknown aggregation function type type: " << aggFnType);
        }
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
};

/**
 * Tests the lift, combine, lower and reset functions of the Sum Aggregation
 */
TEST_P(AggregationFunctionDataTypeTest, scanEmitPipelineSum) {
    physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    auto testParam = NES::Runtime::Execution::Expressions::AggregationFunctionDataTypeTest_scanEmitPipelineSum_Test::GetParam();
    std::string aggFunctionType = "sum";
    PhysicalTypePtr dataType = this->getDataType(testParam, physicalDataTypeFactory);
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto sumAgg = Aggregation::SumAggregationFunction(dataType, dataType, readFieldExpression, "result");

    // create an aggregation value based on the aggregation function type and data type
    auto sumValue = getAggregationValue(aggFunctionType, testParam);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) sumValue.get());

    // create an incoming value using the selected data type
    auto incomingValue = getIncomingValue(testParam);
    auto inputRecord = Record({{"value", incomingValue}});
    sumAgg.lift(memref, inputRecord);
    sumAgg.combine(memref, memref);
    auto result = Record();
    sumAgg.lower(memref, result);

    EXPECT_EQ(result.read("result")->getType()->toString(), testParam);// Check if the type corresponds to the input type
}

/**
 * Tests the lift, combine, lower and reset functions of the Avg Aggregation
 */
// TODO #3602 Disabled because this test checks that the output type is the same as the input type.
// However, the average of two ints can be a float.
TEST_P(AggregationFunctionDataTypeTest, DISABLED_scanEmitPipelineAvg) {
    physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    auto testParam = NES::Runtime::Execution::Expressions::AggregationFunctionDataTypeTest_scanEmitPipelineSum_Test::GetParam();
    std::string aggFunctionType = "avg";
    PhysicalTypePtr dataType = this->getDataType(testParam, physicalDataTypeFactory);
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto avgAgg = Aggregation::AvgAggregationFunction(dataType, dataType, readFieldExpression, "result");

    // create an aggregation value based on the aggregation function type and data type
    auto avgValue = getAggregationValue(aggFunctionType, testParam);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) avgValue.get());

    // create an incoming value using the selected data type
    auto incomingValue = getIncomingValue(testParam);
    auto inputRecord = Record({{"value", incomingValue}});
    avgAgg.lift(memref, inputRecord);
    avgAgg.combine(memref, memref);
    auto result = Record();
    avgAgg.lower(memref, result);

    EXPECT_EQ(result.read("result")->getType()->toString(), testParam);// Check if the type corresponds to the input type
}

/**
 * Tests the lift, combine, lower and reset functions of the Min Aggregation
 */
TEST_P(AggregationFunctionDataTypeTest, scanEmitPipelineMin) {
    physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    auto testParam = NES::Runtime::Execution::Expressions::AggregationFunctionDataTypeTest_scanEmitPipelineSum_Test::GetParam();
    std::string aggFunctionType = "min";
    PhysicalTypePtr dataType = this->getDataType(testParam, physicalDataTypeFactory);
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto minAgg = Aggregation::MinAggregationFunction(dataType, dataType, readFieldExpression, "result");

    // create an aggregation value based on the aggregation function type and data type
    auto minValue = getAggregationValue(aggFunctionType, testParam);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) minValue.get());

    // create an incoming value using the selected data type
    auto incomingValue = getIncomingValue(testParam);

    auto inputRecord = Record({{"value", incomingValue}});
    minAgg.lift(memref, inputRecord);
    minAgg.combine(memref, memref);
    auto result = Record();
    minAgg.lower(memref, result);

    EXPECT_EQ(result.read("result")->getType()->toString(), testParam);// Check if the type corresponds to the input type
}

/**
 * Tests the lift, combine, lower and reset functions of the Max Aggregation
 */
TEST_P(AggregationFunctionDataTypeTest, scanEmitPipelineMax) {
    physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    auto testParam = NES::Runtime::Execution::Expressions::AggregationFunctionDataTypeTest_scanEmitPipelineSum_Test::GetParam();
    std::string aggFunctionType = "max";
    PhysicalTypePtr dataType = this->getDataType(testParam, physicalDataTypeFactory);
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto maxAgg = Aggregation::MaxAggregationFunction(dataType, dataType, readFieldExpression, "result");

    // create an aggregation value based on the aggregation function type and data type
    auto maxValue = getAggregationValue(aggFunctionType, testParam);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) maxValue.get());

    // create an incoming value using the selected data type
    auto incomingValue = getIncomingValue(testParam);
    auto inputRecord = Record({{"value", incomingValue}});
    maxAgg.lift(memref, inputRecord);
    maxAgg.combine(memref, memref);
    auto result = Record();
    maxAgg.lower(memref, result);

    EXPECT_EQ(result.read("result")->getType()->toString(), testParam);// Check if the type corresponds to the input type
}

/**
 * Tests the lift, combine, lower and reset functions of the Count Aggregation
 */
TEST_P(AggregationFunctionDataTypeTest, scanEmitPipelineCount) {
    physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    auto testParam = NES::Runtime::Execution::Expressions::AggregationFunctionDataTypeTest_scanEmitPipelineSum_Test::GetParam();
    auto ui64 = "ui64";
    std::string aggFunctionType = "count";
    PhysicalTypePtr dataType = this->getDataType(testParam, physicalDataTypeFactory);
    PhysicalTypePtr unsignedIntegerType = this->getDataType(ui64, physicalDataTypeFactory);
    auto readFieldExpression = std::make_shared<Expressions::ReadFieldExpression>("value");

    auto countAgg = Aggregation::CountAggregationFunction(dataType, unsignedIntegerType, readFieldExpression, "result");

    // create an aggregation value based on the aggregation function type and data type
    auto countValue = getAggregationValue(aggFunctionType, ui64);
    auto memref = Nautilus::Value<Nautilus::MemRef>((int8_t*) countValue.get());

    // create an incoming value using the selected data type
    auto incomingValue = getIncomingValue(testParam);
    auto inputRecord = Record({{"value", incomingValue}});
    countAgg.lift(memref, inputRecord);
    countAgg.combine(memref, memref);
    auto result = Record();
    countAgg.lower(memref, result);

    // Check if the type count agg is of ui64 type irrespective of the input type
    EXPECT_EQ(result.read("result")->getType()->toString(), ui64);
}

// TODO #3468: parameterize the aggregation function instead of repeating the similar test
INSTANTIATE_TEST_CASE_P(scanEmitPipelineAgg,
                        AggregationFunctionDataTypeTest,
                        ::testing::Values("i8", "i16", "i32", "i64", "ui8", "ui16", "ui32", "ui64", "f32", "f64"),
                        [](const testing::TestParamInfo<AggregationFunctionDataTypeTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution::Expressions
