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

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <magic_enum.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
namespace NES
{

/// Stores the left and right basic types and the expected result for joining/casting both operations
struct TypeConversionTestInput
{
    BasicType left;
    BasicType right;
    DataTypePtr expectedResult;
};

class NumericTypeConversionTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<TypeConversionTestInput>
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("NumericTypeConversionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("NumericTypeConversionTest test class SetUpTestCase.");
    }
};


TEST_P(NumericTypeConversionTest, SimpleTest)
{
    auto [left, right, result] = GetParam();
    auto join = [](const BasicType left, const BasicType right)
    { return NES::DataTypeFactory::createType(left)->join(NES::DataTypeFactory::createType(right)); };

    const auto leftRight = join(left, right);
    const auto rightLeft = join(right, left);
    NES_INFO("Joining {} and {} results in {}", magic_enum::enum_name(left), magic_enum::enum_name(right), leftRight->toString());
    NES_INFO("Joining {} and {} results in {}", magic_enum::enum_name(right), magic_enum::enum_name(left), rightLeft->toString());
    EXPECT_TRUE(*leftRight == *result);
    EXPECT_TRUE(*rightLeft == *result);
}

/// This tests the join operation for all possible combinations of basic types
/// As a ground truth, we use the C++ type promotion rules
INSTANTIATE_TEST_CASE_P(
    ThisIsARealTest,
    NumericTypeConversionTest,
    ::testing::Values<TypeConversionTestInput>(
        TypeConversionTestInput{BasicType::UINT8, BasicType::UINT8, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::UINT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::UINT32, DataTypeFactory::createUInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::UINT64, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::INT8, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::INT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::INT32, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::UINT8, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::UINT16, BasicType::UINT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::UINT32, DataTypeFactory::createUInt32()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::UINT64, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::INT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::INT32, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::UINT16, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::UINT32, BasicType::UINT32, DataTypeFactory::createUInt32()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::UINT64, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::INT16, DataTypeFactory::createUInt32()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::INT32, DataTypeFactory::createUInt32()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::UINT32, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::UINT64, BasicType::UINT64, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT64, BasicType::INT16, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT64, BasicType::INT32, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT64, BasicType::INT64, DataTypeFactory::createUInt64()},
        TypeConversionTestInput{BasicType::UINT64, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::UINT64, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::INT8, BasicType::INT8, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT8, BasicType::INT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT8, BasicType::INT32, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT8, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::INT8, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::INT8, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::INT16, BasicType::INT16, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT16, BasicType::INT32, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT16, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::INT16, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::INT16, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::INT32, BasicType::INT32, DataTypeFactory::createInt32()},
        TypeConversionTestInput{BasicType::INT32, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::INT32, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::INT32, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::INT64, BasicType::INT64, DataTypeFactory::createInt64()},
        TypeConversionTestInput{BasicType::INT64, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::INT64, BasicType::FLOAT64, DataTypeFactory::createDouble()},

        TypeConversionTestInput{BasicType::FLOAT32, BasicType::FLOAT32, DataTypeFactory::createFloat()},
        TypeConversionTestInput{BasicType::FLOAT64, BasicType::FLOAT32, DataTypeFactory::createDouble()},
        TypeConversionTestInput{BasicType::FLOAT64, BasicType::FLOAT64, DataTypeFactory::createDouble()}),
    [](const testing::TestParamInfo<NES::NumericTypeConversionTest::ParamType>& info)
    {
        return fmt::format(
            "{}_{}_{}",
            magic_enum::enum_name(info.param.left),
            magic_enum::enum_name(info.param.right),
            info.param.expectedResult->toString());
    });
}
