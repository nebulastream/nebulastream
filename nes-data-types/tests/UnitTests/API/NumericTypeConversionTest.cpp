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

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
/// Stores the left and right basic types and the expected result for joining/casting both operations
struct TypeConversionTestInput
{
    DataType::Type left;
    DataType::Type right;
    DataType::NULLABLE leftIsNullable;
    DataType::NULLABLE rightIsNullable;
    DataType expectedResult;
};

class NumericTypeConversionTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<TypeConversionTestInput>
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NumericTypeConversionTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("NumericTypeConversionTest test class SetUpTestCase.");
    }
};

TEST_P(NumericTypeConversionTest, SimpleTest)
{
    auto [leftParam, rightParam, leftNullable, rightNullable, resultParam] = GetParam();
    auto join = [](const DataType::Type left,
                   const DataType::Type right,
                   const DataType::NULLABLE leftIsNullable,
                   const DataType::NULLABLE rightIsNullable)
    { return DataTypeProvider::provideDataType(left, leftIsNullable).join(DataTypeProvider::provideDataType(right, rightIsNullable)); };

    const auto leftRight = join(leftParam, rightParam, leftNullable, rightNullable);
    const auto rightLeft = join(rightParam, leftParam, leftNullable, rightNullable);
    NES_INFO("Joining {} and {} results in {}", DataType{leftParam, leftNullable}, DataType{rightParam, rightNullable}, leftRight);
    NES_INFO("Joining {} and {} results in {}", DataType{rightParam, rightNullable}, DataType{leftParam, leftNullable}, rightLeft);
    EXPECT_TRUE(leftRight == resultParam) << "Expected " << resultParam << " but got " << *leftRight;
    EXPECT_TRUE(rightLeft == resultParam) << "Expected " << resultParam << " but got " << *rightLeft;
}

/// This tests the join operation for all possible combinations of basic types
/// As a ground truth, we use the C++ type promotion rules
INSTANTIATE_TEST_CASE_P(
    ThisIsARealTest,
    NumericTypeConversionTest,
    ::testing::Values<TypeConversionTestInput>(
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::UINT8,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::UINT16,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::UINT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::UINT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::INT8,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT8,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::UINT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::UINT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::UINT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::INT64,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT16,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::UINT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::UINT64,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT32,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::UINT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::UINT64,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::INT8,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT8,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::INT16,
            DataType::Type::INT16,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT16,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT16,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT16,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT16,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::INT32,
            DataType::Type::INT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT32,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT32,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT32,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::INT64,
            DataType::Type::INT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT64,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::INT64,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},

        TypeConversionTestInput{
            DataType::Type::FLOAT32,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::IS_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::FLOAT64,
            DataType::Type::FLOAT32,
            DataType::NULLABLE::IS_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)},
        TypeConversionTestInput{
            DataType::Type::FLOAT64,
            DataType::Type::FLOAT64,
            DataType::NULLABLE::NOT_NULLABLE,
            DataType::NULLABLE::NOT_NULLABLE,
            DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE)}),
    [](const testing::TestParamInfo<NumericTypeConversionTest::ParamType>& info)
    {
        return fmt::format(
            "{}_{}_{}_{}_{}",
            magic_enum::enum_name(info.param.left),
            magic_enum::enum_name(info.param.right),
            magic_enum::enum_name(info.param.leftIsNullable),
            magic_enum::enum_name(info.param.rightIsNullable),
            magic_enum::enum_name(info.param.expectedResult.type));
    });
}
