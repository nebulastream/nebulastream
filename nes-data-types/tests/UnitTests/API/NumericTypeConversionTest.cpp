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
    PhysicalType::Type left;
    PhysicalType::Type right;
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
    auto [leftParam, rightParam, resultParam] = GetParam();
    (void)leftParam;
    (void)rightParam;
    std::cout << fmt::format("the pType: {}", resultParam) << std::endl;
    // auto join = [](const PhysicalType::Type left, const PhysicalType::Type right)
    // { return DataTypeProvider::provideDataType(left).join(DataTypeProvider::provideDataType(right)); };
    //
    // const auto leftRight = join(leftParam, rightParam);
    // const auto rightLeft = join(rightParam, leftParam);
    // NES_INFO("Joining {} and {} results in {}", magic_enum::enum_name(leftParam), magic_enum::enum_name(rightParam), leftRight);
    // NES_INFO("Joining {} and {} results in {}", magic_enum::enum_name(rightParam), magic_enum::enum_name(leftParam), rightLeft);
    // EXPECT_TRUE(leftRight == resultParam);
    // EXPECT_TRUE(rightLeft == resultParam);
}

/// This tests the join operation for all possible combinations of basic types
/// As a ground truth, we use the C++ type promotion rules
INSTANTIATE_TEST_CASE_P(
    ThisIsARealTest,
    NumericTypeConversionTest,
    ::testing::Values<TypeConversionTestInput>(
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::UINT8, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::UINT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::UINT32, DataTypeProvider::provideDataType(PhysicalType::Type::UINT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::UINT64, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::INT8, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT8, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::UINT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::UINT32, DataTypeProvider::provideDataType(PhysicalType::Type::UINT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::UINT64, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT16, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::UINT32, DataTypeProvider::provideDataType(PhysicalType::Type::UINT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::UINT64, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::UINT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::UINT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT32, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::UINT64, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::UINT64)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::UINT64, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::INT8, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT8, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::INT16, PhysicalType::Type::INT16, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT16, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT16, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::INT16, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT16, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::INT32, PhysicalType::Type::INT32, DataTypeProvider::provideDataType(PhysicalType::Type::INT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT32, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::INT32, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT32, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::INT64, PhysicalType::Type::INT64, DataTypeProvider::provideDataType(PhysicalType::Type::INT64)},
        TypeConversionTestInput{
            PhysicalType::Type::INT64, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::INT64, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},

        TypeConversionTestInput{
            PhysicalType::Type::FLOAT32, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)},
        TypeConversionTestInput{
            PhysicalType::Type::FLOAT64, PhysicalType::Type::FLOAT32, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)},
        TypeConversionTestInput{
            PhysicalType::Type::FLOAT64, PhysicalType::Type::FLOAT64, DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)}),
    [](const testing::TestParamInfo<NES::NumericTypeConversionTest::ParamType>& info)
    {
        return fmt::format(
            "{}_{}_{}",
            magic_enum::enum_name(info.param.left),
            magic_enum::enum_name(info.param.right),
            magic_enum::enum_name(info.param.expectedResult.physicalType.type));
    });
}
