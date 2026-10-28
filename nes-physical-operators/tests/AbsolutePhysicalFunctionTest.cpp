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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/ArithmeticalFunctions/AbsolutePhysicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{
namespace
{
constexpr size_t TOTAL_MEMORY_IN_BYTES = 65536;
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr uint32_t POOLED_BUFFER_SIZE = 4096;

struct AbsoluteInt8TestCase
{
    int8_t input;
    bool nullable;
    bool isNull;
    int16_t expected;
};

class AbsolutePhysicalFunctionTest : public Testing::BaseUnitTest, public testing::WithParamInterface<AbsoluteInt8TestCase>
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AbsolutePhysicalFunctionTest.log", LogLevel::LOG_DEBUG); }
};

TEST_P(AbsolutePhysicalFunctionTest, ReturnsDeclaredTypeAndPreservesNullability)
{
    const auto& testCase = GetParam();
    const auto nullability = testCase.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    const auto fieldName = Identifier::parse("value");
    Record record;
    record.write(fieldName, VarVal{testCase.input, testCase.nullable, nautilus::val<bool>{testCase.isNull}});

    Arena arena{BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>())};
    ArenaRef arenaRef{&arena};
    const AbsolutePhysicalFunction function{
        FieldAccessPhysicalFunction{fieldName}, DataType{DataType::Type::INT8, nullability}, DataType{DataType::Type::INT16, nullability}};
    const auto result = function.execute(record, arenaRef);

    result.customVisit(
        []<typename T>(const T& value)
        {
            EXPECT_TRUE((std::is_same_v<T, nautilus::val<int16_t>>));
            return VarVal{value};
        });
    EXPECT_EQ(result.isNullable(), testCase.nullable);
    EXPECT_EQ(result.isNull(), testCase.isNull);
    if (!testCase.isNull)
    {
        EXPECT_EQ(result.getRawValueAs<nautilus::val<int16_t>>(), testCase.expected);
    }
}

INSTANTIATE_TEST_SUITE_P(
    Int8,
    AbsolutePhysicalFunctionTest,
    testing::Values(
        AbsoluteInt8TestCase{-128, false, false, 128},
        AbsoluteInt8TestCase{-7, false, false, 7},
        AbsoluteInt8TestCase{0, false, false, 0},
        AbsoluteInt8TestCase{7, false, false, 7},
        AbsoluteInt8TestCase{-128, true, false, 128},
        AbsoluteInt8TestCase{-7, true, false, 7},
        AbsoluteInt8TestCase{0, true, false, 0},
        AbsoluteInt8TestCase{7, true, false, 7},
        AbsoluteInt8TestCase{-7, true, true, 0},
        AbsoluteInt8TestCase{7, true, true, 0}));
}
}
