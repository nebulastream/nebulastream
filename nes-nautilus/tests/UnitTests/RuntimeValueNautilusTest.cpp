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

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/RuntimeValueNautilus.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
constexpr std::array TYPES{
    DataType::Type::BOOLEAN,
    DataType::Type::CHAR,
    DataType::Type::UINT8,
    DataType::Type::UINT16,
    DataType::Type::UINT32,
    DataType::Type::UINT64,
    DataType::Type::INT8,
    DataType::Type::INT16,
    DataType::Type::INT32,
    DataType::Type::INT64,
    DataType::Type::FLOAT32,
    DataType::Type::FLOAT64,
    DataType::Type::VARSIZED};
}

TEST(RuntimeValueNautilusTest, EveryScalarRoundTripsWithoutTupleBuffers)
{
    for (const auto type : TYPES)
    {
        visitRuntimeType(
            type,
            []<typename T>()
            {
                if constexpr (std::same_as<T, std::string_view>)
                {
                    const RuntimeValue input{std::string{"a\0b", 3}};
                    const auto traced = toVarVal(input.view());
                    EXPECT_EQ(traced.getDataType(), input.getDataType());
                    const auto output = fromVarVal(traced, input.getDataType());
                    EXPECT_EQ(output.view().get<T>(), input.view().get<T>());
                }
                else
                {
                    const RuntimeValue input{std::numeric_limits<T>::max()};
                    const auto traced = toVarVal(input.view());
                    EXPECT_EQ(traced.getDataType(), input.getDataType());
                    const auto output = fromVarVal(traced, input.getDataType());
                    EXPECT_EQ(output.view().get<T>(), input.view().get<T>());
                }
            });
    }
}

TEST(RuntimeValueNautilusTest, NullAndNullableNonNullRetainTheirTypes)
{
    for (const auto type : TYPES)
    {
        const DataType nullable{type, DataType::NULLABLE::IS_NULLABLE};
        const auto input = RuntimeValue::null(nullable);
        const auto traced = toVarVal(input.view());
        EXPECT_EQ(traced.getDataType(), nullable);
        const auto output = fromVarVal(traced, nullable);
        EXPECT_TRUE(output.isNull());
        EXPECT_EQ(output.getDataType(), nullable);
    }
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const RuntimeValue input{nullable, int64_t{-17}};
    const auto output = fromVarVal(toVarVal(input.view()), nullable);
    EXPECT_FALSE(output.isNull());
    EXPECT_EQ(output.getDataType(), nullable);
    EXPECT_EQ(output.view().get<int64_t>(), -17);
}

TEST(RuntimeValueNautilusTest, MaterializedBytesDoNotBorrowInputStorage)
{
    RuntimeValue input{std::string{"short"}};
    const auto output = fromVarVal(toVarVal(input.view()), input.getDataType());
    input = RuntimeValue{std::string(8192, 'x')};
    EXPECT_EQ(output.view().get<std::string_view>(), "short");

    const RuntimeValue empty{std::string{}};
    const auto emptyOutput = fromVarVal(toVarVal(empty.view()), empty.getDataType());
    EXPECT_TRUE(emptyOutput.view().bytes().empty());
}

#ifndef NO_ASSERT
class RuntimeValueNautilusDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RuntimeValueNautilusDeathTest.log", LogLevel::LOG_DEBUG, false); }
};

TEST_F(RuntimeValueNautilusDeathTest, NumericAndNullabilityConversionsViolatePreconditions)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const VarVal integer{int64_t{42}};
    const DataType unsignedType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(static_cast<void>(fromVarVal(integer, unsignedType)), "Precondition violated.*does not match");
    const VarVal null{int64_t{0}, true, true};
    const DataType nonNullable{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(static_cast<void>(fromVarVal(null, nonNullable)), "Precondition violated.*does not match");
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(static_cast<void>(fromVarVal(integer, nullable)), "Precondition violated.*does not match");
    const VarVal nullableValue{int64_t{42}, true, false};
    EXPECT_DEATH(static_cast<void>(fromVarVal(nullableValue, nonNullable)), "Precondition violated.*does not match");
}
#endif

}
