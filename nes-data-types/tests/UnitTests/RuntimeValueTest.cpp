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
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

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

TEST(RuntimeValueTest, ExactNativeTypesAndTypedNulls)
{
    for (const auto type : TYPES)
    {
        visitRuntimeType(
            type,
            [type]<typename T>()
            {
                const DataType nullable{type, DataType::NULLABLE::IS_NULLABLE};
                const auto null = RuntimeValue::null(nullable);
                EXPECT_TRUE(null.isNull());
                EXPECT_EQ(null.view().getDataType(), nullable);
                EXPECT_TRUE(null.view().toOwned().isNull());
                if constexpr (std::same_as<T, std::string_view>)
                {
                    const RuntimeValue value{std::string{"a\0b", 3}};
                    EXPECT_EQ(value.view().get<T>(), std::string_view("a\0b", 3));
                    EXPECT_EQ(value.view().bytes().size(), 3);
                }
                else
                {
                    const auto native = std::numeric_limits<T>::max();
                    const RuntimeValue value{native};
                    EXPECT_EQ(value.getDataType().type, type);
                    EXPECT_FALSE(value.getDataType().nullable);
                    EXPECT_EQ(value.view().get<T>(), native);
                    const auto copy = value.view().toOwned();
                    EXPECT_EQ(copy.view().get<T>(), native);
                }
            });
    }
}

TEST(RuntimeValueTest, NullableNonNullRetainsItsType)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const RuntimeValue value{type, int64_t{42}};
    EXPECT_FALSE(value.isNull());
    EXPECT_EQ(value.view().get<int64_t>(), 42);
    EXPECT_EQ(value.view().toOwned().getDataType(), type);
}

#ifndef NO_ASSERT
class RuntimeValueDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RuntimeValueDeathTest.log", LogLevel::LOG_DEBUG, false); }
};

TEST_F(RuntimeValueDeathTest, MismatchedPayloadUndefinedAndNonNullableNullViolatePreconditions)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH((RuntimeValue{type, uint64_t{42}}), "Precondition violated.*payload");
    EXPECT_DEATH(static_cast<void>(RuntimeValue::null(type)), "Precondition violated.*non-nullable");
    EXPECT_DEATH((RuntimeValue{DataType{}, int64_t{42}}), "Precondition violated.*UNDEFINED");
    EXPECT_DEATH(static_cast<void>(RuntimeValue::null(DataType{})), "Precondition violated.*UNDEFINED");
    EXPECT_DEATH((RuntimeValueView{type, uint64_t{42}}), "Precondition violated.*payload");
    EXPECT_DEATH(visitRuntimeType(DataType::Type::UNDEFINED, []<typename>() { }), "Precondition violated.*representation");
    const RuntimeValue value{int64_t{42}};
    EXPECT_DEATH(static_cast<void>(value.view().get<uint64_t>()), "Precondition violated.*native type");
    EXPECT_DEATH(static_cast<void>(value.view().bytes()), "Precondition violated.*native type");
}

TEST_F(RuntimeValueDeathTest, ReadingNullPayloadViolatesPrecondition)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    for (const auto type : TYPES)
    {
        visitRuntimeType(
            type,
            [type]<typename T>()
            {
                const auto null = RuntimeValue::null(DataType{type, DataType::NULLABLE::IS_NULLABLE});
                EXPECT_DEATH(static_cast<void>(null.view().get<T>()), "Precondition violated.*null");
            });
    }
}
#endif

TEST(RuntimeValueTest, ByteOwnershipSurvivesSmallAndLargeStringMoves)
{
    for (const size_t size : {size_t{3}, size_t{8192}})
    {
        std::string original(size, 'x');
        RuntimeValue value{std::string_view{original}};
        original.assign(size, 'y');
        auto moved = std::move(value);
        EXPECT_EQ(moved.view().get<std::string_view>(), std::string(size, 'x'));
        auto ownedCopy = moved.view().toOwned();
        moved = RuntimeValue{std::string{"replaced"}};
        EXPECT_EQ(ownedCopy.view().get<std::string_view>(), std::string(size, 'x'));
    }
}

TEST(RuntimeValueTest, BorrowedViewCanMaterializeIndependentStorage)
{
    std::string original{"borrowed"};
    const RuntimeValueView view{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, std::string_view{original}};
    const auto owned = view.toOwned();
    original[0] = 'B';
    EXPECT_EQ(view.get<std::string_view>(), "Borrowed");
    EXPECT_EQ(owned.view().get<std::string_view>(), "borrowed");
}

TEST(RuntimeValueTest, ParsesConstantsOnceWithoutNumericPrecisionLoss)
{
    const auto unsignedValue
        = RuntimeValue::fromLiteral(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "18446744073709551615");
    EXPECT_EQ(unsignedValue.view().get<uint64_t>(), std::numeric_limits<uint64_t>::max());
    const auto signedValue
        = RuntimeValue::fromLiteral(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}, "-9223372036854775808");
    EXPECT_EQ(signedValue.view().get<int64_t>(), std::numeric_limits<int64_t>::min());
    const DataType tiny{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(static_cast<void>(RuntimeValue::fromLiteral(tiny, "256")), Exception);
    EXPECT_THROW(static_cast<void>(RuntimeValue::fromLiteral(tiny, "-1")), Exception);
    EXPECT_THROW(static_cast<void>(RuntimeValue::fromLiteral(tiny, "12oops")), Exception);
}

TEST(RuntimeValueTest, CheckedIntegerConversionAcceptsWidthsButNotNegativeValues)
{
    const RuntimeValue tiny{uint8_t{42}};
    const RuntimeValue large{std::numeric_limits<uint64_t>::max()};
    const RuntimeValue positive{int16_t{17}};
    const RuntimeValue negative{int64_t{-1}};
    EXPECT_EQ(tiny.view().getUnsignedInteger(), 42);
    EXPECT_EQ(positive.view().getUnsignedInteger(), 17);
    EXPECT_EQ(large.view().getUnsignedInteger(), std::numeric_limits<uint64_t>::max());
    EXPECT_THROW(static_cast<void>(negative.view().getUnsignedInteger()), Exception);
}

#ifndef NO_ASSERT
TEST_F(RuntimeValueDeathTest, IntegerConversionRequiresAnInteger)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const RuntimeValue boolean{true};
    const RuntimeValue floating{1.0};
    EXPECT_DEATH(static_cast<void>(boolean.view().getUnsignedInteger()), "Precondition violated.*integer");
    EXPECT_DEATH(static_cast<void>(floating.view().getUnsignedInteger()), "Precondition violated.*integer");
}
#endif

}
