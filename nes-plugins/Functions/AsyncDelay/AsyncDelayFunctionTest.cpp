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
#include <cstdint>
#include <future>
#include <limits>
#include <optional>
#include <string>
#include <DataTypes/DataType.hpp>
#include <Functions/AsyncDelayLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <AsyncDelayAsyncFunction.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

TEST(AsyncDelayFunctionTest, ReplacingTheValueUpdatesTheDeclaredTypeAndNullabilityWithoutReinference)
{
    const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullableText{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
    const auto value = ConstantValueLogicalFunction{integer, "42"};
    const auto delay = ConstantValueLogicalFunction{integer, "0"};
    const AsyncDelayLogicalFunction original{value, delay, std::nullopt};
    const auto replacement = ConstantValueLogicalFunction{nullableText, "hello"};
    const auto rewritten = original.withChildren({replacement, delay});
    EXPECT_EQ(rewritten.getDataType(), nullableText);
    EXPECT_EQ(rewritten.getAsyncOutputTypes()[0], nullableText);
    EXPECT_EQ(rewritten.getAsyncOutputTypes()[1], (DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}));
    const DataType nullableInteger{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto widened = original.withChildren({ConstantValueLogicalFunction{nullableInteger, "42"}, delay});
    EXPECT_EQ(widened.getDataType(), nullableInteger);
    EXPECT_EQ(widened.getAsyncOutputTypes()[0], nullableInteger);
    EXPECT_EQ(original.getDataType(), integer);
}

TEST(AsyncDelayFunctionTest, ZeroDelayPreservesTypedNullAndReturnsNonNullableUnsignedMilliseconds)
{
    boost::asio::io_context context;
    AsyncDelayAsyncFunction function{std::nullopt};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto body = [&]() -> boost::asio::awaitable<void>
    {
        const auto result = co_await function.call(AsyncValues::make(RuntimeValue::null(nullable), uint64_t{0}));
        EXPECT_EQ(result.size(), 2);
        EXPECT_EQ(result[0].getDataType(), nullable);
        EXPECT_TRUE(result[0].isNull());
        EXPECT_EQ(result[1].getDataType(), (DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}));
        EXPECT_EQ(result[1].get<uint64_t>(), 0);
    };
    auto future = boost::asio::co_spawn(context, body(), boost::asio::use_future);
    context.run();
    future.get();
}

TEST(AsyncDelayFunctionTest, UnrepresentableDurationsAreInputErrorsBeforeStartingATimer)
{
    for (const auto millis : {std::numeric_limits<uint64_t>::max(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max())})
    {
        boost::asio::io_context context;
        AsyncDelayAsyncFunction function{std::nullopt};
        const auto body
            = [&]() -> boost::asio::awaitable<void> { static_cast<void>(co_await function.call(AsyncValues::make(int64_t{42}, millis))); };
        auto future = boost::asio::co_spawn(context, body(), boost::asio::use_future);
        context.run();
        EXPECT_THROW(future.get(), Exception);
    }
}

}
