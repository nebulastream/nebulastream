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

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
boost::asio::awaitable<std::optional<AsyncValues>> echoAfterSuspension(AsyncValues values)
{
    /// The borrowed view remains valid because the coroutine owns the input handle.
    const auto borrowed = values[0].get<std::string_view>();
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{1}};
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_return AsyncValues::make(borrowed, values[1].toOwned());
}
}

TEST(AsyncValuesTest, CopyAndMoveRetainSmallLargeAndBinaryPayloads)
{
    for (const auto& text : {std::string{"short"}, std::string(8192, 'x'), std::string{"a\0b", 3}})
    {
        auto original = AsyncValues::make(text, uint64_t{42});
        auto retained = original;
        original = AsyncValues::make();
        auto moved = std::move(retained);
        EXPECT_EQ(moved[0].get<std::string_view>(), text);
        EXPECT_EQ(moved[1].get<uint64_t>(), 42);
    }
}

TEST(AsyncValuesTest, ArgumentsAndResultsOwnTheirStorageAcrossSuspension)
{
    boost::asio::io_context context;
    auto input = AsyncValues::make(std::string{"small string"}, std::numeric_limits<uint64_t>::max());
    auto result = boost::asio::co_spawn(context, echoAfterSuspension(input), boost::asio::use_future);
    input = AsyncValues::make();
    context.run();
    const auto output = result.get().value();
    EXPECT_EQ(output[0].get<std::string_view>(), "small string");
    EXPECT_EQ(output[1].get<uint64_t>(), std::numeric_limits<uint64_t>::max());
}

TEST(AsyncValuesTest, EmptyAndTypedNullAreDifferent)
{
    static_assert(!std::is_default_constructible_v<AsyncValues>);
    const auto empty = AsyncValues::make();
    EXPECT_TRUE(empty.empty());
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto values = AsyncValues::make(RuntimeValue::null(nullable));
    EXPECT_EQ(values.size(), 1);
    EXPECT_TRUE(values[0].isNull());
    EXPECT_EQ(values[0].getDataType(), nullable);
}

TEST(AsyncValuesTest, BindingPreservesConstantPositionsAndTypedNulls)
{
    const DataType integer{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
    const AsyncFunctionBinding binding{
        .argumentTypes = {integer, integer, nullable},
        .constantArguments = {std::nullopt, RuntimeValue{uint64_t{4}}, RuntimeValue::null(nullable)},
        .resultTypes = {nullable, integer}};
    EXPECT_FALSE(binding.constantArguments[0]);
    ASSERT_TRUE(binding.constantArguments[1]);
    EXPECT_EQ(binding.constantArguments[1]->view().get<uint64_t>(), 4);
    ASSERT_TRUE(binding.constantArguments[2]);
    EXPECT_TRUE(binding.constantArguments[2]->isNull());
}

}
