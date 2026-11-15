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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
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
boost::asio::awaitable<SharedRuntimeValues> echoAfterSuspension(SharedRuntimeValues values)
{
    /// The borrowed view remains valid because the coroutine owns the input handle.
    const auto borrowed = values[0].get<std::string_view>();
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{1}};
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_return SharedRuntimeValues::make(borrowed, values[1].toOwned());
}

class EchoFunction final : public AsyncFunction
{
    class Prepared final : public PreparedAsyncCall
    {
    public:
        Prepared(EchoFunction& function, SharedRuntimeValues arguments)
            : PreparedAsyncCall(function.retryPolicy), function(function), arguments(std::move(arguments))
        {
            ++function.livePrepared;
        }

        ~Prepared() override { --function.livePrepared; }

        boost::asio::awaitable<SharedRuntimeValues> executeAttempt() override
        {
            ++function.attempts;
            if (function.failAttempt)
            {
                throw std::runtime_error{"attempt failed"};
            }
            co_return co_await echoAfterSuspension(arguments);
        }

    private:
        EchoFunction& function;
        SharedRuntimeValues arguments;
    };

public:
    std::unique_ptr<PreparedAsyncCall> prepare(SharedRuntimeValues arguments) override
    {
        ++preparations;
        if (failPreparation)
        {
            throw std::runtime_error{"preparation failed"};
        }
        return std::make_unique<Prepared>(*this, std::move(arguments));
    }

    size_t attempts = 0;
    size_t preparations = 0;
    size_t livePrepared = 0;
    std::optional<size_t> retryPolicy = size_t{4};
    bool failPreparation = false;
    bool failAttempt = false;
};
}

TEST(SharedRuntimeValuesTest, CopyAndMoveRetainSmallLargeAndBinaryPayloads)
{
    for (const auto& text : {std::string{"short"}, std::string(8192, 'x'), std::string{"a\0b", 3}})
    {
        auto original = SharedRuntimeValues::make(text, uint64_t{42});
        auto retained = original;
        original = SharedRuntimeValues::make();
        auto moved = std::move(retained);
        EXPECT_TRUE(retained.empty());
        EXPECT_EQ(retained.size(), 0);
        EXPECT_EQ(moved[0].get<std::string_view>(), text);
        EXPECT_EQ(moved[1].get<uint64_t>(), 42);
    }
}

TEST(SharedRuntimeValuesTest, ArgumentsAndResultsOwnTheirStorageAcrossSuspension)
{
    boost::asio::io_context context;
    auto input = SharedRuntimeValues::make(std::string{"small string"}, std::numeric_limits<uint64_t>::max());
    auto result = boost::asio::co_spawn(context, echoAfterSuspension(input), boost::asio::use_future);
    input = SharedRuntimeValues::make();
    context.run();
    const auto output = result.get();
    EXPECT_EQ(output[0].get<std::string_view>(), "small string");
    EXPECT_EQ(output[1].get<uint64_t>(), std::numeric_limits<uint64_t>::max());
}

TEST(SharedRuntimeValuesTest, PreparationOwnsArgumentsAndFixesRetryMetadata)
{
    EchoFunction function;

    auto arguments = SharedRuntimeValues::make(std::string{"retained"}, uint64_t{42});
    const auto prepared = function.prepare(arguments);
    arguments = SharedRuntimeValues::make();
    EXPECT_EQ(function.preparations, 1);
    EXPECT_EQ(prepared->retryOverride, size_t{4});
    function.retryPolicy = 0;
    EXPECT_EQ(prepared->retryOverride, size_t{4});
    static_assert(std::is_const_v<decltype(prepared->retryOverride)>);
    boost::asio::io_context context;
    const auto run = [&]() -> boost::asio::awaitable<void>
    {
        for (size_t attempt = 0; attempt < 2; ++attempt)
        {
            const auto result = co_await prepared->executeAttempt();
            EXPECT_EQ(result[0].get<std::string_view>(), "retained");
            EXPECT_EQ(result[1].get<uint64_t>(), 42);
        }
    };
    auto result = boost::asio::co_spawn(context, run(), boost::asio::use_future);
    context.run();
    result.get();
    EXPECT_EQ(function.preparations, 1);
    EXPECT_EQ(function.attempts, 2);
    EXPECT_EQ(function.prepare({})->retryOverride, size_t{0});
    function.retryPolicy = std::nullopt;
    EXPECT_EQ(function.prepare({})->retryOverride, std::nullopt);
}

TEST(SharedRuntimeValuesTest, InvokeOnceOwnsPreparedCallAcrossSuspension)
{
    EchoFunction function;
    boost::asio::io_context context;
    auto arguments = SharedRuntimeValues::make(std::string{"retained"}, uint64_t{42});
    auto result = boost::asio::co_spawn(context, invokeOnce(function, arguments), boost::asio::use_future);
    arguments = {};
    context.run();
    const auto values = result.get();
    EXPECT_EQ(values[0].get<std::string_view>(), "retained");
    EXPECT_EQ(values[1].get<uint64_t>(), 42);
    EXPECT_EQ(function.preparations, 1);
    EXPECT_EQ(function.attempts, 1);
    EXPECT_EQ(function.livePrepared, 0);
}

TEST(SharedRuntimeValuesTest, InvokeOncePropagatesPreparationAndAttemptFailuresWithoutRetries)
{
    for (const bool failPreparation : {false, true})
    {
        EchoFunction function;
        function.failPreparation = failPreparation;
        function.failAttempt = true;
        boost::asio::io_context context;
        auto result = boost::asio::co_spawn(context, invokeOnce(function, {}), boost::asio::use_future);
        context.run();
        EXPECT_THROW(result.get(), std::runtime_error);
        EXPECT_EQ(function.preparations, 1);
        EXPECT_EQ(function.attempts, failPreparation ? 0 : 1);
        EXPECT_EQ(function.livePrepared, 0);
    }
}

TEST(SharedRuntimeValuesTest, PreparedCallDefaultsToExecutorRetryPolicyAndMayReturnEmptySuccess)
{
    class EmptyFunction final : public AsyncFunction
    {
        class Prepared final : public PreparedAsyncCall
        {
        public:
            boost::asio::awaitable<SharedRuntimeValues> executeAttempt() override { co_return SharedRuntimeValues{}; }
        };

    public:
        std::unique_ptr<PreparedAsyncCall> prepare(SharedRuntimeValues) override { return std::make_unique<Prepared>(); }
    } function;

    EXPECT_EQ(function.prepare({})->retryOverride, std::nullopt);
    EXPECT_EQ(function.maxConcurrency(), std::nullopt);
    boost::asio::io_context context;
    auto result = boost::asio::co_spawn(context, invokeOnce(function, {}), boost::asio::use_future);
    context.run();
    EXPECT_TRUE(result.get().empty());
}

TEST(SharedRuntimeValuesTest, EmptyAndTypedNullAreDifferent)
{
    static_assert(std::is_default_constructible_v<SharedRuntimeValues>);
    const SharedRuntimeValues empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.size(), 0);
    EXPECT_TRUE(SharedRuntimeValues::make().empty());
    const auto copy = empty;
    auto moved = copy;
    const auto retained = std::move(moved);
    EXPECT_TRUE(retained.empty());
    EXPECT_TRUE(moved.empty());
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto values = SharedRuntimeValues::make(RuntimeValue::null(nullable));
    EXPECT_EQ(values.size(), 1);
    EXPECT_TRUE(values[0].isNull());
    EXPECT_EQ(values[0].getDataType(), nullable);
}

TEST(SharedRuntimeValuesTest, BindingPreservesConstantPositionsAndTypedNulls)
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
