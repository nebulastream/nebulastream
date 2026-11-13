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
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/AsyncDelayLogicalFunction.hpp>
#include <Functions/AsyncOutputLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <AsyncDelayAsyncFunction.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

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

TEST(AsyncDelayFunctionTest, RejectsInvalidConstantDelaysAndConcurrencyAtInference)
{
    const Schema<Field, Unordered> schema{};
    const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto value = ConstantValueLogicalFunction{integer, "1"};
    const auto zero = ConstantValueLogicalFunction{integer, "0"};
    const AsyncDelayLogicalFunction valid{value, zero, std::nullopt};
    EXPECT_NO_THROW(static_cast<void>(valid.withInferredDataType(schema)));
    const AsyncDelayLogicalFunction negative{value, ConstantValueLogicalFunction{integer, "-1"}, std::nullopt};
    EXPECT_THROW(static_cast<void>(negative.withInferredDataType(schema)), Exception);
    const DataType unsignedInteger{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const AsyncDelayLogicalFunction tooLong{value, ConstantValueLogicalFunction{unsignedInteger, "18446744073709551615"}, std::nullopt};
    EXPECT_THROW(static_cast<void>(tooLong.withInferredDataType(schema)), Exception);
    for (const auto& limit : {"0", "-1"})
    {
        const AsyncDelayLogicalFunction invalid{value, zero, ConstantValueLogicalFunction{integer, limit}};
        EXPECT_THROW(static_cast<void>(invalid.withInferredDataType(schema)), Exception);
    }
    const AsyncDelayLogicalFunction bounded{value, zero, ConstantValueLogicalFunction{integer, "2"}};
    EXPECT_NO_THROW(static_cast<void>(bounded.withInferredDataType(schema)));
}

TEST(AsyncDelayFunctionTest, ErrorCodesDistinguishTypesFromInferenceAndRuntimeValues)
{
    const Schema<Field, Unordered> schema{};
    const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto value = ConstantValueLogicalFunction{integer, "1"};
    const auto zero = ConstantValueLogicalFunction{integer, "0"};
    const auto negative = ConstantValueLogicalFunction{integer, "-1"};
    const auto text = ConstantValueLogicalFunction{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "text"};
    const AsyncDelayLogicalFunction badDelay{value, negative, std::nullopt};
    const AsyncDelayLogicalFunction badConcurrency{value, zero, zero};
    const AsyncDelayLogicalFunction nonConstantConcurrency{value, zero, LogicalFunction{AddLogicalFunction{value, value}}};
    const AsyncDelayLogicalFunction wrongType{value, text, std::nullopt};
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(badDelay.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(badConcurrency.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(nonConstantConcurrency.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(wrongType.withInferredDataType(schema)), ErrorCode::DifferentFieldTypeExpected);
    const AsyncDelayLogicalFunction invalidLiteral{
        value, ConstantValueLogicalFunction{DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}, "256"}, std::nullopt};
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(invalidLiteral.withInferredDataType(schema)), ErrorCode::InvalidLiteral);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(AsyncDelayLogicalFunction::createASYNC_DELAY({.children = {}})), ErrorCode::InvalidQuerySyntax);

    const LogicalFunction call = AsyncDelayLogicalFunction{value, zero, std::nullopt};
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(AsyncOutputLogicalFunction{call, 2}.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(AsyncOutputLogicalFunction{value, 0}.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(AsyncOutputLogicalFunction::createASYNC_OUTPUT({.children = {}})), ErrorCode::InvalidQuerySyntax);

    AsyncDelayAsyncFunction function{std::nullopt};
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(function.prepare(SharedRuntimeValues::make(int64_t{1}, int64_t{-1}))), ErrorCode::ArithmeticalError);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(function.prepare(SharedRuntimeValues::make(int64_t{1}, std::numeric_limits<uint64_t>::max()))),
        ErrorCode::ArithmeticalError);
}

TEST(AsyncDelayFunctionTest, ZeroDelayPreservesTypedNullAndReturnsNonNullableUnsignedMilliseconds)
{
    boost::asio::io_context context;
    AsyncDelayAsyncFunction function{std::nullopt};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto body = [&]() -> boost::asio::awaitable<void>
    {
        const auto result = co_await function.call(SharedRuntimeValues::make(RuntimeValue::null(nullable), uint64_t{0}));
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

TEST(AsyncDelayFunctionTest, ConcurrencyBindingAndPreparedInvocationReuse)
{
    AsyncFunctionBinding binding;
    binding.constantArguments.resize(3);
    binding.constantArguments[2] = RuntimeValue{uint64_t{2}};
    auto function = AsyncDelayAsyncFunction::create(AsyncFunctionRegistryArguments{.binding = std::move(binding)});
    ASSERT_EQ(function->maxConcurrency(), size_t{2});
    boost::asio::io_context context;
    const auto body = [&]() -> boost::asio::awaitable<void>
    {
        for (const auto millis : {uint64_t{0}, uint64_t{1}})
        {
            const auto prepared = function->prepare(SharedRuntimeValues::make(int64_t{7}, millis, uint64_t{2}));
            for (int attempt = 0; attempt < 2; ++attempt)
            {
                const auto result = co_await prepared->call();
                EXPECT_EQ(result[0].get<int64_t>(), 7);
                EXPECT_EQ(result[1].get<uint64_t>(), millis);
            }
        }
    };
    auto future = boost::asio::co_spawn(context, body(), boost::asio::use_future);
    context.run();
    future.get();
}

TEST(AsyncDelayFunctionTest, DynamicBadDelayFailsSynchronouslyDuringPreparation)
{
    AsyncDelayAsyncFunction function{std::nullopt};
    EXPECT_THROW(static_cast<void>(function.prepare(SharedRuntimeValues::make(int64_t{1}, int64_t{-1}))), Exception);
}

TEST(AsyncDelayFunctionTest, UnrepresentableDurationsAreInputErrorsBeforeStartingATimer)
{
    for (const auto millis : {std::numeric_limits<uint64_t>::max(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max())})
    {
        boost::asio::io_context context;
        AsyncDelayAsyncFunction function{std::nullopt};
        const auto body = [&]() -> boost::asio::awaitable<void>
        { static_cast<void>(co_await function.call(SharedRuntimeValues::make(int64_t{42}, millis))); };
        auto future = boost::asio::co_spawn(context, body(), boost::asio::use_future);
        context.run();
        EXPECT_THROW(future.get(), Exception);
    }
}

}
