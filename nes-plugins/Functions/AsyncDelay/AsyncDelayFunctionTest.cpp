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
/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/OperatorCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; cleaner misattributes calls.
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <AsyncDelayAsyncOperator.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};

LogicalFunction delayCall(std::vector<LogicalFunction> args)
{
    return LogicalFunctionProvider::provide("ASYNC_DELAY", std::move(args));
}
}

TEST(AsyncDelayFunctionTest, NativeConfigurationSignatureAndInitialValueType)
{
    const auto op = AsyncDelayAsyncOperator::create({.inputs = AsyncInputAccessors{{integer, integer}}});
    EXPECT_EQ(op.getType(), "ASYNC_DELAY");
    const auto output = op.getOutputSchema();
    ASSERT_EQ(output.size(), 1);
    EXPECT_EQ(output.begin()->getDataType(), integer);
    EXPECT_EQ(static_cast<const Identifier&>(output.begin()->getFullyQualifiedName()), Identifier::parse("value"));
    EXPECT_THROW(AsyncDelayAsyncOperator::create({.inputs = AsyncInputAccessors{{integer}}}), Exception);
    EXPECT_THROW(AsyncDelayAsyncOperator::create({.inputs = AsyncInputAccessors{{integer, integer, integer, integer}}}), Exception);
    const auto scalar = LogicalFunctionProvider::provide(
        "ASYNC_DELAY", {ConstantValueLogicalFunction{integer, "1"}, ConstantValueLogicalFunction{integer, "0"}});
    EXPECT_EQ(scalar.getAs<OperatorCallLogicalFunction>()->getOutputName(), Identifier::parse("value"));
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(LogicalFunctionProvider::provide("ASYNC_DELAY", {ConstantValueLogicalFunction{integer, "1"}})
                              .withInferredDataType(Schema<Field, Unordered>{})),
        ErrorCode::InvalidQuerySyntax);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(LogicalFunctionProvider::provide(
                              "ASYNC_DELAY",
                              {ConstantValueLogicalFunction{integer, "1"},
                               ConstantValueLogicalFunction{integer, "0"},
                               ConstantValueLogicalFunction{integer, "1"},
                               ConstantValueLogicalFunction{integer, "2"}})
                              .withInferredDataType(Schema<Field, Unordered>{})),
        ErrorCode::InvalidQuerySyntax);
}

TEST(AsyncDelayFunctionTest, LogicalInferenceValidatesOptionsAndCarriesConcurrency)
{
    const Schema<Field, Unordered> schema{};
    const auto one = ConstantValueLogicalFunction{integer, "1"};
    const auto zero = ConstantValueLogicalFunction{integer, "0"};
    const auto configured = delayCall({one, zero, ConstantValueLogicalFunction{integer, "2"}}).withInferredDataType(schema);
    const auto& arguments = configured.getAs<OperatorCallLogicalFunction>()->getArguments();
    ASSERT_EQ(arguments.size(), 3U);
    EXPECT_EQ(arguments.at(2).getDataType(), integer);
    const auto nullable = DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(delayCall({one, ConstantValueLogicalFunction{nullable, "0"}}).withInferredDataType(schema)),
        ErrorCode::DifferentFieldTypeExpected);
    for (const auto& invalid : {std::string{"0"}, std::string{"-1"}})
    {
        ASSERT_EXCEPTION_ERRORCODE(
            static_cast<void>(delayCall({one, zero, ConstantValueLogicalFunction{integer, invalid}}).withInferredDataType(schema)),
            ErrorCode::CannotInferSchema);
    }
}

TEST(AsyncDelayFunctionTest, LogicalInferenceAndRewriteKeepArgumentsOutsideConfiguration)
{
    const auto one = ConstantValueLogicalFunction{integer, "1"};
    const auto zero = ConstantValueLogicalFunction{integer, "0"};
    const auto call = delayCall({one, zero}).withInferredDataType(Schema<Field, Unordered>{});
    EXPECT_EQ(call.getDataType(), integer);
    EXPECT_EQ(call.getAs<OperatorCallLogicalFunction>()->getArguments().size(), 2);
    const auto nullable = DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto widened
        = call.withChildren({ConstantValueLogicalFunction{nullable, "2"}, zero}).withInferredDataType(Schema<Field, Unordered>{});
    EXPECT_EQ(widened.getDataType(), nullable);
    const auto withConcurrency
        = delayCall({one, zero, ConstantValueLogicalFunction{integer, "2"}}).withInferredDataType(Schema<Field, Unordered>{});
    EXPECT_EQ(withConcurrency.getDataType(), integer);
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto reflected = writer.reflect(withConcurrency);
    const auto restored = reader.unreflect<LogicalFunction>(reflected);
    EXPECT_TRUE(std::ranges::equal(restored.getChildren(), withConcurrency.getChildren(), StructuralFunctionEqual{}));
    ASSERT_EQ(restored.getAs<OperatorCallLogicalFunction>()->getArguments().size(), 3U);
    EXPECT_EQ(
        restored.getAs<OperatorCallLogicalFunction>()->getArguments().at(2).getAs<ConstantValueLogicalFunction>()->getConstantValue(), "2");
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(delayCall({one, ConstantValueLogicalFunction{integer, "-1"}}).withInferredDataType(Schema<Field, Unordered>{})),
        ErrorCode::CannotInferSchema);
}

TEST(AsyncDelayFunctionTest, ZeroDelayReturnsOnlyTypedValueAndPreparedCallCanBeReused)
{
    boost::asio::io_context ioContext;
    const AsyncOperator op = AsyncDelayAsyncOperator{.valueType = integer, .concurrencyLimit = std::nullopt};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const AsyncOperator nullableOp = AsyncDelayAsyncOperator{.valueType = nullable, .concurrencyLimit = std::nullopt};
    auto future = boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) co_spawn is declared by public boost/asio/co_spawn.hpp.
        ioContext, invokeOnce(nullableOp, SharedRuntimeValues::make(RuntimeValue::null(nullable), uint64_t{0})), boost::asio::use_future);
    ioContext.run();
    const auto result = future.get();
    ASSERT_TRUE(result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0].getDataType(), nullable);
    EXPECT_TRUE((*result)[0].isNull());

    AsyncExecutionContext context;
    const auto prepared = op.prepare(context, SharedRuntimeValues::make(int64_t{9}, uint64_t{0})).value();
    EXPECT_EQ(prepared->getRetryOverride(), std::nullopt);
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        boost::asio::io_context retryIo;
        auto attemptFuture = boost::asio::co_spawn(retryIo, prepared->executeAttempt(), boost::asio::use_future);
        retryIo.run();
        const auto attemptValues = attemptFuture.get().value();
        EXPECT_EQ(attemptValues[0].get<int64_t>(), 9);
    }
}

TEST(AsyncDelayFunctionTest, FactoryAccessorsReadTheCurrentRowOnceBeforeSequentialAttempts)
{
    const DataType uintType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const AsyncInputAccessors inputs{{integer, uintType}};
    const auto op = AsyncDelayAsyncOperator::create({.inputs = inputs});
    const auto values = std::make_shared<std::vector<RuntimeValue>>(
        std::initializer_list<RuntimeValue>{RuntimeValue{int64_t{17}}, RuntimeValue{uint64_t{0}}});
    const auto reads = std::make_shared<size_t>(0);
    const auto row = inputs.createRowHandle(
        [values, reads](const size_t index)
        {
            ++*reads;
            return values->at(index);
        });
    AsyncExecutionContext context;
    const auto prepared = op.prepare(context, row).value();
    EXPECT_EQ(*reads, 2);
    values->at(0) = RuntimeValue{int64_t{42}};
    for (size_t attempt = 0; attempt < 2; ++attempt)
    {
        boost::asio::io_context io;
        auto future = boost::asio::co_spawn(io, prepared->executeAttempt(), boost::asio::use_future);
        io.run();
        const auto result = future.get().value();
        EXPECT_EQ(result[0].get<int64_t>(), 17);
    }
    EXPECT_EQ(*reads, 2);
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(AsyncDelayFunctionTest, InferredConcurrencyConfigurationExecutesThroughFreshContexts)
{
    const DataType uintType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto config = AsyncDelayAsyncOperator::create({.inputs = AsyncInputAccessors{{integer, uintType}}}, size_t{2});
    ASSERT_EQ(config.maxConcurrency(), size_t{2});
    boost::asio::io_context ioContext;
    const auto work = [&]() -> boost::asio::awaitable<void>
    {
        const auto one = co_await invokeOnce(config, SharedRuntimeValues::make(int64_t{10}, uint64_t{0}));
        const auto two = co_await invokeOnce(config, SharedRuntimeValues::make(int64_t{20}, uint64_t{1}));
        EXPECT_TRUE(one);
        if (!one)
        {
            co_return;
        }
        EXPECT_TRUE(two);
        if (!two)
        {
            co_return;
        }
        EXPECT_EQ((*one)[0].get<int64_t>(), 10);
        EXPECT_EQ((*two)[0].get<int64_t>(), 20);
    };
    auto future = boost::asio::co_spawn(ioContext, work(), boost::asio::use_future);
    ioContext.run();
    future.get();

    AsyncExecutionContext context;
    const auto prepared = config.prepare(context, SharedRuntimeValues::make(int64_t{30}, uint64_t{0})).value();
    EXPECT_EQ(prepared->getRetryOverride(), std::nullopt);
    for (size_t attempt = 0; attempt < 2; ++attempt)
    {
        boost::asio::io_context attemptIo;
        auto attemptFuture = boost::asio::co_spawn(attemptIo, prepared->executeAttempt(), boost::asio::use_future);
        attemptIo.run();
        const auto attemptValues = attemptFuture.get().value();
        EXPECT_EQ(attemptValues[0].get<int64_t>(), 30);
    }
}

/// NOLINTEND(misc-include-cleaner)

TEST(AsyncDelayFunctionTest, DynamicInvalidAndUnrepresentableDurationsFailBeforeTimer)
{
    const AsyncOperator op = AsyncDelayAsyncOperator{.valueType = integer, .concurrencyLimit = std::nullopt};
    for (const auto millis : {int64_t{-1}, std::numeric_limits<int64_t>::max()})
    {
        AsyncExecutionContext context;
        const auto prepared = op.prepare(context, SharedRuntimeValues::make(int64_t{1}, millis));
        EXPECT_FALSE(prepared);
        EXPECT_EQ(prepared.error().kind, AsyncError::Type::Permanent);
    }
    AsyncExecutionContext context;
    const auto tooLarge = op.prepare(context, SharedRuntimeValues::make(int64_t{1}, std::numeric_limits<uint64_t>::max()));
    EXPECT_FALSE(tooLarge);
    EXPECT_EQ(tooLarge.error().kind, AsyncError::Type::Permanent);
    AsyncExecutionContext wrongTypeContext;
    const auto wrongType = op.prepare(wrongTypeContext, SharedRuntimeValues::make(int64_t{1}, std::string{"not millis"}));
    EXPECT_FALSE(wrongType);
    EXPECT_EQ(wrongType.error().kind, AsyncError::Type::Permanent);
}

TEST(AsyncDelayFunctionTest, RejectsInvalidConstantDelayAndConcurrencyOptions)
{
    const auto one = ConstantValueLogicalFunction{integer, "1"};
    const auto zero = ConstantValueLogicalFunction{integer, "0"};
    const Schema<Field, Unordered> schema{};
    const auto negativeMillis = delayCall({one, ConstantValueLogicalFunction{integer, "-1"}});
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(negativeMillis.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    const auto unrepresentableMillis = delayCall(
        {one, ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "18446744073709551615"}});
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(unrepresentableMillis.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    for (const auto& limit : {std::string{"0"}, std::string{"-1"}})
    {
        const auto invalid = delayCall({one, zero, ConstantValueLogicalFunction{integer, limit}});
        ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(invalid.withInferredDataType(schema)), ErrorCode::CannotInferSchema);
    }
    const auto nullableMillis
        = delayCall({one, ConstantValueLogicalFunction{DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}, "2"}});
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(nullableMillis.withInferredDataType(schema)), ErrorCode::DifferentFieldTypeExpected);
}

TEST(AsyncDelayFunctionTest, DynamicPreparationRejectsInvalidTypesAndDurations)
{
    const AsyncOperator config = AsyncDelayAsyncOperator{.valueType = integer, .concurrencyLimit = std::nullopt};
    for (const auto& millis :
         {RuntimeValue{int64_t{-1}}, RuntimeValue{std::numeric_limits<uint64_t>::max()}, RuntimeValue{std::string{"bad"}}})
    {
        AsyncExecutionContext context;
        const auto result = config.prepare(context, SharedRuntimeValues{RuntimeValue{int64_t{7}}, millis});
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().kind, AsyncError::Type::Permanent);
        if (!millis.view().getDataType().isInteger())
        {
            EXPECT_EQ(result.error().exception.code(), ErrorCode::DifferentFieldTypeExpected);
        }
    }
}

TEST(AsyncDelayFunctionTest, PreparationRejectsAValueOutsideTheDeclaredOutputType)
{
    const AsyncOperator config = AsyncDelayAsyncOperator{.valueType = integer, .concurrencyLimit = std::nullopt};
    AsyncExecutionContext context;
    const auto result = config.prepare(context, SharedRuntimeValues::make(std::string{"wrong type"}, uint64_t{0}));
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, AsyncError::Type::Permanent);
    EXPECT_EQ(result.error().exception.code(), ErrorCode::DifferentFieldTypeExpected);
}

TEST(AsyncDelayFunctionTest, InferredConfigurationRetainsLimitAndPreparedCallAcrossAttempts)
{
    const DataType uintType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto config = AsyncDelayAsyncOperator::create({.inputs = AsyncInputAccessors{{integer, uintType}}}, size_t{3});
    EXPECT_EQ(config.maxConcurrency(), size_t{3});
    AsyncExecutionContext context;
    const auto prepared = config.prepare(context, SharedRuntimeValues::make(int64_t{7}, uint64_t{0})).value();
    EXPECT_EQ(prepared->getRetryOverride(), std::nullopt);
    for (size_t attempt = 0; attempt < 2; ++attempt)
    {
        boost::asio::io_context ioContext;
        auto future = boost::asio::co_spawn(ioContext, prepared->executeAttempt(), boost::asio::use_future);
        ioContext.run();
        const auto resultValues = future.get().value();
        EXPECT_EQ(resultValues[0].get<int64_t>(), 7);
    }
}

TEST(AsyncDelayFunctionTest, CancellationIsReturnedAsAsyncError)
{
    boost::asio::io_context ioContext;
    const AsyncOperator op = AsyncDelayAsyncOperator{.valueType = integer, .concurrencyLimit = std::nullopt};
    AsyncExecutionContext context;
    auto prepared = op.prepare(context, SharedRuntimeValues::make(int64_t{42}, uint64_t{1000})).value();
    boost::asio::cancellation_signal cancellation;
    boost::asio::steady_timer stop{ioContext, std::chrono::milliseconds{1}};
    stop.async_wait([&](const auto&) { cancellation.emit(boost::asio::cancellation_type::terminal); });
    auto future = boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) co_spawn is declared by public boost/asio/co_spawn.hpp.
        ioContext, prepared->executeAttempt(), boost::asio::bind_cancellation_slot(cancellation.slot(), boost::asio::use_future));
    ioContext.run();
    EXPECT_EQ(future.get().error().kind, AsyncError::Type::Cancelled);
}
}

/// NOLINTEND(readability-magic-numbers)
