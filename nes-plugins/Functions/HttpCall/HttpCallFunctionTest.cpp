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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/AsyncCallLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; cleaner misattributes co_spawn.
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/error.hpp>
#include <boost/beast/http/message.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for request type; cleaner misses this test's use.
#include <boost/beast/http/read.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for read; cleaner misses this test's use.
#include <boost/beast/http/string_body.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for string_body; cleaner misses this test's use.
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; cleaner misattributes error_code.
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <HttpCallAsyncOperator.hpp>

namespace NES
{
namespace
{
const DataType text{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};
const DataType integer{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

RuntimeValue textValue(const std::string& value)
{
    return *RuntimeValue::fromLiteral(text, value);
}

RuntimeValue integerValue(const std::string& value)
{
    return *RuntimeValue::fromLiteral(integer, value);
}

LogicalFunction constant(DataType type, std::string value)
{
    return ConstantValueLogicalFunction{type, std::move(value)};
}

LogicalFunction httpCall(std::vector<LogicalFunction> args)
{
    return LogicalFunctionProvider::provide("HTTP_CALL", std::move(args));
}
}

TEST(HttpCallFunctionTest, NativeConfigurationHasStableScalarOutputAndValidatesArity)
{
    const auto op = HttpCallAsyncOperator::create({.argumentTypes = {text, text}});
    EXPECT_EQ(op.getType(), "HTTP_CALL");
    ASSERT_EQ(op.getOutputSchema().size(), 1);
    EXPECT_EQ(op.getOutputSchema().begin()->getDataType(), text);
    EXPECT_EQ(static_cast<const Identifier&>(op.getOutputSchema().begin()->getFullyQualifiedName()), Identifier::parse("response"));
    EXPECT_THROW(HttpCallAsyncOperator::create({.argumentTypes = {text}}), Exception);
    EXPECT_THROW(HttpCallAsyncOperator::create({.argumentTypes = {text, text, integer, integer, integer}}), Exception);
}

TEST(HttpCallFunctionTest, NativeInferenceValidatesTypesAndConstantOptions)
{
    const HttpCallAsyncOperator config;
    EXPECT_NO_THROW(
        static_cast<void>(config.withInferredArguments(
            {{text, textValue("body")},
             {text, textValue("http://localhost")},
             {integer, integerValue("100")},
             {integer, integerValue("0")}})));
    for (const auto& invalid : {std::string{"not a URL"}, std::string{"https://localhost"}, std::string{"http:/no-host"}})
    {
        EXPECT_THROW(static_cast<void>(config.withInferredArguments({{text, textValue("body")}, {text, textValue(invalid)}})), Exception);
    }
    EXPECT_THROW(
        static_cast<void>(
            config.withInferredArguments({{text, textValue("body")}, {text, textValue("http://localhost")}, {integer, integerValue("0")}})),
        Exception);
    EXPECT_THROW(
        static_cast<void>(config.withInferredArguments(
            {{text, textValue("body")}, {text, textValue("http://localhost")}, {integer, integerValue("18446744073709551615")}})),
        Exception);
    EXPECT_THROW(
        static_cast<void>(config.withInferredArguments(
            {{text, textValue("body")},
             {text, textValue("http://localhost")},
             {integer, integerValue("10")},
             {integer, integerValue("18446744073709551615")}})),
        Exception);
    const DataType signedInteger{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto negative = RuntimeValue::fromLiteral(signedInteger, "-1");
    ASSERT_TRUE(negative);
    EXPECT_THROW(
        static_cast<void>(
            config.withInferredArguments({{text, textValue("body")}, {text, textValue("http://localhost")}, {signedInteger, *negative}})),
        Exception);
    const DataType floatType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE};
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(
            config.withInferredArguments({{text, std::nullopt}, {text, std::nullopt}, {integer, std::nullopt}, {floatType, std::nullopt}})),
        ErrorCode::DifferentFieldTypeExpected);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(
            config.withInferredArguments({{text, textValue("body")}, {text, textValue("http://localhost")}, {integer, integerValue("0")}})),
        ErrorCode::CannotInferSchema);
    const auto nullableInteger = DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_THROW(
        static_cast<void>(config.withInferredArguments(
            {{text, textValue("body")}, {text, textValue("http://localhost")}, {nullableInteger, std::nullopt}})),
        Exception);
}

TEST(HttpCallFunctionTest, LogicalInferenceAndChildRewritePreserveCallArguments)
{
    const auto call
        = httpCall({constant(text, "body"), constant(text, "http://localhost/echo"), constant(integer, "30000"), constant(integer, "2")});
    const auto inferred = call.withInferredDataType(Schema<Field, Unordered>{});
    EXPECT_EQ(inferred.getDataType(), text);
    const auto originalArguments = call.getChildren();
    for (std::vector<LogicalFunction>::difference_type count = 2; count <= 4; ++count)
    {
        const auto scalar = httpCall(std::vector<LogicalFunction>(originalArguments.begin(), originalArguments.begin() + count))
                                .withInferredDataType(Schema<Field, Unordered>{});
        EXPECT_EQ(scalar.getDataType(), text);
        EXPECT_EQ(scalar.getAs<AsyncCallLogicalFunction>()->getOutputName(), Identifier::parse("response"));
    }
    const auto asyncCall = inferred.getAs<AsyncCallLogicalFunction>();
    ASSERT_EQ(asyncCall->getArguments().size(), 4);
    EXPECT_EQ(asyncCall->getOutputName(), Identifier::parse("response"));
    EXPECT_EQ(inferred.withChildren(call.getChildren()).withInferredDataType(Schema<Field, Unordered>{}).getChildren(), call.getChildren());
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"), constant(text, "http://localhost"), constant(integer, "0")})
                              .withInferredDataType(Schema<Field, Unordered>{})),
        Exception);
    const auto schema = Schema<Field, Unordered>{};
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"), constant(text, "not a URL")}).withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}, "256")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}, "256")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE}, "body"),
                                    constant(text, "http://localhost")})
                              .withInferredDataType(schema)),
        ErrorCode::DifferentFieldTypeExpected);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}, "-1")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}, "-1")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"), constant(text, "http://localhost"), constant(integer, "18446744073709551615")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    ASSERT_EXCEPTION_ERRORCODE(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(integer, "18446744073709551615")})
                              .withInferredDataType(schema)),
        ErrorCode::CannotInferSchema);
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "timeout")})
                              .withInferredDataType(schema)),
        Exception);
    const auto nullableRetry = ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "2"};
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"), constant(text, "http://localhost"), constant(integer, "30000"), nullableRetry})
                              .withInferredDataType(schema)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "3.0")})
                              .withInferredDataType(schema)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "2")})
                              .withInferredDataType(schema)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "2.0")})
                              .withInferredDataType(schema)),
        Exception);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(LogicalFunctionProvider::provide("HTTP_CALL", {})), ErrorCode::InvalidQuerySyntax);
    auto tooMany
        = httpCall({constant(text, "body"), constant(text, "http://localhost"), constant(integer, "30000"), constant(integer, "0")})
              .getChildren();
    tooMany.push_back(constant(integer, "1"));
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(LogicalFunctionProvider::provide("HTTP_CALL", tooMany)), ErrorCode::InvalidQuerySyntax);
}

TEST(HttpCallFunctionTest, RetryDefaultsAndExplicitCountsAreFixedAtPreparation)
{
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext context;
    const auto defaults = config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"})).value();
    EXPECT_EQ(defaults->getRetryOverride(), size_t{0});
    const auto defaultsWithTimeout
        = config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000})).value();
    EXPECT_EQ(defaultsWithTimeout->getRetryOverride(), size_t{0});
    const auto explicitZero
        = config
              .prepare(
                  context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, uint8_t{0}))
              .value();
    EXPECT_EQ(explicitZero->getRetryOverride(), size_t{0});
    const auto positive
        = config
              .prepare(
                  context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, uint8_t{2}))
              .value();
    EXPECT_EQ(positive->getRetryOverride(), size_t{2});
    EXPECT_EQ(
        config
            .prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, int64_t{0}))
            .value()
            ->getRetryOverride(),
        size_t{0});
    EXPECT_EQ(
        config
            .prepare(
                context,
                SharedRuntimeValues::make(
                    std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, std::numeric_limits<uint64_t>::max()))
            .error()
            .kind,
        AsyncError::Type::Permanent);
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"not a URL"})).error().kind,
        AsyncError::Type::Permanent);
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{0})).error().kind,
        AsyncError::Type::Permanent);
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{1}, int64_t{-1}))
            .error()
            .kind,
        AsyncError::Type::Permanent);
}

TEST(HttpCallFunctionTest, EachPreparedInvocationKeepsItsOwnTimeoutAndRetryOptions)
{
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext firstContext;
    AsyncExecutionContext secondContext;
    const auto first = config.prepare(
        firstContext, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost/first"}, uint64_t{1234}, uint64_t{3}));
    const auto second = config.prepare(
        secondContext, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost/second"}, uint64_t{500}, uint64_t{0}));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ((*first)->getRetryOverride(), size_t{3});
    EXPECT_EQ((*second)->getRetryOverride(), size_t{0});
    EXPECT_EQ(
        config.prepare(firstContext, SharedRuntimeValues::make(std::string{"body"}, std::string{"not a URL"})).error().kind,
        AsyncError::Type::Permanent);
}

/// NOLINTBEGIN(misc-include-cleaner) Public provider attribution is verified; cleaner misses this callsite.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallFunctionTest, FreshExecutionContextsPrepareIndependentConnectionState)
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor reserved{ioContext, {boost::asio::ip::tcp::v4(), 0}};
    const auto url = "http://127.0.0.1:" + std::to_string(reserved.local_endpoint().port());
    reserved.close();
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext firstContext;
    AsyncExecutionContext secondContext;
    const auto first = config.prepare(firstContext, SharedRuntimeValues::make(std::string{"one"}, url, uint64_t{1000}));
    const auto second = config.prepare(secondContext, SharedRuntimeValues::make(std::string{"two"}, url, uint64_t{1000}));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    auto firstFuture = boost::asio::co_spawn(ioContext, (*first)->executeAttempt(), boost::asio::use_future);
    auto secondFuture = boost::asio::co_spawn(ioContext, (*second)->executeAttempt(), boost::asio::use_future);
    ioContext.run();
    const auto firstResult = firstFuture.get();
    const auto secondResult = secondFuture.get();
    ASSERT_FALSE(firstResult);
    ASSERT_FALSE(secondResult);
    EXPECT_EQ(firstResult.error().kind, AsyncError::Type::Retryable);
    EXPECT_EQ(secondResult.error().kind, AsyncError::Type::Retryable);
}

/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

TEST(HttpCallFunctionTest, UnrepresentableTimeoutsAreRejectedBeforeDns)
{
    const AsyncOperator config = HttpCallAsyncOperator{};
    for (const auto timeout : {std::numeric_limits<uint64_t>::max(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max())})
    {
        boost::asio::io_context ioContext;
        auto future = boost::asio::co_spawn(
            ioContext,
            invokeOnce(config, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://unused.invalid"}, timeout)),
            boost::asio::use_future);
        ioContext.run();
        EXPECT_EQ(future.get().error().kind, AsyncError::Type::Permanent);
    }
}

TEST(HttpCallFunctionTest, StatusAndTransportClassificationRetainRetryWhitelist)
{
    for (unsigned status = 100; status < 600; ++status)
    {
        const auto result = detail::checkHttpStatus(status, "http://test/endpoint", "response");
        const bool success = status >= 200 && status < 300;
        const bool retryable = status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
        EXPECT_EQ(result.has_value(), success) << status;
        if (!success)
        {
            EXPECT_EQ(result.error().kind, retryable ? AsyncError::Type::Retryable : AsyncError::Type::Permanent) << status;
        }
    }
    namespace asioError = boost::asio::error;
    /// NOLINTNEXTLINE(misc-include-cleaner) error_code is declared by public boost/system/error_code.hpp.
    const std::vector<boost::system::error_code> transient{
        asioError::connection_refused,
        asioError::connection_reset,
        asioError::connection_aborted,
        asioError::timed_out,
        asioError::network_unreachable,
        asioError::network_down,
        asioError::host_unreachable,
        asioError::try_again,
        asioError::eof,
        asioError::host_not_found_try_again,
        boost::beast::error::timeout,
        boost::beast::http::error::end_of_stream};
    for (const auto& error : transient)
    {
        EXPECT_EQ(detail::classifyHttpTransportError(error, "http://host").kind, AsyncError::Type::Retryable);
    }
    const std::vector<boost::system::error_code> permanent{
        asioError::host_not_found,
        asioError::no_recovery,
        asioError::invalid_argument,
        asioError::access_denied,
        boost::beast::http::error::bad_status,
        boost::beast::http::error::bad_content_length,
        boost::beast::http::error::partial_message,
        boost::beast::http::error::body_limit};
    for (const auto& error : permanent)
    {
        EXPECT_EQ(detail::classifyHttpTransportError(error, "http://host").kind, AsyncError::Type::Permanent);
    }
    EXPECT_EQ(detail::classifyHttpTransportError(asioError::operation_aborted, "http://host").kind, AsyncError::Type::Cancelled);
}

TEST(HttpCallFunctionTest, LogicalCallReflectionRetainsOptionsAndReplacementArguments)
{
    const auto call
        = httpCall({constant(text, "body"), constant(text, "http://localhost/one"), constant(integer, "30000"), constant(integer, "2")});
    const auto reflected = ReflectionContext{}.reflect(call);
    const auto restored = ReflectionContext{}.unreflect<LogicalFunction>(reflected);
    EXPECT_EQ(restored.getType(), call.getType());
    EXPECT_EQ(restored.getChildren(), call.getChildren());
    EXPECT_EQ(restored.getDataType(), call.getDataType());
    EXPECT_NE(
        call.explain(ExplainVerbosity::Short),
        httpCall({constant(text, "body"), constant(text, "http://localhost/other"), constant(integer, "30000"), constant(integer, "2")})
            .explain(ExplainVerbosity::Short));
    const auto operatorCopy = AsyncOperator{HttpCallAsyncOperator{}};
    const auto reflectedOperator = ReflectionContext{}.reflect(operatorCopy);
    const auto restoredOperator = ReflectionContext{}.unreflect<AsyncOperator>(reflectedOperator);
    EXPECT_EQ(restoredOperator.getType(), operatorCopy.getType());
    EXPECT_EQ(restoredOperator.getOutputSchema(), operatorCopy.getOutputSchema());
    auto changed = call.getChildren();
    changed[3] = constant(integer, "3");
    EXPECT_EQ(call.withChildren(changed).getChildren(), changed);
}

TEST(HttpCallFunctionTest, PreparedInvocationOwnsInputsAcrossRepeatedAttempts)
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor reserved{ioContext, {boost::asio::ip::tcp::v4(), 0}};
    const auto url = "http://127.0.0.1:" + std::to_string(reserved.local_endpoint().port());
    reserved.close();
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext context;
    auto values = SharedRuntimeValues::make(std::string{"original"}, url, uint64_t{1000}, uint8_t{2});
    const auto prepared = config.prepare(context, values).value();
    values = SharedRuntimeValues::make(std::string{"changed"}, std::string{"not a URL"}, int64_t{-1}, int64_t{-1});
    EXPECT_EQ(prepared->getRetryOverride(), size_t{2});
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        auto future = boost::asio::co_spawn(ioContext, prepared->executeAttempt(), boost::asio::use_future);
        ioContext.run();
        const auto result = future.get();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().kind, AsyncError::Type::Retryable);
        ioContext.restart();
    }
}

TEST(HttpCallFunctionTest, InvalidRetryCountsAreUserInputErrors)
{
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext context;
    const auto negative = config.prepare(
        context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, int64_t{-1}));
    const auto overflow = config.prepare(
        context,
        SharedRuntimeValues::make(
            std::string{"body"}, std::string{"http://localhost"}, uint64_t{30000}, std::numeric_limits<uint64_t>::max()));
    ASSERT_FALSE(negative);
    ASSERT_FALSE(overflow);
    EXPECT_EQ(negative.error().kind, AsyncError::Type::Permanent);
    EXPECT_EQ(overflow.error().kind, AsyncError::Type::Permanent);
}

TEST(HttpCallFunctionTest, DynamicBadOptionsFailSynchronouslyDuringPreparation)
{
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext context;
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"not a URL"})).error().kind,
        AsyncError::Type::Permanent);
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{0})).error().kind,
        AsyncError::Type::Permanent);
    EXPECT_EQ(
        config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, uint64_t{100}, int64_t{-1}))
            .error()
            .kind,
        AsyncError::Type::Permanent);
    const auto wrongType = config.prepare(
        context, SharedRuntimeValues::make(std::string{"body"}, std::string{"http://localhost"}, std::string{"not a timeout"}));
    ASSERT_FALSE(wrongType);
    EXPECT_EQ(wrongType.error().kind, AsyncError::Type::Permanent);
    EXPECT_EQ(wrongType.error().exception.code(), ErrorCode::DifferentFieldTypeExpected);
}

TEST(HttpCallFunctionTest, RetriesMustBeNonNullableIntegers)
{
    const auto schema = Schema<Field, Unordered>{};
    const auto prefix
        = std::vector<LogicalFunction>{constant(text, "body"), constant(text, "http://localhost"), constant(integer, "30000")};
    EXPECT_NO_THROW(
        static_cast<void>(httpCall({constant(text, "body"),
                                    constant(text, "http://localhost"),
                                    constant(integer, "30000"),
                                    constant(DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}, "2")})
                              .withInferredDataType(schema)));
    for (const auto& invalid : std::vector<LogicalFunction>{
             constant(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "two"),
             constant(DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "2.0"),
             ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "2"}})
    {
        auto arguments = prefix;
        arguments.push_back(invalid);
        EXPECT_THROW(static_cast<void>(httpCall(std::move(arguments)).withInferredDataType(schema)), Exception);
    }
}

/// NOLINTBEGIN(misc-include-cleaner) Public provider attribution is verified; cleaner misses this callsite.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallFunctionTest, DirectPreparedAttemptReportsCancellationAsAnAsyncError)
{
    namespace asio = boost::asio;
    namespace http = boost::beast::http;
    asio::io_context serverIo;
    asio::ip::tcp::acceptor acceptor{serverIo, {asio::ip::tcp::v4(), 0}};
    const auto url = "http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port());
    const std::jthread server(
        [&]
        {
            asio::ip::tcp::socket socket{serverIo};
            acceptor.accept(socket);
            boost::beast::flat_buffer buffer;
            http::request<http::string_body> request;
            boost::system::error_code error;
            http::read(socket, buffer, request, error);
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        });

    asio::io_context clientIo;
    const AsyncOperator config = HttpCallAsyncOperator{};
    AsyncExecutionContext context;
    auto prepared = config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, url, uint64_t{30000})).value();
    asio::cancellation_signal cancellation;
    asio::steady_timer stop{clientIo, std::chrono::milliseconds{10}};
    stop.async_wait([&](const auto&) { cancellation.emit(asio::cancellation_type::terminal); });
    auto future = asio::co_spawn(clientIo, prepared->executeAttempt(), asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    clientIo.run();
    const auto result = future.get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, AsyncError::Type::Cancelled);
}

/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

TEST(HttpCallFunctionTest, PreparedInvocationRetainsSlotStateAfterContextDestruction)
{
    namespace asio = boost::asio;
    asio::io_context ioContext;
    asio::ip::tcp::acceptor reserved{ioContext, {asio::ip::tcp::v4(), 0}};
    const auto url = "http://127.0.0.1:" + std::to_string(reserved.local_endpoint().port());
    reserved.close();
    const AsyncOperator config = HttpCallAsyncOperator{};
    auto prepared = [&]
    {
        AsyncExecutionContext context;
        return config.prepare(context, SharedRuntimeValues::make(std::string{"body"}, url, uint64_t{1000})).value();
    }();

    auto future = asio::co_spawn(ioContext, prepared->executeAttempt(), asio::use_future);
    ioContext.run();
    const auto result = future.get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, AsyncError::Type::Retryable);
}

TEST(HttpCallFunctionTest, RefusedConnectionRemainsRetryableAndWarnsAboutPostReplay)
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor reserved{ioContext, {boost::asio::ip::tcp::v4(), 0}};
    const auto url = "http://127.0.0.1:" + std::to_string(reserved.local_endpoint().port());
    reserved.close();
    const AsyncOperator config = HttpCallAsyncOperator{};
    auto future = boost::asio::co_spawn(
        ioContext, invokeOnce(config, SharedRuntimeValues::make(std::string{"body"}, url, uint64_t{1000})), boost::asio::use_future);
    ioContext.run();
    const auto result = future.get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, AsyncError::Type::Retryable);
    const auto warning = detail::checkHttpStatus(503, url, "busy");
    ASSERT_FALSE(warning);
    const std::string message{warning.error().exception.what()};
    EXPECT_NE(message.find(url), std::string::npos);
    EXPECT_NE(message.find("503"), std::string::npos);
    EXPECT_NE(message.find("busy"), std::string::npos);
    EXPECT_NE(message.find("POST may duplicate server-side effects"), std::string::npos);
}
}

/// NOLINTEND(readability-magic-numbers)
