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
#include <future>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/HttpCallLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <HttpCallAsyncFunction.hpp>

namespace NES
{
namespace
{
LogicalFunction constant(const DataType::Type type, std::string value)
{
    return ConstantValueLogicalFunction{DataType{type, DataType::NULLABLE::NOT_NULLABLE}, std::move(value)};
}

HttpCallLogicalFunction logicalCall(std::optional<LogicalFunction> retries = std::nullopt)
{
    return HttpCallLogicalFunction{
        constant(DataType::Type::VARSIZED, "payload"),
        constant(DataType::Type::VARSIZED, "http://localhost/echo"),
        constant(DataType::Type::UINT64, "30000"),
        std::move(retries)};
}
}

TEST(HttpCallFunctionTest, RetriesDefaultToZero)
{
    HttpCallAsyncFunction function;
    EXPECT_EQ(function.maxRetries(AsyncValues::make(std::string{"body"}, std::string{"http://localhost/echo"})), size_t{0});
    EXPECT_EQ(
        function.maxRetries(AsyncValues::make(std::string{"body"}, std::string{"http://localhost/echo"}, uint64_t{30000})), size_t{0});
}

TEST(HttpCallFunctionTest, ExplicitRetriesAcceptIntegerWidthsAndZero)
{
    HttpCallAsyncFunction function;
    EXPECT_EQ(
        function.maxRetries(AsyncValues::make(std::string{"body"}, std::string{"http://localhost/echo"}, uint64_t{30000}, uint8_t{2})),
        size_t{2});
    EXPECT_EQ(
        function.maxRetries(AsyncValues::make(std::string{"body"}, std::string{"http://localhost/echo"}, uint64_t{30000}, int64_t{0})),
        size_t{0});
}

TEST(HttpCallFunctionTest, InvalidRetryCountsAreUserInputErrors)
{
    HttpCallAsyncFunction function;
    EXPECT_THROW(
        static_cast<void>(function.maxRetries(
            AsyncValues::make(std::string{"body"}, std::string{"http://localhost/echo"}, uint64_t{30000}, int64_t{-1}))),
        Exception);
    EXPECT_THROW(
        static_cast<void>(function.maxRetries(
            AsyncValues::make(
                std::string{"body"}, std::string{"http://localhost/echo"}, uint64_t{30000}, std::numeric_limits<uint64_t>::max()))),
        Exception);
}

TEST(HttpCallFunctionTest, UnrepresentableTimeoutsAreRejectedBeforeDns)
{
    for (const auto millis : {std::numeric_limits<uint64_t>::max(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max())})
    {
        boost::asio::io_context context;
        HttpCallAsyncFunction function;
        const auto body = [&]() -> boost::asio::awaitable<void>
        {
            static_cast<void>(co_await function.call(AsyncValues::make(std::string{"body"}, std::string{"http://unused.invalid"}, millis)));
        };
        auto future = boost::asio::co_spawn(context, body(), boost::asio::use_future);
        context.run();
        try
        {
            future.get();
            ADD_FAILURE() << "An unrepresentable timeout must fail";
        }
        catch (const Exception& error)
        {
            EXPECT_NE(std::string{error.what()}.find("representable steady-clock deadline"), std::string::npos);
        }
    }
}

TEST(HttpCallFunctionTest, RetryArgumentSurvivesReflectionEqualityAndChildReplacement)
{
    const auto original = logicalCall(constant(DataType::Type::UINT64, "2"));
    const auto reflected = ReflectionContext{}.reflect(LogicalFunction{original});
    const auto copy = ReflectionContext{}.unreflect<LogicalFunction>(reflected);
    EXPECT_EQ(copy, LogicalFunction{original});
    EXPECT_EQ(original.getChildren().size(), 4);
    EXPECT_NE(original, logicalCall(constant(DataType::Type::UINT64, "3")));
    auto children = original.getChildren();
    children[3] = constant(DataType::Type::UINT64, "3");
    EXPECT_EQ(original.withChildren(children), logicalCall(children[3]));
    EXPECT_EQ(original.withChildren(logicalCall().getChildren()), logicalCall());
    EXPECT_NE(original.explain(ExplainVerbosity::Short), logicalCall().explain(ExplainVerbosity::Short));
}

TEST(HttpCallFunctionTest, RetriesMustBeANonNullableInteger)
{
    const Schema<Field, Unordered> schema{};
    EXPECT_NO_THROW(static_cast<void>(logicalCall(constant(DataType::Type::UINT8, "2")).withInferredDataType(schema)));
    EXPECT_THROW(static_cast<void>(logicalCall(constant(DataType::Type::VARSIZED, "two")).withInferredDataType(schema)), Exception);
    EXPECT_THROW(static_cast<void>(logicalCall(constant(DataType::Type::FLOAT64, "2.0")).withInferredDataType(schema)), Exception);
    const auto nullable = ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "2"};
    EXPECT_THROW(static_cast<void>(logicalCall(nullable).withInferredDataType(schema)), Exception);
}

}
