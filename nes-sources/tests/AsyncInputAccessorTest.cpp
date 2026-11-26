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
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade; cleaner misattributes setupLogging.
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType NULLABLE{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};

AsyncCallGraphBuilder inputBuilder()
{
    return AsyncCallGraphBuilder{
        Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{QualifiedIdentifier::parse("INPUT"), INTEGER}}};
}
}

TEST(AsyncInputAccessorTest, ConstantValuesWorkWithAnEmptyStandaloneRow)
{
    const AsyncInputAccessor accessor{RuntimeValue{int64_t{42}}};
    const auto value = accessor(AsyncRowHandle{});
    EXPECT_EQ(value.view().get<int64_t>(), 42);
    EXPECT_EQ(accessor.getDataType(), INTEGER);
}

TEST(AsyncInputAccessorTest, InputDescriptorsReadThroughGraphOwnedRowContext)
{
    auto builder = inputBuilder();
    const AsyncInputAccessor accessor{builder.input(QualifiedIdentifier::parse("INPUT"))};
    const auto graph = builder.build();
    const auto row = graph.createRowHandle([](const InputFieldReference&) { return RuntimeValue{int64_t{17}}; }, {});
    const auto value = accessor(row);
    EXPECT_EQ(value.view().get<int64_t>(), 17);
    EXPECT_EQ(accessor.getDataType(), INTEGER);
}

TEST(AsyncInputAccessorTest, RowsRetainTheirProviderAndAccessorsRetainTheirSchema)
{
    const auto [accessor, row] = []
    {
        auto builder = inputBuilder();
        const AsyncInputAccessor original{builder.input(QualifiedIdentifier::parse("INPUT"))};
        const auto copied = original;
        const auto graph = builder.build();
        const auto owner = std::make_shared<int64_t>(23);
        return std::pair{copied, graph.createRowHandle([owner](const InputFieldReference&) { return RuntimeValue{*owner}; }, {})};
    }();
    const auto value = accessor(row);
    EXPECT_EQ(value.view().get<int64_t>(), 23);
    EXPECT_EQ(accessor.getDataType(), INTEGER);
}

TEST(AsyncInputAccessorTest, TypedNullAndOwningBinaryConstantReflectWithoutGraphContext)
{
    const ReflectionContext context;
    const AsyncInputAccessor nullValue{RuntimeValue::null(NULLABLE)};
    const auto reflectedNull = context.reflect(nullValue);
    const auto& accessorVariant = std::get<rfl::Generic::Object>(reflectedNull->variant());
    EXPECT_EQ(std::get<int64_t>(accessorVariant.at("index").variant()), 0);
    const auto restoredNull = context.unreflect<AsyncInputAccessor>(reflectedNull);
    EXPECT_EQ(restoredNull.getDataType(), NULLABLE);
    EXPECT_TRUE(restoredNull(AsyncRowHandle{}).isNull());
    const auto binary = []
    {
        std::string temporary(256, 'x');
        temporary[8] = '\0';
        return AsyncInputAccessor{RuntimeValue{std::move(temporary)}};
    }();
    const auto restored = context.unreflect<AsyncInputAccessor>(context.reflect(binary));
    const auto value = restored(AsyncRowHandle{});
    const auto bytes = value.view().get<std::string_view>();
    ASSERT_EQ(bytes.size(), 256);
    EXPECT_EQ(bytes[8], '\0');
    EXPECT_EQ(bytes.back(), 'x');
}

TEST(AsyncInputAccessorTest, RepeatedRowsResolveTheirOwnValues)
{
    auto builder = inputBuilder();
    const AsyncInputAccessor accessor{builder.input(QualifiedIdentifier::parse("INPUT"))};
    const auto graph = builder.build();
    const auto first = graph.createRowHandle([](const InputFieldReference&) { return RuntimeValue{int64_t{31}}; }, {});
    const auto second = graph.createRowHandle([](const InputFieldReference&) { return RuntimeValue{int64_t{47}}; }, {});
    const auto firstValue = accessor(first);
    const auto secondValue = accessor(second);
    const auto repeated = accessor(first);
    EXPECT_EQ(firstValue.view().get<int64_t>(), 31);
    EXPECT_EQ(secondValue.view().get<int64_t>(), 47);
    EXPECT_EQ(repeated.view().get<int64_t>(), 31);
}

#ifndef NO_ASSERT
TEST(AsyncInputAccessorTest, RejectsRowsFromAnotherGraphEvenWhenSignaturesAgree)
{
    /// NOLINTNEXTLINE(misc-include-cleaner) setupLogging is declared by the directly included public Logger.hpp facade.
    Logger::setupLogging("AsyncInputAccessorScopeTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    auto first = inputBuilder();
    auto second = inputBuilder();
    const AsyncInputAccessor accessor{first.input(QualifiedIdentifier::parse("INPUT"))};
    const auto foreign = second.build().createRowHandle([](const InputFieldReference&) { return RuntimeValue{int64_t{1}}; }, {});
    EXPECT_DEATH(static_cast<void>(accessor(foreign)), "Precondition violated.*another graph");
}
#endif
}
