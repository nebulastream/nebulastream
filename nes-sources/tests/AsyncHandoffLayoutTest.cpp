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
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncHandoffLayout.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade; cleaner misattributes setupLogging.
#include <gtest/gtest.h>
#include <AsyncTestOperator.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType SMALL{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE};
const auto KEEP = UnqualifiedUnboundField{Identifier::parse("KEEP"), SMALL};
const auto RESULT = UnqualifiedUnboundField{Identifier::parse("RESULT"), INTEGER};

AsyncCallGraph graph(const DataType& resultType, const QualifiedIdentifier& outputField)
{
    AsyncCallGraphBuilder builder{Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}};
    builder.exportOutput(builder.call(makeAsyncTestOperator("Function", {resultType}), {})->output(0), outputField);
    return builder.build();
}

AsyncCallGraph graph()
{
    return graph(INTEGER, QualifiedIdentifier::parse("RESULT"));
}
}

TEST(AsyncHandoffLayoutTest, MatchingLayoutsCopyOnlyUnchangedFields)
{
    const AsyncHandoffLayout layout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}};
    EXPECT_EQ(layout.getRowSize(), sizeof(uint8_t) + sizeof(int64_t));
    ASSERT_EQ(layout.getCopiedFields().size(), 1);
    const auto& copied = layout.getCopiedFields().at(0);
    EXPECT_EQ(copied.offset, 0);
    EXPECT_EQ(copied.size, sizeof(uint8_t));
}

TEST(AsyncHandoffLayoutTest, NullableFieldsIncludeTheirNullMarkers)
{
    const DataType nullableSmall{DataType::Type::UINT8, DataType::NULLABLE::IS_NULLABLE};
    const DataType nullableInteger{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const UnqualifiedUnboundField keep{Identifier::parse("KEEP"), nullableSmall};
    const UnqualifiedUnboundField result{Identifier::parse("RESULT"), nullableInteger};
    AsyncCallGraphBuilder builder{Schema<UnqualifiedUnboundField, Ordered>{result, keep}};
    builder.exportOutput(
        builder.call(makeAsyncTestOperator("NullableFunction", {nullableInteger}), {})->output(0), QualifiedIdentifier::parse("RESULT"));
    const AsyncHandoffLayout layout{builder.build(), Schema<UnqualifiedUnboundField, Ordered>{result, keep}};

    EXPECT_EQ(layout.getRowSize(), sizeof(uint8_t) + sizeof(int64_t) + 2);
    ASSERT_EQ(layout.getCopiedFields().size(), 1);
    const auto& copied = layout.getCopiedFields().at(0);
    EXPECT_EQ(copied.offset, sizeof(int64_t) + 1);
    EXPECT_EQ(copied.size, sizeof(uint8_t) + 1);
}

#ifndef NO_ASSERT
namespace
{
class AsyncHandoffLayoutDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        /// NOLINTNEXTLINE(misc-include-cleaner) Public Logger.hpp declares this facade API.
        Logger::setupLogging("AsyncHandoffLayoutDeathTest.log", LogLevel::LOG_DEBUG, false);
    }
};

TEST_F(AsyncHandoffLayoutDeathTest, ReorderedFieldsViolateConstructionContract)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{RESULT, KEEP}}), "Precondition violated.*identical ordered");
}

TEST_F(AsyncHandoffLayoutDeathTest, MissingOrExtraFieldsViolateConstructionContract)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{RESULT}}), "Precondition violated.*identical ordered");
    const auto extra = UnqualifiedUnboundField{Identifier::parse("EXTRA"), INTEGER};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT, extra}}),
        "Precondition violated.*identical ordered");
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{extra, RESULT}}), "Precondition violated.*identical ordered");
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{RESULT, RESULT}}),
        "Precondition violated.*identical ordered");
}

TEST_F(AsyncHandoffLayoutDeathTest, InvalidGraphOutputsViolateConstructionContractEvenWhenSchemasAgree)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(INTEGER, QualifiedIdentifier::parse("ABSENT")), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}}),
        "Precondition violated.*not in the handoff schema");
    /// Equal byte widths are not enough: the graph's FLOAT64 result must not fill an INT64 slot.
    const DataType floating{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(floating, QualifiedIdentifier::parse("RESULT")), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}}),
        "Precondition violated.*Async output field");

    const Schema<QualifiedUnboundField, Ordered> qualified{
        QualifiedUnboundField{QualifiedIdentifier::parse("LEFT.KEEP"), SMALL},
        QualifiedUnboundField{QualifiedIdentifier::parse("LEFT.RESULT"), INTEGER}};
    AsyncCallGraphBuilder builder{qualified};
    builder.exportOutput(
        builder.call(makeAsyncTestOperator("QualifiedFunction", {INTEGER}), {})->output(0), QualifiedIdentifier::parse("RESULT"));
    const auto suffixOnlyOutput = builder.build();
    EXPECT_DEATH((AsyncHandoffLayout{suffixOnlyOutput, qualified}), "Precondition violated.*not in the handoff schema");
}

TEST_F(AsyncHandoffLayoutDeathTest, MismatchedTypesViolateConstructionContract)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto wrongKeep = UnqualifiedUnboundField{Identifier::parse("KEEP"), INTEGER};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{wrongKeep, RESULT}}),
        "Precondition violated.*identical ordered");
    const auto wrongResult = UnqualifiedUnboundField{Identifier::parse("RESULT"), SMALL};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, wrongResult}}),
        "Precondition violated.*identical ordered");
    const UnqualifiedUnboundField nullableKeep{Identifier::parse("KEEP"), DataType{DataType::Type::UINT8, DataType::NULLABLE::IS_NULLABLE}};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{nullableKeep, RESULT}}),
        "Precondition violated.*identical ordered");
}
}
#endif

}
