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
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncHandoffLayout.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType SMALL{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE};
const auto KEEP = UnqualifiedUnboundField{Identifier::parse("KEEP"), SMALL};
const auto RESULT = UnqualifiedUnboundField{Identifier::parse("RESULT"), INTEGER};

AsyncCallGraph graph()
{
    AsyncCallGraphBuilder builder{Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}};
    builder.exportOutput(builder.call("Function", {}, {INTEGER})->output(0), QualifiedIdentifier::parse("RESULT"));
    return builder.build();
}
}

TEST(AsyncHandoffLayoutTest, MatchingLayoutsCopyOnlyUnchangedFields)
{
    const AsyncHandoffLayout layout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT}};
    EXPECT_TRUE(layout.matchesInputLayout());
    EXPECT_EQ(layout.getInputRowSize(), sizeof(uint8_t) + sizeof(int64_t));
    EXPECT_EQ(layout.getInputRowSize(), layout.getOutputRowSize());
    ASSERT_EQ(layout.getCopiedFields().size(), 1);
    EXPECT_EQ(layout.getCopiedFields()[0].inputOffset, 0);
    EXPECT_EQ(layout.getCopiedFields()[0].outputOffset, 0);
    EXPECT_EQ(layout.getCopiedFields()[0].size, sizeof(uint8_t));
}

TEST(AsyncHandoffLayoutTest, ReorderedLayoutsRetainCorrectCopyOffsets)
{
    const AsyncHandoffLayout layout{graph(), Schema<UnqualifiedUnboundField, Ordered>{RESULT, KEEP}};
    EXPECT_FALSE(layout.matchesInputLayout());
    ASSERT_EQ(layout.getCopiedFields().size(), 1);
    EXPECT_EQ(layout.getCopiedFields()[0].inputOffset, 0);
    EXPECT_EQ(layout.getCopiedFields()[0].outputOffset, sizeof(int64_t));
}

#ifndef NO_ASSERT
class AsyncHandoffLayoutDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncHandoffLayoutDeathTest.log", LogLevel::LOG_DEBUG, false); }
};

TEST_F(AsyncHandoffLayoutDeathTest, MissingOrExtraFieldsViolateConstructionContract)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH((AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{RESULT}}), "Precondition violated.*exactly");
    const auto extra = UnqualifiedUnboundField{Identifier::parse("EXTRA"), INTEGER};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, RESULT, extra}}), "Precondition violated.*exactly");
}

TEST_F(AsyncHandoffLayoutDeathTest, MismatchedTypesViolateConstructionContract)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto wrongKeep = UnqualifiedUnboundField{Identifier::parse("KEEP"), INTEGER};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{wrongKeep, RESULT}}), "Precondition violated.*input field");
    const auto wrongResult = UnqualifiedUnboundField{Identifier::parse("RESULT"), SMALL};
    EXPECT_DEATH(
        (AsyncHandoffLayout{graph(), Schema<UnqualifiedUnboundField, Ordered>{KEEP, wrongResult}}), "Precondition violated.*output field");
}
#endif

}
