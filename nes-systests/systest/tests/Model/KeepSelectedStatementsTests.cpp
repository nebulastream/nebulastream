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
#include <optional>
#include <unordered_set>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

namespace
{

TestStatement query(const uint64_t number)
{
    return SelectStatement{.sql = "", .id = SystestQueryId{number}, .expected = Expectation{ExpectedRows{}}, .overrides = {}};
}

TestStatement create()
{
    return CreateStatement{.sql = "CREATE", .attach = std::nullopt};
}

TestStatement differential(const uint64_t first, const uint64_t second)
{
    return DifferentialStatement{
        .firstSql = "", .firstId = SystestQueryId{first}, .secondSql = "", .secondId = SystestQueryId{second}, .overrides = {}};
}

}

class KeepSelectedStatementsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("KeepSelectedStatements.log", LogLevel::LOG_DEBUG); }
};

/// A run without query numbers runs the whole file, so the empty selection has to keep everything rather than nothing.
TEST_F(KeepSelectedStatementsTest, EmptySelectionKeepsEveryStatement)
{
    std::vector statements{create(), query(1), query(2), query(3)};
    keepSelectedStatements(statements, {});
    EXPECT_EQ(statements.size(), 4U);
}

/// The CREATEs stay, because the selected query may read from any of them.
TEST_F(KeepSelectedStatementsTest, DropsTheStatementsTheSelectionOmitsAndKeepsTheCreates)
{
    std::vector statements{create(), query(1), query(2), query(3)};
    keepSelectedStatements(statements, {SystestQueryId{2}});
    ASSERT_EQ(statements.size(), 2U);
    EXPECT_TRUE(std::holds_alternative<CreateStatement>(statements.at(0)));
    EXPECT_EQ(queryNumbersOf(statements.at(1)), (std::vector{SystestQueryId{2}}));
}

/// A differential block is one statement covering both its query numbers, so selecting either number keeps the block.
TEST_F(KeepSelectedStatementsTest, EitherNumberOfADifferentialBlockKeepsIt)
{
    std::vector selectedBySecond{differential(1, 2)};
    keepSelectedStatements(selectedBySecond, {SystestQueryId{2}});
    EXPECT_EQ(selectedBySecond.size(), 1U);

    std::vector unselected{differential(1, 2)};
    keepSelectedStatements(unselected, {SystestQueryId{3}});
    EXPECT_TRUE(unselected.empty());
}

}
