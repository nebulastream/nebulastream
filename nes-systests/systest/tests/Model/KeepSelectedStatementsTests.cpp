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

TestStatement createQuery(const uint64_t number)
{
    return SelectStatement{.sql = "", .id = SystestQueryId{number}, .expected = Expectation{ExpectedRows{}}, .overrides = {}};
}

TestStatement createCreateStatement()
{
    return CreateStatement{.sql = "CREATE", .attach = std::nullopt};
}

TestStatement createDifferential(const uint64_t first, const uint64_t second)
{
    return DifferentialStatement{
        .firstSql = "", .firstId = SystestQueryId{first}, .secondSql = "", .secondId = SystestQueryId{second}, .overrides = {}};
}

TestStatement createExplain(const uint64_t number)
{
    return ExplainStatement{.sql = "", .id = SystestQueryId{number}, .expected = ExpectedPlan{.lines = {}}};
}

}

class KeepSelectedStatementsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("KeepSelectedStatements.log", LogLevel::LOG_DEBUG); }
};

TEST_F(KeepSelectedStatementsTest, EmptySelectionKeepsEveryStatement)
{
    std::vector statements{createCreateStatement(), createQuery(1), createQuery(2), createQuery(3)};
    retainSelectedStatements(statements, {});
    EXPECT_EQ(statements.size(), 4U);
}

TEST_F(KeepSelectedStatementsTest, DropsTheStatementsTheSelectionOmitsAndKeepsTheCreates)
{
    std::vector statements{createCreateStatement(), createQuery(1), createQuery(2), createQuery(3)};
    retainSelectedStatements(statements, {SystestQueryId{2}});
    ASSERT_EQ(statements.size(), 2U);
    EXPECT_TRUE(std::holds_alternative<CreateStatement>(statements.at(0)));
    EXPECT_EQ(getQueryNumbersOf(statements.at(1)), (std::vector{SystestQueryId{2}}));
}

TEST_F(KeepSelectedStatementsTest, EitherNumberOfADifferentialBlockKeepsIt)
{
    std::vector selectedBySecond{createDifferential(1, 2)};
    retainSelectedStatements(selectedBySecond, {SystestQueryId{2}});
    EXPECT_EQ(selectedBySecond.size(), 1U);

    std::vector unselected{createDifferential(1, 2)};
    retainSelectedStatements(unselected, {SystestQueryId{3}});
    EXPECT_TRUE(unselected.empty());
}

TEST_F(KeepSelectedStatementsTest, OnlyCreatesLeftMeansNothingToRun)
{
    std::vector statements{createCreateStatement(), createQuery(1), createQuery(2)};
    EXPECT_TRUE(hasTestCases(statements));
    retainSelectedStatements(statements, {SystestQueryId{3}});
    EXPECT_FALSE(hasTestCases(statements));
    EXPECT_FALSE(hasTestCases({}));
}

/// Unlike a CREATE, an EXPLAIN has a query number.
TEST_F(KeepSelectedStatementsTest, ASelectionDropsAnUnselectedExplain)
{
    std::vector statements{createCreateStatement(), createQuery(1), createExplain(2)};
    retainSelectedStatements(statements, {SystestQueryId{1}});
    ASSERT_EQ(statements.size(), 2U);
    EXPECT_EQ(getQueryNumbersOf(statements.at(1)), (std::vector{SystestQueryId{1}}));
}

}
