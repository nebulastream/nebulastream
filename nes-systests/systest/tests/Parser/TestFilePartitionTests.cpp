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

#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Parser/TestFilePartition.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

/// Builders fix the fields that a partition never reads.
TestStatement createCreateStatement(std::string sql)
{
    return CreateStatement{.sql = std::move(sql), .attach = std::nullopt};
}

TestStatement createQuery(std::string sql, ConfigurationOverride overrides)
{
    return SelectStatement{.sql = std::move(sql), .id = SystestQueryId{1}, .expected = ExpectedRows{}, .overrides = std::move(overrides)};
}

TestStatement createQuery(std::string sql)
{
    return createQuery(std::move(sql), {});
}

TestStatement createExplain(std::string sql)
{
    return ExplainStatement{.sql = std::move(sql), .id = SystestQueryId{1}, .expected = ExpectedPlan{}};
}

std::vector<std::string> collectSqlOf(const TestFilePartition& part)
{
    std::vector<std::string> sql;
    sql.reserve(part.file.statements.size());
    for (const auto& statement : part.file.statements)
    {
        sql.push_back(std::visit(
            Overloaded{
                [](const CreateStatement& each) { return each.sql; },
                [](const SelectStatement& each) { return each.sql; },
                [](const DifferentialStatement& each) { return each.firstSql; },
                [](const ExplainStatement& each) { return each.sql; }},
            statement));
    }
    return sql;
}

}

TEST(TestFilePartitionTest, KeepsAFileWithoutQueriesWhole)
{
    const ParsedTestFile file{.path = "setup.test", .statements = {createCreateStatement("CREATE A")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(collectSqlOf(parts.front()), (std::vector<std::string>{"CREATE A"}));
}

TEST(TestFilePartitionTest, PlacesADifferentialBlockWithItsOverrides)
{
    const ParsedTestFile file{
        .path = "differential.test",
        .statements
        = {createCreateStatement("CREATE A"),
           createQuery("Q1", {{"a", "1"}}),
           DifferentialStatement{
               .firstSql = "D1",
               .firstId = SystestQueryId{2},
               .secondSql = "D2",
               .secondId = SystestQueryId{2},
               .overrides = {{"a", "1"}}}}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(collectSqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "D1"}));
}

TEST(TestFilePartitionTest, KeepsAFileThatConfiguresNothingWhole)
{
    const ParsedTestFile file{.path = "one.test", .statements = {createCreateStatement("CREATE A"), createQuery("Q1"), createQuery("Q2")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(collectSqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "Q2"}));
}

TEST(TestFilePartitionTest, SplitsQueriesThatAskForDifferentOverrides)
{
    const ParsedTestFile file{
        .path = "two.test",
        .statements
        = {createCreateStatement("CREATE A"),
           createQuery("Q1", {{"a", "1"}}),
           createQuery("Q2", {{"a", "2"}}),
           createQuery("Q3", {{"a", "1"}})}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(collectSqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1", "Q3"}));
    EXPECT_EQ(collectSqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "Q2"}));
}

/// The parser expands config alternatives into one statement each, all with the same number.
TEST(TestFilePartitionTest, SplitsTheAlternativesOfOneQueryApart)
{
    const ParsedTestFile file{
        .path = "alternatives.test",
        .statements = {createCreateStatement("CREATE A"), createQuery("Q1", {{"bloom", "true"}}), createQuery("Q1", {{"bloom", "false"}})}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(parts.at(0).overrides.at("bloom"), "true");
    EXPECT_EQ(parts.at(1).overrides.at("bloom"), "false");
    EXPECT_EQ(collectSqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1"}));
    EXPECT_EQ(collectSqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "Q1"}));
}

TEST(TestFilePartitionTest, KeepsAnExplainBesideAPlainQuery)
{
    const ParsedTestFile file{
        .path = "explain.test", .statements = {createCreateStatement("CREATE A"), createQuery("Q1"), createExplain("EXPLAIN Q1")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(collectSqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "EXPLAIN Q1"}));
}

TEST(TestFilePartitionTest, GivesAnExplainItsOwnPartWhenEveryQueryAsksForOverrides)
{
    const ParsedTestFile file{
        .path = "mixed.test",
        .statements = {createCreateStatement("CREATE A"), createQuery("Q1", {{"a", "1"}}), createExplain("EXPLAIN Q1")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(collectSqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1"}));
    EXPECT_EQ(collectSqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "EXPLAIN Q1"}));
}

TEST(TestFilePartitionTest, RepeatsACreateBelowAQueryIntoEveryPart)
{
    const ParsedTestFile file{
        .path = "late.test",
        .statements
        = {createCreateStatement("CREATE A"),
           createQuery("Q1", {{"a", "1"}}),
           createCreateStatement("CREATE B"),
           createQuery("Q2", {{"a", "2"}})}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(collectSqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "CREATE B", "Q1"}));
    EXPECT_EQ(collectSqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "CREATE B", "Q2"}));
}
}
