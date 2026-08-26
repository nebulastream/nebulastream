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

#include <string>
#include <string_view>
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

TestStatement create(std::string sql)
{
    return CreateStatement{.sql = std::move(sql), .attach = std::nullopt};
}

TestStatement query(std::string sql, const ConfigurationOverride& overrides = {})
{
    return SelectStatement{.sql = std::move(sql), .id = SystestQueryId{1}, .expected = ExpectedRows{}, .overrides = overrides};
}

TestStatement explain(std::string sql)
{
    return ExplainStatement{.sql = std::move(sql), .id = SystestQueryId{1}, .expected = ExpectedPlan{}};
}

/// The SQL of every statement of a part, so a part can be stated as what it holds rather than as a variant.
std::vector<std::string> sqlOf(const TestFilePart& part)
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

ConfigurationOverride asking(const std::string_view key, std::string value)
{
    ConfigurationOverride overrides;
    overrides[key] = std::move(value);
    return overrides;
}

}

/// A file holding only CREATE statements has no case to split on, so it stays one part holding the file unchanged.
TEST(TestFilePartitionTest, KeepsAFileWithoutQueriesWhole)
{
    const ParsedTestFile file{.path = "setup.test", .statements = {create("CREATE A")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(sqlOf(parts.front()), (std::vector<std::string>{"CREATE A"}));
}

/// A differential block asks for a configuration like a query does, so it joins the part whose overrides match.
TEST(TestFilePartitionTest, PlacesADifferentialBlockWithItsOverrides)
{
    const ParsedTestFile file{
        .path = "differential.test",
        .statements
        = {create("CREATE A"),
           query("Q1", asking("a", "1")),
           DifferentialStatement{
               .firstSql = "D1",
               .firstId = SystestQueryId{2},
               .secondSql = "D2",
               .secondId = SystestQueryId{2},
               .overrides = asking("a", "1")}}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(sqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "D1"}));
}

/// A file whose queries ask for nothing stays one part, in the order it was written.
TEST(TestFilePartitionTest, KeepsAFileThatConfiguresNothingWhole)
{
    const ParsedTestFile file{.path = "one.test", .statements = {create("CREATE A"), query("Q1"), query("Q2")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(sqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "Q2"}));
}

/// Two queries asking for different configurations need two workers, and each part repeats the setup.
TEST(TestFilePartitionTest, SplitsQueriesThatAskForDifferentOverrides)
{
    const ParsedTestFile file{
        .path = "two.test",
        .statements = {create("CREATE A"), query("Q1", asking("a", "1")), query("Q2", asking("a", "2")), query("Q3", asking("a", "1"))}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(sqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1", "Q3"}));
    EXPECT_EQ(sqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "Q2"}));
}

/// A query the reader ran out into one statement per listed alternative arrives here as several statements sharing a
/// number, and each has to reach the worker its own configuration asks for.
TEST(TestFilePartitionTest, SplitsTheAlternativesOfOneQueryApart)
{
    const ParsedTestFile file{
        .path = "alternatives.test",
        .statements = {create("CREATE A"), query("Q1", asking("bloom", "true")), query("Q1", asking("bloom", "false"))}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(parts.at(0).overrides.at("bloom"), "true");
    EXPECT_EQ(parts.at(1).overrides.at("bloom"), "false");
    EXPECT_EQ(sqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1"}));
    EXPECT_EQ(sqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "Q1"}));
}

/// An EXPLAIN asks for no configuration of its own, and a file that also holds a plain query must not lose it.
TEST(TestFilePartitionTest, KeepsAnExplainBesideAPlainQuery)
{
    const ParsedTestFile file{.path = "explain.test", .statements = {create("CREATE A"), query("Q1"), explain("EXPLAIN Q1")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 1U);
    EXPECT_EQ(sqlOf(parts.front()), (std::vector<std::string>{"CREATE A", "Q1", "EXPLAIN Q1"}));
}

/// An EXPLAIN belongs with the queries that ask for nothing, and a file whose queries all ask for something gives it a part of its own.
TEST(TestFilePartitionTest, GivesAnExplainItsOwnPartWhenEveryQueryAsksForOverrides)
{
    const ParsedTestFile file{
        .path = "mixed.test", .statements = {create("CREATE A"), query("Q1", asking("a", "1")), explain("EXPLAIN Q1")}};

    const auto parts = partitionByOverrides(file);

    ASSERT_EQ(parts.size(), 2U);
    EXPECT_EQ(sqlOf(parts.at(0)), (std::vector<std::string>{"CREATE A", "Q1"}));
    EXPECT_EQ(sqlOf(parts.at(1)), (std::vector<std::string>{"CREATE A", "EXPLAIN Q1"}));
}

}
