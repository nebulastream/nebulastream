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

#include <Parser/TestFileBuilder.hpp>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// The configuration lines in scope for the next query.
/// A global line holds until the file ends, and a later global line replaces the keys of an earlier one.
/// A local line holds for one query, and any duplicate key with other local lines in scope is an error.
/// A line may list several values, and the query runs once per pairing of values across the lines in scope.
class CurrentOverrides
{
public:
    void addGlobal(const std::vector<ConfigurationOverride>& line) { global = combine(line, global, OnDuplicateKey::Replace); }

    void addLocal(const std::vector<ConfigurationOverride>& line) { local = combine(line, local, OnDuplicateKey::Reject); }

    /// Returns the pairings for the next query and clears the local lines.
    /// The result always holds at least one entry, so a query with no overrides runs exactly once.
    [[nodiscard]] std::vector<ConfigurationOverride> takeForNextQuery()
    {
        auto overrides = combine(local, global, OnDuplicateKey::Reject);
        local = {};
        return overrides;
    }

    void discardLocal() { local = {}; }

private:
    std::vector<ConfigurationOverride> global;
    std::vector<ConfigurationOverride> local;
};

/// An EXPLAIN answers with the plan it prints, so an expected error is rejected.
ExpectedPlan expectedPlan(const Expectation& expected)
{
    if (const auto* lines = std::get_if<ExpectedRows>(&expected))
    {
        return ExpectedPlan{.lines = lines->rows};
    }
    throw SLTUnexpectedToken("an EXPLAIN expects the plan it prints, not an error");
}

/// A query waiting for the result block that completes it.
struct PendingQuery
{
    std::string sql;
    std::vector<ConfigurationOverride> overrides;
    bool isExplain = false;
    bool sequential = false;
};

/// Collects the statements of one test file from the parser.
/// The parser reports a query and its expected result in two callbacks, so the query is marked pending until the expected result completes it.
class TestFileBuilder
{
public:
    void addCreate(std::string sql, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> attach)
    {
        std::optional<AttachedData> attached;
        if (attach.has_value())
        {
            attached = attach->first == TestDataIngestionType::INLINE
                ? AttachedData{InlineRows{.rows = std::move(attach->second)}}
                : AttachedData{AttachedFile{.path = std::filesystem::path{attach->second.front()}}};
        }
        statements.emplace_back(CreateStatement{.sql = std::move(sql), .attach = std::move(attached)});
    }

    void addGlobalOverrides(const std::vector<ConfigurationOverride>& line) { overrides.addGlobal(line); }

    void addLocalOverrides(const std::vector<ConfigurationOverride>& line) { overrides.addLocal(line); }

    /// Reads the overrides in scope now, rather than when the result completes the query,
    /// because a configuration line that follows the query belongs to the next one.
    void beginQuery(std::string sql, const bool sequential)
    {
        pendingQuery = PendingQuery{.sql = std::move(sql), .overrides = overrides.takeForNextQuery(), .sequential = sequential};
    }

    /// An EXPLAIN is not executed, its plan is created by optimizer configuration defined by the run.
    /// The configuration in scope has no effect on it, so we discard it here (instead of applying it to the next query).
    void beginExplain(std::string sql)
    {
        overrides.discardLocal();
        pendingQuery = PendingQuery{.sql = std::move(sql), .overrides = {}, .isExplain = true};
    }

    void completeQuery(const SystestQueryId id, const Expectation& expected)
    {
        if (not pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("a result or an expected error must follow a query");
        }
        auto [sql, queryOverrides, isExplain, sequential] = std::move(*pendingQuery);
        pendingQuery.reset();

        if (isExplain)
        {
            statements.emplace_back(ExplainStatement{.sql = std::move(sql), .id = id, .expected = expectedPlan(expected)});
            return;
        }
        /// One statement per pairing of overrides, all with the same number, because they are one query of the file run more than once.
        for (auto& alternative : queryOverrides)
        {
            statements.emplace_back(
                SelectStatement{.sql = sql, .id = id, .expected = expected, .overrides = std::move(alternative), .sequential = sequential});
        }
    }

    /// The parser reports the first half as a plain query before reporting the block, so the pending query holds the block's overrides.
    void
    addDifferential(const std::string& firstSql, const std::string& secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
    {
        auto blockOverrides = pendingQuery.has_value() ? std::move(pendingQuery->overrides) : std::vector{ConfigurationOverride{}};
        const auto sequential = pendingQuery.has_value() and pendingQuery->sequential;
        pendingQuery.reset();
        /// One statement per pairing of overrides, as for a normal query statement.
        for (auto& alternative : blockOverrides)
        {
            statements.emplace_back(DifferentialStatement{
                .firstSql = firstSql,
                .firstId = firstId,
                .secondSql = secondSql,
                .secondId = secondId,
                .overrides = std::move(alternative),
                .sequential = sequential});
        }
    }

    [[nodiscard]] std::vector<TestStatement> take() &&
    {
        if (pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("the file ends with a query that has no result or expected error: {}", pendingQuery->sql);
        }
        return std::move(statements);
    }

private:
    CurrentOverrides overrides;
    std::optional<PendingQuery> pendingQuery;
    std::vector<TestStatement> statements;
};

}

ParsedTestFile buildTestFile(SystestParser& parser, const std::filesystem::path& path)
{
    TestFileBuilder builder;

    parser.registerOnCreateCallback([&](std::string sql, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> attach)
                                    { builder.addCreate(std::move(sql), std::move(attach)); });

    parser.registerOnQueryCallback([&](std::string sql, SystestQueryId, const bool sequential)
                                   { builder.beginQuery(std::move(sql), sequential); });

    parser.registerOnExplainQueryCallback([&](std::string sql, SystestQueryId) { builder.beginExplain(std::move(sql)); });

    parser.registerOnGlobalConfigurationCallback([&](const std::vector<ConfigurationOverride>& overrides)
                                                 { builder.addGlobalOverrides(overrides); });

    parser.registerOnConfigurationCallback([&](const std::vector<ConfigurationOverride>& overrides)
                                           { builder.addLocalOverrides(overrides); });

    parser.registerOnResultTuplesCallback([&](std::vector<std::string> rows, const SystestQueryId id)
                                          { builder.completeQuery(id, ExpectedRows{.rows = std::move(rows)}); });

    parser.registerOnErrorExpectationCallback([&](const SystestParser::ErrorExpectation& error, const SystestQueryId id)
                                              { builder.completeQuery(id, ExpectedError{.code = error.code, .message = error.message}); });

    parser.registerOnDifferentialQueryBlockCallback(
        [&](const std::string& firstSql, const std::string& secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
        { builder.addDifferential(firstSql, secondSql, firstId, secondId); });

    parser.parse();
    return ParsedTestFile{.path = path, .statements = std::move(builder).take()};
}

}
