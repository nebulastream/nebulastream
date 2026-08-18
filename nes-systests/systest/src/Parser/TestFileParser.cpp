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

#include <Parser/TestFileParser.hpp>

#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/SystestQueryId.hpp>
#include <Model/TestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Whether a set of alternatives asks for nothing, which is what a file that wrote no configuration line has.
/// Kept apart from an empty vector so combining with it yields the other side rather than nothing at all.
bool asksForNothing(const std::vector<ConfigurationOverride>& alternatives)
{
    return alternatives.empty() or (alternatives.size() == 1 and alternatives.front().overrideParameters.empty());
}

/// Combines two sets of alternatives into every pairing of one from each, so a query asking for two settings that each
/// list two values runs four times.
/// A key both sides set takes the value from `overrides`, which is the line written closer to the query.
std::vector<ConfigurationOverride>
combine(const std::vector<ConfigurationOverride>& overrides, const std::vector<ConfigurationOverride>& otherOverrides)
{
    if (asksForNothing(overrides) and asksForNothing(otherOverrides))
    {
        return {ConfigurationOverride{}};
    }
    if (asksForNothing(overrides))
    {
        return otherOverrides;
    }
    if (asksForNothing(otherOverrides))
    {
        return overrides;
    }

    std::vector<ConfigurationOverride> combined;
    combined.reserve(overrides.size() * otherOverrides.size());
    for (const auto& override : overrides)
    {
        for (const auto& other : otherOverrides)
        {
            auto merged = other;
            for (const auto& [key, value] : override.overrideParameters)
            {
                merged.overrideParameters.insert_or_assign(key, value);
            }
            combined.push_back(std::move(merged));
        }
    }
    return combined;
}

/// The settings that apply to the next query the reader meets.
/// A global line adds to them until the file ends.
/// A local line adds to them for one query only, and that query clears it.
///
/// A line may list values rather than one, and each value is an alternative the query runs under. Several lines
/// combine into every pairing of their values, so the query runs once per pairing.
class CurrentSettings
{
public:
    void addGlobal(const std::vector<ConfigurationOverride>& line) { global = combine(line, global); }

    void addLocal(const std::vector<ConfigurationOverride>& line) { local = combine(line, local); }

    /// Returns the alternatives of the next query and clears the local line, which that query has now used up.
    /// Always holds at least one entry, so a query with no settings still runs exactly once.
    [[nodiscard]] std::vector<ConfigurationOverride> takeForNextQuery()
    {
        auto settings = combine(local, global);
        local = {};
        return settings;
    }

private:
    std::vector<ConfigurationOverride> global;
    std::vector<ConfigurationOverride> local;
};

/// A query the reader has seen, waiting for the answer that follows it.
struct PendingQuery
{
    std::string sql;
    /// One entry per set of settings the query runs under, and one empty entry when it asks for none.
    std::vector<ConfigurationOverride> settings;
    bool isExplain = false;
    /// Whether the file asked for `SEQUENTIAL_EXECUTION` where this query was written.
    bool sequential = false;
};

/// Collects the statements of one test file as the parser reports them.
/// The parser reports a query and its expected answer in two separate callbacks, so a query waits in the pending slot
/// until its answer arrives and completes it.
class TestFileBuilder
{
public:
    void addCreate(std::string sql, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> attach)
    {
        std::optional<AttachedData> attached;
        if (attach.has_value())
        {
            /// The line parser reports a file reference as a single element that holds the path.
            attached = attach->first == TestDataIngestionType::INLINE
                ? AttachedData{InlineRows{.rows = std::move(attach->second)}}
                : AttachedData{AttachedFile{.path = std::filesystem::path{attach->second.at(0)}}};
        }
        statements.emplace_back(Systest::CreateStatement{.sql = std::move(sql), .attach = std::move(attached)});
    }

    void addGlobalSettings(const std::vector<ConfigurationOverride>& line) { settings.addGlobal(line); }

    void addLocalSettings(const std::vector<ConfigurationOverride>& line) { settings.addLocal(line); }

    /// Takes the settings here rather than when the answer arrives, because a configuration line below a query is the next query's.
    void beginQuery(std::string sql, const bool sequential)
    {
        pendingQuery = PendingQuery{.sql = std::move(sql), .settings = settings.takeForNextQuery(), .sequential = sequential};
    }

    /// An EXPLAIN runs on whichever worker its sources were declared on, so it takes no settings and leaves a local line
    /// for the next query.
    void beginExplain(std::string sql) { pendingQuery = PendingQuery{.sql = std::move(sql), .settings = {}, .isExplain = true}; }

    /// Appends the pending query together with the answer that completed it, and clears the pending slot.
    void completeQuery(const SystestQueryId id, Expectation expected)
    {
        /// A result block with no query above it makes the test file malformed, which fails this file rather than the run.
        if (not pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("a result or an expected error must follow a query");
        }
        auto [sql, querySettings, isExplain, sequential] = std::move(*pendingQuery);
        pendingQuery.reset();

        if (isExplain)
        {
            /// An EXPLAIN expects the plan it prints, and no other answer makes sense for one.
            const auto* lines = std::get_if<ExpectedRows>(&expected);
            if (lines == nullptr)
            {
                throw SLTUnexpectedToken("an EXPLAIN expects the plan it prints, not an error");
            }
            statements.emplace_back(Systest::ExplainStatement{.sql = std::move(sql), .id = id, .expected = lines->rows});
            return;
        }
        /// One statement per set of settings the query runs under, all asserting the same answer.
        /// They share the query's number, because they are the same query of the file run more than once.
        for (auto& alternative : querySettings)
        {
            statements.emplace_back(Systest::QueryStatement{
                .sql = sql, .id = id, .expected = expected, .settings = std::move(alternative), .sequential = sequential});
        }
    }

    /// Appends both halves of a differential block, neither of which completes as a query of its own.
    /// The parser reports the first half as a plain query before reporting the block, so the pending slot holds that half
    /// and the block takes over the settings it already took.
    void
    addDifferential(const std::string& firstSql, const std::string& secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
    {
        auto blockSettings
            = pendingQuery.has_value() ? std::move(pendingQuery->settings) : std::vector<ConfigurationOverride>{ConfigurationOverride{}};
        const auto sequential = pendingQuery.has_value() and pendingQuery->sequential;
        pendingQuery.reset();
        /// A block runs once per set of settings, as a plain query does, and both halves stay together in each.
        for (auto& alternative : blockSettings)
        {
            statements.emplace_back(Systest::DifferentialStatement{
                .firstSql = firstSql,
                .firstId = firstId,
                .secondSql = secondSql,
                .secondId = secondId,
                .settings = std::move(alternative),
                .sequential = sequential});
        }
    }

    [[nodiscard]] std::vector<Systest::Statement> take() && { return std::move(statements); }

private:
    CurrentSettings settings;
    std::optional<PendingQuery> pendingQuery;
    std::vector<Systest::Statement> statements;
};

}

TestFile parseTestFile(SystestParser& parser, const std::filesystem::path& path)
{
    TestFileBuilder builder;

    parser.registerOnCreateCallback([&](std::string sql, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> attach)
                                    { builder.addCreate(std::move(sql), std::move(attach)); });

    parser.registerOnQueryCallback([&](std::string sql, SystestQueryId, const bool sequential)
                                   { builder.beginQuery(std::move(sql), sequential); });

    parser.registerOnExplainQueryCallback([&](std::string sql, SystestQueryId) { builder.beginExplain(std::move(sql)); });

    parser.registerOnGlobalConfigurationCallback([&](const std::vector<ConfigurationOverride>& overrides)
                                                 { builder.addGlobalSettings(overrides); });

    parser.registerOnConfigurationCallback([&](const std::vector<ConfigurationOverride>& overrides)
                                           { builder.addLocalSettings(overrides); });

    parser.registerOnResultTuplesCallback([&](ExpectedResult rows, const SystestQueryId id)
                                          { builder.completeQuery(id, ExpectedRows{.rows = std::move(rows)}); });

    parser.registerOnErrorExpectationCallback([&](const SystestParser::ErrorExpectation& error, const SystestQueryId id)
                                              { builder.completeQuery(id, ExpectedError{.code = error.code, .message = error.message}); });

    parser.registerOnDifferentialQueryBlockCallback(
        [&](std::string firstSql, std::string secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
        { builder.addDifferential(std::move(firstSql), std::move(secondSql), firstId, secondId); });

    parser.parse();
    return TestFile{.path = path, .statements = std::move(builder).take()};
}

}
