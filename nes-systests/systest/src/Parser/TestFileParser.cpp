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

#include <cstdint>
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

/// An empty vector and a single empty override both mean no overrides, and combining treats them alike.
bool hasNoOverrides(const std::vector<ConfigurationOverride>& alternatives)
{
    return alternatives.empty() or (alternatives.size() == 1 and alternatives.front().empty());
}

/// What combining does with a key that both sides set.
enum class OnDuplicateKey : uint8_t
{
    Replace,
    Reject
};

/// Combines two sets of alternatives into every pairing of one from each, so two lines that each list two values yield four pairings.
/// A key that both sides set takes the value from `overrides` under `Replace` and throws under `Reject`.
std::vector<ConfigurationOverride> combine(
    const std::vector<ConfigurationOverride>& overrides,
    const std::vector<ConfigurationOverride>& otherOverrides,
    const OnDuplicateKey onDuplicateKey)
{
    if (hasNoOverrides(overrides) and hasNoOverrides(otherOverrides))
    {
        return {ConfigurationOverride{}};
    }
    if (hasNoOverrides(overrides))
    {
        return otherOverrides;
    }
    if (hasNoOverrides(otherOverrides))
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
            for (const auto& [key, value] : override)
            {
                if (onDuplicateKey == OnDuplicateKey::Reject and merged.contains(key))
                {
                    throw SLTUnexpectedToken("Configuration key '{}' is set more than once for the same query", key);
                }
                merged[key] = value;
            }
            combined.push_back(std::move(merged));
        }
    }
    return combined;
}

/// The configuration lines in scope for the next query.
/// A global line holds until the file ends, and a later global line replaces the keys it restates.
/// A local line holds for one query, and a key it shares with any other line in scope is an error.
/// A line may list several values, and the query runs once per pairing of values across the lines in scope.
class CurrentSettings
{
public:
    void addGlobal(const std::vector<ConfigurationOverride>& line) { global = combine(line, global, OnDuplicateKey::Replace); }

    void addLocal(const std::vector<ConfigurationOverride>& line) { local = combine(line, local, OnDuplicateKey::Reject); }

    /// Returns the pairings for the next query and clears the local lines.
    /// The result always holds at least one entry, so a query with no settings runs exactly once.
    [[nodiscard]] std::vector<ConfigurationOverride> takeForNextQuery()
    {
        auto settings = combine(local, global, OnDuplicateKey::Reject);
        local = {};
        return settings;
    }

private:
    std::vector<ConfigurationOverride> global;
    std::vector<ConfigurationOverride> local;
};

/// A query waiting for the result block that completes it.
struct PendingQuery
{
    std::string sql;
    std::vector<ConfigurationOverride> settings;
    bool isExplain = false;
    bool sequential = false;
};

/// Collects the statements of one test file as the parser reports them.
/// The parser reports a query and its answer in two callbacks, so the query waits as pending until the answer completes it.
class TestFileBuilder
{
public:
    void addCreate(std::string sql, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> attach)
    {
        std::optional<AttachedData> attached;
        if (attach.has_value())
        {
            /// The parser reports a file attach as a single element holding the path.
            attached = attach->first == TestDataIngestionType::INLINE
                ? AttachedData{InlineRows{.rows = std::move(attach->second)}}
                : AttachedData{AttachedFile{.path = std::filesystem::path{attach->second.at(0)}}};
        }
        statements.emplace_back(Systest::CreateStatement{.sql = std::move(sql), .attach = std::move(attached)});
    }

    void addGlobalSettings(const std::vector<ConfigurationOverride>& line) { settings.addGlobal(line); }

    void addLocalSettings(const std::vector<ConfigurationOverride>& line) { settings.addLocal(line); }

    /// Takes the settings at the query, because a configuration line between it and its result belongs to the next query.
    void beginQuery(std::string sql, const bool sequential)
    {
        pendingQuery = PendingQuery{.sql = std::move(sql), .settings = settings.takeForNextQuery(), .sequential = sequential};
    }

    /// An EXPLAIN takes no settings, so a local line in scope stays for the next query.
    void beginExplain(std::string sql) { pendingQuery = PendingQuery{.sql = std::move(sql), .settings = {}, .isExplain = true}; }

    void completeQuery(const SystestQueryId id, Expectation expected)
    {
        if (not pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("a result or an expected error must follow a query");
        }
        auto [sql, querySettings, isExplain, sequential] = std::move(*pendingQuery);
        pendingQuery.reset();

        if (isExplain)
        {
            const auto* lines = std::get_if<ExpectedRows>(&expected);
            if (lines == nullptr)
            {
                throw SLTUnexpectedToken("an EXPLAIN expects the plan it prints, not an error");
            }
            statements.emplace_back(Systest::ExplainStatement{.sql = std::move(sql), .id = id, .expected = lines->rows});
            return;
        }
        /// One statement per pairing of settings, all with the same number, because they are one query of the file run more than once.
        for (auto& alternative : querySettings)
        {
            statements.emplace_back(Systest::QueryStatement{
                .sql = sql, .id = id, .expected = expected, .settings = std::move(alternative), .sequential = sequential});
        }
    }

    /// The parser reports the first half as a plain query before reporting the block, so the pending query holds the block's settings.
    void
    addDifferential(const std::string& firstSql, const std::string& secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
    {
        auto blockSettings
            = pendingQuery.has_value() ? std::move(pendingQuery->settings) : std::vector<ConfigurationOverride>{ConfigurationOverride{}};
        const auto sequential = pendingQuery.has_value() and pendingQuery->sequential;
        pendingQuery.reset();
        /// One statement per pairing of settings, as for a plain query.
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
        [&](const std::string& firstSql, const std::string& secondSql, const SystestQueryId firstId, const SystestQueryId secondId)
        { builder.addDifferential(firstSql, secondSql, firstId, secondId); });

    parser.parse();
    return TestFile{.path = path, .statements = std::move(builder).take()};
}

}
