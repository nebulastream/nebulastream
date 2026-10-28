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

#pragma once

#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>

/// Shared by the reference engine prototypes that run a keyed tumbling window query over a CSV file and check its output.
namespace NES::PrototypeQuery
{

/// `physicalSource` is the part of a CREATE PHYSICAL SOURCE statement after `FOR stream`, e.g. `fileSource(input)`.
inline void registerCatalog(
    const std::shared_ptr<SourceCatalog>& sources,
    const std::shared_ptr<SinkCatalog>& sinks,
    const std::string& physicalSource,
    const std::filesystem::path& output)
{
    StatementBinder binder(sources, [](auto* query) { return AntlrSQLQueryParser::bindLogicalQueryPlan(query); });
    SourceStatementHandler sourceHandler(sources, DefaultHost{"localhost"});
    SinkStatementHandler sinkHandler(sinks, DefaultHost{"localhost"});
    const std::vector<std::string> declarations{
        "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, ts UINT64 NOT NULL);",
        "CREATE PHYSICAL SOURCE FOR stream " + physicalSource + ";",
        "CREATE SINK output(start UINT64 NOT NULL, end UINT64 NOT NULL, parity UINT64 NOT NULL, rowCount UINT64 NOT NULL, total UINT64 NOT "
        "NULL) TYPE File SET('"
            + output.string() + R"(' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'true' AS "SINK".APPEND);)"};
    for (const auto& sql : declarations)
    {
        auto statement = binder.parseAndBindSingle(sql);
        if (not statement)
        {
            throw std::runtime_error(statement.error().what());
        }
        const auto handled = std::visit(
            [&](const auto& bound) -> bool
            {
                using T = std::decay_t<decltype(bound)>;
                if constexpr (std::is_same_v<T, CreateLogicalSourceStatement> || std::is_same_v<T, CreatePhysicalSourceStatement>)
                {
                    if (auto result = sourceHandler(bound); not result)
                    {
                        throw std::runtime_error(result.error().what());
                    }
                    return true;
                }
                else if constexpr (std::is_same_v<T, CreateSinkStatement>)
                {
                    if (auto result = sinkHandler(bound); not result)
                    {
                        throw std::runtime_error(result.error().what());
                    }
                    return true;
                }
                return false;
            },
            *statement);
        if (not handled)
        {
            throw std::runtime_error("Unexpected catalog statement");
        }
    }
}

inline std::unique_ptr<CompiledQueryPlan> compile(
    const std::string& sql,
    QueryId queryId,
    const RuleBasedOptimizer& optimizer,
    QueryCompilation::QueryCompiler& compiler,
    std::optional<LogicalPlan>& donorPlan)
{
    auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
    plan.setQueryId(queryId);
    auto optimized = optimizer.optimize(std::move(plan));
    auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(optimized);
    /// The window build lowers its absorb only when it is paired with a donor plan.
    request->donorQueryPlan = donorPlan;
    auto result = compiler.compileQuery(std::move(request));
    donorPlan = std::move(optimized);
    return result;
}

/// (window start, parity) -> (count, sum)
using Windows = std::map<std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>>;

constexpr uint64_t WINDOW_SIZE = 100;
/// The query's event time is `ts + 2`.
constexpr uint64_t EVENT_TIME_SHIFT = 2;

inline Windows expectedWindows(uint64_t rows)
{
    Windows windows;
    for (uint64_t row = 0; row < rows; ++row)
    {
        auto& [count, sum] = windows[{(row + EVENT_TIME_SHIFT) / WINDOW_SIZE * WINDOW_SIZE, row % 2}];
        ++count;
        sum += row;
    }
    return windows;
}

/// Throws on a window reported twice; lines that are not results (e.g. a repeated header) are skipped.
inline Windows readWindows(const std::filesystem::path& output)
{
    Windows windows;
    std::ifstream result(output);
    std::string line;
    while (std::getline(result, line))
    {
        if (line.empty() || std::isdigit(static_cast<unsigned char>(line.front())) == 0)
        {
            continue;
        }
        std::vector<uint64_t> fields;
        size_t begin = 0;
        for (size_t comma = line.find(','); comma != std::string::npos; begin = comma + 1, comma = line.find(',', begin))
        {
            fields.push_back(std::stoull(line.substr(begin, comma - begin)));
        }
        fields.push_back(std::stoull(line.substr(begin)));
        if (fields.size() != 5 || fields[1] != fields[0] + WINDOW_SIZE)
        {
            throw std::runtime_error("Malformed window output: " + line);
        }
        if (not windows.emplace(std::pair{fields[0], fields[2]}, std::pair{fields[3], fields[4]}).second)
        {
            throw std::runtime_error("Window reported twice: " + line);
        }
    }
    return windows;
}

inline void waitForOutput(const std::filesystem::path& output, const std::chrono::seconds timeout, const auto& done)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (std::filesystem::exists(output) && done(readWindows(output)))
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error("Timed out waiting for window output");
}

/// Keyed tumbling window over `stream`. The nested projections keep CSV parsing in its own pipeline, separate from the window build.
inline const std::string WINDOW_QUERY = "SELECT start, end, parity, COUNT(*) AS rowCount, SUM(id) AS total FROM "
                                        "(SELECT id, id % 2 AS parity, shifted + 1 AS event_time FROM "
                                        "(SELECT id, ts + 1 AS shifted FROM stream)) GROUP BY (parity) "
                                        "WINDOW TUMBLING(event_time, size 100 ms) INTO output;";

inline std::string fileSource(const std::filesystem::path& input)
{
    return "TYPE File SET('CSV' AS INPUT_FORMATTER.\"TYPE\", '" + input.string() + "' AS \"SOURCE\".FILE_PATH)";
}

/// Rows `id,ts` with id = ts = row, generated as fast as the source can for practically ever.
inline std::string unboundedGeneratorSource()
{
    return R"(TYPE Generator SET(
        'CSV' AS INPUT_FORMATTER."TYPE",
        'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
        'FIXED' AS "SOURCE".GENERATOR_RATE_TYPE,
        'emit_rate 1000000000000' AS "SOURCE".GENERATOR_RATE_CONFIG,
        'SEQUENCE UINT64 0 1000000000000000 1, SEQUENCE UINT64 0 1000000000000000 1' AS "SOURCE".GENERATOR_SCHEMA))";
}

/// Each (window, parity) result covers WINDOW_SIZE / 2 input rows.
constexpr uint64_t ROWS_PER_RESULT = WINDOW_SIZE / 2;

/// The expected (count, sum) of one result, in closed form: rows r with r % 2 == parity and window start <= r + 2 < start + WINDOW_SIZE.
inline std::pair<uint64_t, uint64_t> expectedWindow(uint64_t start, uint64_t parity)
{
    const uint64_t low = start >= EVENT_TIME_SHIFT ? start - EVENT_TIME_SHIFT : 0;
    const uint64_t high = start + WINDOW_SIZE - EVENT_TIME_SHIFT;
    const uint64_t first = low + ((parity + 2 - low % 2) % 2);
    if (first >= high)
    {
        return {0, 0};
    }
    const uint64_t count = (high - 1 - first) / 2 + 1;
    return {count, count * first + count * (count - 1)};
}

/// For an unbounded input: the results must be exactly the windows from 0 up to the last reported one, each with its expected values.
inline void verifyWindowPrefix(const Windows& actual)
{
    if (actual.empty())
    {
        throw std::runtime_error("No windows were produced");
    }
    const auto lastStart = actual.rbegin()->first.first;
    if (actual.size() != 2 * (lastStart / WINDOW_SIZE + 1))
    {
        throw std::runtime_error("Windows are missing before window " + std::to_string(lastStart));
    }
    for (const auto& [key, value] : actual)
    {
        if (value != expectedWindow(key.first, key.second))
        {
            throw std::runtime_error(
                "Window " + std::to_string(key.first) + " parity " + std::to_string(key.second) + " is "
                + std::to_string(value.first) + "/" + std::to_string(value.second));
        }
    }
}

/// Writes `rows` lines of `id,ts` with id = ts = row.
inline void writeInput(const std::filesystem::path& input, uint64_t rows)
{
    std::ofstream file(input);
    for (uint64_t row = 0; row < rows; ++row)
    {
        file << row << ',' << row << '\n';
    }
}

/// Throws with the first window that differs from an uninterrupted run.
inline void verifyWindows(const Windows& actual, const Windows& expected)
{
    if (actual == expected)
    {
        return;
    }
    for (const auto& [key, value] : expected)
    {
        const auto found = actual.find(key);
        if (found == actual.end() || found->second != value)
        {
            throw std::runtime_error(
                "Window " + std::to_string(key.first) + " parity " + std::to_string(key.second) + " is "
                + (found == actual.end() ? std::string("missing")
                                         : std::to_string(found->second.first) + "/" + std::to_string(found->second.second))
                + ", expected " + std::to_string(value.first) + "/" + std::to_string(value.second));
        }
    }
    throw std::runtime_error("Unexpected extra windows in output");
}
}
