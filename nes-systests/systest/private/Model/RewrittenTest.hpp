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

#include <filesystem>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>

namespace NES
{

/// The rewriter decides the path and the contents, and the runner writes the file before submitting the statement.
struct InlineData
{
    /// The CSV file that will store the rows declared under `ATTACH INLINE`
    std::filesystem::path path;
    /// Holds the actual input tuples declared under `ATTACH INLINE`
    std::vector<std::string> rows;
    bool operator==(const InlineData& other) const = default;
};

/// The data for a TCP source, send by an in-process TCP server.
/// The runner learns the port only once that server binds, so it adds the endpoint to the statement afterwards.
struct ServedData
{
    /// Either inline rows, or read from a file.
    std::variant<std::vector<std::string>, std::filesystem::path> content;
};

/// A setup statement the runner submits as is: sink, model and logical source DDL, and a physical source with no data to stage.
struct PlainStatement
{
    std::string sql;
};

/// A physical source whose inline rows the runner writes to a CSV before submitting.
struct StatementWithInlineData
{
    std::string sql;
    InlineData data;
};

/// A TCP source whose data a server sends.
/// The runner starts the server and merges its endpoint into this statement's sql before submitting.
struct StatementWithServedData
{
    std::string sql;
    ServedData data;
};

/// One statement to submit before the queries, as the action the runner takes for it.
using SetupStatement = std::variant<PlainStatement, StatementWithInlineData, StatementWithServedData>;

/// The statement text of any setup alternative.
inline const std::string& sqlOf(const SetupStatement& statement)
{
    return std::visit([](const auto& alternative) -> const std::string& { return alternative.sql; }, statement);
}

/// One query to submit.
struct RewrittenQuery
{
    std::string sql;
    /// The query's number in the test file, so a reported result points back at it.
    SystestQueryId id;
    /// Absent when there is nothing to compare: the sink discards its input (e.g., `VoidSink`), or the statement does not
    /// parse and never runs.
    std::optional<std::filesystem::path> resultFile;

    Expectation expectation;
};

/// Both halves of a differential block, which asserts only that the two results agree.
/// Each half writes a result file of its own, because one shared file would compare a result against itself.
struct RewrittenDifferential
{
    std::string firstSql;
    SystestQueryId firstId;
    std::filesystem::path firstResultFile;
    std::string secondSql;
    SystestQueryId secondId;
    std::filesystem::path secondResultFile;
};

/// One EXPLAIN to submit: it starts no query and writes no file, and the plan it prints is the answer.
struct RewrittenExplain
{
    std::string sql;
    SystestQueryId id;
    ExpectedPlan expected;
};

/// One test case in a test file.
/// A case is the unit that gets a verdict (i.e., PASSED || FAILED).
struct RewrittenCase
{
    std::variant<RewrittenQuery, RewrittenDifferential, RewrittenExplain> action;
    /// The data files of this case's sources, once per reference, so a query that joins a source with itself counts
    /// it twice, and a differential block counts both halves.
    /// A measurement derives throughput from their size, and a source that generates its own rows contributes none.
    std::vector<std::filesystem::path> inputFiles;
    /// Whether the case before this one has to reach a terminal state before this one is submitted.
    /// A file marked `SEQUENTIAL_EXECUTION` sets this on every case.
    bool runsAfterPrevious = false;
};

inline SystestQueryId caseNumber(const RewrittenCase& testCase)
{
    if (const auto* query = std::get_if<RewrittenQuery>(&testCase.action))
    {
        return query->id;
    }
    if (const auto* explain = std::get_if<RewrittenExplain>(&testCase.action))
    {
        return explain->id;
    }
    return std::get<RewrittenDifferential>(testCase.action).firstId;
}

/// One runnable test file.
/// The rewriter produces this and the runner consumes it.
struct RewrittenTest
{
    /// Name for reporting failure/progress.
    std::string name;
    /// The prefix that qualifying put in front of every catalog-visible name, so a consumer comparing printed
    /// plans can strip it and read the names the test wrote originally.
    std::string qualifyingPrefix;
    std::vector<SetupStatement> setupStatements;
    std::vector<RewrittenCase> cases;
};

}
