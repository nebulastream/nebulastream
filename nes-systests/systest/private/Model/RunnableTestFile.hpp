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
#include <unordered_map>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{

/// Rows declared under `ATTACH INLINE` and the CSV file that the runner writes them to before submitting the statement.
struct InlineData
{
    std::filesystem::path path;
    std::vector<std::string> rows;
    bool operator==(const InlineData& other) const = default;
};

/// The data for a TCP source, sent by an in-process TCP server.
/// The runner learns the port only once that server binds, so it adds the endpoint to the statement afterwards.
struct ServedData
{
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

/// One statement to submit before the test cases, with the data that the runner stages for it.
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
    std::vector<std::filesystem::path> inputFiles;

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

/// One EXPLAIN to submit: it starts no query and writes no file, and the plan that it prints is the answer.
struct RewrittenExplain
{
    std::string sql;
    SystestQueryId id;
    ExpectedPlan expected;
};

/// The unit that gets a verdict.
struct RewrittenTestCase
{
    std::variant<RewrittenQuery, RewrittenDifferential, RewrittenExplain> action;
};

inline SystestQueryId testCaseNumber(const RewrittenTestCase& testCase)
{
    return std::visit(
        Overloaded{
            [](const RewrittenQuery& query) { return query.id; },
            [](const RewrittenDifferential& differential) { return differential.firstId; },
            [](const RewrittenExplain& explain) { return explain.id; }},
        testCase.action);
}

/// Maps the canonical spelling of a prefixed name back to the canonical spelling that the test declared.
/// Canonical, because a printed plan spells names that way.
using OriginalNames = std::unordered_map<std::string, std::string>;

/// The rewriter produces this and the runner consumes it.
struct RunnableTestFile
{
    /// Name for reporting failure/progress.
    std::string name;
    OriginalNames originalNames;
    std::vector<SetupStatement> setupStatements;
    std::vector<RewrittenTestCase> testCases;
};

}
