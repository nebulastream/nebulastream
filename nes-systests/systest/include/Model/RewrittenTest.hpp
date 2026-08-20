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

#include <Model/Expectation.hpp>
#include <Model/SystestQueryId.hpp>

namespace NES
{

/// The CSV file that an `ATTACH INLINE` physical source reads, and the block's declared rows for it.
/// The rewriter decides the path and the contents, and the runner writes the file before submitting the statement.
struct InlineData
{
    std::filesystem::path path;
    std::vector<std::string> rows;
    bool operator==(const InlineData& other) const = default;
};

/// The data for a source that reads from a socket: the test's declared rows, or the file that it pointed at.
/// A server sends this data while the source reads it.
/// The runner learns the port only once that server binds, so it adds the endpoint to the statement then.
struct ServedData
{
    std::variant<std::vector<std::string>, std::filesystem::path> content;
};

/// The data that the runner puts in place before submitting a setup statement.
/// Absent when the source produces its own data or reads a file that already exists.
using StagedData = std::variant<InlineData, ServedData>;

/// One setup statement to submit before the queries: source or sink DDL, and the data to stage for it.
struct RewrittenCreateStatement
{
    std::string sql;
    std::optional<StagedData> staged;
};

/// One query to submit, the result file that its sink writes, and the expected answer in that file.
/// A query whose sink discards its input writes no file, so the result file is absent and there is nothing to compare.
/// The id is the query's number in the test file, so a reported result points back at it.
struct RewrittenQuery
{
    std::string sql;
    SystestQueryId id;
    std::optional<std::filesystem::path> resultFile;
    /// The data files of this query's sources, once per reference, so a query that joins a source
    /// with itself counts it twice.
    /// A measurement derives throughput from their size, and a source that generates its own rows contributes none.
    std::vector<std::filesystem::path> inputFiles;

    Expectation expectation;
};

/// Both halves of a differential block, which asserts one thing: the two results agree.
/// Neither half states an answer of its own, so there is no expectation, and one verdict covers the pair.
/// Each half writes a result file of its own, because one shared file would compare a result against itself.
struct RewrittenDifferential
{
    std::string firstSql;
    SystestQueryId firstId;
    std::filesystem::path firstResultFile;
    std::string secondSql;
    SystestQueryId secondId;
    std::filesystem::path secondResultFile;
    /// The data files that the sources of both halves read, once per reference.
    std::vector<std::filesystem::path> inputFiles;
};

/// One case of a test file: what to submit and check, and where it stands in the file's order.
/// A case is the unit that the scheduler hands out and the unit that gets one verdict.
struct RewrittenCase
{
    std::variant<RewrittenQuery, RewrittenDifferential> action;
    /// Whether the case before this one has to reach a terminal state before this one is submitted, which a file
    /// marked `SEQUENTIAL_EXECUTION` sets on every case.
    bool runsAfterPrevious = false;
};

/// The number that a case reports under, which for a differential block is the block's number in the test file.
inline SystestQueryId caseNumber(const RewrittenCase& testCase)
{
    if (const auto* query = std::get_if<RewrittenQuery>(&testCase.action))
    {
        return query->id;
    }
    return std::get<RewrittenDifferential>(testCase.action).firstId;
}

/// One test file ready to run: the setup DDL to submit, then the cases to run and check.
/// The rewriter produces this and the runner consumes it, so it belongs to neither.
struct RewrittenTest
{
    /// The file name that a failure is reported under.
    std::string name;
    /// Which settings part of the file this is, counted from zero, because every part reports the same name.
    uint32_t variant = 0;
    std::vector<RewrittenCreateStatement> createStmts;
    std::vector<RewrittenCase> cases;
};

}
