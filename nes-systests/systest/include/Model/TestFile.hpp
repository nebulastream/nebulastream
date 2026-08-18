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

#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>

namespace NES
{

/// Rows that the test wrote directly beneath the `ATTACH`.
struct InlineRows
{
    std::vector<std::string> rows;
};

/// The data file that the `ATTACH` pointed at, as the test wrote it: relative to the test data directory.
struct AttachedFile
{
    std::filesystem::path path;
};

/// The data that an `ATTACH` clause supplies for a physical source.
using AttachedData = std::variant<InlineRows, AttachedFile>;

namespace Systest
{
/// One source or sink DDL statement, and the data that an `ATTACH` clause supplied for a physical source.
struct CreateStatement
{
    std::string sql;
    std::optional<AttachedData> attach;
};

/// One query and its expected answer: the result rows, or an error it should raise.
/// The settings are the ones that applied where the test wrote the query, and they select the worker that runs it.
struct QueryStatement
{
    std::string sql;
    SystestQueryId id;
    Expectation expected;
    ConfigurationOverride settings;
    /// Whether the query above this one in the file has to finish first, which `SEQUENTIAL_EXECUTION` asks for.
    bool sequential = false;
};

/// Two queries whose results have to agree, which is all a differential block asserts.
/// Neither half states an expected answer, so this holds two statements rather than two expectations.
struct DifferentialStatement
{
    std::string firstSql;
    SystestQueryId firstId;
    std::string secondSql;
    SystestQueryId secondId;
    ConfigurationOverride settings;
    /// Whether the query above this block has to finish first, which `SEQUENTIAL_EXECUTION` asks for.
    bool sequential = false;
};

/// One `EXPLAIN` and its expected plan text.
/// An `EXPLAIN` answers with text and starts no query, so it writes no result file and the checker compares line by line.
struct ExplainStatement
{
    std::string sql;
    SystestQueryId id;
    ExpectedResult expected;
};

using Statement = std::variant<CreateStatement, QueryStatement, DifferentialStatement, ExplainStatement>;

}

/// The statements of one test file, in file order.
struct TestFile
{
    std::filesystem::path path;
    std::vector<Systest::Statement> statements;
};

}
