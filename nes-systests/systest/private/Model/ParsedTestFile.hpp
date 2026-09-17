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
#include <unordered_set>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>

namespace NES
{

/// Rows that the test wrote directly beneath the `ATTACH`.
struct InlineRows
{
    std::vector<std::string> rows;
};

/// The data file the `ATTACH` names, relative to the test data directory.
struct AttachedFile
{
    std::filesystem::path path;
};

/// The data that an `ATTACH` clause supplies for a physical source.
using AttachedData = std::variant<InlineRows, AttachedFile>;

struct CreateStatement
{
    std::string sql;
    std::optional<AttachedData> attach;
};

/// One query and its expected answer.
struct SelectStatement
{
    std::string sql;
    SystestQueryId id;
    Expectation expected;
    ConfigurationOverride overrides;
};

/// Two queries whose results must match.
struct DifferentialStatement
{
    std::string firstSql;
    SystestQueryId firstId;
    std::string secondSql;
    SystestQueryId secondId;
    ConfigurationOverride overrides;
};

/// One `EXPLAIN` and the plan text it expects.
struct ExplainStatement
{
    std::string sql;
    SystestQueryId id;
    ExpectedPlan expected;
};

using TestStatement = std::variant<CreateStatement, SelectStatement, DifferentialStatement, ExplainStatement>;

/// The query numbers that a statement answers to on the command line.
/// A CREATE has none, and a differential block has two.
[[nodiscard]] std::vector<SystestQueryId> queryNumbersOf(const TestStatement& statement);

/// Drops every statement with a query number outside the selection.
/// An empty selection selects everything.
/// The CREATEs stay, because every remaining statement may depend on them.
/// Selecting happens before rewriting, so a malformed statement that the run does not select cannot fail the file.
void keepSelectedStatements(std::vector<TestStatement>& statements, const std::unordered_set<SystestQueryId>& selected);

/// The statements of one test file, in file order.
struct ParsedTestFile
{
    std::filesystem::path path;
    std::vector<TestStatement> statements;
};

}
