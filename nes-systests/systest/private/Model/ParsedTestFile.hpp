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

struct InlineRows
{
    std::vector<std::string> rows;
};

struct AttachedFile
{
    std::filesystem::path path;
};

using AttachedData = std::variant<InlineRows, AttachedFile>;

struct CreateStatement
{
    std::string sql;
    std::optional<AttachedData> attach;
};

struct SelectStatement
{
    std::string sql;
    SystestQueryId id;
    Expectation expected;
    ConfigurationOverride overrides;
};

struct DifferentialStatement
{
    std::string firstSql;
    SystestQueryId firstId;
    std::string secondSql;
    SystestQueryId secondId;
    ConfigurationOverride overrides;
};

struct ExplainStatement
{
    std::string sql;
    SystestQueryId id;
    ExpectedPlan expected;
};

using TestStatement = std::variant<CreateStatement, SelectStatement, DifferentialStatement, ExplainStatement>;

/// A CREATE has no query number, a differential block two.
[[nodiscard]] std::vector<SystestQueryId> getQueryNumbersOf(const TestStatement& statement);

/// Empty for CREATE (every partition repeats it) and EXPLAIN (runs under defaults).
[[nodiscard]] ConfigurationOverride getOverridesOf(const TestStatement& statement);

/// An empty selection keeps every statement.
/// CREATEs always stay, since any query may read them.
void retainSelectedStatements(std::vector<TestStatement>& statements, const std::unordered_set<SystestQueryId>& selected);

/// False when only CREATEs remain.
[[nodiscard]] bool hasTestCases(const std::vector<TestStatement>& statements);

/// The statements of one test file, in file order.
struct ParsedTestFile
{
    std::filesystem::path path;
    std::vector<TestStatement> statements;
};

}
