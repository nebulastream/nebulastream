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
    /// Set by `SEQUENTIAL_EXECUTION`: the previous query has to finish before this one starts.
    bool sequential = false;
};

/// Two queries whose results must match.
struct DifferentialStatement
{
    std::string firstSql;
    SystestQueryId firstId;
    std::string secondSql;
    SystestQueryId secondId;
    ConfigurationOverride overrides;
    /// Set by `SEQUENTIAL_EXECUTION`: the previous query has to finish before this block starts.
    bool sequential = false;
};

/// One `EXPLAIN` and the plan text it expects.
struct ExplainStatement
{
    std::string sql;
    SystestQueryId id;
    ExpectedPlan expected;
};

using TestStatement = std::variant<CreateStatement, SelectStatement, DifferentialStatement, ExplainStatement>;

/// The statements of one test file, in file order.
struct ParsedTestFile
{
    std::filesystem::path path;
    std::vector<TestStatement> statements;
};

}
