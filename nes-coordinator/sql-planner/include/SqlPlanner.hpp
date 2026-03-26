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

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Phases/QueryOptimizer.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <SQLQueryParser/StatementBinder.hpp>

namespace NES
{

struct CatalogRef;
class NetworkTopology;

/// --------------------------------------------------------------------------
/// StatementResult: the output of the SqlPlanner.
///
/// Each variant carries the parsed/planned data for one statement type.
/// The Rust RequestHandler matches on these to execute catalog writes.
/// --------------------------------------------------------------------------

struct CreateLogicalSourceResult
{
    std::string name;
    Schema schema;
};

struct CreatePhysicalSourceResult
{
    std::string logicalSourceName;
    std::string sourceType;
    std::unordered_map<std::string, std::string> sourceConfig;
    std::unordered_map<std::string, std::string> parserConfig;
};

struct CreateSinkResult
{
    std::string name;
    std::string sinkType;
    Schema schema;
    std::unordered_map<std::string, std::string> sinkConfig;
    std::unordered_map<std::string, std::string> formatConfig;
};

struct CreateWorkerResult
{
    std::string hostAddr;
    std::string dataAddr;
    std::optional<size_t> capacity;
    std::vector<std::string> peers;
    std::unordered_map<std::string, std::string> config;
};

struct DropLogicalSourceResult
{
    std::string name;
};

struct DropPhysicalSourceResult
{
    uint64_t id;
};

struct DropSinkResult
{
    std::string name;
};

struct DropQueryResult
{
    std::optional<std::string> name;
    std::optional<int64_t> id;
    StopMode stopMode = StopMode::Graceful;
};

struct DropWorkerResult
{
    std::string host;
};

struct QueryPlanResult
{
    /// User-provided query name from SQL.
    std::optional<std::string> name;
    std::unordered_map<Host, std::vector<LogicalPlan>> plan;
};

struct ExplainQueryResult
{
    std::string explanation;
};

struct ShowLogicalSourcesResult
{
    std::optional<std::string> name;
};

struct ShowPhysicalSourcesResult
{
    std::optional<std::string> logicalSourceName;
    std::optional<uint32_t> id;
};

struct ShowSinksResult
{
    std::optional<std::string> name;
};

struct ShowQueriesResult
{
    std::optional<std::string> name;
    std::optional<int64_t> id;
};

struct ShowWorkersResult
{
    std::optional<std::string> host;
};

struct WorkerStatusResult
{
    std::vector<std::string> host;
};

using StatementResult = std::variant<
    CreateLogicalSourceResult,
    CreatePhysicalSourceResult,
    CreateSinkResult,
    CreateWorkerResult,
    DropLogicalSourceResult,
    DropPhysicalSourceResult,
    DropSinkResult,
    DropQueryResult,
    DropWorkerResult,
    QueryPlanResult,
    ExplainQueryResult,
    ShowLogicalSourcesResult,
    ShowPhysicalSourcesResult,
    ShowSinksResult,
    ShowQueriesResult,
    ShowWorkersResult,
    WorkerStatusResult>;

/// --------------------------------------------------------------------------
/// SqlPlanner: umbrella class orchestrating the full SQL pipeline.
///
/// Parsing -> Binding -> Semantic Analysis -> Optimization -> Placement -> Decomposition
///
/// Takes a SQL string and returns a StatementResult describing what the
/// coordinator should write to the database. The planner is read-only:
/// all DB writes happen in Rust after the planner returns.
/// --------------------------------------------------------------------------
class SqlPlanner
{
public:
    SqlPlanner(
        const CatalogRef& catalog,
        const NetworkTopology& topology,
        QueryOptimizerConfiguration optimizerConfig = {});

    /// Parse and plan a single SQL statement.
    [[nodiscard]] std::expected<StatementResult, Exception> execute(std::string_view sql) const;

private:
    StatementBinder binder;
    SemanticAnalyzer analyzer;
    QueryOptimizer optimizer;
};

}
