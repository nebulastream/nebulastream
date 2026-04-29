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

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <rfl/Generic.hpp>
#include <rfl/Literal.hpp>
#include <rfl/Rename.hpp>
#include <rfl/TaggedUnion.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizer.hpp>

#include <Model.hpp>

namespace NES
{

struct PlannerContext;

/// --------------------------------------------------------------------------
/// StatementResult: the output of the SqlPlanner.
///
/// Each variant carries the parsed/planned data for one statement type.
/// The Rust RequestHandler matches on these to execute catalog writes.
/// --------------------------------------------------------------------------

/// A statement may omit its HOST clause; the policy decides what that means. `RequireHostConfig` rejects such statements,
/// `DefaultHost` substitutes a fixed address (used by embedded deployments, where the only worker is the local one).
struct RequireHostConfig {
};

struct DefaultHost {
    std::string hostName;
};

using HostPolicy = std::variant<RequireHostConfig, DefaultHost>;

/// Serialized straight onto the wire as Rust's `Statement::CreateLogicalSource`. `Tag` supplies the variant discriminator and the
/// field names must match `model::source::logical::CreateLogicalSource`; `if_not_exists` is `#[serde(default)]` there, so it is omitted.
/// `schema` is an `rfl::Generic` rather than a `Schema` because it has to serialize as a nested JSON object, not a string.
struct CreateLogicalSourceResult
{
    using Tag = rfl::Literal<"CreateLogicalSource">;
    std::string name;
    rfl::Generic schema;
};

/// A flat JSON object of strings, matching a `serde_json::Value` on the Rust side.
using ConfigObject = std::unordered_map<std::string, std::string>;

struct CreatePhysicalSourceResult
{
    using Tag = rfl::Literal<"CreatePhysicalSource">;
    rfl::Rename<"logical_source", std::string> logicalSourceName;
    rfl::Rename<"host_addr", std::string> host;
    rfl::Rename<"source_type", std::string> sourceType;
    rfl::Rename<"source_config", ConfigObject> sourceConfig;
    rfl::Rename<"parser_config", ConfigObject> parserConfig;
};

struct CreateSinkResult
{
    using Tag = rfl::Literal<"CreateSink">;
    std::string name;
    rfl::Rename<"host_addr", std::string> host;
    rfl::Rename<"sink_type", std::string> sinkType;
    rfl::Generic schema;
    /// The sink config with the output-formatter options nested under `OUTPUT_FORMATTER`, mirroring the SQL surface.
    rfl::Generic config;
};

struct CreateWorkerResult
{
    using Tag = rfl::Literal<"CreateWorker">;
    rfl::Rename<"host_addr", std::string> hostAddr;
    rfl::Rename<"data_addr", std::string> dataAddr;
    rfl::Rename<"max_operators", std::optional<int32_t> > maxOperators;
    std::vector<std::string> peers;
    ConfigObject config;
};

struct DropLogicalSourceResult
{
    using Tag = rfl::Literal<"DropLogicalSource">;
    std::optional<std::string> name;
};

struct DropPhysicalSourceResult
{
    using Tag = rfl::Literal<"DropPhysicalSource">;
    std::optional<uint64_t> id;
    rfl::Rename<"logical_source", std::optional<std::string> > logicalSourceName;
};

struct DropSinkResult {
    using Tag = rfl::Literal<"DropSink">;
    std::optional<std::string> name;
};

/// Mirrors Rust's `GetQuery`, which `DropQuery` nests as its `filters` field.
struct QueryFilters {
    std::optional<std::vector<uint64_t> > ids;
    std::optional<std::string> name;
};

struct DropQueryResult {
    using Tag = rfl::Literal<"DropQuery">;
    /// `stop_mode` is `#[serde(default)]` (Graceful) on the Rust side; nothing downstream acts on it yet.
    QueryFilters filters;
};

struct DropWorkerResult {
    using Tag = rfl::Literal<"DropWorker">;
    rfl::Rename<"host_addr", std::string> host;
};

/// `fragments`, `source_ids` and `sink_ids` are `#[serde(default)]` on the Rust side and are grafted onto the
/// deserialized statement in `planner_bridge.rs` from `PlannedStatement`, so they are not serialized here.
struct CreateQueryResult {
    using Tag = rfl::Literal<"CreateQuery">;
    std::optional<std::string> name;
    std::string sql;
};

struct ExplainQueryResult {
    using Tag = rfl::Literal<"ExplainQuery">;
    std::string explanation;
};

struct ShowLogicalSourcesResult {
    using Tag = rfl::Literal<"GetLogicalSource">;
    std::optional<std::string> name;
};

struct ShowPhysicalSourcesResult {
    using Tag = rfl::Literal<"GetPhysicalSource">;
    std::optional<uint64_t> id;
    rfl::Rename<"logical_source", std::optional<std::string> > logicalSourceName;
};

struct ShowSinksResult {
    using Tag = rfl::Literal<"GetSink">;
    std::optional<std::string> name;
};

struct ShowQueriesResult {
    using Tag = rfl::Literal<"GetQuery">;
    std::optional<std::vector<uint64_t> > ids;
    std::optional<std::string> name;
};

/// `SHOW WORKER STATUS` also lands here: Rust's `GetWorkerStatus` takes a single mandatory address, so it cannot express
/// the statement's host list. TODO: give worker status its own variant.
struct ShowWorkersResult {
    using Tag = rfl::Literal<"GetWorker">;
    rfl::Rename<"host_addr", std::optional<std::string> > host;
};

struct CreateModelResult {
    using Tag = rfl::Literal<"CreateMlModel">;
    std::string name;
    std::string path;
    /// `rfl::Generic` (not a raw `ImportedModel`) because rfl can only serialize the reflected form; `Model` has no
    /// rfl `ReflectionType`, only NES's `Reflector<ImportedModel>`. Same reason `inputs`/`outputs` are `Generic`.
    rfl::Rename<"imported", rfl::Generic> imported;
    rfl::Rename<"input_schema", rfl::Generic> inputs;
    rfl::Rename<"output_schema", rfl::Generic> outputs;
};

struct ShowModelsResult {
    using Tag = rfl::Literal<"GetMlModel">;
    std::optional<std::string> name;
};

struct DropModelResult {
    using Tag = rfl::Literal<"DropMlModel">;
    std::optional<std::string> name;
};

/// Internally tagged on a `"tag"` field, matching Rust's `#[serde(tag = "tag")] enum Statement`.
using StatementResult = rfl::TaggedUnion<
    "tag",
    CreateLogicalSourceResult,
    ShowLogicalSourcesResult,
    DropLogicalSourceResult,
    CreatePhysicalSourceResult,
    ShowPhysicalSourcesResult,
    DropPhysicalSourceResult,
    CreateSinkResult,
    ShowSinksResult,
    DropSinkResult,
    CreateQueryResult,
    ExplainQueryResult,
    ShowQueriesResult,
    DropQueryResult,
    CreateWorkerResult,
    ShowWorkersResult,
    DropWorkerResult,
    CreateModelResult,
    ShowModelsResult,
    DropModelResult>;

/// Everything `plan()` produces. `statement` serializes directly to the JSON the Rust coordinator deserializes as
/// `model::Statement`. `fragments` is non-empty only for a query, whose per-worker plans travel out of band as
/// protobuf bytes rather than inside the JSON.
struct PlanOutput {
    StatementResult statement;
    std::unordered_map<Host, std::vector<LogicalPlan> > fragments;
};

class SqlPlanner
{
public:
    explicit SqlPlanner(const PlannerContext &ctx, QueryOptimizerConfiguration optimizerConfig, HostPolicy hostPolicy);

    [[nodiscard]] std::expected<PlanOutput, Exception> plan(std::string_view sql) const;

private:
    StatementBinder binder;
    SemanticAnalyzer analyzer;
    QueryOptimizer optimizer;
    HostPolicy hostPolicy;
};

}
