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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <rfl/Generic.hpp>
#include <rfl/Literal.hpp>
#include <rfl/Rename.hpp>
#include <rfl/TaggedUnion.hpp>

/// The planner's answer to one SQL statement, in the shape that the coordinator's own statement deserializes from.
/// Each variant mirrors one Rust variant: same tag, same field names, and identifiers folded to their canonical spelling.
namespace NES
{

/// The Rust side defaults `if_not_exists`, so it is omitted here.
/// The schema is reflected into a generic value because it has to serialize as a nested JSON object, not a string.
struct PlannedCreateLogicalSource
{
    using Tag = rfl::Literal<"CreateLogicalSource">;
    std::string name;
    rfl::Generic schema;
};

/// A flat JSON object of strings; the Rust side accepts any JSON value here.
using ConfigObject = std::unordered_map<std::string, std::string>;

struct PlannedCreatePhysicalSource
{
    using Tag = rfl::Literal<"CreatePhysicalSource">;
    rfl::Rename<"logical_source", std::string> logicalSourceName;
    rfl::Rename<"host_addr", std::string> host;
    rfl::Rename<"source_type", std::string> sourceType;
    rfl::Rename<"source_config", ConfigObject> sourceConfig;
    rfl::Rename<"parser_config", ConfigObject> parserConfig;
};

struct PlannedCreateSink
{
    using Tag = rfl::Literal<"CreateSink">;
    std::string name;
    rfl::Rename<"host_addr", std::string> host;
    rfl::Rename<"sink_type", std::string> sinkType;
    rfl::Generic schema;
    /// The formatter options are nested under `OUTPUT_FORMATTER`, as they are in the SQL statement.
    rfl::Generic config;
};

struct PlannedCreateWorker
{
    using Tag = rfl::Literal<"CreateWorker">;
    rfl::Rename<"host_addr", std::string> hostAddr;
    rfl::Rename<"data_addr", std::string> dataAddr;
    rfl::Rename<"max_operators", std::optional<int32_t>> maxOperators;
    std::vector<std::string> peers;
    ConfigObject config;
};

struct PlannedDropLogicalSource
{
    using Tag = rfl::Literal<"DropLogicalSource">;
    std::optional<std::string> name;
};

struct PlannedDropPhysicalSource
{
    using Tag = rfl::Literal<"DropPhysicalSource">;
    std::optional<uint64_t> id;
    rfl::Rename<"logical_source", std::optional<std::string>> logicalSourceName;
};

struct PlannedDropSink
{
    using Tag = rfl::Literal<"DropSink">;
    std::optional<std::string> name;
};

/// The drop-query statement nests its filters as one JSON object under `filters`, so they are a separate struct here.
struct QueryFilters
{
    std::optional<std::vector<uint64_t>> ids;
    std::optional<std::string> name;
};

struct PlannedDropQuery
{
    using Tag = rfl::Literal<"DropQuery">;
    QueryFilters filters;
};

struct PlannedDropWorker
{
    using Tag = rfl::Literal<"DropWorker">;
    rfl::Rename<"host_addr", std::string> host;
};

/// The fragments and the source and sink ids are sent out of band and not serialized here.
/// The Rust side defaults them on deserialization and fills them in afterwards.
struct PlannedCreateQuery
{
    using Tag = rfl::Literal<"CreateQuery">;
    std::optional<std::string> name;
    std::string sql;
};

struct PlannedExplainQuery
{
    using Tag = rfl::Literal<"ExplainQuery">;
    std::string explanation;
};

struct PlannedShowLogicalSources
{
    using Tag = rfl::Literal<"GetLogicalSource">;
    std::optional<std::string> name;
};

struct PlannedShowPhysicalSources
{
    using Tag = rfl::Literal<"GetPhysicalSource">;
    std::optional<uint64_t> id;
    rfl::Rename<"logical_source", std::optional<std::string>> logicalSourceName;
};

struct PlannedShowSinks
{
    using Tag = rfl::Literal<"GetSink">;
    std::optional<std::string> name;
};

struct PlannedShowQueries
{
    using Tag = rfl::Literal<"GetQuery">;
    std::optional<std::vector<uint64_t>> ids;
    std::optional<std::string> name;
};

/// `SHOW WORKER STATUS` is also mapped here, because the coordinator's worker-status request takes a single mandatory address
/// and cannot express the statement's host list.
/// Worker status could get its own variant.
struct PlannedShowWorkers
{
    using Tag = rfl::Literal<"GetWorker">;
    rfl::Rename<"host_addr", std::optional<std::string>> host;
};

/// `SHOW VERSION` has no host clause, so it asks about every worker.
/// The coordinator queries the workers for it, because a version is not in the catalog.
struct PlannedShowVersion
{
    using Tag = rfl::Literal<"GetWorkerVersion">;
    rfl::Rename<"host_addr", std::optional<std::string>> host;
};

struct PlannedCreateModel
{
    using Tag = rfl::Literal<"CreateMlModel">;
    std::string name;
    std::string path;
    /// Reflected into generic values because the model types have no rfl reflection of their own, only NES's reflector.
    /// The same applies to both schemas below.
    rfl::Rename<"imported", rfl::Generic> imported;
    rfl::Rename<"input_schema", rfl::Generic> inputs;
    rfl::Rename<"output_schema", rfl::Generic> outputs;
};

struct PlannedShowModels
{
    using Tag = rfl::Literal<"GetMlModel">;
    std::optional<std::string> name;
};

struct PlannedDropModel
{
    using Tag = rfl::Literal<"DropMlModel">;
    std::optional<std::string> name;
};

/// Internally tagged on a `"tag"` field, like the coordinator's statement enum.
using CoordinatorStatement = rfl::TaggedUnion<
    "tag",
    PlannedCreateLogicalSource,
    PlannedShowLogicalSources,
    PlannedDropLogicalSource,
    PlannedCreatePhysicalSource,
    PlannedShowPhysicalSources,
    PlannedDropPhysicalSource,
    PlannedCreateSink,
    PlannedShowSinks,
    PlannedDropSink,
    PlannedCreateQuery,
    PlannedExplainQuery,
    PlannedShowQueries,
    PlannedDropQuery,
    PlannedCreateWorker,
    PlannedShowWorkers,
    PlannedShowVersion,
    PlannedDropWorker,
    PlannedCreateModel,
    PlannedShowModels,
    PlannedDropModel>;

}
