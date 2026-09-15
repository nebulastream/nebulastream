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

/// One struct per statement that the coordinator accepts.
/// The planner fills one and sends it to the coordinator as JSON.
/// Tags and field names must match the statement types in nes-coordinator/model/src.
namespace NES
{

/// `if_not_exists` is left out and defaults to false.
/// The schema is a generic value so that it becomes a nested JSON object, not a string.
struct PlannedCreateLogicalSource
{
    using Tag = rfl::Literal<"CreateLogicalSource">;
    std::string name;
    rfl::Generic schema;
};

/// A JSON object with string values.
using ConfigObject = std::unordered_map<std::string, std::string>;

struct PlannedCreatePhysicalSource
{
    using Tag = rfl::Literal<"CreatePhysicalSource">;
    rfl::Rename<"logical_source", std::string> logicalSourceName;
    rfl::Rename<"host_addr", std::string> host;
    rfl::Rename<"source_type", std::string> sourceType;
    rfl::Rename<"source_config", rfl::Generic> sourceConfig;
    rfl::Rename<"parser_config", rfl::Generic> parserConfig;
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

/// The filters of a drop-query statement are one nested JSON object.
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

/// The per-worker plans of the query are not part of the JSON.
/// They are protobuf bytes and are passed next to it.
struct PlannedCreateQuery
{
    using Tag = rfl::Literal<"CreateQuery">;
    std::optional<std::string> name;
    std::string sql;
    rfl::Rename<"source_ids", std::vector<int64_t>> sourceIds;
    rfl::Rename<"sink_ids", std::vector<int64_t>> sinkIds;
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

struct PlannedShowWorkers
{
    using Tag = rfl::Literal<"GetWorker">;
    rfl::Rename<"host_addr", std::optional<std::string>> host;
    rfl::Rename<"desired_state", std::string> desiredState{"Active"};
};

/// `SHOW VERSION` has no host clause, so it covers every worker.
/// The coordinator requests the version from the workers, because it is not in the catalog.
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
    /// Generic values, because the model types can only be serialized with the NES reflector.
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

/// The JSON field `tag` says which statement it is.
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
