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
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Reflection.hpp>
#include <CatalogConfig.hpp>
#include <Model.hpp>

/// One struct per statement that the coordinator accepts.
/// The planner fills one and sends it to the coordinator as JSON, with the tag next to the fields:
///     DROP SINK WHERE NAME = 'mySink'  ->  {"tag": "DropSink", "name": "MYSINK"}
/// Tags and field names must match the statement types in `nes-coordinator/model/src`, which read camelCase field names.
/// In a DROP or SHOW, an optional field is a filter, and an unset filter is not applied.
/// A statement without any filter therefore selects every entry:
///     DROP SINK ALL  ->  {"tag": "DropSink", "name": null}
namespace NES
{

struct PlannedCreateLogicalSource
{
    static constexpr std::string_view TAG = "CreateLogicalSource";
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    bool ifNotExists;
};

/// A JSON object with string values.
using ConfigObject = std::unordered_map<std::string, std::string>;

struct PlannedCreatePhysicalSource
{
    static constexpr std::string_view TAG = "CreatePhysicalSource";
    Identifier logicalSource;
    std::string hostAddr;
    Identifier sourceType;
    CatalogConfig::Options sourceConfig;
    CatalogConfig::Options parserConfig;
    bool ifNotExists;
};

struct PlannedCreateSink
{
    static constexpr std::string_view TAG = "CreateSink";
    Identifier name;
    std::string hostAddr;
    Identifier sinkType;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    /// The formatter options are nested under `OUTPUT_FORMATTER`, as they are in the SQL statement.
    CatalogConfig::SinkOptions config;
    bool ifNotExists;
};

struct PlannedCreateWorker
{
    static constexpr std::string_view TAG = "CreateWorker";
    std::string hostAddr;
    std::string dataAddr;
    std::optional<int32_t> maxOperators;
    std::vector<std::string> peers;
    ConfigObject config;
    bool ifNotExists;
};

struct PlannedDropLogicalSource
{
    static constexpr std::string_view TAG = "DropLogicalSource";
    std::optional<Identifier> name;
};

struct PlannedDropPhysicalSource
{
    static constexpr std::string_view TAG = "DropPhysicalSource";
    std::optional<PhysicalSourceId> id;
    std::optional<Identifier> logicalSource;
};

struct PlannedDropSink
{
    static constexpr std::string_view TAG = "DropSink";
    std::optional<Identifier> name;
};

struct QueryFilters
{
    std::optional<std::vector<QueryId>> ids;
    std::optional<Identifier> name;
};

struct PlannedDropQuery
{
    static constexpr std::string_view TAG = "DropQuery";
    QueryFilters filters;
};

struct PlannedStopQuery
{
    static constexpr std::string_view TAG = "StopQuery";
    QueryFilters filters;
};

struct PlannedDropWorker
{
    static constexpr std::string_view TAG = "DropWorker";
    std::string hostAddr;
};

struct PlannedCreateQuery
{
    static constexpr std::string_view TAG = "CreateQuery";
    std::optional<Identifier> name;
    std::string sql;
    std::vector<PhysicalSourceId> sourceIds;
    std::vector<SinkId> sinkIds;
    bool ifNotExists;
};

struct PlannedExplainQuery
{
    static constexpr std::string_view TAG = "ExplainQuery";
    std::string explanation;
};

struct PlannedShowLogicalSources
{
    static constexpr std::string_view TAG = "GetLogicalSource";
    std::optional<Identifier> name;
};

struct PlannedShowPhysicalSources
{
    static constexpr std::string_view TAG = "GetPhysicalSource";
    std::optional<PhysicalSourceId> id;
    std::optional<Identifier> logicalSource;
};

struct PlannedShowSinks
{
    static constexpr std::string_view TAG = "GetSink";
    std::optional<Identifier> name;
};

struct PlannedShowQueries
{
    static constexpr std::string_view TAG = "GetQuery";
    std::optional<std::vector<QueryId>> ids;
    std::optional<Identifier> name;
};

struct PlannedShowWorkers
{
    static constexpr std::string_view TAG = "GetWorker";
    std::optional<std::string> hostAddr;
    std::string desiredState{"Active"};
};

/// The coordinator requests the version from the workers, because it is not in the catalog.
struct PlannedShowVersion
{
    static constexpr std::string_view TAG = "GetWorkerVersion";
    std::optional<std::string> hostAddr;
};

struct PlannedCreateModel
{
    static constexpr std::string_view TAG = "CreateMlModel";
    std::string name;
    std::string path;
    ImportedModel imported;
    Schema<UnqualifiedUnboundField, Ordered> inputSchema;
    Schema<UnqualifiedUnboundField, Ordered> outputSchema;
    bool ifNotExists;
};

struct PlannedShowModels
{
    static constexpr std::string_view TAG = "GetMlModel";
    std::optional<std::string> name;
};

struct PlannedDropModel
{
    static constexpr std::string_view TAG = "DropMlModel";
    std::optional<std::string> name;
};

/// The JSON field `tag` says which statement it is.
using CoordinatorStatement = std::variant<
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
    PlannedStopQuery,
    PlannedCreateWorker,
    PlannedShowWorkers,
    PlannedShowVersion,
    PlannedDropWorker,
    PlannedCreateModel,
    PlannedShowModels,
    PlannedDropModel>;

/// The generic reflection of a variant writes a DROP SINK as `{"index": 8, "value": {"name": "MYSINK"}}`,
/// which breaks the coordinator whenever an alternative is added or reordered.
template <>
struct Reflector<CoordinatorStatement>
{
    Reflected operator()(const CoordinatorStatement& statement, const ReflectionContext& context) const;
};

}
